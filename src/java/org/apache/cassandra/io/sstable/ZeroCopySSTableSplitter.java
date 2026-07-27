/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.MurmurHash;

/**
 * Splits one BIG-format SSTable into K children by copying verbatim compression-chunk runs of Data.db and
 * rebuilding every other component from an Index.db-only pass. No decompression, no row deserialization.
 *
 * <h2>Why a chunk run, and not an exact byte cut</h2>
 * Uncompressed chunk boundaries are pinned to exact multiples of {@code chunkLength}:
 * {@link CompressionMetadata#chunkFor(long)} indexes the offsets array with
 * {@code 8 * (position / chunkLength)} and there is no per-chunk uncompressed length on disk. Therefore only
 * the <em>last</em> chunk of a file may be uncompressed-short, and a child can only ever be a verbatim run of
 * whole chunks {@code [i, j]}.
 *
 * <h2>Consequences of the general (suffix) form</h2>
 * A child whose first partition does not sit on a chunk boundary carries a <em>dead prefix</em> of
 * {@code lo mod chunkLength} bytes at the head of its Data.db. Index positions are rebased by
 * {@code shift = i * chunkLength} rather than by {@code lo}, so the child's first partition lands at
 * uncompressed offset {@code lo mod chunkLength}. That is tolerated by the read, compaction, cleanup and
 * repair-validation paths, all of which enter Data.db only at positions read from Index.db. It is
 * <em>not</em> tolerated by:
 * <ul>
 *   <li>entire-sstable zero-copy streaming, which requires
 *       {@code transferLength == sstable.uncompressedLength()}; such a child falls back to partial streaming;</li>
 *   <li>{@code Scrubber}/{@code Verifier}, which walk Data.db linearly from 0. Both have been given a
 *       three-line change to seek to the first index position instead of requiring it to be zero.</li>
 * </ul>
 *
 * <h2>Trailing slack is forbidden</h2>
 * {@code CompressionMetadata.compressedFileLength} is taken from the physical file length, and the last
 * chunk's length is derived as {@code compressedFileLength - offsets[C-1] - 4}. A single trailing byte
 * inflates that length and can flip the reader's {@code length < maxCompressedLength} test, causing
 * compressed bytes to be handed back as raw data. The child's Data.db is therefore truncated to exactly
 * {@code O(j+1) - O(i)} and asserted.
 *
 * <h2>Uncompressed SSTables</h2>
 * Not supported; {@link #split(SSTableReader, int, LifecycleTransaction)} throws
 * {@link UnsupportedOperationException} whose message starts with {@link #UNCOMPRESSED_UNSUPPORTED_MESSAGE}.
 * An uncompressed split is a different algorithm, not a degenerate case of this one: the cut is exact (no
 * chunk grid, no dead prefix, {@code shift == lo}) and CRC.db must be regenerated wholesale because its
 * 64 KiB grid is addressed from origin 0 and a suffix cut is misaligned against it. Producing a child with a
 * stale or sliced CRC.db would corrupt outbound partial streaming silently, so this refuses instead.
 * Use {@link #isSupported(SSTableReader)} to test up front.
 */
public final class ZeroCopySSTableSplitter
{
    private static final Logger logger = LoggerFactory.getLogger(ZeroCopySSTableSplitter.class);

    /**
     * Prefix of the {@link UnsupportedOperationException} message raised for an uncompressed parent. Exposed so
     * tests can assert the refusal without string-matching the whole sentence.
     */
    public static final String UNCOMPRESSED_UNSUPPORTED_MESSAGE =
        "ZeroCopySSTableSplitter requires a compressed sstable";

    /** FileChannel.transferTo caps near 0x7ffff000 and may return short counts; stay well under it. */
    private static final long TRANSFER_CHUNK = 1L << 30;

    /** Same buffer size the digest/checksum writers use. */
    private static final int COPY_BUFFER_SIZE = 64 * 1024;

    /** {@code MetadataCollector.defaultPartitionSizeHistogram()} is package-private; this is bit-identical. */
    private static final int PARTITION_SIZE_HISTOGRAM_BUCKETS = 150;

    /** {@code MetadataCollector.cardinality} is {@code new HyperLogLogPlus(13, 25)} (CASSANDRA-5906). */
    private static final int HLL_P = 13;
    private static final int HLL_SP = 25;

    /** Every component this class can write, plus the Statistics.db tmp file rewriteSSTableMetadata leaves behind. */
    private static final List<Component> WRITTEN_COMPONENTS = ImmutableList.of(Component.DATA,
                                                                               Component.PRIMARY_INDEX,
                                                                               Component.COMPRESSION_INFO,
                                                                               Component.STATS,
                                                                               Component.SUMMARY,
                                                                               Component.FILTER,
                                                                               Component.DIGEST,
                                                                               Component.TOC);

    private ZeroCopySSTableSplitter()
    {
    }

    // ------------------------------------------------------------------------------------------------
    // Arithmetic. Deliberately static and free of any sstable dependency so it can be unit tested alone.
    // ------------------------------------------------------------------------------------------------

    /**
     * Index of the compression chunk containing {@code uncompressedPosition}.
     * Mirrors {@code CompressionMetadata.chunkFor}, which does {@code 8 * (position / chunkLength)}.
     */
    public static long chunkIndexFor(long uncompressedPosition, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (uncompressedPosition < 0)
            throw new IllegalArgumentException("negative uncompressed position: " + uncompressedPosition);
        return uncompressedPosition / chunkLength;
    }

    /** First (inclusive) chunk of a child whose first live byte is at parent uncompressed offset {@code lo}. */
    public static long firstChunk(long lo, int chunkLength)
    {
        return chunkIndexFor(lo, chunkLength);
    }

    /**
     * Last (inclusive) chunk of a child whose live bytes end at exclusive parent uncompressed offset
     * {@code hi}. Note this is {@code (hi - 1) / L}, not {@code hi / L}: when {@code hi} lands exactly on a
     * chunk boundary the final chunk is the one <em>before</em> it, and using {@code hi / L} would read one
     * chunk too far (and throw {@code CorruptSSTableException(EOFException)} at the end of the file).
     */
    public static long lastChunk(long hi, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (hi <= 0)
            throw new IllegalArgumentException("child must contain at least one byte, hi=" + hi);
        return (hi - 1) / chunkLength;
    }

    /**
     * The child's {@code CompressionInfo.dataLength}: from the start of its first chunk up to the end of its
     * last live partition. There is no trailing slack -- {@code getPositionsForRanges} uses
     * {@code uncompressedLength()} as its right bound.
     */
    public static long childDataLength(long hi, long firstChunk, int chunkLength)
    {
        checkChunkLength(chunkLength);
        long dataLength = hi - firstChunk * chunkLength;
        if (dataLength <= 0)
            throw new IllegalArgumentException("non-positive child dataLength " + dataLength +
                                               " (hi=" + hi + ", firstChunk=" + firstChunk + ", L=" + chunkLength + ')');
        return dataLength;
    }

    /** Bytes at the head of the child Data.db that belong to no partition: {@code lo mod chunkLength}. */
    public static long deadPrefixBytes(long lo, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (lo < 0)
            throw new IllegalArgumentException("negative uncompressed position: " + lo);
        return lo % chunkLength;
    }

    /**
     * The whole chunk-range computation for one child, as an immutable value so a test can assert on it
     * directly.
     *
     * @param lo          first live byte, inclusive, in PARENT uncompressed space (a partition start)
     * @param hi          last live byte + 1, exclusive, in PARENT uncompressed space (a partition end)
     * @param chunkLength the parent's compression chunk length
     */
    public static ChunkRange chunkRange(long lo, long hi, int chunkLength)
    {
        checkChunkLength(chunkLength);
        if (lo < 0)
            throw new IllegalArgumentException("negative lo: " + lo);
        if (hi <= lo)
            throw new IllegalArgumentException("empty child range [" + lo + ", " + hi + ')');

        long i = firstChunk(lo, chunkLength);
        long j = lastChunk(hi, chunkLength);
        if (i > j)
            throw new IllegalStateException("firstChunk " + i + " > lastChunk " + j +
                                            " for [" + lo + ", " + hi + ") L=" + chunkLength);

        long chunkCount = j - i + 1;
        long dataLength = childDataLength(hi, i, chunkLength);

        // The reason a verbatim run works at all: the last chunk holds at least one live byte (so it is
        // mapped and decompressed) and at most a full chunk of them (so dataLength never overruns the run).
        if (!((chunkCount - 1) * (long) chunkLength < dataLength && dataLength <= chunkCount * (long) chunkLength))
            throw new IllegalStateException(String.format("invariant (C-1)*L < Dp <= C*L violated: " +
                                                          "C=%d L=%d Dp=%d lo=%d hi=%d",
                                                          chunkCount, chunkLength, dataLength, lo, hi));

        return new ChunkRange(lo, hi, chunkLength, i, j, chunkCount, dataLength,
                              i * (long) chunkLength, deadPrefixBytes(lo, chunkLength));
    }

    private static void checkChunkLength(int chunkLength)
    {
        if (chunkLength <= 0)
            throw new IllegalArgumentException("chunkLength must be positive: " + chunkLength);
    }

    /**
     * Immutable result of {@link #chunkRange(long, long, int)}. All chunk indices are into the PARENT's
     * offsets array; all byte counts are in the child's own space.
     */
    public static final class ChunkRange
    {
        /** First live byte of the child, inclusive, in parent uncompressed space. */
        public final long lo;
        /** Last live byte of the child + 1, exclusive, in parent uncompressed space. */
        public final long hi;
        /** The parent's compression chunk length. */
        public final int chunkLength;
        /** {@code i}: first parent chunk copied, inclusive. */
        public final long firstChunk;
        /** {@code j}: last parent chunk copied, inclusive. */
        public final long lastChunk;
        /** {@code C = j - i + 1}: the child's chunkCount. */
        public final long chunkCount;
        /** {@code Dp = hi - i*L}: the child's CompressionInfo dataLength. */
        public final long dataLength;
        /** {@code shift = i*L}: subtracted from every Index.db position. */
        public final long shift;
        /** {@code lo mod L}: bytes at the head of the child Data.db owned by no partition. */
        public final long deadPrefixBytes;

        ChunkRange(long lo, long hi, int chunkLength, long firstChunk, long lastChunk,
                   long chunkCount, long dataLength, long shift, long deadPrefixBytes)
        {
            this.lo = lo;
            this.hi = hi;
            this.chunkLength = chunkLength;
            this.firstChunk = firstChunk;
            this.lastChunk = lastChunk;
            this.chunkCount = chunkCount;
            this.dataLength = dataLength;
            this.shift = shift;
            this.deadPrefixBytes = deadPrefixBytes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (!(o instanceof ChunkRange))
                return false;
            ChunkRange that = (ChunkRange) o;
            return lo == that.lo && hi == that.hi && chunkLength == that.chunkLength
                   && firstChunk == that.firstChunk && lastChunk == that.lastChunk
                   && chunkCount == that.chunkCount && dataLength == that.dataLength
                   && shift == that.shift && deadPrefixBytes == that.deadPrefixBytes;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lo, hi, chunkLength, firstChunk, lastChunk, chunkCount, dataLength, shift, deadPrefixBytes);
        }

        @Override
        public String toString()
        {
            return String.format("ChunkRange[lo=%d hi=%d L=%d chunks=[%d,%d] C=%d Dp=%d shift=%d dead=%d]",
                                 lo, hi, chunkLength, firstChunk, lastChunk, chunkCount, dataLength, shift, deadPrefixBytes);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Results
    // ------------------------------------------------------------------------------------------------

    /** One produced child sstable. */
    public static final class Child
    {
        /** Descriptor of the child, in the parent's directory, version and format. */
        public final Descriptor descriptor;
        /** The child's first partition key (minimal copy). */
        public final DecoratedKey first;
        /** The child's last partition key (minimal copy). */
        public final DecoratedKey last;
        /** First parent chunk copied, inclusive. */
        public final long firstChunk;
        /** Last parent chunk copied, inclusive. */
        public final long lastChunk;
        /** Exact physical byte length of the child Data.db, {@code O(j+1) - O(i)}. */
        public final long physicalBytes;
        /** The child's CompressionInfo dataLength, {@code hi - i*L}. */
        public final long dataLength;
        /** Value subtracted from every Index.db position, {@code i*L}. */
        public final long shift;
        /** Bytes at the head of the child Data.db owned by no partition, {@code lo mod L}. */
        public final long deadPrefixBytes;
        /** Number of partitions in the child. */
        public final long partitionCount;
        /** Components written for the child; the exact set passed to {@code SSTableReader.open}. */
        public final Set<Component> components;
        /** The opened, validated child reader. The caller owns this reference and must release it. */
        public final SSTableReader reader;

        Child(Descriptor descriptor, DecoratedKey first, DecoratedKey last, ChunkRange range,
              long physicalBytes, long partitionCount, Set<Component> components, SSTableReader reader)
        {
            this.descriptor = descriptor;
            this.first = first;
            this.last = last;
            this.firstChunk = range.firstChunk;
            this.lastChunk = range.lastChunk;
            this.physicalBytes = physicalBytes;
            this.dataLength = range.dataLength;
            this.shift = range.shift;
            this.deadPrefixBytes = range.deadPrefixBytes;
            this.partitionCount = partitionCount;
            this.components = components;
            this.reader = reader;
        }

        @Override
        public String toString()
        {
            return String.format("Child[%s chunks=[%d,%d] physical=%d dataLength=%d shift=%d dead=%d partitions=%d]",
                                 descriptor, firstChunk, lastChunk, physicalBytes, dataLength, shift, deadPrefixBytes, partitionCount);
        }
    }

    /** Outcome of a whole split. */
    public static final class Result
    {
        /** The children, in token order. */
        public final List<Child> children;
        /** Sum of every child's physical Data.db length. */
        public final long totalPhysicalBytesCopied;
        /** Sum of every child's dead prefix. */
        public final long totalDeadPrefixBytes;
        /**
         * Compressed bytes physically present in two children because a split boundary fell inside a chunk.
         * Bounded by one chunk per interior boundary.
         */
        public final long duplicatedChunkBytes;
        /** Wall clock of the whole split. */
        public final long nanos;

        Result(List<Child> children, long totalPhysicalBytesCopied, long totalDeadPrefixBytes,
               long duplicatedChunkBytes, long nanos)
        {
            this.children = children;
            this.totalPhysicalBytesCopied = totalPhysicalBytesCopied;
            this.totalDeadPrefixBytes = totalDeadPrefixBytes;
            this.duplicatedChunkBytes = duplicatedChunkBytes;
            this.nanos = nanos;
        }

        @Override
        public String toString()
        {
            return String.format("Result[children=%d physical=%d dead=%d duplicated=%d %.1fms]",
                                 children.size(), totalPhysicalBytesCopied, totalDeadPrefixBytes,
                                 duplicatedChunkBytes, nanos / 1_000_000.0);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Entry points
    // ------------------------------------------------------------------------------------------------

    /**
     * @return true iff {@link #split} can handle this parent, i.e. it is a compressed BIG-format sstable.
     *         An uncompressed or non-BIG parent is refused with {@link UnsupportedOperationException}.
     */
    public static boolean isSupported(SSTableReader parent)
    {
        return parent.descriptor.formatType == SSTableFormat.Type.BIG && parent.compression;
    }

    /**
     * Split at the partition boundaries nearest to {@code numChildren} approximately-equal byte shares of the
     * parent's uncompressed length.
     *
     * @param numChildren number of children to produce; must be >= 1 and <= the parent's partition count
     * @param txn         optional; if non-null every child is {@code trackNew}'d on it once fully written
     * @throws UnsupportedOperationException if the parent is not a compressed BIG-format sstable
     */
    public static Result split(SSTableReader parent, int numChildren, LifecycleTransaction txn)
    {
        Preconditions.checkArgument(numChildren >= 1, "numChildren must be >= 1, got %s", numChildren);
        requireSupported(parent);

        long start = Clock.Global.nanoTime();
        Scan scan = scan(parent, null);
        if (numChildren > scan.positions.length)
            throw new IllegalArgumentException("cannot split " + scan.positions.length + " partitions into " +
                                               numChildren + " children");
        int[] runStarts = chooseByByteShare(scan.positions, parent.uncompressedLength(), numChildren);
        return build(parent, scan.positions, runStarts, txn, start);
    }

    /**
     * Split at explicit boundaries. Child {@code b} covers keys {@code [boundaries[b-1], boundaries[b])}, with
     * the first child unbounded below and the last unbounded above -- so this produces up to
     * {@code boundaries.size() + 1} children. Boundaries must be strictly increasing.
     * <p>
     * A boundary range containing no partition produces no child (an empty sstable is not representable:
     * {@code IndexSummaryBuilder.build} asserts a non-zero key count and {@code getPositionsForRanges} asserts
     * {@code first < last}). So the returned list may be shorter than {@code boundaries.size() + 1}.
     *
     * @param txn optional; if non-null every child is {@code trackNew}'d on it once fully written
     * @throws UnsupportedOperationException if the parent is not a compressed BIG-format sstable
     */
    public static Result split(SSTableReader parent, List<DecoratedKey> boundaries, LifecycleTransaction txn)
    {
        Preconditions.checkNotNull(boundaries, "boundaries");
        requireSupported(parent);
        for (int b = 1; b < boundaries.size(); b++)
        {
            if (boundaries.get(b - 1).compareTo(boundaries.get(b)) >= 0)
                throw new IllegalArgumentException("boundaries must be strictly increasing: " +
                                                   boundaries.get(b - 1) + " >= " + boundaries.get(b));
        }

        long start = Clock.Global.nanoTime();
        Scan scan = scan(parent, boundaries);
        return build(parent, scan.positions, scan.runStarts, txn, start);
    }

    private static void requireSupported(SSTableReader parent)
    {
        Preconditions.checkNotNull(parent, "parent");
        if (parent.descriptor.formatType != SSTableFormat.Type.BIG)
            throw new UnsupportedOperationException("ZeroCopySSTableSplitter only supports the BIG sstable " +
                                                    "format, got " + parent.descriptor.formatType);
        if (!parent.compression)
            throw new UnsupportedOperationException(UNCOMPRESSED_UNSUPPORTED_MESSAGE + ": " + parent.descriptor +
                                                    " has no CompressionInfo.db. An uncompressed split is a " +
                                                    "different algorithm -- the cut is exact rather than " +
                                                    "chunk-aligned, and CRC.db (whose 64KiB grid is addressed " +
                                                    "from origin 0) has to be regenerated wholesale rather " +
                                                    "than sliced. Refusing rather than emitting a child with " +
                                                    "a misaligned CRC.db.");
        if (!parent.descriptor.fileFor(Component.STATS).exists())
            throw new IllegalStateException("parent has no Statistics.db: " + parent.descriptor +
                                            "; MetadataSerializer would silently fabricate defaults");
    }

    // ------------------------------------------------------------------------------------------------
    // Pass 1: positions (and optionally keys) from the parent Index.db
    // ------------------------------------------------------------------------------------------------

    /** Outcome of the first Index.db pass. */
    private static final class Scan
    {
        /** Every partition's uncompressed Data.db start offset, in on-disk order. */
        final long[] positions;
        /**
         * Only populated for the explicit-boundary form: the START record index of each run, where run
         * {@code b} is {@code [runStarts[b], runStarts[b + 1])} with an implicit terminator of
         * {@code positions.length}.
         */
        final int[] runStarts;

        Scan(long[] positions, int[] runStarts)
        {
            this.positions = positions;
            this.runStarts = runStarts;
        }
    }

    /**
     * One sequential walk of the parent Index.db collecting every partition's uncompressed Data.db start
     * offset. When {@code boundaries} is non-null the run starts are resolved in the same pass, so the keys
     * never have to be retained (a wide sstable would otherwise cost ~150 bytes of heap per partition).
     */
    private static Scan scan(SSTableReader parent, List<DecoratedKey> boundaries)
    {
        IPartitioner partitioner = parent.getPartitioner();
        long[] positions = new long[1024];
        int count = 0;

        int[] runStarts = boundaries == null ? null : new int[boundaries.size() + 1];
        int nextBoundary = 0;

        // A buffered reader rather than an mmap, so no record can straddle a mapping boundary.
        try (RandomAccessReader in = RandomAccessReader.open(parent.descriptor.fileFor(Component.PRIMARY_INDEX)))
        {
            long indexSize = in.length();
            while (in.getFilePointer() != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                long position = RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = (int) in.readUnsignedVInt();
                if (promotedSize > 0)
                    in.skipBytesFully(promotedSize);

                if (count == positions.length)
                    positions = Arrays.copyOf(positions, positions.length * 2);

                if (boundaries != null && nextBoundary < boundaries.size())
                {
                    DecoratedKey dk = partitioner.decorateKey(key);
                    // run b + 1 starts at the first record whose key is >= boundaries[b]
                    while (nextBoundary < boundaries.size() && dk.compareTo(boundaries.get(nextBoundary)) >= 0)
                        runStarts[++nextBoundary] = count;
                }
                positions[count++] = position;
            }
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.descriptor.filenameFor(Component.PRIMARY_INDEX));
        }

        if (count == 0)
            throw new IllegalStateException("parent Index.db is empty: " + parent.descriptor);

        // boundaries past the parent's last key produce trailing empty runs
        while (boundaries != null && nextBoundary < boundaries.size())
            runStarts[++nextBoundary] = count;

        return new Scan(Arrays.copyOf(positions, count), runStarts);
    }

    // ------------------------------------------------------------------------------------------------
    // Split-point selection: the START index of each run; run b is
    // [runStarts[b], runStarts[b+1]) with an implicit terminator of positions.length.
    // ------------------------------------------------------------------------------------------------

    @VisibleForTesting
    static int[] chooseByByteShare(long[] positions, long uncompressedLength, int numChildren)
    {
        int n = positions.length;
        int[] runStarts = new int[numChildren];
        runStarts[0] = 0;

        long base = positions[0];
        long total = uncompressedLength - base;
        int cursor = 0;
        for (int m = 1; m < numChildren; m++)
        {
            long target = base + (total * m) / numChildren;
            while (cursor < n && positions[cursor] < target)
                cursor++;

            int candidate = cursor;
            // snap to whichever partition boundary is nearer the target
            if (candidate > 0 && candidate < n
                && (positions[candidate] - target) > (target - positions[candidate - 1]))
                candidate--;

            // never emit an empty child, and always leave room for the runs still to be placed
            candidate = Math.max(candidate, runStarts[m - 1] + 1);
            candidate = Math.min(candidate, n - (numChildren - m));
            runStarts[m] = candidate;
            cursor = Math.max(cursor, candidate);
        }
        return runStarts;
    }

    // ------------------------------------------------------------------------------------------------
    // Pass 2: build every child from a single sequential walk of the parent Index.db
    // ------------------------------------------------------------------------------------------------

    private static Result build(SSTableReader parent, long[] positions, int[] runStarts,
                                LifecycleTransaction txn, long startNanos)
    {
        CompressionMetadata meta = parent.getCompressionMetadata();  // owned by parent's dfile; never close it
        final int chunkLength = meta.chunkLength();
        final long parentDataLength = meta.dataLength;
        final long parentCompressedLength = meta.compressedFileLength;
        final int parentChunkCount = (int) ((parentDataLength + chunkLength - 1) / chunkLength);

        if (parent.uncompressedLength() != parentDataLength)
            throw new IllegalStateException("uncompressedLength " + parent.uncompressedLength() +
                                            " != CompressionMetadata.dataLength " + parentDataLength);

        // The four parent metadata components, read once. allOf() is mandatory: unselected types are skipped
        // on read and would be silently dropped from the child's Statistics.db.
        Map<MetadataType, MetadataComponent> parentMetadata = readParentMetadata(parent.descriptor);
        StatsMetadata parentStats = (StatsMetadata) parentMetadata.get(MetadataType.STATS);

        Supplier<Descriptor> descriptors = descriptorAllocator(parent);

        List<Child> children = new ArrayList<>(runStarts.length);
        List<Descriptor> created = new ArrayList<>(runStarts.length);
        long physicalTotal = 0;
        long deadTotal = 0;
        long duplicated = 0;

        boolean success = false;
        try (RandomAccessReader index = RandomAccessReader.open(parent.descriptor.fileFor(Component.PRIMARY_INDEX)))
        {
            ChunkRange previous = null;
            for (int b = 0; b < runStarts.length; b++)
            {
                int from = runStarts[b];
                int to = (b + 1 < runStarts.length) ? runStarts[b + 1] : positions.length;
                if (from >= to)
                    continue;  // empty boundary range -> no child

                long lo = positions[from];
                long hi = (to < positions.length) ? positions[to] : parentDataLength;
                ChunkRange range = chunkRange(lo, hi, chunkLength);

                long copyFrom = chunkOffset(meta, range.firstChunk, parentChunkCount, parentCompressedLength, chunkLength);
                long copyTo = chunkOffset(meta, range.lastChunk + 1, parentChunkCount, parentCompressedLength, chunkLength);
                long physicalBytes = copyTo - copyFrom;
                if (physicalBytes <= 0)
                    throw new IllegalStateException("non-positive physical length " + physicalBytes + " for " + range);

                Descriptor child = descriptors.get();
                created.add(child);
                children.add(buildChild(parent, child, index, positions, from, to, range,
                                        meta, copyFrom, physicalBytes, parentMetadata, parentStats, txn));

                physicalTotal += physicalBytes;
                deadTotal += range.deadPrefixBytes;
                if (previous != null && previous.lastChunk == range.firstChunk)
                {
                    duplicated += chunkOffset(meta, range.firstChunk + 1, parentChunkCount, parentCompressedLength, chunkLength)
                                  - chunkOffset(meta, range.firstChunk, parentChunkCount, parentCompressedLength, chunkLength);
                }
                previous = range;
            }
            success = true;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("failed splitting " + parent.descriptor, e);
        }
        finally
        {
            if (!success)
                cleanUp(children, created);
        }

        Result result = new Result(ImmutableList.copyOf(children), physicalTotal, deadTotal, duplicated,
                                   Clock.Global.nanoTime() - startNanos);
        logger.info("Split {} into {} children: {}", parent.descriptor, children.size(), result);
        return result;
    }

    /**
     * {@code O(k)}: the absolute Data.db offset at which chunk {@code k} begins, with {@code O(N)} defined as
     * the physical file length. Because the offsets table is contiguous, {@code O(k+1)} is already the end of
     * chunk {@code k} INCLUDING its 4-byte inline CRC32 -- no fixup is needed, and adding one would truncate
     * the checksum of the child's last chunk.
     */
    private static long chunkOffset(CompressionMetadata meta, long k, int chunkCount,
                                    long compressedFileLength, int chunkLength)
    {
        if (k < 0 || k > chunkCount)
            throw new IllegalArgumentException("chunk " + k + " out of range [0, " + chunkCount + ']');
        if (k == chunkCount)
            return compressedFileLength;
        return meta.chunkFor(k * (long) chunkLength).offset;
    }

    @SuppressWarnings("resource")
    private static Child buildChild(SSTableReader parent,
                                    Descriptor child,
                                    RandomAccessReader index,
                                    long[] positions,
                                    int from,
                                    int to,
                                    ChunkRange range,
                                    CompressionMetadata meta,
                                    long copyFrom,
                                    long physicalBytes,
                                    Map<MetadataType, MetadataComponent> parentMetadata,
                                    StatsMetadata parentStats,
                                    LifecycleTransaction txn) throws IOException
    {
        TableMetadata metadata = parent.metadata();
        int chunkLength = range.chunkLength;
        int partitionCount = to - from;

        Set<Component> components = Sets.newHashSet(Component.DATA,
                                                              Component.PRIMARY_INDEX,
                                                              Component.COMPRESSION_INFO,
                                                              Component.STATS,
                                                              Component.SUMMARY,
                                                              Component.DIGEST);

        // ---------- Data.db: verbatim compressed chunk run ----------
        copyRange(parent.descriptor.fileFor(Component.DATA), child.fileFor(Component.DATA), copyFrom, physicalBytes);
        long actual = child.fileFor(Component.DATA).length();
        if (actual != physicalBytes)
            throw new IllegalStateException("child Data.db is " + actual + " bytes, expected exactly " +
                                            physicalBytes + " (trailing slack corrupts the last chunk's length)");

        // ---------- CompressionInfo.db: same params, rebased offsets, offsets[0] == 0 ----------
        writeCompressionInfo(child, meta, range, copyFrom);

        // ---------- Index.db + FILTER + SUMMARY + HLL + partition-size histogram, one pass ----------
        EstimatedHistogram partitionSizes = new EstimatedHistogram(PARTITION_SIZE_HISTOGRAM_BUCKETS);
        ICardinality cardinality = new HyperLogLogPlus(HLL_P, HLL_SP);
        double fpChance = metadata.params.bloomFilterFpChance;
        // fpChance == 1.0 yields an AlwaysPresentFilter, which saveBloomFilter would ClassCastException on.
        // The read path already treats a missing Filter.db as always-present, so just omit the component.
        IFilter bf = fpChance < 1.0 ? FilterFactory.getFilter(partitionCount, fpChance) : null;
        DecoratedKey first = null;
        DecoratedKey last = null;

        try
        {
            try (SequentialWriter out = new SequentialWriter(child.fileFor(Component.PRIMARY_INDEX), writerOption());
                 IndexSummaryBuilder summary = new IndexSummaryBuilder(partitionCount,
                                                                       metadata.params.minIndexInterval,
                                                                       Downsampling.BASE_SAMPLING_LEVEL))
            {
                for (int r = from; r < to; r++)
                {
                    ByteBuffer key = ByteBufferUtil.readWithShortLength(index);
                    long position = RowIndexEntry.Serializer.readPosition(index);
                    int promotedSize = (int) index.readUnsignedVInt();
                    byte[] promoted = null;
                    if (promotedSize > 0)
                    {
                        promoted = new byte[promotedSize];
                        index.readFully(promoted);
                    }
                    if (position != positions[r])
                        throw new IllegalStateException("index walk desynchronised at record " + r + ": saw " +
                                                        position + ", expected " + positions[r]);

                    DecoratedKey dk = parent.getPartitioner().decorateKey(key);
                    // MetadataCollector.addKey hashes the raw key bytes, position/remaining passed explicitly
                    long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);

                    long childIndexStart = out.position();
                    ByteBufferUtil.writeWithShortLength(key, out);
                    // The ONLY rewritten field. Canonical minimal vint, never padded -- so the child's records
                    // are shorter than the parent's and its index offsets are NOT the parent's minus a constant.
                    out.writeUnsignedVInt(position - range.shift);
                    out.writeUnsignedVInt(promotedSize);
                    if (promoted != null)
                        out.write(promoted, 0, promotedSize);

                    if (first == null)
                        first = dk;
                    last = dk;
                    if (bf != null)
                        bf.add(dk);
                    summary.maybeAddEntry(dk, childIndexStart);
                    cardinality.offerHashed(hashed);
                    // exact estimatedPartitionSize: rowSize_i == position_{i+1} - position_i identically
                    long end = (r + 1 < positions.length) ? positions[r + 1] : meta.dataLength;
                    partitionSizes.add(end - position);
                }
                out.finish();

                first = SSTable.getMinimalKey(first);
                last = SSTable.getMinimalKey(last);
                try (IndexSummary built = summary.build(parent.getPartitioner()))
                {
                    SSTableReader.saveSummary(child, first, last, built);
                }
            }
            requireNonEmpty(child, Component.SUMMARY);

            // ---------- Filter.db ----------
            if (bf != null)
            {
                SSTableReader.saveBloomFilter(child, bf);
                // saveBloomFilter swallows IOException and deletes the file; an online open() would then
                // silently rebuild it, hiding the failure. Check explicitly.
                requireNonEmpty(child, Component.FILTER);
                components.add(Component.FILTER);
            }
        }
        finally
        {
            if (bf != null)
                bf.close();
        }

        // ---------- Statistics.db ----------
        writeStatistics(child, parentMetadata, parentStats, partitionSizes, cardinality,
                        physicalBytes, range.dataLength);

        // ---------- Digest.crc32: CRC32 over EVERY physical byte of the child Data.db ----------
        writeDigest(child);

        // ---------- TOC.txt, last: appendTOC opens in APPEND mode so it must run exactly once ----------
        components.add(Component.TOC);
        SSTable.appendTOC(child, components);

        SSTableReader reader = SSTableReader.open(child, components, parent.metadata);
        try
        {
            validateChild(reader, range, physicalBytes, partitionCount, chunkLength);
        }
        catch (Throwable t)
        {
            reader.selfRef().release();
            throw t;
        }

        if (txn != null)
            txn.trackNew(reader);

        return new Child(child, first, last, range, physicalBytes, partitionCount,
                         ImmutableSet.copyOf(components), reader);
    }

    // ------------------------------------------------------------------------------------------------
    // Component writers
    // ------------------------------------------------------------------------------------------------

    /**
     * Verbatim byte-range copy of {@code [from, from + count)} from the parent Data.db. transferTo returns
     * short counts and caps near 0x7ffff000, so it MUST be looped; {@code n <= 0} means EOF, not "retry".
     */
    private static void copyRange(File src, File dst, long from, long count) throws IOException
    {
        try (FileChannel in = src.newReadChannel();
             FileChannel outChannel = dst.newWriteChannel(File.WriteMode.OVERWRITE))
        {
            long position = from;
            long remaining = count;
            while (remaining > 0)
            {
                long n = in.transferTo(position, Math.min(remaining, TRANSFER_CHUNK), outChannel);
                if (n <= 0)
                    throw new IOException(String.format("short transferTo of %s at %d with %d left",
                                                        src, position, remaining));
                position += n;
                remaining -= n;
            }
            outChannel.truncate(count);   // never leave a trailing byte
            outChannel.force(true);
        }
    }

    /**
     * Child CompressionInfo.db via the same {@code Writer} every real sstable is written with, so the child
     * cannot drift from the format. Only dataLength, chunkCount and the offsets differ from the parent.
     */
    private static void writeCompressionInfo(Descriptor child, CompressionMetadata meta, ChunkRange range, long copyFrom)
    {
        CompressionMetadata.Writer writer =
            CompressionMetadata.Writer.open(meta.parameters, child.filenameFor(Component.COMPRESSION_INFO));
        boolean prepared = false;
        try
        {
            for (long k = range.firstChunk; k <= range.lastChunk; k++)
            {
                long offset = meta.chunkFor(k * (long) range.chunkLength).offset - copyFrom;
                if (k == range.firstChunk && offset != 0)
                    throw new IllegalStateException("child offsets[0] must be 0, got " + offset);
                writer.addOffset(offset);
            }
            writer.finalizeLength(range.dataLength, Math.toIntExact(range.chunkCount));
            writer.prepareToCommit();   // doPrepare() is what writes and fsyncs the file
            prepared = true;
            writer.commit();
        }
        catch (Throwable t)
        {
            // doAbort() only frees memory, it does not delete an already-written file
            if (!prepared)
                writer.abort();
            child.fileFor(Component.COMPRESSION_INFO).deleteIfExists();
            throw t;
        }
        finally
        {
            writer.close();
        }
    }

    /**
     * The child's Statistics.db: the parent's four components with exactly two derived replacements
     * (estimatedPartitionSize and the COMPACTION cardinality) plus a recomputed compressionRatio.
     * <p>
     * HEADER is passed through by reference and is MANDATORY to inherit byte-for-byte: rows in the copied
     * Data.db encode timestamps/localDeletionTime/TTL as unsigned vint deltas off
     * {@code stats.minTimestamp/minLocalDeletionTime/minTTL} and encode their columns as a bitmap subset of
     * {@code header.columns()}. Tightening any of those silently corrupts every relocated row with all CRCs
     * still passing.
     * <p>
     * {@code commitLogIntervals} and {@code originatingHostId} are inherited as an ATOMIC PAIR from the same
     * parent StatsMetadata (see docs/splits-research.md 4.5). Copying the parent's interval set into all K
     * children leaves the per-table union in CommitLogReplayer bit-identical because IntervalSet.Builder.add
     * is normalising and idempotent. The bug this avoids is stamping the child with the LOCAL host id (which
     * every MetadataCollector constructor does) while inheriting a foreign parent's intervals: the replayer
     * gates on {@code originatingHostId.equals(localhostId)} and would then interpret foreign segment ids
     * against the local commitlog, discarding acked-but-unflushed mutations.
     */
    private static void writeStatistics(Descriptor child,
                                        Map<MetadataType, MetadataComponent> parentMetadata,
                                        StatsMetadata parentStats,
                                        EstimatedHistogram partitionSizes,
                                        ICardinality cardinality,
                                        long physicalBytes,
                                        long dataLength) throws IOException
    {
        StatsMetadata childStats = new StatsMetadata(partitionSizes,                              // DERIVED, exact
                                                     parentStats.estimatedCellPerPartitionCount,  // needs row iteration
                                                     parentStats.commitLogIntervals,              // atomic pair, see javadoc
                                                     parentStats.minTimestamp,
                                                     parentStats.maxTimestamp,
                                                     parentStats.minLocalDeletionTime,
                                                     parentStats.maxLocalDeletionTime,
                                                     parentStats.minTTL,
                                                     parentStats.maxTTL,
                                                     (double) physicalBytes / dataLength,         // DERIVED, exact
                                                     parentStats.estimatedTombstoneDropTime,
                                                     parentStats.sstableLevel,
                                                     parentStats.minClusteringValues,
                                                     parentStats.maxClusteringValues,
                                                     parentStats.hasLegacyCounterShards,
                                                     parentStats.repairedAt,
                                                     parentStats.totalColumnsSet,
                                                     parentStats.totalRows,
                                                     parentStats.originatingHostId,               // atomic pair, see javadoc
                                                     parentStats.pendingRepair,
                                                     parentStats.isTransient);

        Map<MetadataType, MetadataComponent> components = new EnumMap<>(parentMetadata);
        components.put(MetadataType.STATS, childStats);
        components.put(MetadataType.COMPACTION, new CompactionMetadata(cardinality));
        // VALIDATION (partitioner + fp chance) and HEADER pass through by reference: no schema lookup,
        // nothing that can throw, byte-identical to the parent.

        child.getMetadataSerializer().rewriteSSTableMetadata(child, components);   // tmp file + rename
        requireNonEmpty(child, Component.STATS);
    }

    /**
     * Digest.crc32 is the plain decimal ASCII of a java.util.zip.CRC32 over EVERY physical byte of Data.db,
     * with no newline and no prefix. That is correct for a compressed sstable too: the writer folds the inline
     * per-chunk CRCs into the full checksum ({@code appendDirect(bb, checksumIncrementalResult=true)}).
     */
    private static void writeDigest(Descriptor child) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[COPY_BUFFER_SIZE];
        try (InputStream in = child.fileFor(Component.DATA).newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
                crc.update(buffer, 0, n);
        }
        try (FileOutputStreamPlus out = new FileOutputStreamPlus(child.fileFor(Component.DIGEST)))
        {
            out.write(String.valueOf(crc.getValue()).getBytes(StandardCharsets.UTF_8));
            out.flush();
            out.sync();
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Validation and plumbing
    // ------------------------------------------------------------------------------------------------

    /** Cheap post-write checks; every one of them catches a distinct off-by-one. */
    private static void validateChild(SSTableReader child, ChunkRange range, long physicalBytes,
                                      int partitionCount, int chunkLength)
    {
        long onDisk = child.descriptor.fileFor(Component.DATA).length();
        if (onDisk != physicalBytes)
            throw new IllegalStateException("child Data.db length " + onDisk + " != " + physicalBytes);
        if (child.uncompressedLength() != range.dataLength)
            throw new IllegalStateException("child uncompressedLength " + child.uncompressedLength() +
                                            " != " + range.dataLength);

        CompressionMetadata childMeta = child.getCompressionMetadata();
        if (childMeta.chunkFor(0).offset != 0)
            throw new IllegalStateException("child offsets[0] != 0: " + childMeta.chunkFor(0).offset);
        if (childMeta.chunkLength() != chunkLength)
            throw new IllegalStateException("child chunkLength " + childMeta.chunkLength() + " != " + chunkLength);

        RowIndexEntry entry = child.getPosition(child.first, SSTableReader.Operator.EQ, false);
        if (entry == null)
            throw new IllegalStateException("child cannot find its own first key " + child.first);
        long expectedFirst = range.lo - range.shift;
        if (entry.position != expectedFirst)
            throw new IllegalStateException("child first position " + entry.position + " != " + expectedFirst);
        if (entry.position != range.deadPrefixBytes)
            throw new IllegalStateException("child first position " + entry.position +
                                            " != dead prefix " + range.deadPrefixBytes);
        if (entry.position >= chunkLength)
            throw new IllegalStateException("child first position " + entry.position +
                                            " must be inside the first chunk (L=" + chunkLength + ')');
        if (child.first.compareTo(child.last) > 0)
            throw new IllegalStateException("child first > last: " + child.first + " > " + child.last);

        logger.trace("Child {} ok: {} partitions, {} physical bytes, dead prefix {}",
                     child.descriptor, partitionCount, physicalBytes, range.deadPrefixBytes);
    }

    private static Map<MetadataType, MetadataComponent> readParentMetadata(Descriptor parent)
    {
        Map<MetadataType, MetadataComponent> components;
        try
        {
            components = parent.getMetadataSerializer().deserialize(parent, EnumSet.allOf(MetadataType.class));
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, parent.filenameFor(Component.STATS));
        }
        for (MetadataType type : MetadataType.values())
        {
            if (components.get(type) == null)
                throw new IllegalStateException("parent Statistics.db is missing " + type + ": " + parent);
        }
        return components;
    }

    /**
     * Fresh descriptors in the parent's directory, version and format. Prefers the live
     * ColumnFamilyStore's id generator so we cannot collide with a concurrent flush or compaction; falls back
     * to a directory-derived generator plus an existence loop for offline use.
     */
    private static Supplier<Descriptor> descriptorAllocator(SSTableReader parent)
    {
        Descriptor template = parent.descriptor;
        ColumnFamilyStore cfs = null;
        try
        {
            cfs = Schema.instance.getColumnFamilyStoreInstance(parent.metadata().id);
        }
        catch (Throwable t)
        {
            logger.debug("No live ColumnFamilyStore for {}, falling back to a directory-derived id generator",
                         template, t);
        }

        if (cfs != null)
        {
            ColumnFamilyStore store = cfs;
            return () -> store.newSSTableDescriptor(template.directory, template.version, template.formatType);
        }

        Supplier<SSTableId> ids = new Directories(parent.metadata()).getUIDGenerator(SSTableIdFactory.instance.defaultBuilder());
        return () -> {
            for (int attempt = 0; attempt < 1000; attempt++)
            {
                Descriptor candidate = new Descriptor(template.version, template.directory, template.ksname,
                                                      template.cfname, ids.get(), template.formatType);
                if (!candidate.fileFor(Component.DATA).exists())
                    return candidate;
            }
            throw new IllegalStateException("could not allocate an unused sstable id in " + template.directory);
        };
    }

    private static SequentialWriterOption writerOption()
    {
        return SequentialWriterOption.newBuilder()
                                     .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                     .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKiB() * 1024)
                                     .build();
    }

    private static void requireNonEmpty(Descriptor descriptor, Component component)
    {
        File file = descriptor.fileFor(component);
        if (!file.exists() || file.length() == 0)
            throw new IllegalStateException("failed to write " + component + " for " + descriptor +
                                            " (the save* helpers swallow IOException and delete the file)");
    }

    /** Best-effort removal of every partially written child, so a failed split leaves no orphans behind. */
    private static void cleanUp(List<Child> children, List<Descriptor> created)
    {
        for (Child child : children)
        {
            try
            {
                child.reader.selfRef().release();
            }
            catch (Throwable t)
            {
                logger.warn("Failed releasing child {} during cleanup", child.descriptor, t);
            }
        }
        for (Descriptor descriptor : created)
        {
            for (Component component : WRITTEN_COMPONENTS)
            {
                deleteQuietly(descriptor.fileFor(component), descriptor);
            }
            deleteQuietly(new File(descriptor.tmpFilenameFor(Component.STATS)), descriptor);
        }
        children.clear();
        created.clear();
    }

    private static void deleteQuietly(File file, Descriptor descriptor)
    {
        try
        {
            file.deleteIfExists();
        }
        catch (Throwable t)
        {
            logger.warn("Failed deleting {} while cleaning up {}", file, descriptor, t);
        }
    }
}
