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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.Scrubber;
import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BloomFilterSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.IFilter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end correctness of {@link ZeroCopySSTableSplitter}: the children must be readable, and their
 * concatenation must be indistinguishable from the parent.
 *
 * <p>The load bearing assertions are:
 * <ul>
 *   <li>{@link #assertConcatenatedContentEquals} -- every partition, row, cell, timestamp and deletion of the
 *       children concatenated in token order equals the parent, exactly;</li>
 *   <li>{@link #assertPointReads} -- every parent key is found in exactly one child and reads back identically;</li>
 *   <li>{@link #assertStructure} -- the chunk arithmetic of FACT 9 recomputed independently from the parent's
 *       Index.db and CompressionInfo.db, including "no trailing slack" and "offsets[0] == 0";</li>
 *   <li>{@link #assertComponents} -- Filter/Summary/Digest/TOC are the ones on disk and are self-consistent.</li>
 * </ul>
 */
public class ZeroCopySSTableSplitterTest extends CQLTester
{
    private static final SSTableReadsListener NOOP = SSTableReadsListener.NOOP_LISTENER;

    // ----------------------------------------------------------------------------------------------------
    // Tests
    // ----------------------------------------------------------------------------------------------------

    /**
     * The core test. 80 narrow partitions (no promoted index), 4 children, then everything: content
     * equivalence, point reads, structure, components, dead prefixes, and a reopen purely from disk.
     */
    @Test
    public void splitFourWaysIsEquivalentToTheParent() throws Throwable
    {
        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 5, 480);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue(parent.compression);
        assertTrue(ZeroCopySSTableSplitter.isSupported(parent));
        assertEquals(4096, parent.getCompressionMetadata().chunkLength());
        // more than one chunk, otherwise the whole exercise is trivial
        assertTrue(parent.uncompressedLength() > 20L * 4096);

        Result result = ZeroCopySSTableSplitter.split(parent, 4, null);
        try
        {
            assertEquals(4, result.children.size());
            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);

            // The dead prefix must genuinely exist for at least one child, and that child must still read.
            Child dead = firstChildWithDeadPrefix(result);
            assertNotNull("no child started off a chunk boundary; the dead-prefix path was not exercised", dead);
            assertTrue(dead.deadPrefixBytes > 0);
            RowIndexEntry firstEntry = dead.reader.getPosition(dead.first, SSTableReader.Operator.EQ, false);
            assertNotNull(firstEntry);
            assertEquals(dead.deadPrefixBytes, firstEntry.position);
            assertTrue(firstEntry.position < dead.reader.getCompressionMetadata().chunkLength());
            try (UnfilteredRowIterator expected = parent.rowIterator(dead.first, Slices.ALL, allColumns(cfs), false, NOOP);
                 UnfilteredRowIterator actual = dead.reader.rowIterator(dead.first, Slices.ALL, allColumns(cfs), false, NOOP))
            {
                assertSamePartition(expected, actual);
            }
        }
        finally
        {
            release(result);
        }

        // Reopen purely from the on-disk files: nothing may depend on in-memory state.
        List<SSTableReader> reopened = new ArrayList<>();
        try
        {
            for (Child child : result.children)
                reopened.add(SSTableReader.open(child.descriptor, child.components, cfs.metadata));

            for (int i = 0; i < reopened.size(); i++)
            {
                assertEquals(result.children.get(i).first, reopened.get(i).first);
                assertEquals(result.children.get(i).last, reopened.get(i).last);
                assertEquals(result.children.get(i).dataLength, reopened.get(i).uncompressedLength());
            }
            assertConcatenatedContentEquals(parent, reopened);
        }
        finally
        {
            for (SSTableReader reader : reopened)
                reader.selfRef().release();
        }
    }

    /**
     * Wide partitions: every partition carries a promoted index blob and spans several compression chunks.
     * The blob is copied verbatim, so slice reads (which navigate it) must return identical results, and the
     * column-index cache is forced to zero so the blob is re-read from the CHILD's Index.db on every lookup
     * (ShallowIndexedEntry) rather than being served from an on-heap copy.
     */
    @Test
    public void widePartitionsPreserveThePromotedIndex() throws Throwable
    {
        int previousCacheSize = DatabaseDescriptor.getColumnIndexCacheSizeInKiB();
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        try
        {
            createCompressedTable(4);
            disableCompaction();
            int partitions = 12;
            int rowsPerPartition = 40;
            insertPartitions(partitions, rowsPerPartition, 1000);
            flush();

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            SSTableReader parent = onlySSTable(cfs);
            List<Rec> parentIndex = readIndex(parent.descriptor);
            assertEquals(partitions, parentIndex.size());

            int chunkLength = parent.getCompressionMetadata().chunkLength();
            for (int r = 0; r < parentIndex.size(); r++)
            {
                assertTrue("partition " + r + " has no promoted index", parentIndex.get(r).promoted != null);
                long end = r + 1 < parentIndex.size() ? parentIndex.get(r + 1).position : parent.uncompressedLength();
                assertTrue("partition " + r + " does not span multiple chunks",
                           end - parentIndex.get(r).position > chunkLength);
            }

            Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
            try
            {
                assertEquals(3, result.children.size());
                assertStructure(cfs, parent, result);
                assertComponents(cfs, result);
                assertConcatenatedContentEquals(parent, readers(result));
                assertPointReads(parent, result);

                for (Child child : result.children)
                {
                    RowIndexEntry entry = child.reader.getPosition(child.first, SSTableReader.Operator.EQ, false);
                    assertNotNull(entry);
                    assertTrue("child lost the promoted index for " + child.first, entry.isIndexed());
                }

                assertSliceReadsMatch(cfs, parent, result, rowsPerPartition);
            }
            finally
            {
                release(result);
            }
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexCacheSize(previousCacheSize);
        }
    }

    /** One child: the Data.db copy must be byte identical to the parent's, and there is no dead prefix. */
    @Test
    public void singleChildCopiesTheParentByteForByte() throws Throwable
    {
        createCompressedTable(4);
        disableCompaction();
        insertPartitions(25, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);

        Result result = ZeroCopySSTableSplitter.split(parent, 1, null);
        try
        {
            assertEquals(1, result.children.size());
            Child only = result.children.get(0);
            assertEquals(0, only.firstChunk);
            assertEquals(0, only.shift);
            assertEquals(0, only.deadPrefixBytes);
            assertEquals(0, result.totalDeadPrefixBytes);
            assertEquals(0, result.duplicatedChunkBytes);
            assertEquals(parent.uncompressedLength(), only.dataLength);
            assertEquals(parent.descriptor.fileFor(Component.DATA).length(), only.physicalBytes);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);

            assertArrayEquals("a one-way split must reproduce Data.db exactly",
                              Files.readAllBytes(parent.descriptor.fileFor(Component.DATA).toPath()),
                              Files.readAllBytes(only.descriptor.fileFor(Component.DATA).toPath()));
            assertEquals(readDigest(parent.descriptor), readDigest(only.descriptor));
        }
        finally
        {
            release(result);
        }
    }

    /**
     * A single-partition sstable is still splittable one way, and cannot be split further. Also covers the
     * "very first and very last partition are the same partition" boundary.
     */
    @Test
    public void singlePartitionSSTable() throws Throwable
    {
        createCompressedTable(4);
        disableCompaction();
        insertPartitions(1, 12, 900);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertEquals(1, readIndex(parent.descriptor).size());
        assertTrue(parent.uncompressedLength() > parent.getCompressionMetadata().chunkLength());

        try
        {
            ZeroCopySSTableSplitter.split(parent, 2, null);
            fail("a single-partition sstable cannot be split two ways");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot split"));
        }

        Result result = ZeroCopySSTableSplitter.split(parent, 1, null);
        try
        {
            assertEquals(1, result.children.size());
            assertEquals(1, result.children.get(0).partitionCount);
            assertEquals(parent.first, result.children.get(0).first);
            assertEquals(parent.last, result.children.get(0).last);
            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
        }
        finally
        {
            release(result);
        }
    }

    /**
     * One child per partition, all of them inside a single compression chunk. Every child then copies the
     * same physical chunk and only its own Index.db entry keeps it apart; the concatenation must still be
     * exactly the parent.
     */
    @Test
    public void oneChildPerPartitionInsideASingleChunk() throws Throwable
    {
        createCompressedTable(4);
        disableCompaction();
        insertPartitions(3, 1, 100);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        assertTrue("expected the whole sstable to fit in one chunk",
                   parent.uncompressedLength() <= parent.getCompressionMetadata().chunkLength());

        try
        {
            ZeroCopySSTableSplitter.split(parent, 4, null);
            fail("expected a refusal for more children than partitions");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot split"));
        }

        try
        {
            ZeroCopySSTableSplitter.split(parent, 0, null);
            fail("expected a refusal for numChildren < 1");
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }

        Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        try
        {
            assertEquals(3, result.children.size());
            for (Child child : result.children)
            {
                assertEquals(1, child.partitionCount);
                assertEquals(0, child.firstChunk);
                assertEquals(0, child.lastChunk);
                assertEquals(0, child.shift);
            }
            // children 1 and 2 start inside chunk 0, so they must carry a dead prefix
            assertEquals(0, result.children.get(0).deadPrefixBytes);
            assertTrue(result.children.get(1).deadPrefixBytes > 0);
            assertTrue(result.children.get(2).deadPrefixBytes > 0);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);
        }
        finally
        {
            release(result);
        }
    }

    /** The explicit-boundary form, including the "boundary range contains no partition" case. */
    @Test
    public void explicitBoundaries() throws Throwable
    {
        createCompressedTable(4);
        disableCompaction();
        insertPartitions(60, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        List<Rec> parentIndex = readIndex(parent.descriptor);
        assertEquals(60, parentIndex.size());

        DecoratedKey first = parent.decorateKey(parentIndex.get(17).key);
        DecoratedKey second = parent.decorateKey(parentIndex.get(41).key);

        Result result = ZeroCopySSTableSplitter.split(parent, Arrays.asList(first, second), null);
        try
        {
            assertEquals(3, result.children.size());
            assertEquals(17, result.children.get(0).partitionCount);
            assertEquals(24, result.children.get(1).partitionCount);
            assertEquals(19, result.children.get(2).partitionCount);
            assertEquals(first, result.children.get(1).first);
            assertEquals(second, result.children.get(2).first);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
            assertPointReads(parent, result);
        }
        finally
        {
            release(result);
        }

        // A boundary equal to the very first key leaves an empty leading run: no child is emitted for it.
        Result degenerate = ZeroCopySSTableSplitter.split(parent, Collections.singletonList(parent.first), null);
        try
        {
            assertEquals(1, degenerate.children.size());
            assertEquals(60, degenerate.children.get(0).partitionCount);
            assertConcatenatedContentEquals(parent, readers(degenerate));
        }
        finally
        {
            release(degenerate);
        }

        try
        {
            ZeroCopySSTableSplitter.split(parent, Arrays.asList(second, first), null);
            fail("expected non-increasing boundaries to be rejected");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("strictly increasing"));
        }
    }

    /**
     * A split boundary that lands exactly on a compression chunk boundary: the second child then has no dead
     * prefix and the two children share no chunk at all.
     * <p>
     * Every partition here has an identical serialised size S (fixed width key, fixed width value, fixed
     * timestamp), so partition r starts at r*S and some r = L / gcd(S, L) &lt;= L is necessarily a multiple
     * of the 1 KiB chunk length -- which is why 1200 partitions are written.
     */
    @Test
    public void splitBoundaryOnAChunkBoundary() throws Throwable
    {
        createCompressedTable(1);
        disableCompaction();
        int partitions = 1200;
        String value = fixedText(24);
        for (int p = 0; p < partitions; p++)
            execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?) USING TIMESTAMP 1000", key(p), 0, value);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);
        int chunkLength = parent.getCompressionMetadata().chunkLength();
        assertEquals(1024, chunkLength);

        List<Rec> parentIndex = readIndex(parent.descriptor);
        assertEquals(partitions, parentIndex.size());
        long size = parentIndex.get(1).position - parentIndex.get(0).position;
        for (int r = 1; r < parentIndex.size(); r++)
            assertEquals("partitions were expected to be identically sized",
                         size, parentIndex.get(r).position - parentIndex.get(r - 1).position);

        int aligned = -1;
        for (int r = 1; r < parentIndex.size(); r++)
        {
            if (parentIndex.get(r).position % chunkLength == 0)
            {
                aligned = r;
                break;
            }
        }
        assertTrue("no partition start landed on a chunk boundary (partition size " + size + ')', aligned > 0);

        DecoratedKey boundary = parent.decorateKey(parentIndex.get(aligned).key);
        Result result = ZeroCopySSTableSplitter.split(parent, Collections.singletonList(boundary), null);
        try
        {
            assertEquals(2, result.children.size());
            Child head = result.children.get(0);
            Child tail = result.children.get(1);

            assertEquals(aligned, head.partitionCount);
            assertEquals(partitions - aligned, tail.partitionCount);
            assertEquals(0, tail.deadPrefixBytes);
            assertEquals(parentIndex.get(aligned).position, tail.shift);
            assertEquals("an aligned boundary must not duplicate a chunk", head.lastChunk + 1, tail.firstChunk);
            assertEquals(0, result.duplicatedChunkBytes);
            // the head's Data.db ends exactly where the tail's begins: no byte is in both children
            assertEquals(parent.descriptor.fileFor(Component.DATA).length(),
                         head.physicalBytes + tail.physicalBytes);

            assertStructure(cfs, parent, result);
            assertComponents(cfs, result);
            assertConcatenatedContentEquals(parent, readers(result));
        }
        finally
        {
            release(result);
        }
    }

    /**
     * FACT 7: Scrubber and Verifier both walk Data.db linearly from position 0 and were patched to start at
     * the first index position instead. So both must now ACCEPT a child that carries a dead prefix.
     */
    @Test
    public void verifierAndScrubberAcceptAChildWithADeadPrefix() throws Throwable
    {
        createCompressedTable(4);
        disableCompaction();
        insertPartitions(40, 4, 500);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader parent = onlySSTable(cfs);

        Result result = ZeroCopySSTableSplitter.split(parent, 3, null);
        SSTableReader consumedByTxn = null;
        try
        {
            Child dead = firstChildWithDeadPrefix(result);
            assertNotNull("no child started off a chunk boundary", dead);

            // Extended verification reads every partition off Data.db linearly and validates Digest.crc32,
            // the index, the summary and the bloom filter. It throws CorruptSSTableException on failure.
            for (Child child : result.children)
            {
                try (Verifier verifier = new Verifier(cfs, child.reader, true,
                                                      Verifier.options().extendedVerification(true).build()))
                {
                    verifier.verify();
                }
            }

            // Scrubber rewrites the child from a linear Data.db walk; every partition must come back good.
            // LifecycleTransaction.offline() hands the reader to a dummy Tracker that owns and releases it
            // (LifecycleTransaction.java:143-149), so this child must not be released again below.
            consumedByTxn = dead.reader;
            Scrubber.ScrubResult scrubResult;
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.SCRUB, dead.reader);
                 Scrubber scrubber = new Scrubber(cfs, txn, false, true))
            {
                scrubResult = scrubber.scrubWithResult();
            }
            assertEquals(dead.partitionCount, scrubResult.goodPartitions);
            assertEquals(0, scrubResult.badPartitions);
            assertEquals(0, scrubResult.emptyPartitions);
        }
        finally
        {
            releaseExcept(result, consumedByTxn);
            LifecycleTransaction.waitForDeletions();
        }
    }

    /** An uncompressed parent is refused up front rather than producing a child with a misaligned CRC.db. */
    @Test
    public void uncompressedParentIsRefused() throws Throwable
    {
        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        disableCompaction();
        insertPartitions(10, 2, 300);
        flush();

        SSTableReader parent = onlySSTable(getCurrentColumnFamilyStore());
        assertFalse(parent.compression);
        assertFalse(ZeroCopySSTableSplitter.isSupported(parent));

        try
        {
            ZeroCopySSTableSplitter.split(parent, 2, null);
            fail("expected an uncompressed parent to be refused");
        }
        catch (UnsupportedOperationException e)
        {
            assertTrue(e.getMessage(),
                       e.getMessage().startsWith(ZeroCopySSTableSplitter.UNCOMPRESSED_UNSUPPORTED_MESSAGE));
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Content equivalence
    // ----------------------------------------------------------------------------------------------------

    /**
     * The single most important assertion in this file: the children, scanned in order and concatenated,
     * produce exactly the parent's partition stream -- same keys in the same order, same partition level
     * deletions, same rows/range tombstones, same cells, same timestamps.
     */
    private static void assertConcatenatedContentEquals(SSTableReader parent, List<SSTableReader> children)
    {
        int compared = 0;
        try (ISSTableScanner parentScanner = parent.getScanner())
        {
            for (SSTableReader child : children)
            {
                try (ISSTableScanner childScanner = child.getScanner())
                {
                    while (childScanner.hasNext())
                    {
                        assertTrue("children yielded more partitions than the parent has (at " + compared + ')',
                                   parentScanner.hasNext());
                        try (UnfilteredRowIterator expected = parentScanner.next();
                             UnfilteredRowIterator actual = childScanner.next())
                        {
                            assertSamePartition(expected, actual);
                        }
                        compared++;
                    }
                }
            }
            assertFalse("the parent has partitions that no child covers (after " + compared + ')',
                        parentScanner.hasNext());
        }
        assertTrue("nothing was compared", compared > 0);
    }

    private static void assertSamePartition(UnfilteredRowIterator expected, UnfilteredRowIterator actual)
    {
        String context = "partition " + expected.partitionKey();
        assertEquals(context, expected.partitionKey(), actual.partitionKey());
        assertEquals(context + ": partition level deletion",
                     expected.partitionLevelDeletion(), actual.partitionLevelDeletion());
        assertEquals(context + ": static row", expected.staticRow(), actual.staticRow());
        assertEquals(context + ": columns", expected.columns(), actual.columns());
        assertEquals(context + ": reverse order", expected.isReverseOrder(), actual.isReverseOrder());

        int i = 0;
        while (expected.hasNext())
        {
            assertTrue(context + ": child ran out of rows after " + i, actual.hasNext());
            assertEquals(context + ": unfiltered " + i, expected.next(), actual.next());
            i++;
        }
        assertFalse(context + ": child has extra rows after " + i, actual.hasNext());
        assertTrue(context + ": expected at least one row", i > 0);
    }

    /** Every parent key is owned by exactly one child, reads back identically there, and is absent elsewhere. */
    private void assertPointReads(SSTableReader parent, Result result) throws IOException
    {
        ColumnFilter columns = ColumnFilter.all(parent.metadata());
        for (Rec rec : readIndex(parent.descriptor))
        {
            DecoratedKey key = parent.decorateKey(rec.key);
            int owners = 0;
            for (Child child : result.children)
            {
                boolean inRange = key.compareTo(child.first) >= 0 && key.compareTo(child.last) <= 0;
                RowIndexEntry entry = child.reader.getPosition(key, SSTableReader.Operator.EQ, false);
                if (!inRange)
                {
                    assertNull("child " + child.descriptor + " must not contain " + key, entry);
                    continue;
                }
                owners++;
                // getPosition(EQ) consults the bloom filter first, so a null here would also be a filter
                // false negative, i.e. silent data loss.
                assertNotNull("child " + child.descriptor + " lost " + key, entry);
                try (UnfilteredRowIterator expected = parent.rowIterator(key, Slices.ALL, columns, false, NOOP);
                     UnfilteredRowIterator actual = child.reader.rowIterator(key, Slices.ALL, columns, false, NOOP))
                {
                    assertSamePartition(expected, actual);
                }
            }
            assertEquals("exactly one child must own " + key, 1, owners);
        }
    }

    /** Clustering-level slice reads inside wide partitions: this is what navigates the copied promoted index. */
    private void assertSliceReadsMatch(ColumnFamilyStore cfs, SSTableReader parent, Result result, int rowsPerPartition)
    throws IOException
    {
        TableMetadata metadata = cfs.metadata();
        ClusteringComparator comparator = metadata.comparator;
        Slices slices = Slices.with(comparator, Slice.make(comparator.make(rowsPerPartition / 3),
                                                           comparator.make(2 * rowsPerPartition / 3)));
        ColumnFilter columns = ColumnFilter.all(metadata);

        int checked = 0;
        for (Child child : result.children)
        {
            for (Rec rec : readIndex(child.descriptor))
            {
                DecoratedKey key = metadata.partitioner.decorateKey(rec.key);
                for (boolean reversed : new boolean[]{ false, true })
                {
                    try (UnfilteredRowIterator expected = parent.rowIterator(key, slices, columns, reversed, NOOP);
                         UnfilteredRowIterator actual = child.reader.rowIterator(key, slices, columns, reversed, NOOP))
                    {
                        assertSamePartition(expected, actual);
                    }
                }
                checked++;
            }
        }
        assertTrue(checked > 0);
    }

    // ----------------------------------------------------------------------------------------------------
    // Structural assertions -- FACT 9 recomputed independently of the implementation
    // ----------------------------------------------------------------------------------------------------

    private void assertStructure(ColumnFamilyStore cfs, SSTableReader parent, Result result) throws IOException
    {
        List<Rec> parentIndex = readIndex(parent.descriptor);
        int n = parentIndex.size();

        CompressionMetadata meta = parent.getCompressionMetadata();
        int chunkLength = meta.chunkLength();
        long parentUncompressed = parent.uncompressedLength();
        assertEquals(meta.dataLength, parentUncompressed);
        long parentPhysical = parent.descriptor.fileFor(Component.DATA).length();
        assertEquals(parentPhysical, meta.compressedFileLength);
        int parentChunks = (int) ((parentUncompressed + chunkLength - 1) / chunkLength);

        StatsMetadata parentStats = parent.getSSTableMetadata();

        long physicalSum = 0;
        long deadSum = 0;
        long duplicatedSum = 0;
        long partitionSum = 0;
        int cursor = 0;
        long previousLastChunk = -1;

        for (Child child : result.children)
        {
            String context = "child " + child.descriptor;
            int from = cursor;
            assertTrue(context + " starts past the end of the parent", from < n);
            int to = from + (int) child.partitionCount;
            assertTrue(context + " runs past the end of the parent", to <= n);

            assertEquals(context + ": first key", parentIndex.get(from).key, child.first.getKey());
            assertEquals(context + ": last key", parentIndex.get(to - 1).key, child.last.getKey());
            assertEquals(context + ": reader.first", child.first, child.reader.first);
            assertEquals(context + ": reader.last", child.last, child.reader.last);
            assertTrue(context + ": first > last", child.first.compareTo(child.last) <= 0);

            long lo = parentIndex.get(from).position;
            long hi = to < n ? parentIndex.get(to).position : parentUncompressed;
            long firstChunk = lo / chunkLength;
            long lastChunk = (hi - 1) / chunkLength;
            long dataLength = hi - firstChunk * chunkLength;
            long physicalBytes = chunkOffset(meta, lastChunk + 1, parentChunks, parentPhysical, chunkLength)
                                 - chunkOffset(meta, firstChunk, parentChunks, parentPhysical, chunkLength);

            assertEquals(context + ": firstChunk", firstChunk, child.firstChunk);
            assertEquals(context + ": lastChunk", lastChunk, child.lastChunk);
            assertEquals(context + ": shift", firstChunk * chunkLength, child.shift);
            assertEquals(context + ": deadPrefixBytes", lo % chunkLength, child.deadPrefixBytes);
            assertEquals(context + ": dataLength", dataLength, child.dataLength);
            assertEquals(context + ": physicalBytes", physicalBytes, child.physicalBytes);
            // (C - 1) * L < Dp <= C * L
            long chunkCount = lastChunk - firstChunk + 1;
            assertTrue(context + ": (C-1)*L < Dp", (chunkCount - 1) * chunkLength < dataLength);
            assertTrue(context + ": Dp <= C*L", dataLength <= chunkCount * chunkLength);

            // FACT 6: not one byte of trailing slack on disk.
            assertEquals(context + ": physical Data.db length",
                         physicalBytes, child.descriptor.fileFor(Component.DATA).length());
            assertEquals(context + ": uncompressedLength", dataLength, child.reader.uncompressedLength());

            CompressionMetadata childMeta = child.reader.getCompressionMetadata();
            assertEquals(context + ": offsets[0]", 0, childMeta.chunkFor(0).offset);
            assertEquals(context + ": chunkLength", chunkLength, childMeta.chunkLength());
            assertEquals(context + ": maxCompressedLength", meta.maxCompressedLength(), childMeta.maxCompressedLength());
            assertEquals(context + ": CompressionInfo dataLength", dataLength, childMeta.dataLength);
            assertEquals(context + ": compressedFileLength", physicalBytes, childMeta.compressedFileLength);
            // the last chunk plus its 4 byte inline CRC32 must end exactly at the physical end of the file
            CompressionMetadata.Chunk tail = childMeta.chunkFor((chunkCount - 1) * chunkLength);
            assertEquals(context + ": last chunk overruns the file", physicalBytes, tail.offset + tail.length + 4);

            // Index.db: same keys, same promoted blobs, positions rebased by exactly shift.
            List<Rec> childIndex = readIndex(child.descriptor);
            assertEquals(context + ": partition count", child.partitionCount, childIndex.size());
            assertEquals(context + ": first index position", lo % chunkLength, childIndex.get(0).position);
            assertTrue(context + ": first index position must be inside the first chunk",
                       childIndex.get(0).position < chunkLength);
            for (int r = 0; r < childIndex.size(); r++)
            {
                Rec expected = parentIndex.get(from + r);
                Rec actual = childIndex.get(r);
                assertEquals(context + ": key " + r, expected.key, actual.key);
                assertEquals(context + ": position " + r,
                             expected.position - firstChunk * chunkLength, actual.position);
                assertArrayEquals(context + ": promoted index blob " + r + " must be copied verbatim",
                                  expected.promoted, actual.promoted);
            }

            RowIndexEntry firstEntry = child.reader.getPosition(child.first, SSTableReader.Operator.EQ, false);
            assertNotNull(context + ": cannot find its own first key", firstEntry);
            assertEquals(context + ": first entry position", lo % chunkLength, firstEntry.position);

            // Statistics.db: the header and the min/max encoding bases MUST be inherited verbatim or every
            // relocated row silently decodes wrong; the two derived fields must be recomputed.
            StatsMetadata childStats = child.reader.getSSTableMetadata();
            assertEquals(context + ": header columns", parent.header.columns(), child.reader.header.columns());
            assertEquals(context + ": header stats", parent.header.stats(), child.reader.header.stats());
            assertEquals(context + ": minTimestamp", parentStats.minTimestamp, childStats.minTimestamp);
            assertEquals(context + ": maxTimestamp", parentStats.maxTimestamp, childStats.maxTimestamp);
            assertEquals(context + ": minLocalDeletionTime",
                         parentStats.minLocalDeletionTime, childStats.minLocalDeletionTime);
            assertEquals(context + ": maxLocalDeletionTime",
                         parentStats.maxLocalDeletionTime, childStats.maxLocalDeletionTime);
            assertEquals(context + ": minTTL", parentStats.minTTL, childStats.minTTL);
            assertEquals(context + ": maxTTL", parentStats.maxTTL, childStats.maxTTL);
            assertEquals(context + ": sstableLevel", parentStats.sstableLevel, childStats.sstableLevel);
            assertEquals(context + ": repairedAt", parentStats.repairedAt, childStats.repairedAt);
            assertEquals(context + ": originatingHostId", parentStats.originatingHostId, childStats.originatingHostId);
            assertEquals(context + ": compressionRatio",
                         (double) physicalBytes / dataLength, childStats.compressionRatio, 1e-9);
            assertEquals(context + ": estimatedPartitionSize count",
                         child.partitionCount, childStats.estimatedPartitionSize.count());

            physicalSum += physicalBytes;
            deadSum += lo % chunkLength;
            partitionSum += child.partitionCount;
            if (previousLastChunk == firstChunk)
                duplicatedSum += chunkOffset(meta, firstChunk + 1, parentChunks, parentPhysical, chunkLength)
                                 - chunkOffset(meta, firstChunk, parentChunks, parentPhysical, chunkLength);
            previousLastChunk = lastChunk;
            cursor = to;
        }

        assertEquals("children must cover every parent partition exactly once", n, cursor);
        assertEquals("partition counts must sum to the parent's", n, partitionSum);
        assertEquals(physicalSum, result.totalPhysicalBytesCopied);
        assertEquals(deadSum, result.totalDeadPrefixBytes);
        assertEquals(duplicatedSum, result.duplicatedChunkBytes);
        assertEquals(parent.first, result.children.get(0).first);
        assertEquals(parent.last, result.children.get(result.children.size() - 1).last);
    }

    /** {@code O(k)}, with {@code O(N)} defined as the physical file length. */
    private static long chunkOffset(CompressionMetadata meta, long k, int chunkCount, long compressedFileLength, int chunkLength)
    {
        return k == chunkCount ? compressedFileLength : meta.chunkFor(k * (long) chunkLength).offset;
    }

    // ----------------------------------------------------------------------------------------------------
    // Component sanity
    // ----------------------------------------------------------------------------------------------------

    private void assertComponents(ColumnFamilyStore cfs, Result result) throws IOException
    {
        TableMetadata metadata = cfs.metadata();
        for (Child child : result.children)
        {
            String context = "child " + child.descriptor;

            // TOC.txt lists exactly the components that exist on disk, and nothing else exists on disk.
            assertEquals(context + ": TOC", child.components, SSTable.readTOC(child.descriptor, false));
            assertEquals(context + ": files on disk",
                         child.components, SSTable.discoverComponentsFor(child.descriptor));
            assertTrue(context + ": no Filter.db", child.components.contains(Component.FILTER));
            assertTrue(context + ": no CompressionInfo.db", child.components.contains(Component.COMPRESSION_INFO));
            assertFalse(context + ": a compressed sstable must not have a CRC.db",
                        child.components.contains(Component.CRC));
            for (Component component : child.components)
                assertTrue(context + ": missing " + component, child.descriptor.fileFor(component).exists());

            // Digest.crc32 is the decimal CRC32 of every physical byte of Data.db.
            assertEquals(context + ": digest",
                         Long.toString(crc32Of(child.descriptor.fileFor(Component.DATA))),
                         readDigest(child.descriptor));

            List<Rec> childIndex = readIndex(child.descriptor);

            // Bloom filter: a false negative is data loss, so every owned key must be present.
            try (FileInputStreamPlus in = child.descriptor.fileFor(Component.FILTER).newInputStream();
                 IFilter filter = BloomFilterSerializer.deserialize(in, child.descriptor.version.hasOldBfFormat()))
            {
                for (Rec rec : childIndex)
                    assertTrue(context + ": bloom filter false negative",
                               filter.isPresent(metadata.partitioner.decorateKey(rec.key)));
            }

            // Summary.db deserialises standalone with the schema's index interval (otherwise the read path
            // silently deletes it and rebuilds), and carries the child's own first/last keys.
            try (DataInputStream in = new DataInputStream(
                     Files.newInputStream(child.descriptor.fileFor(Component.SUMMARY).toPath())))
            {
                IndexSummary summary = IndexSummary.serializer.deserialize(in,
                                                                           metadata.partitioner,
                                                                           metadata.params.minIndexInterval,
                                                                           metadata.params.maxIndexInterval);
                try
                {
                    assertTrue(context + ": empty summary", summary.size() > 0);
                    assertEquals(context + ": summary minIndexInterval",
                                 metadata.params.minIndexInterval, summary.getMinIndexInterval());
                }
                finally
                {
                    summary.close();
                }
                assertEquals(context + ": summary first key",
                             child.first, metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(in)));
                assertEquals(context + ": summary last key",
                             child.last, metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(in)));
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Plumbing
    // ----------------------------------------------------------------------------------------------------

    /** One parent/child Index.db record, parsed independently of {@code ZeroCopySSTableSplitter}. */
    private static final class Rec
    {
        final ByteBuffer key;
        final long position;
        final byte[] promoted;   // null when promotedSize == 0

        Rec(ByteBuffer key, long position, byte[] promoted)
        {
            this.key = key;
            this.position = position;
            this.promoted = promoted;
        }
    }

    private static List<Rec> readIndex(Descriptor descriptor) throws IOException
    {
        List<Rec> records = new ArrayList<>();
        try (RandomAccessReader in = RandomAccessReader.open(descriptor.fileFor(Component.PRIMARY_INDEX)))
        {
            long length = in.length();
            while (in.getFilePointer() != length)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                long position = RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = (int) in.readUnsignedVInt();
                byte[] promoted = null;
                if (promotedSize > 0)
                {
                    promoted = new byte[promotedSize];
                    in.readFully(promoted);
                }
                records.add(new Rec(key, position, promoted));
            }
        }
        return records;
    }

    private static long crc32Of(File file) throws IOException
    {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[8192];
        try (FileInputStreamPlus in = file.newInputStream())
        {
            int n;
            while ((n = in.read(buffer)) > 0)
                crc.update(buffer, 0, n);
        }
        return crc.getValue();
    }

    private static String readDigest(Descriptor descriptor) throws IOException
    {
        byte[] bytes = Files.readAllBytes(descriptor.fileFor(Component.DIGEST).toPath());
        return new String(bytes, StandardCharsets.UTF_8).trim();
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        return live.iterator().next();
    }

    private static List<SSTableReader> readers(Result result)
    {
        List<SSTableReader> readers = new ArrayList<>(result.children.size());
        for (Child child : result.children)
            readers.add(child.reader);
        return readers;
    }

    private static Child firstChildWithDeadPrefix(Result result)
    {
        for (Child child : result.children)
        {
            if (child.deadPrefixBytes > 0)
                return child;
        }
        return null;
    }

    private static void release(Result result)
    {
        releaseExcept(result, null);
    }

    /**
     * A child handed to {@link LifecycleTransaction#offline} is owned by that transaction's dummy Tracker,
     * which releases it on close (LifecycleTransaction.java:143-149). Releasing it again here would throw
     * "Attempted to release a reference that has already been released" and mask the real assertions.
     */
    private static void releaseExcept(Result result, SSTableReader consumed)
    {
        for (Child child : result.children)
            if (child.reader != consumed)
                child.reader.selfRef().release();
    }

    private static ColumnFilter allColumns(ColumnFamilyStore cfs)
    {
        return ColumnFilter.all(cfs.metadata());
    }

    private String createCompressedTable(int chunkLengthInKb) throws Throwable
    {
        return createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                           "WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '" +
                           chunkLengthInKb + "'}");
    }

    private void insertPartitions(int partitions, int rowsPerPartition, int valueBytes) throws Throwable
    {
        for (int p = 0; p < partitions; p++)
            for (int c = 0; c < rowsPerPartition; c++)
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", key(p), c, randomText(valueBytes));
    }

    private static String key(int p)
    {
        return String.format("k%06d", p);
    }

    /** Near-incompressible payload, so the sstable really does span many compression chunks. */
    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

    private static String fixedText(int length)
    {
        char[] chars = new char[length];
        Arrays.fill(chars, 'v');
        return new String(chars);
    }
}
