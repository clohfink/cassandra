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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * used to transfer the part(or whole) of a SSTable data file
 */
public class CassandraOutgoingFile implements OutgoingStream
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraOutgoingFile.class);

    private final Ref<SSTableReader> ref;
    private final long estimatedKeys;
    private final List<SSTableReader.PartitionPositionBounds> sections;
    private final String filename;
    private final boolean shouldStreamEntireSSTable;
    /**
     * Set when the sections do not cover the whole sstable but can still be sent through the entire-sstable
     * protocol as a synthesised slice; null otherwise. See {@link ZeroCopySSTableSlice}.
     */
    private final ZeroCopySSTableSlice.Plan slicePlan;
    private final StreamOperation operation;
    /**
     * The header for the two paths that send this file as-is: entire-sstable and legacy row-by-row. A slice builds its
     * own from the manifest it measured, in {@link #writeSlice}, and this one must NOT be built as if it were a slice:
     * {@link #writeSlice} may give up before writing a byte, and {@link #write} then sends a legacy body under it.
     */
    private final CassandraStreamHeader header;
    /**
     * Estimated component sizes for the slice we intend to send, for the stream plan's progress totals only; null
     * unless {@link #isSliced()}. Never serialised. See {@link #estimateSliceManifest}.
     */
    private final ComponentManifest sliceManifest;

    public CassandraOutgoingFile(StreamOperation operation, Ref<SSTableReader> ref,
                                 List<SSTableReader.PartitionPositionBounds> sections, List<Range<Token>> normalizedRanges,
                                 long estimatedKeys)
    {
        Preconditions.checkNotNull(ref.get());
        Range.assertNormalized(normalizedRanges);
        this.operation = operation;
        this.ref = ref;
        this.estimatedKeys = estimatedKeys;
        this.sections = sections;

        SSTableReader sstable = ref.get();

        this.filename = sstable.getFilename();
        this.shouldStreamEntireSSTable = computeShouldStreamEntireSSTables();
        this.slicePlan = shouldStreamEntireSSTable ? null : computeSlicePlan();

        // isEntireSSTable is the only thing the receiver dispatches on (CassandraIncomingFile), so this header -- the one
        // the fallback in write() puts on the wire -- describes the sections, not the slice.
        this.header = makeHeader(sstable, operation, sections, estimatedKeys, shouldStreamEntireSSTable,
                                 shouldStreamEntireSSTable ? ComponentManifest.create(sstable.descriptor) : null,
                                 sstable.first);
        this.sliceManifest = isSliced() ? estimateSliceManifest(sstable, slicePlan) : null;
    }

    private static CassandraStreamHeader makeHeader(SSTableReader sstable,
                                                    StreamOperation operation,
                                                    List<SSTableReader.PartitionPositionBounds> sections,
                                                    long estimatedKeys,
                                                    boolean isEntireSSTable,
                                                    ComponentManifest manifest,
                                                    DecoratedKey firstKey)
    {
        boolean keepSSTableLevel = operation == StreamOperation.BOOTSTRAP || operation == StreamOperation.REBUILD;

        CompressionInfo compressionInfo = sstable.compression
                ? CompressionInfo.newLazyInstance(sstable.getCompressionMetadata(), sections)
                : null;

        return CassandraStreamHeader.builder()
                                    .withSSTableFormat(sstable.descriptor.formatType)
                                    .withSSTableVersion(sstable.descriptor.version)
                                    .withSSTableLevel(keepSSTableLevel ? sstable.getSSTableLevel() : 0)
                                    .withEstimatedKeys(estimatedKeys)
                                    .withSections(sections)
                                    .withCompressionInfo(compressionInfo)
                                    .withSerializationHeader(sstable.header.toComponent())
                                    .isEntireSSTable(isEntireSSTable)
                                    .withComponentManifest(manifest)
                                    .withFirstKey(firstKey)
                                    .withTableId(sstable.metadata().id)
                                    .build();
    }

    @VisibleForTesting
    public static CassandraOutgoingFile fromStream(OutgoingStream stream)
    {
        Preconditions.checkArgument(stream instanceof CassandraOutgoingFile);
        return (CassandraOutgoingFile) stream;
    }

    @VisibleForTesting
    public Ref<SSTableReader> getRef()
    {
        return ref;
    }

    @Override
    public String getName()
    {
        return filename;
    }

    @Override
    public long getEstimatedSize()
    {
        // A slice sends its manifest's components rather than the sections, so the sections' size would describe the
        // wrong transfer. Both are estimates; see estimateSliceManifest.
        return sliceManifest != null ? sliceManifest.totalSize() : header.size();
    }

    @Override
    public TableId getTableId()
    {
        return ref.get().metadata().id;
    }

    @Override
    public int getNumFiles()
    {
        if (sliceManifest != null)
            return sliceManifest.components().size();
        return header.isEntireSSTable ? header.componentManifest.components().size() : 1;
    }

    @Override
    public long getRepairedAt()
    {
        return ref.get().getRepairedAt();
    }

    @Override
    public TimeUUID getPendingRepair()
    {
        return ref.get().getPendingRepair();
    }

    @Override
    public void write(StreamSession session, StreamingDataOutputPlus out, int version) throws IOException
    {
        SSTableReader sstable = ref.get();

        if (shouldStreamEntireSSTable)
        {
            // Acquire lock to avoid concurrent sstable component mutation because of stats update or index summary
            // redistribution, otherwise file sizes recorded in component manifest will be different from actual
            // file sizes.
            // Recreate the latest manifest and hard links for mutatable components in case they are modified.
            try (ComponentContext context = sstable.runWithLock(ignored -> ComponentContext.create(sstable.descriptor)))
            {
                CassandraStreamHeader current = makeHeader(sstable, operation, sections, estimatedKeys, true,
                                                           context.manifest(), sstable.first);
                CassandraStreamHeader.serializer.serialize(current, out, version);
                out.flush();

                CassandraEntireSSTableStreamWriter writer = new CassandraEntireSSTableStreamWriter(sstable, session, context);
                writer.write(out);
            }
        }
        // A slice goes through the entire-sstable protocol too, under a header of its own. writeSlice returns false only
        // if it gave up before writing anything, falling through to the row-by-row path below, which sends `header` --
        // whose isEntireSSTable is false, so the receiver parses the body it is given rather than splitting it into
        // components.
        else if (!isSliced() || !writeSlice(sstable, session, out, version))
        {
            // legacy streaming is not affected by stats metadata mutation and index sumary redistribution
            CassandraStreamHeader.serializer.serialize(header, out, version);
            out.flush();

            CassandraStreamWriter writer = header.isCompressed() ?
                                           new CassandraCompressedStreamWriter(sstable, header, session) :
                                           new CassandraStreamWriter(sstable, header, session);
            writer.write(out);
        }
    }

    /**
     * Send the planned slice: synthesise every component but Data.db for the chunk run covering the requested sections,
     * then stream those plus the run itself as if they were a whole sstable.
     * <p>
     * All the work that can fail happens BEFORE the first byte reaches {@code out}, so a failure is recoverable: the
     * caller falls back to the row-by-row path, which has no preconditions to fail. Deliberately true even of failures
     * that look like corruption -- a stream is not the place to refuse service over one -- but they are logged at WARN
     * because that is what they are.
     *
     * @return false if nothing was written and the caller must fall back
     */
    private boolean writeSlice(SSTableReader sstable, StreamSession session, StreamingDataOutputPlus out, int version)
    throws IOException
    {
        Descriptor target = null;
        ZeroCopySSTableSlice.Slice slice;
        ComponentManifest manifest;
        Map<Component, File> synthesised = new HashMap<>(ZeroCopySSTableSlice.ALL_SYNTHESISED.size());
        List<ComponentContext.ByteRange> dataRanges = new ArrayList<>(slicePlan.runs.size());
        try
        {
            target = ZeroCopySSTableSlice.newDescriptor(sstable);
            Descriptor sliceDescriptor = target;
            // The slice inherits the parent's Statistics.db, which a stats mutation or index summary redistribution can
            // rewrite underneath it; this is the lock those take.
            slice = sstable.runWithLock(ignored -> ZeroCopySSTableSlice.write(sstable, slicePlan, sliceDescriptor));

            Map<Component, Long> sizes = new HashMap<>(slice.components.size() + 1);
            for (Component component : slice.components)
            {
                synthesised.put(component, slice.descriptor.fileFor(component));
                sizes.put(component, slice.sizes.get(component));
            }
            // The only component that is not a file of the slice's own: it is byte ranges of the parent's.
            for (ZeroCopySSTableSlice.Run run : slicePlan.runs)
                dataRanges.add(new ComponentContext.ByteRange(run.srcStart, run.physicalBytes()));
            sizes.put(Component.DATA, slicePlan.physicalBytes);
            manifest = ComponentManifest.ordered(sizes);
        }
        catch (Throwable t)
        {
            // Everything that can throw is in here, so this is the only place a synthesised file can be orphaned before
            // the ComponentContext below takes over deleting them.
            if (target != null)
                ZeroCopySSTableSlice.delete(target, ZeroCopySSTableSlice.ALL_SYNTHESISED);
            logger.warn("[Stream #{}] Failed slicing {} for {}, falling back to partition-by-partition streaming",
                        session.planId(), sstable.getFilename(), session.peer, t);
            StreamingMetrics.partialZeroCopyStreamsFailed.inc();
            return false;
        }

        try (ComponentContext context = ComponentContext.slice(synthesised, dataRanges, manifest))
        {
            // The receiver picks a data directory from the first key and takes the sstable's identity from the manifest,
            // so both have to describe the SLICE, not the parent it was cut from. The partition count is exact here,
            // unlike the estimate the plan was assembled with.
            CassandraStreamHeader current = makeHeader(sstable, operation, sections, slice.partitionCount, true,
                                                       context.manifest(), slice.first);
            CassandraStreamHeader.serializer.serialize(current, out, version);
            out.flush();

            logger.debug("[Stream #{}] Streaming slice of {} to {}: {}, plan {}",
                         session.planId(), sstable.getFilename(), session.peer, slice, slicePlan);
            StreamingMetrics.partialZeroCopyStreamsOut.inc();
            StreamingMetrics.partialZeroCopyStreamDeadBytes.inc(slicePlan.deadBytes);

            new CassandraEntireSSTableStreamWriter(sstable, session, context).write(out);
        }

        return true;
    }

    @VisibleForTesting
    public boolean computeShouldStreamEntireSSTables()
    {
        // don't stream if full sstable transfers are disabled or legacy counter shards are present
        if (!DatabaseDescriptor.streamEntireSSTables() || ref.get().getSSTableMetadata().hasLegacyCounterShards)
            return false;

        return contained(sections, ref.get());
    }

    /**
     * Whether sections that do NOT cover the whole sstable can still go through the entire-sstable protocol, as a
     * verbatim compression chunk run with synthesised components. Pure arithmetic over the compression metadata; the
     * index is not read until the stream is written.
     */
    @VisibleForTesting
    ZeroCopySSTableSlice.Plan computeSlicePlan()
    {
        // Same protocol, same rate limiter, so the same switch governs it.
        if (!DatabaseDescriptor.streamEntireSSTables() || !DatabaseDescriptor.getZeroCopyPartialStreamEnabled())
            return null;

        ZeroCopySSTableSlice.Plan plan =
            ZeroCopySSTableSlice.plan(ref.get(), sections, DatabaseDescriptor.getZeroCopyPartialStreamMaxDeadSpaceRatio());

        if (!plan.isEligible())
        {
            logger.debug("Not streaming {} as a zero-copy slice: {}", filename, plan.reason);
            return null;
        }
        return plan;
    }

    @VisibleForTesting
    public boolean isSliced()
    {
        return slicePlan != null;
    }

    @VisibleForTesting
    public ZeroCopySSTableSlice.Plan slicePlan()
    {
        return slicePlan;
    }

    /**
     * The component sizes a slice is expected to have, for the progress totals a stream plan is assembled from.
     * <p>
     * Data.db is exact. The others are the parent's scaled by the fraction being sent, because measuring them means an
     * Index.db pass and this runs once per sstable in the plan, before the peer has even been asked whether it wants
     * them. The manifest that goes on the wire is the measured one, built in {@link #writeSlice}.
     * <p>
     * So {@code bytes_to_send} approximates {@code bytes_sent}, off by the error in the index, filter and summary
     * estimates. Nothing depends on the two agreeing: the receiver sizes everything from the manifest it is sent, and
     * {@code StreamingState.progress} clamps at 0.99 until the session ends. Which components are named IS exact,
     * since {@code files_to_send} is a count -- hence conditioning FILTER on the same thing the writer does rather
     * than on the parent's files.
     */
    private static ComponentManifest estimateSliceManifest(SSTableReader sstable, ZeroCopySSTableSlice.Plan plan)
    {
        double fraction = sstable.uncompressedLength() <= 0
                          ? 1.0
                          : Math.min(1.0, (double) plan.usefulBytes / sstable.uncompressedLength());

        Map<Component, Long> sizes = new HashMap<>();
        sizes.put(Component.DATA, plan.physicalBytes);
        for (Component component : plan.components())
        {
            // A filter is written exactly when one can be: fp chance 1.0 means AlwaysPresentFilter, which has nothing
            // to serialise.
            if (component == Component.FILTER && sstable.metadata().params.bloomFilterFpChance >= 1.0)
                continue;

            long parentSize = sstable.descriptor.fileFor(component).length();
            // Statistics.db is per-sstable rather than per-partition, so it does not shrink with the range. CRC.db
            // is four bytes per cell, and the slice has as many cells as it has.
            long size;
            if (component == Component.STATS)
                size = parentSize;
            else if (component == Component.CRC)
                size = 4 + 4 * plan.cellCount();
            else
                size = (long) (parentSize * fraction);
            sizes.put(component, Math.max(1, size));
        }
        return ComponentManifest.ordered(sizes);
    }

    @VisibleForTesting
    public boolean contained(List<SSTableReader.PartitionPositionBounds> sections, SSTableReader sstable)
    {
        if (sections == null || sections.isEmpty())
            return false;

        // Entire-sstable streaming copies component files verbatim, so it is eligible whenever the sections cover all
        // of the sstable's LIVE data, not only when their span equals the physical data length. A zero-copy split
        // child can carry a dead prefix -- bytes before its first indexed partition that no read path enters -- and
        // getPositionsForRanges() starts the first section at the first partition, so the eligible span runs from
        // there to the end of the file. For an ordinary sstable firstPosition == 0 and this reduces to the original
        // transferLength == uncompressedLength check.
        long firstPosition = sstable.getPosition(sstable.first.getToken().minKeyBound(), SSTableReader.Operator.GT).position;
        long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
        return transferLength == sstable.uncompressedLength() - firstPosition;
    }

    @Override
    public void finish()
    {
        ref.release();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraOutgoingFile that = (CassandraOutgoingFile) o;
        return estimatedKeys == that.estimatedKeys &&
               Objects.equals(ref, that.ref) &&
               Objects.equals(sections, that.sections);
    }

    public int hashCode()
    {
        return Objects.hash(ref, estimatedKeys, sections);
    }

    @Override
    public String toString()
    {
        return "CassandraOutgoingFile{" + filename + '}';
    }
}
