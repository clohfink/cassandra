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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSlice;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.PartitionPositionBounds;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.AsyncStreamingOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A partial sstable stream, end to end over an embedded netty channel: {@link CassandraOutgoingFile} synthesises
 * the slice's components and sends them with a byte range of the parent's Data.db, and
 * {@link CassandraEntireSSTableStreamReader} -- unchanged, and unaware that any of this happened -- writes them
 * out as an sstable holding exactly the requested partitions.
 *
 * <p>What this covers that {@code ZeroCopySSTableSliceTest} does not: the component manifest the receiver is
 * driven by, the ranged {@code writeFileToChannel} that sends Data.db without sending the whole file, and that the
 * sender falls back to the row-by-row path rather than sending a slice when it is told not to.
 */
public class CassandraPartialSSTableStreamTest extends CQLTester
{
    private boolean entireSSTables;
    private boolean partialEnabled;
    private double maxDeadSpace;

    @After
    public void restoreConfig()
    {
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = entireSSTables;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(partialEnabled);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(maxDeadSpace);
    }

    private void saveConfig()
    {
        entireSSTables = DatabaseDescriptor.streamEntireSSTables();
        partialEnabled = DatabaseDescriptor.getZeroCopyPartialStreamEnabled();
        maxDeadSpace = DatabaseDescriptor.getZeroCopyPartialStreamMaxDeadSpaceRatio();
    }

    @Test
    public void partialStreamArrivesAsAnSSTableHoldingOnlyTheRange() throws Throwable
    {
        saveConfig();
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);
        assertEquals(80, keys.size());

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<DecoratedKey> expected = keys.subList(20, 60);
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue("the sections do not cover the sstable, so this must be a slice", outgoing.isSliced());
        assertFalse(outgoing.computeShouldStreamEntireSSTables());

        // Everything the plan is assembled from, before an index has been read: the manifest is an estimate but
        // it names the components that will actually be sent, and Data.db's size in it is exact.
        assertEquals(ZeroCopySSTableSlice.COMPRESSED_COMPONENTS.size() + 1, outgoing.getNumFiles());

        Received received = streamAndReceive(cfs, sstable, outgoing);
        try
        {
            assertTrue("the receiver was driven by the entire-sstable path", received.header.isEntireSSTable);
            assertEquals(expected.get(0), received.header.firstKey);
            assertFalse("the sender cannot digest what it sends by sendfile",
                        received.header.componentManifest.components().contains(Component.DIGEST));
            assertEquals(outgoing.slicePlan().physicalBytes, received.header.componentManifest.sizeOf(Component.DATA));

            assertEquals(1, received.sstables.size());
            SSTableReader arrived = received.sstables.iterator().next();
            assertEquals(expected.get(0), arrived.first);
            assertEquals(expected.get(expected.size() - 1), arrived.last);
            assertEquals(outgoing.slicePlan().physicalBytes, arrived.onDiskLength());

            assertContentMatches(sstable, arrived, expected);
            assertOnlyTheseKeysArePresent(arrived, keys, expected);
            assertReceivedDigestIsValid(arrived);
        }
        finally
        {
            received.close();
        }
    }

    /**
     * Several ranges, far enough apart to be separate runs: Data.db arrives as those ranges concatenated, with
     * everything between them never sent.
     */
    @Test
    public void multiRunPartialStreamArrivesAsOneSSTable() throws Throwable
    {
        saveConfig();
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(120, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);

        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(new Range<>(keys.get(9).getToken(), keys.get(29).getToken()));
        ranges.add(new Range<>(keys.get(59).getToken(), keys.get(79).getToken()));
        ranges.add(new Range<>(keys.get(99).getToken(), keys.get(114).getToken()));
        List<Range<Token>> normalized = Range.normalize(ranges);

        List<DecoratedKey> expected = new ArrayList<>();
        expected.addAll(keys.subList(10, 30));
        expected.addAll(keys.subList(60, 80));
        expected.addAll(keys.subList(100, 115));

        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(normalized);
        CassandraOutgoingFile outgoing = new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, sstable.ref(), sections,
                                                                  normalized, sstable.estimatedKeys());
        assertTrue(outgoing.isSliced());
        assertTrue("expected several runs, got " + outgoing.slicePlan(), outgoing.slicePlan().runs.size() > 1);

        Received received = streamAndReceive(cfs, sstable, outgoing);
        try
        {
            assertEquals(1, received.sstables.size());
            SSTableReader arrived = received.sstables.iterator().next();
            assertEquals(outgoing.slicePlan().physicalBytes, arrived.onDiskLength());
            assertEquals(outgoing.slicePlan().dataLength, arrived.uncompressedLength());
            assertContentMatches(sstable, arrived, expected);
            assertOnlyTheseKeysArePresent(arrived, keys, expected);
            assertReceivedDigestIsValid(arrived);
        }
        finally
        {
            received.close();
        }
    }

    /** The same, for an uncompressed sstable, whose grid is CRC.db's and whose CRC.db is sliced with it. */
    @Test
    public void uncompressedPartialStreamArrivesAsOneSSTable() throws Throwable
    {
        saveConfig();
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(1.0);

        createTable("CREATE TABLE %s (pk text, ck int, val text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        disableCompaction();
        insertPartitions(300, 4, 500);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        assertFalse(sstable.compression);
        List<DecoratedKey> keys = keysInOrder(sstable);

        Range<Token> range = new Range<>(keys.get(49).getToken(), keys.get(249).getToken());
        List<DecoratedKey> expected = keys.subList(50, 250);
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue(outgoing.isSliced());
        assertFalse(outgoing.slicePlan().compressed);

        Received received = streamAndReceive(cfs, sstable, outgoing);
        try
        {
            assertTrue(received.header.componentManifest.components().contains(Component.CRC));
            assertFalse(received.header.componentManifest.components().contains(Component.COMPRESSION_INFO));

            assertEquals(1, received.sstables.size());
            SSTableReader arrived = received.sstables.iterator().next();
            assertEquals(expected.get(0), arrived.first);
            assertEquals(expected.get(expected.size() - 1), arrived.last);
            assertContentMatches(sstable, arrived, expected);
            assertOnlyTheseKeysArePresent(arrived, keys, expected);
            assertReceivedDigestIsValid(arrived);
        }
        finally
        {
            received.close();
        }
    }

    /**
     * With the dead space limit at zero, a range that does not begin on a compression chunk boundary is refused
     * and the ordinary path is used -- which is what the limit is for.
     */
    @Test
    public void fallsBackToRowByRowWhenDeadSpaceIsNotAllowed() throws Throwable
    {
        saveConfig();
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.0);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));
        assertTrue("this range must start mid-chunk for the test to mean anything",
                   sections.get(0).lowerPosition % 4096 != 0);

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertFalse(outgoing.isSliced());
        assertEquals(1, outgoing.getNumFiles());
    }

    /** Turning the feature off leaves the decision exactly where it was. */
    @Test
    public void disabledMeansNoSlice() throws Throwable
    {
        saveConfig();
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(false);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(40, 4, 400);
        flush();

        SSTableReader sstable = onlySSTable(getCurrentColumnFamilyStore());
        List<DecoratedKey> keys = keysInOrder(sstable);
        Range<Token> range = new Range<>(keys.get(9).getToken(), keys.get(29).getToken());

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range,
                                                     sstable.getPositionsForRanges(Collections.singletonList(range)));
        assertFalse(outgoing.isSliced());
    }

    // ----------------------------------------------------------------------------------------------------
    // Wire harness
    // ----------------------------------------------------------------------------------------------------

    private static final class Received implements AutoCloseable
    {
        final CassandraStreamHeader header;
        final Collection<SSTableReader> sstables;

        Received(CassandraStreamHeader header, Collection<SSTableReader> sstables)
        {
            this.header = header;
            this.sstables = sstables;
        }

        @Override
        public void close()
        {
            for (SSTableReader sstable : sstables)
                sstable.selfRef().release();
        }
    }

    /**
     * Write the stream to a buffer through an embedded channel, then read it back the way an incoming stream is
     * read: deserialise the header, then hand the rest to the reader the header selects.
     */
    private Received streamAndReceive(ColumnFamilyStore cfs, SSTableReader sstable, CassandraOutgoingFile outgoing)
    throws Throwable
    {
        int version = MessagingService.current_version;
        StreamSession session = setupStreamingSessionForTest();
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();

        ByteBuf serialized = Unpooled.buffer(1 << 20);
        EmbeddedChannel channel = createMockNettyChannel(serialized);
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            outgoing.write(session, out, version);
            out.flush();
        }

        DataInputBuffer in = new DataInputBuffer(serialized.nioBuffer(), false);
        CassandraStreamHeader header = CassandraStreamHeader.serializer.deserialize(in, version);

        session.prepareReceiving(new StreamSummary(sstable.metadata().id, 1, header.size()));
        StreamMessageHeader messageHeader = new StreamMessageHeader(sstable.metadata().id, peer, session.planId(),
                                                                   false, 0, 0, 0, null);
        SSTableMultiWriter writer = new CassandraEntireSSTableStreamReader(messageHeader, header, session).read(in);
        return new Received(header, writer.finished());
    }

    private EmbeddedChannel createMockNettyChannel(ByteBuf serialized)
    {
        WritableByteChannel wbc = new WritableByteChannel()
        {
            private boolean isOpen = true;

            public int write(ByteBuffer src)
            {
                int size = src.remaining();
                serialized.writeBytes(src);
                return size;
            }

            public boolean isOpen()
            {
                return isOpen;
            }

            public void close()
            {
                isOpen = false;
            }
        };

        return new EmbeddedChannel(new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
            {
                // The header and any SSL-path component arrive as buffers, the zero-copy Data.db as a file
                // region; both have to land in the same buffer in the order they were written.
                if (msg instanceof DefaultFileRegion)
                    ((DefaultFileRegion) msg).transferTo(wbc, 0);
                else if (msg instanceof ByteBuf)
                    serialized.writeBytes((ByteBuf) msg);
                else
                    throw new AssertionError("unexpected outbound message " + msg.getClass());
                super.write(ctx, msg, promise);
            }
        });
    }

    private StreamSession setupStreamingSessionForTest()
    {
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1,
                                                                   new NettyStreamingConnectionFactory(),
                                                                   false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.BOOTSTRAP,
                                                                      Collections.<StreamEventHandler>emptyList(),
                                                                      streamCoordinator);

        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
        streamCoordinator.addSessionInfo(new SessionInfo(peer, 0, peer, Collections.emptyList(),
                                                        Collections.emptyList(), StreamSession.State.INITIALIZED));

        StreamSession session = streamCoordinator.getOrCreateOutboundSession(peer);
        session.init(future);
        return session;
    }

    /**
     * Receive the way {@link CassandraIncomingFile#read} does: dispatch on the header's {@code isEntireSSTable}
     * rather than assuming it. This is what makes a mislabeled header visible -- a slice header with a legacy body
     * (or the reverse) sends the receiver to a reader that cannot parse what it is given.
     */
    private CassandraStreamHeader streamAndReceiveDispatching(SSTableReader sstable, CassandraOutgoingFile outgoing)
    throws Throwable
    {
        int version = MessagingService.current_version;
        StreamSession session = setupStreamingSessionForTest();
        InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();

        ByteBuf serialized = Unpooled.buffer(1 << 20);
        EmbeddedChannel channel = createMockNettyChannel(serialized);
        try (AsyncStreamingOutputPlus out = new AsyncStreamingOutputPlus(channel))
        {
            outgoing.write(session, out, version);
            out.flush();
        }

        DataInputBuffer in = new DataInputBuffer(serialized.nioBuffer(), false);
        CassandraStreamHeader header = CassandraStreamHeader.serializer.deserialize(in, version);

        session.prepareReceiving(new StreamSummary(sstable.metadata().id, 1, header.size()));
        StreamMessageHeader messageHeader = new StreamMessageHeader(sstable.metadata().id, peer, session.planId(),
                                                                   false, 0, 0, 0, null);
        IStreamReader reader;
        if (header.isEntireSSTable)
            reader = new CassandraEntireSSTableStreamReader(messageHeader, header, session);
        else if (header.isCompressed())
            reader = new CassandraCompressedStreamReader(messageHeader, header, session);
        else
            reader = new CassandraStreamReader(messageHeader, header, session);

        // The legacy readers hand back an unfinished writer -- StreamReceiveTask is what commits it in production --
        // so this deliberately does not call finished(). Getting through read() at all is the assertion: it means
        // the reader the header selected could parse the body that followed it.
        SSTableMultiWriter writer = reader.read(in);
        assertNotNull(writer);
        writer.abort(null);
        return header;
    }

    /**
     * A slice that fails to synthesise falls back to partition-by-partition streaming, and the header it sends has
     * to describe THAT, not the slice it gave up on.
     * <p>
     * The receiver dispatches on {@code isEntireSSTable} alone, so a fallback that kept the slice's header would
     * send the peer to {@code CassandraEntireSSTableStreamReader}, which would chop the legacy body into component
     * files by the estimated manifest's sizes -- garbage components and a dead session, for a failure the sender
     * treats as recoverable. The failure is injected by emptying the parent's Statistics.db after the plan is
     * built (the plan checks only that the file exists), which is what a crash inside the splitter's
     * writeStatistics leaves behind; {@code ZeroCopySSTableSlice.write} reads it and throws.
     */
    @Test
    public void slicedStreamThatFailsToSynthesiseFallsBackWithAMatchingHeader() throws Throwable
    {
        saveConfig();
        DatabaseDescriptor.getRawConfig().stream_entire_sstables = true;
        DatabaseDescriptor.setZeroCopyPartialStreamEnabled(true);
        DatabaseDescriptor.setZeroCopyPartialStreamMaxDeadSpaceRatio(0.25);

        createCompressedTable(4);
        disableCompaction();
        insertPartitions(80, 4, 400);
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = onlySSTable(cfs);
        List<DecoratedKey> keys = keysInOrder(sstable);

        Range<Token> range = new Range<>(keys.get(19).getToken(), keys.get(59).getToken());
        List<DecoratedKey> expected = keys.subList(20, 60);
        List<PartitionPositionBounds> sections = sstable.getPositionsForRanges(Collections.singletonList(range));

        CassandraOutgoingFile outgoing = outgoingFile(sstable, range, sections);
        assertTrue("the sections do not cover the sstable, so this must be a slice", outgoing.isSliced());

        File stats = sstable.descriptor.fileFor(Component.STATS);
        byte[] saved = Files.readAllBytes(stats.toPath());

        CassandraStreamHeader header;
        try
        {
            Files.write(stats.toPath(), new byte[0]);
            header = streamAndReceiveDispatching(sstable, outgoing);
        }
        finally
        {
            Files.write(stats.toPath(), saved);
        }

        // These are what the bug was: the plan was eligible (asserted above), so the pre-fix code sent
        // isEntireSSTable=true with the estimated manifest and then a legacy body underneath it, and the receive
        // above would have gone to CassandraEntireSSTableStreamReader and carved that body into components.
        assertFalse("the fallback body is a legacy one, so the header must not claim an entire sstable",
                    header.isEntireSSTable);
        assertNull("a legacy header carries no component manifest", header.componentManifest);
        assertTrue("the parent is compressed, so the fallback body is a compressed-chunk one", header.isCompressed());
        assertEquals("the fallback sends the requested sections, not the slice's runs", sections, header.sections);
    }

    private static CassandraOutgoingFile outgoingFile(SSTableReader sstable, Range<Token> range,
                                                     List<PartitionPositionBounds> sections)
    {
        Ref<SSTableReader> ref = sstable.ref();
        return new CassandraOutgoingFile(StreamOperation.BOOTSTRAP, ref, sections,
                                         Collections.singletonList(range), sstable.estimatedKeys());
    }

    // ----------------------------------------------------------------------------------------------------
    // Assertions and scaffolding
    // ----------------------------------------------------------------------------------------------------

    private static void assertContentMatches(SSTableReader parent, SSTableReader arrived, List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        int compared = 0;
        try (ISSTableScanner parentScanner = parent.getScanner();
             ISSTableScanner scanner = arrived.getScanner())
        {
            while (scanner.hasNext())
            {
                assertTrue("more partitions arrived than were asked for", compared < expected.size());
                try (UnfilteredRowIterator actual = scanner.next())
                {
                    assertEquals("partition " + compared, expected.get(compared), actual.partitionKey());

                    UnfilteredRowIterator sent = null;
                    try
                    {
                        while (parentScanner.hasNext())
                        {
                            UnfilteredRowIterator candidate = parentScanner.next();
                            if (wanted.contains(candidate.partitionKey()))
                            {
                                sent = candidate;
                                break;
                            }
                            candidate.close();
                        }
                        assertNotNull("the sender ran out of partitions at " + compared, sent);
                        assertEquals(sent.partitionKey(), actual.partitionKey());
                        int i = 0;
                        while (sent.hasNext())
                        {
                            assertTrue("row " + i + " of " + actual.partitionKey() + " did not arrive", actual.hasNext());
                            assertEquals(sent.next(), actual.next());
                            i++;
                        }
                        assertFalse("extra rows arrived for " + actual.partitionKey(), actual.hasNext());
                        assertTrue(i > 0);
                    }
                    finally
                    {
                        if (sent != null)
                            sent.close();
                    }
                }
                compared++;
            }
        }
        assertEquals("partitions are missing", expected.size(), compared);
    }

    /**
     * The sender cannot digest what it sends by sendfile, so the receiver does it while writing Data.db. The
     * component has to be there and it has to be right, or `nodetool verify` on the received sstable would either
     * fall back to an extended verification or fail outright.
     */
    private static void assertReceivedDigestIsValid(SSTableReader arrived) throws IOException
    {
        assertTrue("the receiver did not write a digest for the sstable it was sent",
                   arrived.descriptor.fileFor(Component.DIGEST).exists());
        try (DataIntegrityMetadata.FileDigestValidator validator =
                 DataIntegrityMetadata.fileDigestValidator(arrived.descriptor))
        {
            validator.validate();
        }
    }

    private static void assertOnlyTheseKeysArePresent(SSTableReader arrived, List<DecoratedKey> all,
                                                     List<DecoratedKey> expected)
    {
        Set<DecoratedKey> wanted = new HashSet<>(expected);
        for (DecoratedKey key : all)
        {
            RowIndexEntry entry = arrived.getPosition(key, SSTableReader.Operator.EQ);
            if (wanted.contains(key))
                assertNotNull("the received sstable cannot find " + key, entry);
            else
                assertNull("the received sstable exposes " + key + ", which was not sent", entry);
        }
    }

    private static List<DecoratedKey> keysInOrder(SSTableReader sstable) throws IOException
    {
        List<DecoratedKey> keys = new ArrayList<>();
        try (RandomAccessReader in = RandomAccessReader.open(sstable.descriptor.fileFor(Component.PRIMARY_INDEX)))
        {
            long length = in.length();
            while (in.getFilePointer() != length)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                RowIndexEntry.Serializer.readPosition(in);
                int promotedSize = (int) in.readUnsignedVInt();
                if (promotedSize > 0)
                    in.skipBytesFully(promotedSize);
                keys.add(sstable.decorateKey(key));
            }
        }
        return keys;
    }

    private static SSTableReader onlySSTable(ColumnFamilyStore cfs)
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("expected exactly one sstable", 1, live.size());
        return live.iterator().next();
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
                execute("INSERT INTO %s (pk, ck, val) VALUES (?, ?, ?)", String.format("k%06d", p), c,
                        randomText(valueBytes));
    }

    private static String randomText(int length)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++)
            chars[i] = (char) ('!' + random.nextInt(94));
        return new String(chars);
    }

}
