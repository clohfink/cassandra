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

package com.netflix.cassandra.backups;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BackupChunkReaderTest
{
    public static final String KEYSPACE = "BackupChunkReaderTest";
    public static final String CF_COMPRESSED = "Compressed1";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_COMPRESSED)
                                                .compression(CompressionParams.zstd(256)));

        // Disable auto-compaction
        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testReadPartitionFromSingleSSTable() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 1;
        insertDataForPartition(store, partitionKey, 50, 100);
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        assertNotNull("SSTable should exist", sstable);

        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);
        assertNotNull("Index entry should exist for partition", indexEntry);

        try (SSTableContext ctx = new SSTableContext(sstable))
        {
            // Seek to partition position
            ctx.reader.seek(indexEntry.position);

            // Read partition data - read first few bytes to verify it works
            byte[] buffer = new byte[100];
            int bytesRead = ctx.reader.read(buffer, 0, 100);

            assertTrue("Should read some bytes", bytesRead > 0);
            assertEquals("Should read requested bytes", 100, bytesRead);

            // Verify we can seek and read again
            ctx.reader.seek(indexEntry.position);
            byte[] buffer2 = new byte[100];
            int bytesRead2 = ctx.reader.read(buffer2, 0, 100);

            assertEquals("Should read same number of bytes", bytesRead, bytesRead2);

            // Verify file pointer is updated correctly
            assertTrue("File pointer should advance", ctx.reader.getFilePointer() > indexEntry.position);
        }
    }

    @Test
    public void testReadAcrossMultipleChunks() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 2;
        insertDataForPartition(store, partitionKey, 100, 200);
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

        try (SSTableContext ctx = new SSTableContext(sstable))
        {
            ctx.reader.seek(indexEntry.position);

            // Read large amount of data that spans multiple chunks
            byte[] largeBuffer = new byte[5000];
            int totalBytesRead = 0;
            int maxBytes = Math.min(5000, (int)(sstable.uncompressedLength() - indexEntry.position));

            while (totalBytesRead < maxBytes)
            {
                int bytesRead = ctx.reader.read(largeBuffer, totalBytesRead, maxBytes - totalBytesRead);
                if (bytesRead == -1)
                    break;
                totalBytesRead += bytesRead;
            }

            assertTrue("Should read data across multiple chunks", totalBytesRead > 1024); // More than 1 chunk
        }
    }

    @Test
    public void testInterleavedReadFromMultipleSSTables() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create first SSTable with partition key 10
        int partitionKey1 = 10;
        insertDataForPartition(store, partitionKey1, 50, 100);
        Util.flush(store);

        // Create second SSTable with partition key 20
        int partitionKey2 = 20;
        insertDataForPartition(store, partitionKey2, 50, 100);
        Util.flush(store);

        // Don't compact - we want two separate SSTables
        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
        assertEquals("Should have 2 SSTables", 2, sstables.size());

        // Find which SSTable contains which partition
        DecoratedKey dk1 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));

        SSTableReader sstable1 = null;
        SSTableReader sstable2 = null;
        RowIndexEntry<?> indexEntry1 = null;
        RowIndexEntry<?> indexEntry2 = null;

        for (SSTableReader sstable : sstables)
        {
            RowIndexEntry<?> entry1 = sstable.getPosition(dk1, SSTableReader.Operator.EQ);
            if (entry1 != null)
            {
                sstable1 = sstable;
                indexEntry1 = entry1;
            }

            RowIndexEntry<?> entry2 = sstable.getPosition(dk2, SSTableReader.Operator.EQ);
            if (entry2 != null)
            {
                sstable2 = sstable;
                indexEntry2 = entry2;
            }
        }

        assertNotNull("Index entry should exist for partition 1", indexEntry1);
        assertNotNull("SSTable should exist for partition 1", sstable1);
        assertNotNull("Index entry should exist for partition 2", indexEntry2);
        assertNotNull("SSTable should exist for partition 2", sstable2);

        try (SSTableContext ctx1 = new SSTableContext(sstable1);
             SSTableContext ctx2 = new SSTableContext(sstable2))
        {
            // Read from first SSTable and capture data
            ctx1.reader.seek(indexEntry1.position);
            byte[] buffer1FirstRead = new byte[100];
            int bytesRead1 = ctx1.reader.read(buffer1FirstRead, 0, 100);
            assertTrue("Should read from SSTable 1", bytesRead1 > 0);
            long filePointer1After = ctx1.reader.getFilePointer();

            // Read from second SSTable
            ctx2.reader.seek(indexEntry2.position);
            byte[] buffer2 = new byte[100];
            int bytesRead2 = ctx2.reader.read(buffer2, 0, 100);
            assertTrue("Should read from SSTable 2", bytesRead2 > 0);
            long filePointer2After = ctx2.reader.getFilePointer();

            // Read from first SSTable again to test thread-local safety
            // This verifies that reading from ctx2 didn't interfere with ctx1's state
            byte[] buffer1SecondRead = new byte[100];
            int bytesRead3 = ctx1.reader.read(buffer1SecondRead, 0, 100);
            assertTrue("Should read from SSTable 1 again", bytesRead3 > 0);
            long filePointer1Final = ctx1.reader.getFilePointer();

            // Verify each reader's file pointer advanced correctly and independently
            assertTrue("First reader file pointer should have advanced from first read",
                       filePointer1After > indexEntry1.position);
            assertTrue("Second reader file pointer should have advanced",
                       filePointer2After > indexEntry2.position);
            assertTrue("First reader file pointer should have advanced from second read (thread-local safety)",
                       filePointer1Final > filePointer1After);

            // Byte-level verification: Read the same position from reader1 again and verify data integrity
            ctx1.reader.seek(indexEntry1.position);
            byte[] buffer1Verify = new byte[100];
            int bytesReadVerify = ctx1.reader.read(buffer1Verify, 0, 100);
            assertEquals("Verification read should return same number of bytes", bytesRead1, bytesReadVerify);

            // Verify the data is identical - this would catch ThreadLocal buffer corruption
            for (int i = 0; i < bytesRead1; i++)
            {
                if (buffer1FirstRead[i] != buffer1Verify[i])
                {
                    throw new AssertionError(String.format(
                        "Data corruption detected at byte %d: first read = 0x%02x, verification read = 0x%02x. " +
                        "This indicates ThreadLocal buffer was corrupted by interleaved reads.",
                        i, buffer1FirstRead[i] & 0xFF, buffer1Verify[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testRapidChunkTransitionsWithMultipleReaders() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create large partitions that span multiple compression chunks (1KB chunks)
        int partitionKey1 = 30;
        insertDataForPartition(store, partitionKey1, 100, 200); // ~20KB of data
        Util.flush(store);

        int partitionKey2 = 40;
        insertDataForPartition(store, partitionKey2, 100, 200); // ~20KB of data
        Util.flush(store);

        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
        assertEquals("Should have 2 SSTables", 2, sstables.size());

        // Find which SSTable contains which partition
        DecoratedKey dk1 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));

        SSTableReader sstable1 = null;
        SSTableReader sstable2 = null;
        RowIndexEntry<?> indexEntry1 = null;
        RowIndexEntry<?> indexEntry2 = null;

        for (SSTableReader sstable : sstables)
        {
            RowIndexEntry<?> entry1 = sstable.getPosition(dk1, SSTableReader.Operator.EQ);
            if (entry1 != null)
            {
                sstable1 = sstable;
                indexEntry1 = entry1;
            }

            RowIndexEntry<?> entry2 = sstable.getPosition(dk2, SSTableReader.Operator.EQ);
            if (entry2 != null)
            {
                sstable2 = sstable;
                indexEntry2 = entry2;
            }
        }

        assertNotNull("Index entry should exist for partition 1", indexEntry1);
        assertNotNull("SSTable should exist for partition 1", sstable1);
        assertNotNull("Index entry should exist for partition 2", indexEntry2);
        assertNotNull("SSTable should exist for partition 2", sstable2);

        try (SSTableContext ctx1 = new SSTableContext(sstable1);
             SSTableContext ctx2 = new SSTableContext(sstable2))
        {
            // Force rapid chunk transitions by reading across chunk boundaries
            // This tests if ThreadLocal buffer corruption occurs during interleaved chunk fetches

            // Read chunk 0 from reader1 (starting position)
            ctx1.reader.seek(indexEntry1.position);
            byte[] chunk0Reader1 = new byte[512];
            int read1 = ctx1.reader.read(chunk0Reader1, 0, 512);
            assertTrue("Should read from reader1 chunk 0", read1 > 0);

            // Read chunk 0 from reader2 (starting position)
            ctx2.reader.seek(indexEntry2.position);
            byte[] chunk0Reader2 = new byte[512];
            int read2 = ctx2.reader.read(chunk0Reader2, 0, 512);
            assertTrue("Should read from reader2 chunk 0", read2 > 0);

            // Force reader1 to fetch next chunk by seeking beyond current chunk
            long nextChunkPos1 = indexEntry1.position + 1500; // Jump to next chunk
            ctx1.reader.seek(nextChunkPos1);
            byte[] chunk1Reader1 = new byte[512];
            int read3 = ctx1.reader.read(chunk1Reader1, 0, 512);
            assertTrue("Should read from reader1 chunk 1", read3 > 0);

            // Force reader2 to fetch next chunk
            long nextChunkPos2 = indexEntry2.position + 1500; // Jump to next chunk
            ctx2.reader.seek(nextChunkPos2);
            byte[] chunk1Reader2 = new byte[512];
            int read4 = ctx2.reader.read(chunk1Reader2, 0, 512);
            assertTrue("Should read from reader2 chunk 1", read4 > 0);

            // Verify data integrity by re-reading the same positions
            ctx1.reader.seek(indexEntry1.position);
            byte[] verifyChunk0Reader1 = new byte[512];
            int verifyRead1 = ctx1.reader.read(verifyChunk0Reader1, 0, 512);
            assertEquals("Should read same number of bytes on verification", read1, verifyRead1);

            // Byte-level verification for chunk 0 from reader1
            for (int i = 0; i < read1; i++)
            {
                if (chunk0Reader1[i] != verifyChunk0Reader1[i])
                {
                    throw new AssertionError(String.format(
                        "Data corruption in chunk 0 at byte %d after rapid transitions: " +
                        "first read = 0x%02x, verify read = 0x%02x",
                        i, chunk0Reader1[i] & 0xFF, verifyChunk0Reader1[i] & 0xFF));
                }
            }

            // Verify chunk 1 from reader1
            ctx1.reader.seek(nextChunkPos1);
            byte[] verifyChunk1Reader1 = new byte[512];
            int verifyRead3 = ctx1.reader.read(verifyChunk1Reader1, 0, 512);
            assertEquals("Should read same number of bytes on verification", read3, verifyRead3);

            for (int i = 0; i < read3; i++)
            {
                if (chunk1Reader1[i] != verifyChunk1Reader1[i])
                {
                    throw new AssertionError(String.format(
                        "Data corruption in chunk 1 at byte %d after rapid transitions: " +
                        "first read = 0x%02x, verify read = 0x%02x",
                        i, chunk1Reader1[i] & 0xFF, verifyChunk1Reader1[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testDifferentCompressionChunkSizes() throws Exception
    {
        // Create two column families with different compression chunk sizes
        String CF_SMALL_CHUNKS = "SmallChunks";
        String CF_LARGE_CHUNKS = "LargeChunks";

        SchemaLoader.createKeyspace(KEYSPACE + "2",
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE + "2", CF_SMALL_CHUNKS)
                                                .compression(CompressionParams.lz4(512)),  // 512 byte chunks
                                    SchemaLoader.standardCFMD(KEYSPACE + "2", CF_LARGE_CHUNKS)
                                                .compression(CompressionParams.lz4(2048))); // 2KB chunks

        Keyspace ks = Keyspace.open(KEYSPACE + "2");
        ColumnFamilyStore storeSmall = ks.getColumnFamilyStore(CF_SMALL_CHUNKS);
        ColumnFamilyStore storeLarge = ks.getColumnFamilyStore(CF_LARGE_CHUNKS);

        storeSmall.truncateBlocking();
        storeLarge.truncateBlocking();

        // Create data in both stores
        int partitionKey1 = 50;
        insertDataForPartition(storeSmall, partitionKey1, 30, 100);
        Util.flush(storeSmall);

        int partitionKey2 = 60;
        insertDataForPartition(storeLarge, partitionKey2, 30, 100);
        Util.flush(storeLarge);

        SSTableReader sstableSmall = storeSmall.getLiveSSTables().iterator().next();
        SSTableReader sstableLarge = storeLarge.getLiveSSTables().iterator().next();

        DecoratedKey dk1 = storeSmall.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = storeLarge.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));

        RowIndexEntry<?> indexEntry1 = sstableSmall.getPosition(dk1, SSTableReader.Operator.EQ);
        RowIndexEntry<?> indexEntry2 = sstableLarge.getPosition(dk2, SSTableReader.Operator.EQ);

        assertNotNull("Index entry should exist for small chunk partition", indexEntry1);
        assertNotNull("Index entry should exist for large chunk partition", indexEntry2);

        try (SSTableContext ctxSmall = new SSTableContext(sstableSmall);
             SSTableContext ctxLarge = new SSTableContext(sstableLarge))
        {
            // Read from small chunk SSTable (512 bytes)
            ctxSmall.reader.seek(indexEntry1.position);
            byte[] bufferSmall1 = new byte[300];
            int readSmall1 = ctxSmall.reader.read(bufferSmall1, 0, 300);
            assertTrue("Should read from small chunk SSTable", readSmall1 > 0);

            // Read from large chunk SSTable (2048 bytes)
            // This tests if ThreadLocal buffer handles size changes correctly
            ctxLarge.reader.seek(indexEntry2.position);
            byte[] bufferLarge = new byte[1000];
            int readLarge = ctxLarge.reader.read(bufferLarge, 0, 1000);
            assertTrue("Should read from large chunk SSTable", readLarge > 0);

            // Read from small chunk SSTable again
            // If ThreadLocal buffer was corrupted by the larger chunk read, this would fail
            byte[] bufferSmall2 = new byte[300];
            int readSmall2 = ctxSmall.reader.read(bufferSmall2, 0, 300);
            assertTrue("Should read from small chunk SSTable again", readSmall2 > 0);

            // Verify data integrity by re-reading from the beginning
            ctxSmall.reader.seek(indexEntry1.position);
            byte[] verifyBufferSmall = new byte[300];
            int verifyReadSmall = ctxSmall.reader.read(verifyBufferSmall, 0, 300);
            assertEquals("Should read same number of bytes", readSmall1, verifyReadSmall);

            // Byte-level verification
            for (int i = 0; i < readSmall1; i++)
            {
                if (bufferSmall1[i] != verifyBufferSmall[i])
                {
                    throw new AssertionError(String.format(
                        "Data corruption with different chunk sizes at byte %d: " +
                        "first read = 0x%02x, verify read = 0x%02x. " +
                        "ThreadLocal buffer may have been corrupted by interleaved reads with different chunk sizes.",
                        i, bufferSmall1[i] & 0xFF, verifyBufferSmall[i] & 0xFF));
                }
            }

            // Verify large chunk data
            ctxLarge.reader.seek(indexEntry2.position);
            byte[] verifyBufferLarge = new byte[1000];
            int verifyReadLarge = ctxLarge.reader.read(verifyBufferLarge, 0, 1000);
            assertEquals("Should read same number of bytes from large chunk", readLarge, verifyReadLarge);

            for (int i = 0; i < readLarge; i++)
            {
                if (bufferLarge[i] != verifyBufferLarge[i])
                {
                    throw new AssertionError(String.format(
                        "Data corruption in large chunk at byte %d: first read = 0x%02x, verify read = 0x%02x",
                        i, bufferLarge[i] & 0xFF, verifyBufferLarge[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testReversedSizeOrdering() throws Exception
    {
        // Test large-to-small chunk size transitions (reverse of testDifferentCompressionChunkSizes)
        // This tests if ThreadLocal buffer shrinking (via limit()) works correctly
        String CF_SMALL_CHUNKS = "SmallChunks2";
        String CF_LARGE_CHUNKS = "LargeChunks2";

        SchemaLoader.createKeyspace(KEYSPACE + "3",
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE + "3", CF_SMALL_CHUNKS)
                                                .compression(CompressionParams.lz4(512)),
                                    SchemaLoader.standardCFMD(KEYSPACE + "3", CF_LARGE_CHUNKS)
                                                .compression(CompressionParams.lz4(2048)));

        Keyspace ks = Keyspace.open(KEYSPACE + "3");
        ColumnFamilyStore storeSmall = ks.getColumnFamilyStore(CF_SMALL_CHUNKS);
        ColumnFamilyStore storeLarge = ks.getColumnFamilyStore(CF_LARGE_CHUNKS);

        storeSmall.truncateBlocking();
        storeLarge.truncateBlocking();

        int partitionKey1 = 70;
        insertDataForPartition(storeSmall, partitionKey1, 30, 100);
        Util.flush(storeSmall);

        int partitionKey2 = 80;
        insertDataForPartition(storeLarge, partitionKey2, 30, 100);
        Util.flush(storeLarge);

        SSTableReader sstableSmall = storeSmall.getLiveSSTables().iterator().next();
        SSTableReader sstableLarge = storeLarge.getLiveSSTables().iterator().next();

        DecoratedKey dk1 = storeSmall.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = storeLarge.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));

        RowIndexEntry<?> indexEntry1 = sstableSmall.getPosition(dk1, SSTableReader.Operator.EQ);
        RowIndexEntry<?> indexEntry2 = sstableLarge.getPosition(dk2, SSTableReader.Operator.EQ);

        try (SSTableContext ctxSmall = new SSTableContext(sstableSmall);
             SSTableContext ctxLarge = new SSTableContext(sstableLarge))
        {
            // REVERSED: Start with LARGE chunks, then go to SMALL
            // This tests if buffer.clear().limit(smallerSize) correctly shrinks the working area
            ctxLarge.reader.seek(indexEntry2.position);
            byte[] bufferLarge = new byte[1000];
            int readLarge = ctxLarge.reader.read(bufferLarge, 0, 1000);
            assertTrue("Should read from large chunk SSTable", readLarge > 0);

            // Now read from small chunk SSTable
            // ThreadLocal buffer capacity is now 2048+ bytes, but we need only 512 bytes
            ctxSmall.reader.seek(indexEntry1.position);
            byte[] bufferSmall = new byte[300];
            int readSmall = ctxSmall.reader.read(bufferSmall, 0, 300);
            assertTrue("Should read from small chunk SSTable", readSmall > 0);

            // Read from large again
            ctxLarge.reader.seek(indexEntry2.position);
            byte[] bufferLarge2 = new byte[1000];
            int readLarge2 = ctxLarge.reader.read(bufferLarge2, 0, 1000);
            assertTrue("Should read from large chunk SSTable again", readLarge2 > 0);

            // Verify large chunk data integrity
            for (int i = 0; i < readLarge; i++)
            {
                if (bufferLarge[i] != bufferLarge2[i])
                {
                    throw new AssertionError(String.format(
                        "Data corruption in reversed ordering at byte %d: " +
                        "first read = 0x%02x, second read = 0x%02x. " +
                        "Small chunk read may have corrupted ThreadLocal buffer state.",
                        i, bufferLarge[i] & 0xFF, bufferLarge2[i] & 0xFF));
                }
            }

            // Verify small chunk data
            ctxSmall.reader.seek(indexEntry1.position);
            byte[] verifySmall = new byte[300];
            int verifyReadSmall = ctxSmall.reader.read(verifySmall, 0, 300);
            assertEquals("Should read same bytes from small chunk", readSmall, verifyReadSmall);

            for (int i = 0; i < readSmall; i++)
            {
                if (bufferSmall[i] != verifySmall[i])
                {
                    throw new AssertionError(String.format(
                        "Small chunk data corruption at byte %d: first = 0x%02x, verify = 0x%02x",
                        i, bufferSmall[i] & 0xFF, verifySmall[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testBackwardSeeksWithMultipleReaders() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create partitions with enough data to span multiple chunks
        int partitionKey1 = 90;
        insertDataForPartition(store, partitionKey1, 80, 150);
        Util.flush(store);

        int partitionKey2 = 100;
        insertDataForPartition(store, partitionKey2, 80, 150);
        Util.flush(store);

        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
        assertEquals("Should have 2 SSTables", 2, sstables.size());

        DecoratedKey dk1 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));

        SSTableReader sstable1 = null;
        SSTableReader sstable2 = null;
        RowIndexEntry<?> indexEntry1 = null;
        RowIndexEntry<?> indexEntry2 = null;

        for (SSTableReader sstable : sstables)
        {
            RowIndexEntry<?> entry1 = sstable.getPosition(dk1, SSTableReader.Operator.EQ);
            if (entry1 != null)
            {
                sstable1 = sstable;
                indexEntry1 = entry1;
            }

            RowIndexEntry<?> entry2 = sstable.getPosition(dk2, SSTableReader.Operator.EQ);
            if (entry2 != null)
            {
                sstable2 = sstable;
                indexEntry2 = entry2;
            }
        }

        assertNotNull("Index entry should exist for partition 1", indexEntry1);
        assertNotNull("Index entry should exist for partition 2", indexEntry2);

        try (SSTableContext ctx1 = new SSTableContext(sstable1);
             SSTableContext ctx2 = new SSTableContext(sstable2))
        {
            // Read forward from reader1
            ctx1.reader.seek(indexEntry1.position);
            byte[] forward1 = new byte[500];
            int readForward1 = ctx1.reader.read(forward1, 0, 500);
            assertTrue("Should read forward from reader1", readForward1 > 0);
            long positionAfterForward1 = ctx1.reader.getFilePointer();

            // Read forward from reader2
            ctx2.reader.seek(indexEntry2.position);
            byte[] forward2 = new byte[500];
            int readForward2 = ctx2.reader.read(forward2, 0, 500);
            assertTrue("Should read forward from reader2", readForward2 > 0);

            // BACKWARD SEEK: Seek back to beginning of reader1
            // This forces a chunk reload and tests if ThreadLocal buffer handles it correctly
            ctx1.reader.seek(indexEntry1.position);
            byte[] backward1 = new byte[500];
            int readBackward1 = ctx1.reader.read(backward1, 0, 500);
            assertEquals("Should read same bytes after backward seek", readForward1, readBackward1);

            // Verify backward seek didn't corrupt data
            for (int i = 0; i < readForward1; i++)
            {
                if (forward1[i] != backward1[i])
                {
                    throw new AssertionError(String.format(
                        "Backward seek corruption at byte %d: forward = 0x%02x, backward = 0x%02x. " +
                        "Position before backward: %d, position after: %d",
                        i, forward1[i] & 0xFF, backward1[i] & 0xFF,
                        positionAfterForward1, ctx1.reader.getFilePointer()));
                }
            }

            // Seek back in reader2 and verify
            ctx2.reader.seek(indexEntry2.position);
            byte[] backward2 = new byte[500];
            int readBackward2 = ctx2.reader.read(backward2, 0, 500);
            assertEquals("Should read same bytes from reader2", readForward2, readBackward2);

            for (int i = 0; i < readForward2; i++)
            {
                if (forward2[i] != backward2[i])
                {
                    throw new AssertionError(String.format(
                        "Reader2 backward seek corruption at byte %d: forward = 0x%02x, backward = 0x%02x",
                        i, forward2[i] & 0xFF, backward2[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testOverlappingReadsAtSamePosition() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create a single partition
        int partitionKey = 110;
        insertDataForPartition(store, partitionKey, 60, 150);
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);
        assertNotNull("Index entry should exist", indexEntry);

        // Create TWO readers for the SAME SSTable
        try (SSTableContext ctx1 = new SSTableContext(sstable);
             SSTableContext ctx2 = new SSTableContext(sstable))
        {
            // Both readers seek to the EXACT SAME position
            long startPos = indexEntry.position;
            ctx1.reader.seek(startPos);
            ctx2.reader.seek(startPos);

            // Read same amount from both readers
            byte[] buffer1 = new byte[400];
            byte[] buffer2 = new byte[400];

            int read1 = ctx1.reader.read(buffer1, 0, 400);
            int read2 = ctx2.reader.read(buffer2, 0, 400);

            assertEquals("Both readers should read same number of bytes", read1, read2);
            assertTrue("Should read data", read1 > 0);

            // Verify both readers got identical data
            for (int i = 0; i < read1; i++)
            {
                if (buffer1[i] != buffer2[i])
                {
                    throw new AssertionError(String.format(
                        "Overlapping read corruption at byte %d: reader1 = 0x%02x, reader2 = 0x%02x. " +
                        "ThreadLocal buffer may not isolate readers at same position correctly.",
                        i, buffer1[i] & 0xFF, buffer2[i] & 0xFF));
                }
            }

            // Now interleave small reads from both readers
            ctx1.reader.seek(startPos);
            ctx2.reader.seek(startPos);

            byte[] small1a = new byte[50];
            byte[] small2a = new byte[50];
            byte[] small1b = new byte[50];
            byte[] small2b = new byte[50];

            ctx1.reader.read(small1a, 0, 50);
            ctx2.reader.read(small2a, 0, 50);
            ctx1.reader.read(small1b, 0, 50);
            ctx2.reader.read(small2b, 0, 50);

            // Verify first 50 bytes match between readers
            for (int i = 0; i < 50; i++)
            {
                if (small1a[i] != small2a[i])
                {
                    throw new AssertionError(String.format(
                        "Interleaved small read corruption at byte %d: reader1 = 0x%02x, reader2 = 0x%02x",
                        i, small1a[i] & 0xFF, small2a[i] & 0xFF));
                }
            }

            // Verify next 50 bytes match
            for (int i = 0; i < 50; i++)
            {
                if (small1b[i] != small2b[i])
                {
                    throw new AssertionError(String.format(
                        "Second interleaved read corruption at byte %d: reader1 = 0x%02x, reader2 = 0x%02x",
                        i, small1b[i] & 0xFF, small2b[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testChunkBoundaryReads() throws Exception
    {
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create data that will span multiple 1KB chunks
        int partitionKey1 = 120;
        insertDataForPartition(store, partitionKey1, 100, 150); // ~15KB
        Util.flush(store);

        int partitionKey2 = 130;
        insertDataForPartition(store, partitionKey2, 100, 150); // ~15KB
        Util.flush(store);

        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
        assertEquals("Should have 2 SSTables", 2, sstables.size());

        DecoratedKey dk1 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));

        SSTableReader sstable1 = null;
        SSTableReader sstable2 = null;
        RowIndexEntry<?> indexEntry1 = null;
        RowIndexEntry<?> indexEntry2 = null;

        for (SSTableReader sstable : sstables)
        {
            RowIndexEntry<?> entry1 = sstable.getPosition(dk1, SSTableReader.Operator.EQ);
            if (entry1 != null)
            {
                sstable1 = sstable;
                indexEntry1 = entry1;
            }

            RowIndexEntry<?> entry2 = sstable.getPosition(dk2, SSTableReader.Operator.EQ);
            if (entry2 != null)
            {
                sstable2 = sstable;
                indexEntry2 = entry2;
            }
        }

        try (SSTableContext ctx1 = new SSTableContext(sstable1);
             SSTableContext ctx2 = new SSTableContext(sstable2))
        {
            // Read exactly to a chunk boundary (1024 bytes = 1KB chunk size)
            ctx1.reader.seek(indexEntry1.position);
            byte[] chunk1Exact = new byte[1024];
            int read1 = ctx1.reader.read(chunk1Exact, 0, 1024);

            // Interleave with reader2 at boundary
            ctx2.reader.seek(indexEntry2.position);
            byte[] chunk2Exact = new byte[1024];
            int read2 = ctx2.reader.read(chunk2Exact, 0, 1024);

            // Read next byte from reader1 (crosses chunk boundary)
            byte[] nextByte1 = new byte[1];
            int readNext1 = ctx1.reader.read(nextByte1, 0, 1);
            assertEquals("Should read 1 byte after boundary", 1, readNext1);

            // Read next byte from reader2
            byte[] nextByte2 = new byte[1];
            int readNext2 = ctx2.reader.read(nextByte2, 0, 1);
            assertEquals("Should read 1 byte after boundary from reader2", 1, readNext2);

            // Verify by re-reading the boundary + 1 byte from reader1
            ctx1.reader.seek(indexEntry1.position);
            byte[] verifyChunk1 = new byte[1024];
            int verifyRead1 = ctx1.reader.read(verifyChunk1, 0, 1024);
            assertEquals("Should read same at boundary", read1, verifyRead1);

            for (int i = 0; i < Math.min(read1, 1024); i++)
            {
                if (chunk1Exact[i] != verifyChunk1[i])
                {
                    throw new AssertionError(String.format(
                        "Chunk boundary corruption at byte %d: first = 0x%02x, verify = 0x%02x",
                        i, chunk1Exact[i] & 0xFF, verifyChunk1[i] & 0xFF));
                }
            }

            // Verify the byte after boundary
            byte[] verifyNext1 = new byte[1];
            ctx1.reader.read(verifyNext1, 0, 1);
            if (nextByte1[0] != verifyNext1[0])
            {
                throw new AssertionError(String.format(
                    "Byte after chunk boundary corrupted: first = 0x%02x, verify = 0x%02x",
                    nextByte1[0] & 0xFF, verifyNext1[0] & 0xFF));
            }
        }
    }

    @Test
    public void testSharedCacheWithSameChunk() throws Exception
    {
        // Test that multiple readers accessing the SAME chunk (same bucket/key/chunkIndex)
        // correctly share the cache and don't cause corruption
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 140;
        insertDataForPartition(store, partitionKey, 80, 150);
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

        // Create multiple readers for the SAME SSTable
        // They will share the same cache key (bucket/key/chunkIndex)
        try (SSTableContext ctx1 = new SSTableContext(sstable);
             SSTableContext ctx2 = new SSTableContext(sstable);
             SSTableContext ctx3 = new SSTableContext(sstable))
        {
            long startPos = indexEntry.position;

            // Trigger prefetch from first reader (if prefetching is enabled)
            ctx1.reader.seek(startPos);
            byte[] data1 = new byte[500];
            int read1 = ctx1.reader.read(data1, 0, 500);
            assertTrue("Should read from reader1", read1 > 0);

            // Second reader should potentially hit the cache
            ctx2.reader.seek(startPos);
            byte[] data2 = new byte[500];
            int read2 = ctx2.reader.read(data2, 0, 500);
            assertEquals("Should read same bytes", read1, read2);

            // Third reader
            ctx3.reader.seek(startPos);
            byte[] data3 = new byte[500];
            int read3 = ctx3.reader.read(data3, 0, 500);
            assertEquals("Should read same bytes", read1, read3);

            // Verify all readers got identical data (cache didn't corrupt)
            for (int i = 0; i < read1; i++)
            {
                if (data1[i] != data2[i] || data1[i] != data3[i])
                {
                    throw new AssertionError(String.format(
                        "Shared cache corruption at byte %d: reader1=0x%02x, reader2=0x%02x, reader3=0x%02x. " +
                        "Cache may not be safely shared between readers.",
                        i, data1[i] & 0xFF, data2[i] & 0xFF, data3[i] & 0xFF));
                }
            }

            // Now test reading from different offsets within the same chunk
            // All should share the cached chunk
            ctx1.reader.seek(startPos + 100);
            ctx2.reader.seek(startPos + 200);
            ctx3.reader.seek(startPos + 300);

            byte[] offset1 = new byte[100];
            byte[] offset2 = new byte[100];
            byte[] offset3 = new byte[100];

            ctx1.reader.read(offset1, 0, 100);
            ctx2.reader.read(offset2, 0, 100);
            ctx3.reader.read(offset3, 0, 100);

            // Verify by re-reading from same offsets
            ctx1.reader.seek(startPos + 100);
            byte[] verify1 = new byte[100];
            ctx1.reader.read(verify1, 0, 100);

            for (int i = 0; i < 100; i++)
            {
                if (offset1[i] != verify1[i])
                {
                    throw new AssertionError(String.format(
                        "Cache corruption at different offset, byte %d: first=0x%02x, verify=0x%02x",
                        i, offset1[i] & 0xFF, verify1[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testMultipleReadersRapidCacheAccess() throws Exception
    {
        // Test rapid interleaved cache access from multiple readers
        // This stresses the cache invalidation check at line 178
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create data spanning multiple chunks
        int partitionKey = 150;
        insertDataForPartition(store, partitionKey, 120, 150); // ~18KB
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

        try (SSTableContext ctx1 = new SSTableContext(sstable);
             SSTableContext ctx2 = new SSTableContext(sstable))
        {
            // Rapidly alternate between readers across multiple chunks
            // This tests if cache invalidation during copy (line 178) causes issues
            long basePos = indexEntry.position;

            for (int chunkOffset = 0; chunkOffset < 3; chunkOffset++)
            {
                long pos1 = basePos + (chunkOffset * 1500); // Jump across chunks
                long pos2 = basePos + (chunkOffset * 1500) + 100;

                ctx1.reader.seek(pos1);
                byte[] data1 = new byte[200];
                int read1 = ctx1.reader.read(data1, 0, 200);

                ctx2.reader.seek(pos2);
                byte[] data2 = new byte[200];
                int read2 = ctx2.reader.read(data2, 0, 200);

                // Verify data integrity by re-reading
                ctx1.reader.seek(pos1);
                byte[] verify1 = new byte[200];
                int verifyRead1 = ctx1.reader.read(verify1, 0, 200);

                assertEquals("Should read same bytes after rapid access", read1, verifyRead1);

                for (int i = 0; i < read1; i++)
                {
                    if (data1[i] != verify1[i])
                    {
                        throw new AssertionError(String.format(
                            "Rapid cache access corruption at chunk %d, byte %d: first=0x%02x, verify=0x%02x. " +
                            "Cache invalidation during copy may have caused corruption.",
                            chunkOffset, i, data1[i] & 0xFF, verify1[i] & 0xFF));
                    }
                }
            }
        }
    }

    @Test
    public void testCacheInvalidationDuringRead() throws Exception
    {
        // Test the TOCTOU scenario: cache entry is valid when we start copying,
        // but gets invalidated during the copy (line 178 check)
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 160;
        insertDataForPartition(store, partitionKey, 100, 150);
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

        try (SSTableContext ctx1 = new SSTableContext(sstable);
             SSTableContext ctx2 = new SSTableContext(sstable))
        {
            long startPos = indexEntry.position;

            // Reader 1: Read to populate cache
            ctx1.reader.seek(startPos);
            byte[] firstRead = new byte[600];
            int read1 = ctx1.reader.read(firstRead, 0, 600);
            assertTrue("Should read from first reader", read1 > 0);

            // Reader 2: Read same position (may hit cache)
            ctx2.reader.seek(startPos);
            byte[] secondRead = new byte[600];
            int read2 = ctx2.reader.read(secondRead, 0, 600);
            assertEquals("Should read same amount", read1, read2);

            // Verify data integrity even if cache was invalidated during read
            for (int i = 0; i < read1; i++)
            {
                if (firstRead[i] != secondRead[i])
                {
                    throw new AssertionError(String.format(
                        "Cache invalidation corruption at byte %d: reader1=0x%02x, reader2=0x%02x. " +
                        "TOCTOU issue: cache may have been invalidated during copy.",
                        i, firstRead[i] & 0xFF, secondRead[i] & 0xFF));
                }
            }

            // Multiple re-reads to stress the cache
            for (int attempt = 0; attempt < 5; attempt++)
            {
                ctx1.reader.seek(startPos);
                byte[] reread = new byte[600];
                int rereadBytes = ctx1.reader.read(reread, 0, 600);
                assertEquals("Should read consistent bytes", read1, rereadBytes);

                for (int i = 0; i < read1; i++)
                {
                    if (firstRead[i] != reread[i])
                    {
                        throw new AssertionError(String.format(
                            "Data inconsistency on attempt %d at byte %d: expected=0x%02x, got=0x%02x",
                            attempt, i, firstRead[i] & 0xFF, reread[i] & 0xFF));
                    }
                }
            }
        }
    }

    @Test
    public void testSequentialChunkReadsWithCaching() throws Exception
    {
        // Test that reading sequentially through multiple chunks works correctly
        // when chunks may be cached by the prefetcher
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 170;
        insertDataForPartition(store, partitionKey, 150, 150); // Large partition ~22KB
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

        try (SSTableContext ctx = new SSTableContext(sstable))
        {
            long startPos = indexEntry.position;

            // Read large amount of data sequentially (will cross multiple chunks)
            ctx.reader.seek(startPos);
            byte[] sequentialRead = new byte[8000];
            int totalRead = 0;
            int maxToRead = 8000;

            // Read in smaller chunks to exercise cache multiple times
            while (totalRead < maxToRead)
            {
                int toRead = Math.min(800, maxToRead - totalRead);
                int bytesRead = ctx.reader.read(sequentialRead, totalRead, toRead);
                if (bytesRead <= 0)
                    break;
                totalRead += bytesRead;
            }

            assertTrue("Should read substantial data", totalRead > 2048); // At least 2 chunks

            // Verify by reading again from the same position
            ctx.reader.seek(startPos);
            byte[] verifyRead = new byte[8000];
            int totalVerify = 0;

            while (totalVerify < totalRead)
            {
                int toRead = Math.min(800, totalRead - totalVerify);
                int bytesRead = ctx.reader.read(verifyRead, totalVerify, toRead);
                if (bytesRead <= 0)
                    break;
                totalVerify += bytesRead;
            }

            assertEquals("Should read same amount on verification", totalRead, totalVerify);

            // Byte-level verification across multiple chunks
            for (int i = 0; i < totalRead; i++)
            {
                if (sequentialRead[i] != verifyRead[i])
                {
                    int chunkNum = i / 1024;
                    int offsetInChunk = i % 1024;
                    throw new AssertionError(String.format(
                        "Sequential read corruption at byte %d (chunk %d, offset %d): first=0x%02x, verify=0x%02x. " +
                        "Cache may have corrupted data during sequential chunk reads.",
                        i, chunkNum, offsetInChunk, sequentialRead[i] & 0xFF, verifyRead[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testPrefetchPoolExhaustion() throws Exception
    {
        // Test behavior when prefetchPool is exhausted by multiple concurrent readers
        // The pool is static and shared, so we need to create enough readers to exhaust it
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create multiple partitions with enough data to trigger prefetching
        // Flush each one separately to create multiple SSTables
        List<Integer> partitionKeys = new ArrayList<>();
        for (int i = 180; i < 185; i++) // 5 partitions
        {
            insertDataForPartition(store, i, 100, 150); // Large enough for multiple chunks
            Util.flush(store); // Flush each partition separately
            partitionKeys.add(i);
        }

        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());

        // Create many readers simultaneously to stress the prefetchPool
        List<SSTableContext> contexts = new ArrayList<>();
        List<RowIndexEntry<?>> indexEntries = new ArrayList<>();

        try
        {
            // Find partitions and create contexts
            for (SSTableReader sstable : sstables)
            {
                for (Integer partitionKey : partitionKeys)
                {
                    DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
                    RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

                    if (indexEntry != null)
                    {
                        contexts.add(new SSTableContext(sstable));
                        indexEntries.add(indexEntry);
                        break; // Only one partition per SSTable context
                    }
                }
            }

            // Read from all readers simultaneously to potentially exhaust prefetchPool
            List<byte[]> allData = new ArrayList<>();
            for (int i = 0; i < contexts.size(); i++)
            {
                SSTableContext ctx = contexts.get(i);
                RowIndexEntry<?> indexEntry = indexEntries.get(i);

                ctx.reader.seek(indexEntry.position);
                byte[] data = new byte[2000]; // Read across multiple chunks
                int bytesRead = ctx.reader.read(data, 0, 2000);

                assertTrue("Should read data even if prefetch pool exhausted", bytesRead > 0);
                allData.add(data);
            }

            // Verify data integrity by re-reading from each reader
            // This ensures that prefetchPool exhaustion didn't corrupt data
            for (int i = 0; i < contexts.size(); i++)
            {
                SSTableContext ctx = contexts.get(i);
                RowIndexEntry<?> indexEntry = indexEntries.get(i);
                byte[] originalData = allData.get(i);

                ctx.reader.seek(indexEntry.position);
                byte[] verifyData = new byte[2000];
                int verifyRead = ctx.reader.read(verifyData, 0, 2000);

                assertTrue("Should read data on verification", verifyRead > 0);

                // Byte-level verification
                int bytesToCheck = Math.min(verifyRead, 2000);
                for (int j = 0; j < bytesToCheck; j++)
                {
                    if (originalData[j] != verifyData[j])
                    {
                        throw new AssertionError(String.format(
                            "PrefetchPool exhaustion caused corruption in reader %d at byte %d: " +
                            "first=0x%02x, verify=0x%02x. " +
                            "Readers may not handle pool exhaustion gracefully.",
                            i, j, originalData[j] & 0xFF, verifyData[j] & 0xFF));
                    }
                }
            }
        }
        finally
        {
            // Clean up all contexts
            for (SSTableContext ctx : contexts)
            {
                ctx.close();
            }
        }
    }

    @Test
    public void testInterleavedReadsWithPoolContention() throws Exception
    {
        // Test that interleaved reads work correctly when prefetchPool has contention
        // Multiple readers fighting for pool buffers
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create 3 large partitions
        int partitionKey1 = 190;
        int partitionKey2 = 191;
        int partitionKey3 = 192;

        insertDataForPartition(store, partitionKey1, 120, 150);
        Util.flush(store);
        insertDataForPartition(store, partitionKey2, 120, 150);
        Util.flush(store);
        insertDataForPartition(store, partitionKey3, 120, 150);
        Util.flush(store);

        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
        assertEquals("Should have 3 SSTables", 3, sstables.size());

        // Find index entries
        DecoratedKey dk1 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));
        DecoratedKey dk3 = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey3)));

        SSTableReader sstable1 = null, sstable2 = null, sstable3 = null;
        RowIndexEntry<?> entry1 = null, entry2 = null, entry3 = null;

        for (SSTableReader sstable : sstables)
        {
            RowIndexEntry<?> e1 = sstable.getPosition(dk1, SSTableReader.Operator.EQ);
            if (e1 != null) { sstable1 = sstable; entry1 = e1; }

            RowIndexEntry<?> e2 = sstable.getPosition(dk2, SSTableReader.Operator.EQ);
            if (e2 != null) { sstable2 = sstable; entry2 = e2; }

            RowIndexEntry<?> e3 = sstable.getPosition(dk3, SSTableReader.Operator.EQ);
            if (e3 != null) { sstable3 = sstable; entry3 = e3; }
        }

        assertNotNull("Should find entries", entry1);
        assertNotNull("Should find entries", entry2);
        assertNotNull("Should find entries", entry3);

        try (SSTableContext ctx1 = new SSTableContext(sstable1);
             SSTableContext ctx2 = new SSTableContext(sstable2);
             SSTableContext ctx3 = new SSTableContext(sstable3))
        {
            // Interleave reads from all 3 readers rapidly
            // This creates contention for prefetchPool buffers
            byte[][] data1 = new byte[5][500];
            byte[][] data2 = new byte[5][500];
            byte[][] data3 = new byte[5][500];

            for (int round = 0; round < 5; round++)
            {
                long offset = round * 1500L; // Jump to different chunks each round

                ctx1.reader.seek(entry1.position + offset);
                ctx1.reader.read(data1[round], 0, 500);

                ctx2.reader.seek(entry2.position + offset);
                ctx2.reader.read(data2[round], 0, 500);

                ctx3.reader.seek(entry3.position + offset);
                ctx3.reader.read(data3[round], 0, 500);
            }

            // Verify all data by re-reading
            for (int round = 0; round < 5; round++)
            {
                long offset = round * 1500L;

                // Verify reader1
                ctx1.reader.seek(entry1.position + offset);
                byte[] verify1 = new byte[500];
                int read1 = ctx1.reader.read(verify1, 0, 500);

                for (int i = 0; i < read1; i++)
                {
                    if (data1[round][i] != verify1[i])
                    {
                        throw new AssertionError(String.format(
                            "Pool contention corruption in reader1, round %d, byte %d: " +
                            "first=0x%02x, verify=0x%02x",
                            round, i, data1[round][i] & 0xFF, verify1[i] & 0xFF));
                    }
                }

                // Verify reader2
                ctx2.reader.seek(entry2.position + offset);
                byte[] verify2 = new byte[500];
                int read2 = ctx2.reader.read(verify2, 0, 500);

                for (int i = 0; i < read2; i++)
                {
                    if (data2[round][i] != verify2[i])
                    {
                        throw new AssertionError(String.format(
                            "Pool contention corruption in reader2, round %d, byte %d: " +
                            "first=0x%02x, verify=0x%02x",
                            round, i, data2[round][i] & 0xFF, verify2[i] & 0xFF));
                    }
                }

                // Verify reader3
                ctx3.reader.seek(entry3.position + offset);
                byte[] verify3 = new byte[500];
                int read3 = ctx3.reader.read(verify3, 0, 500);

                for (int i = 0; i < read3; i++)
                {
                    if (data3[round][i] != verify3[i])
                    {
                        throw new AssertionError(String.format(
                            "Pool contention corruption in reader3, round %d, byte %d: " +
                            "first=0x%02x, verify=0x%02x",
                            round, i, data3[round][i] & 0xFF, verify3[i] & 0xFF));
                    }
                }
            }
        }
    }

    @Test
    public void testCacheEvictionReturnsBuffersToPool() throws Exception
    {
        // Test that when cache entries are evicted, their buffers are properly returned to prefetchPool
        // This is critical because cache eviction (line 756) should call prefetchPool.offer()
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create many large partitions to trigger cache evictions
        List<Integer> partitionKeys = new ArrayList<>();
        for (int i = 200; i < 210; i++) // 10 partitions
        {
            insertDataForPartition(store, i, 100, 150);
            Util.flush(store);
            partitionKeys.add(i);
        }

        List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
        List<SSTableContext> contexts = new ArrayList<>();
        List<RowIndexEntry<?>> indexEntries = new ArrayList<>();

        try
        {
            // Create contexts for all partitions
            for (SSTableReader sstable : sstables)
            {
                for (Integer partitionKey : partitionKeys)
                {
                    DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
                    RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

                    if (indexEntry != null)
                    {
                        contexts.add(new SSTableContext(sstable));
                        indexEntries.add(indexEntry);
                        break;
                    }
                }
            }

            // First pass: Read from all to potentially cache chunks
            List<byte[][]> allData = new ArrayList<>();
            for (int i = 0; i < contexts.size(); i++)
            {
                SSTableContext ctx = contexts.get(i);
                RowIndexEntry<?> indexEntry = indexEntries.get(i);

                byte[][] readerData = new byte[3][800];
                for (int round = 0; round < 3; round++)
                {
                    ctx.reader.seek(indexEntry.position + (round * 1200L));
                    ctx.reader.read(readerData[round], 0, 800);
                }
                allData.add(readerData);
            }

            // Second pass: Read again - may trigger cache evictions which should return buffers to pool
            // If buffers aren't returned properly, pool will be exhausted
            for (int i = 0; i < contexts.size(); i++)
            {
                SSTableContext ctx = contexts.get(i);
                RowIndexEntry<?> indexEntry = indexEntries.get(i);
                byte[][] originalData = allData.get(i);

                for (int round = 0; round < 3; round++)
                {
                    ctx.reader.seek(indexEntry.position + (round * 1200L));
                    byte[] verifyData = new byte[800];
                    int bytesRead = ctx.reader.read(verifyData, 0, 800);

                    assertTrue("Should still read after cache churn", bytesRead > 0);

                    // Verify data integrity
                    for (int j = 0; j < bytesRead; j++)
                    {
                        if (originalData[round][j] != verifyData[j])
                        {
                            throw new AssertionError(String.format(
                                "Cache eviction/buffer reuse corruption in reader %d, round %d, byte %d: " +
                                "first=0x%02x, verify=0x%02x. " +
                                "Buffer may not have been returned to pool correctly or was corrupted during reuse.",
                                i, round, j, originalData[round][j] & 0xFF, verifyData[j] & 0xFF));
                        }
                    }
                }
            }
        }
        finally
        {
            for (SSTableContext ctx : contexts)
            {
                ctx.close();
            }
        }
    }

    @Test
    public void testMixedChunkSizesWithPrefetchPool() throws Exception
    {
        // Test buffer capacity checks (line 664-667) when pool has buffers of different sizes
        // If a buffer is too small, it should be returned and prefetch should fail gracefully
        String CF_SMALL = "SmallChunksPrefetch";
        String CF_LARGE = "LargeChunksPrefetch";

        SchemaLoader.createKeyspace(KEYSPACE + "4",
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE + "4", CF_SMALL)
                                                .compression(CompressionParams.lz4(512)),
                                    SchemaLoader.standardCFMD(KEYSPACE + "4", CF_LARGE)
                                                .compression(CompressionParams.lz4(2048)));

        Keyspace ks = Keyspace.open(KEYSPACE + "4");
        ColumnFamilyStore storeSmall = ks.getColumnFamilyStore(CF_SMALL);
        ColumnFamilyStore storeLarge = ks.getColumnFamilyStore(CF_LARGE);

        storeSmall.truncateBlocking();
        storeLarge.truncateBlocking();

        int partitionKey1 = 220;
        insertDataForPartition(storeSmall, partitionKey1, 50, 100);
        Util.flush(storeSmall);

        int partitionKey2 = 230;
        insertDataForPartition(storeLarge, partitionKey2, 50, 100);
        Util.flush(storeLarge);

        SSTableReader sstableSmall = storeSmall.getLiveSSTables().iterator().next();
        SSTableReader sstableLarge = storeLarge.getLiveSSTables().iterator().next();

        DecoratedKey dk1 = storeSmall.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey1)));
        DecoratedKey dk2 = storeLarge.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey2)));

        RowIndexEntry<?> entry1 = sstableSmall.getPosition(dk1, SSTableReader.Operator.EQ);
        RowIndexEntry<?> entry2 = sstableLarge.getPosition(dk2, SSTableReader.Operator.EQ);

        try (SSTableContext ctxSmall = new SSTableContext(sstableSmall);
             SSTableContext ctxLarge = new SSTableContext(sstableLarge))
        {
            // Read from small chunks first
            ctxSmall.reader.seek(entry1.position);
            byte[] dataSmall1 = new byte[400];
            int readSmall1 = ctxSmall.reader.read(dataSmall1, 0, 400);
            assertTrue("Should read from small chunk", readSmall1 > 0);

            // Read from large chunks (may have buffer size mismatch with pool)
            ctxLarge.reader.seek(entry2.position);
            byte[] dataLarge = new byte[1500];
            int readLarge = ctxLarge.reader.read(dataLarge, 0, 1500);
            assertTrue("Should read from large chunk despite potential buffer mismatch", readLarge > 0);

            // Read from small chunks again
            ctxSmall.reader.seek(entry1.position);
            byte[] dataSmall2 = new byte[400];
            int readSmall2 = ctxSmall.reader.read(dataSmall2, 0, 400);
            assertEquals("Should read same from small chunk", readSmall1, readSmall2);

            // Verify small chunk data integrity
            for (int i = 0; i < readSmall1; i++)
            {
                if (dataSmall1[i] != dataSmall2[i])
                {
                    throw new AssertionError(String.format(
                        "Mixed chunk size corruption at byte %d: first=0x%02x, second=0x%02x. " +
                        "Buffer size mismatch in prefetchPool may have caused issues.",
                        i, dataSmall1[i] & 0xFF, dataSmall2[i] & 0xFF));
                }
            }

            // Verify large chunk data
            ctxLarge.reader.seek(entry2.position);
            byte[] verifyLarge = new byte[1500];
            int verifyReadLarge = ctxLarge.reader.read(verifyLarge, 0, 1500);
            assertEquals("Should read same from large chunk", readLarge, verifyReadLarge);

            for (int i = 0; i < readLarge; i++)
            {
                if (dataLarge[i] != verifyLarge[i])
                {
                    throw new AssertionError(String.format(
                        "Large chunk corruption at byte %d: first=0x%02x, verify=0x%02x",
                        i, dataLarge[i] & 0xFF, verifyLarge[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testRepeatedCacheEvictionsAndBufferReuse() throws Exception
    {
        // Test that buffers are correctly reused after being returned via cache eviction
        // Specifically tests the cycle: poll buffer -> cache -> evict -> offer buffer -> poll again
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create enough data to cause cache churn
        int partitionKey = 240;
        insertDataForPartition(store, partitionKey, 150, 150); // Large partition
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

        try (SSTableContext ctx = new SSTableContext(sstable))
        {
            // Read through the partition multiple times to cause cache churn
            // Each pass should potentially evict previous chunks and reuse their buffers
            byte[][][] allPasses = new byte[5][][];

            for (int pass = 0; pass < 5; pass++)
            {
                byte[][] passData = new byte[10][500];
                for (int chunk = 0; chunk < 10; chunk++)
                {
                    long position = indexEntry.position + (chunk * 1000L);
                    ctx.reader.seek(position);
                    int bytesRead = ctx.reader.read(passData[chunk], 0, 500);
                    assertTrue("Should read in pass " + pass + " chunk " + chunk, bytesRead > 0);
                }
                allPasses[pass] = passData;
            }

            // Verify that all passes read the same data
            // This ensures buffer reuse didn't cause corruption
            for (int pass = 1; pass < 5; pass++)
            {
                for (int chunk = 0; chunk < 10; chunk++)
                {
                    for (int i = 0; i < 500; i++)
                    {
                        if (allPasses[0][chunk][i] != allPasses[pass][chunk][i])
                        {
                            throw new AssertionError(String.format(
                                "Buffer reuse corruption at pass %d, chunk %d, byte %d: " +
                                "first pass=0x%02x, current pass=0x%02x. " +
                                "Buffer may not have been properly cleared after being returned to pool.",
                                pass, chunk, i,
                                allPasses[0][chunk][i] & 0xFF,
                                allPasses[pass][chunk][i] & 0xFF));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testReaderCloseDoesNotAffectSharedResources() throws Exception
    {
        // Test that closing a reader doesn't corrupt shared cache or prefetchPool for other readers
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 250;
        insertDataForPartition(store, partitionKey, 100, 150);
        Util.flush(store);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

        byte[] dataFromReader1;

        // Reader 1: Read then close
        try (SSTableContext ctx1 = new SSTableContext(sstable))
        {
            ctx1.reader.seek(indexEntry.position);
            dataFromReader1 = new byte[1000];
            int read1 = ctx1.reader.read(dataFromReader1, 0, 1000);
            assertTrue("Reader 1 should read", read1 > 0);
            // Reader 1 closes here - should only clean up its own buffer (line 373-377)
        }

        // Reader 2: Should still work with shared resources after Reader 1 closed
        try (SSTableContext ctx2 = new SSTableContext(sstable))
        {
            ctx2.reader.seek(indexEntry.position);
            byte[] dataFromReader2 = new byte[1000];
            int read2 = ctx2.reader.read(dataFromReader2, 0, 1000);
            assertTrue("Reader 2 should read after Reader 1 closed", read2 > 0);

            // Verify Reader 2 got the same data (shared cache still works)
            for (int i = 0; i < Math.min(dataFromReader1.length, dataFromReader2.length); i++)
            {
                if (dataFromReader1[i] != dataFromReader2[i])
                {
                    throw new AssertionError(String.format(
                        "Reader close affected shared resources at byte %d: " +
                        "reader1=0x%02x, reader2=0x%02x. " +
                        "Closing reader1 may have corrupted shared cache or prefetchPool.",
                        i, dataFromReader1[i] & 0xFF, dataFromReader2[i] & 0xFF));
                }
            }
        }

        // Reader 3: Create and close multiple times rapidly
        for (int attempt = 0; attempt < 5; attempt++)
        {
            try (SSTableContext ctx3 = new SSTableContext(sstable))
            {
                ctx3.reader.seek(indexEntry.position);
                byte[] data = new byte[1000];
                int read = ctx3.reader.read(data, 0, 1000);
                assertTrue("Reader 3 attempt " + attempt + " should read", read > 0);

                // Verify data consistency
                for (int i = 0; i < Math.min(data.length, dataFromReader1.length); i++)
                {
                    if (dataFromReader1[i] != data[i])
                    {
                        throw new AssertionError(String.format(
                            "Repeated reader close/create corrupted data at attempt %d, byte %d: " +
                            "expected=0x%02x, got=0x%02x",
                            attempt, i, dataFromReader1[i] & 0xFF, data[i] & 0xFF));
                    }
                }
            }
        }
    }

    @Test
    public void testSSTableCompactionWithCachedChunks() throws Exception
    {
        // Test that compaction doesn't cause issues with cached chunks from old SSTables
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create multiple SSTables
        int partitionKey1 = 260;
        insertDataForPartition(store, partitionKey1, 80, 150);
        Util.flush(store);

        int partitionKey2 = 261;
        insertDataForPartition(store, partitionKey2, 80, 150);
        Util.flush(store);

        // Get initial SSTables
        List<SSTableReader> initialSSTables = new ArrayList<>(store.getLiveSSTables());
        assertEquals("Should have 2 initial SSTables", 2, initialSSTables.size());

        // Read from both SSTables to populate cache
        // Map partition key to initial data
        Map<Integer, byte[]> initialDataMap = new HashMap<>();

        for (SSTableReader sstable : initialSSTables)
        {
            for (int partKey = partitionKey1; partKey <= partitionKey2; partKey++)
            {
                DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partKey)));
                RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

                if (indexEntry != null)
                {
                    try (SSTableContext ctx = new SSTableContext(sstable))
                    {
                        ctx.reader.seek(indexEntry.position);
                        byte[] data = new byte[1000];
                        int bytesRead = ctx.reader.read(data, 0, 1000);
                        if (bytesRead > 0)
                        {
                            initialDataMap.put(partKey, data);
                        }
                    }
                }
            }
        }

        assertTrue("Should have read initial data", initialDataMap.size() > 0);

        // Compact - this will delete old SSTables
        CompactionManager.instance.performMaximal(store, false);

        // Get new compacted SSTable
        List<SSTableReader> compactedSSTables = new ArrayList<>(store.getLiveSSTables());
        assertTrue("Should have fewer SSTables after compaction",
                   compactedSSTables.size() < initialSSTables.size());

        // Read from new SSTable - cache may still have entries with old bucket/key
        SSTableReader newSStable = compactedSSTables.get(0);

        for (Integer partKey : initialDataMap.keySet())
        {
            DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partKey)));
            RowIndexEntry<?> indexEntry = newSStable.getPosition(dk, SSTableReader.Operator.EQ);

            if (indexEntry != null)
            {
                try (SSTableContext ctx = new SSTableContext(newSStable))
                {
                    ctx.reader.seek(indexEntry.position);
                    byte[] newData = new byte[1000];
                    int bytesRead = ctx.reader.read(newData, 0, 1000);
                    assertTrue("Should read from compacted SSTable", bytesRead > 0);

                    byte[] originalData = initialDataMap.get(partKey);
                    // Data should be the same after compaction
                    for (int j = 0; j < Math.min(bytesRead, originalData.length); j++)
                    {
                        if (originalData[j] != newData[j])
                        {
                            throw new AssertionError(String.format(
                                "Compaction with cached chunks caused corruption at partition %d, byte %d: " +
                                "before compaction=0x%02x, after=0x%02x. " +
                                "Cache may have stale entries from deleted SSTables.",
                                partKey, j, originalData[j] & 0xFF, newData[j] & 0xFF));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testConcurrentReadersAcrossSSTableLifecycle() throws Exception
    {
        // Test concurrent readers while SSTables are being created/deleted
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        // Create initial SSTable
        int partitionKey = 270;
        insertDataForPartition(store, partitionKey, 100, 150);
        Util.flush(store);

        SSTableReader sstable1 = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry1 = sstable1.getPosition(dk, SSTableReader.Operator.EQ);

        // Reader 1: Keep open while we modify SSTables
        try (SSTableContext ctx1 = new SSTableContext(sstable1))
        {
            ctx1.reader.seek(indexEntry1.position);
            byte[] data1 = new byte[1000];
            int read1 = ctx1.reader.read(data1, 0, 1000);
            assertTrue("Should read initial data", read1 > 0);

            // Create more SSTables (flush more data)
            insertDataForPartition(store, partitionKey + 1, 100, 150);
            Util.flush(store);
            insertDataForPartition(store, partitionKey + 2, 100, 150);
            Util.flush(store);

            // Reader 1 should still work on original SSTable
            ctx1.reader.seek(indexEntry1.position);
            byte[] data1Verify = new byte[1000];
            int read1Verify = ctx1.reader.read(data1Verify, 0, 1000);
            assertEquals("Should read same amount after new SSTables", read1, read1Verify);

            // Verify data integrity
            for (int i = 0; i < read1; i++)
            {
                if (data1[i] != data1Verify[i])
                {
                    throw new AssertionError(String.format(
                        "Concurrent SSTable creation corrupted reader at byte %d: " +
                        "first=0x%02x, verify=0x%02x. " +
                        "Shared resources may be affected by new SSTables.",
                        i, data1[i] & 0xFF, data1Verify[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testBufferPoolStateAfterMultipleSSTableCreations() throws Exception
    {
        // Test that prefetchPool remains healthy after many SSTable create/delete cycles
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        List<byte[]> allData = new ArrayList<>();

        // Create and compact multiple times
        for (int cycle = 0; cycle < 3; cycle++)
        {
            // Create SSTables
            int baseKey = 280 + (cycle * 10);
            for (int i = 0; i < 3; i++)
            {
                insertDataForPartition(store, baseKey + i, 80, 150);
                Util.flush(store);
            }

            // Read from all SSTables
            List<SSTableReader> sstables = new ArrayList<>(store.getLiveSSTables());
            for (SSTableReader sstable : sstables)
            {
                for (int i = 0; i < 3; i++)
                {
                    DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(baseKey + i)));
                    RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);

                    if (indexEntry != null)
                    {
                        try (SSTableContext ctx = new SSTableContext(sstable))
                        {
                            ctx.reader.seek(indexEntry.position);
                            byte[] data = new byte[800];
                            int bytesRead = ctx.reader.read(data, 0, 800);

                            if (bytesRead > 0)
                            {
                                allData.add(data);
                            }
                        }
                    }
                }
            }

            // Compact to delete old SSTables
            if (cycle < 2)
            {
                CompactionManager.instance.performMaximal(store, false);
            }
        }

        // Verify all data was read successfully throughout the lifecycle
        assertTrue("Should have read data from multiple cycles", allData.size() > 0);

        // Final verification: Create new reader and ensure prefetchPool still works
        int finalKey = 300;
        insertDataForPartition(store, finalKey, 100, 150);
        Util.flush(store);

        SSTableReader finalSSTable = null;
        for (SSTableReader sstable : store.getLiveSSTables())
        {
            DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(finalKey)));
            if (sstable.getPosition(dk, SSTableReader.Operator.EQ) != null)
            {
                finalSSTable = sstable;
                break;
            }
        }

        assertNotNull("Should find final SSTable", finalSSTable);

        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(finalKey)));
        RowIndexEntry<?> indexEntry = finalSSTable.getPosition(dk, SSTableReader.Operator.EQ);

        try (SSTableContext ctx = new SSTableContext(finalSSTable))
        {
            ctx.reader.seek(indexEntry.position);
            byte[] finalData = new byte[1000];
            int bytesRead = ctx.reader.read(finalData, 0, 1000);
            assertTrue("Should read after multiple SSTable lifecycle operations", bytesRead > 0);

            // Verify by re-reading
            ctx.reader.seek(indexEntry.position);
            byte[] verifyData = new byte[1000];
            int verifyRead = ctx.reader.read(verifyData, 0, 1000);
            assertEquals("Should read consistent data", bytesRead, verifyRead);

            for (int i = 0; i < bytesRead; i++)
            {
                if (finalData[i] != verifyData[i])
                {
                    throw new AssertionError(String.format(
                        "PrefetchPool corrupted after lifecycle operations at byte %d: " +
                        "first=0x%02x, verify=0x%02x. " +
                        "Buffers may not have been properly managed during SSTable lifecycle.",
                        i, finalData[i] & 0xFF, verifyData[i] & 0xFF));
                }
            }
        }
    }

    @Test
    public void testConcurrentCacheHitsInReBuffer() throws Exception
    {
        // Test concurrent access to the cachedChunk != null branch in reBuffer.
        // Populates SHARED_CHUNK_CACHE then has multiple threads read the same chunk simultaneously.
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 310;
        insertDataForPartition(store, partitionKey, 100, 200);
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);
        assertNotNull("Index entry should exist", indexEntry);

        CompressionMetadata metadata = sstable.getCompressionMetadata();
        int chunkLen = metadata.chunkLength();
        int chunkIndex = (int) (indexEntry.position / chunkLen);
        long chunkStart = (long) chunkIndex * chunkLen;

        // Read ground truth: full decompressed chunk via normal S3 fetch path
        byte[] groundTruth = new byte[chunkLen];
        int groundTruthLen;
        try (SSTableContext ctx = new SSTableContext(sstable))
        {
            ctx.reader.seek(chunkStart);
            groundTruthLen = ctx.reader.read(groundTruth, 0, chunkLen);
            assertTrue("Should read chunk data", groundTruthLen > 0);
        }

        // Populate SHARED_CHUNK_CACHE with a pre-completed future for this chunk
        ByteBuffer cachedBuffer = ByteBuffer.allocate(groundTruthLen);
        cachedBuffer.put(groundTruth, 0, groundTruthLen);
        cachedBuffer.flip();

        CachedChunk cached = new CachedChunk(cachedBuffer);
        AsyncPromise<CachedChunk> completedFuture = new AsyncPromise<>();
        completedFuture.setSuccess(cached);
        ChunkKey chunkKey = new ChunkKey("test-bucket", "test-key", chunkIndex);

        Field cacheField = BackupChunkReader.class.getDeclaredField("SHARED_CHUNK_CACHE");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Cache<ChunkKey, org.apache.cassandra.utils.concurrent.Future<CachedChunk>> sharedCache =
            (Cache<ChunkKey, org.apache.cassandra.utils.concurrent.Future<CachedChunk>>) cacheField.get(null);
        sharedCache.put(chunkKey, completedFuture);

        // Derive expected data at indexEntry.position
        int offsetInChunk = (int) (indexEntry.position % chunkLen);
        final int readSize = Math.min(200, groundTruthLen - offsetInChunk);
        byte[] expectedData = new byte[readSize];
        System.arraycopy(groundTruth, offsetInChunk, expectedData, 0, readSize);

        // Launch concurrent readers that should all hit the cache
        int numThreads = 8;
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<java.util.concurrent.Future<byte[]>> futures = new ArrayList<>();

        try
        {
            for (int t = 0; t < numThreads; t++)
            {
                futures.add(executor.submit(() -> {
                    barrier.await();
                    try (SSTableContext ctx = new SSTableContext(sstable))
                    {
                        ctx.reader.seek(indexEntry.position);
                        byte[] data = new byte[readSize];
                        ctx.reader.read(data, 0, readSize);
                        return data;
                    }
                }));
            }

            // Verify all threads got correct data
            for (java.util.concurrent.Future<byte[]> f : futures)
            {
                byte[] data = f.get(10, TimeUnit.SECONDS);
                for (int i = 0; i < readSize; i++)
                {
                    if (expectedData[i] != data[i])
                    {
                        throw new AssertionError(String.format(
                            "Concurrent cache hit corruption at byte %d: expected=0x%02x, got=0x%02x. " +
                            "SHARED_CHUNK_CACHE may not be safe for concurrent reads.",
                            i, expectedData[i] & 0xFF, data[i] & 0xFF));
                    }
                }
            }
        }
        finally
        {
            // Clean up: remove test entry from shared cache to avoid polluting other tests
            // (all SSTableContexts share "test-bucket"/"test-key" so cache keys can collide)
            sharedCache.invalidate(chunkKey);
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testCacheInvalidationFallbackToS3() throws Exception
    {
        // Test that when a cached chunk is pre-invalidated, readers detect it and
        // fall back to direct S3 fetch, still returning correct data.
        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_COMPRESSED);
        store.truncateBlocking();

        int partitionKey = 320;
        insertDataForPartition(store, partitionKey, 100, 200);
        Util.flush(store);
        CompactionManager.instance.performMaximal(store, false);

        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        DecoratedKey dk = store.decorateKey(ByteBufferUtil.bytes(String.valueOf(partitionKey)));
        RowIndexEntry<?> indexEntry = sstable.getPosition(dk, SSTableReader.Operator.EQ);
        assertNotNull("Index entry should exist", indexEntry);

        CompressionMetadata metadata = sstable.getCompressionMetadata();
        int chunkLen = metadata.chunkLength();
        int chunkIndex = (int) (indexEntry.position / chunkLen);
        long chunkStart = (long) chunkIndex * chunkLen;

        // Read ground truth
        byte[] groundTruth = new byte[chunkLen];
        int groundTruthLen;
        try (SSTableContext ctx = new SSTableContext(sstable))
        {
            ctx.reader.seek(chunkStart);
            groundTruthLen = ctx.reader.read(groundTruth, 0, chunkLen);
        }

        // Populate cache with a chunk that is already invalidated
        ByteBuffer cachedBuffer = ByteBuffer.allocate(groundTruthLen);
        cachedBuffer.put(groundTruth, 0, groundTruthLen);
        cachedBuffer.flip();

        CachedChunk cached = new CachedChunk(cachedBuffer);
        cached.invalidate(); // Pre-invalidate: reader must detect and fall back to S3

        AsyncPromise<CachedChunk> completedFuture = new AsyncPromise<>();
        completedFuture.setSuccess(cached);
        ChunkKey chunkKey = new ChunkKey("test-bucket", "test-key", chunkIndex);

        Field cacheField = BackupChunkReader.class.getDeclaredField("SHARED_CHUNK_CACHE");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Cache<ChunkKey, org.apache.cassandra.utils.concurrent.Future<CachedChunk>> sharedCache =
            (Cache<ChunkKey, org.apache.cassandra.utils.concurrent.Future<CachedChunk>>) cacheField.get(null);
        sharedCache.put(chunkKey, completedFuture);

        int offsetInChunk = (int) (indexEntry.position % chunkLen);
        int readSize = Math.min(200, groundTruthLen - offsetInChunk);
        byte[] expectedData = new byte[readSize];
        System.arraycopy(groundTruth, offsetInChunk, expectedData, 0, readSize);

        // Read should hit cache, detect invalidation, fall back to S3 fetch
        try (SSTableContext ctx = new SSTableContext(sstable))
        {
            ctx.reader.seek(indexEntry.position);
            byte[] data = new byte[readSize];
            ctx.reader.read(data, 0, readSize);

            for (int i = 0; i < readSize; i++)
            {
                if (expectedData[i] != data[i])
                {
                    throw new AssertionError(String.format(
                        "Invalidated cache fallback produced wrong data at byte %d: expected=0x%02x, got=0x%02x. " +
                        "Fallback to S3 fetch after cache invalidation may not work correctly.",
                        i, expectedData[i] & 0xFF, data[i] & 0xFF));
                }
            }
        }
        finally
        {
            sharedCache.invalidate(chunkKey);
        }
    }

    /**
     * Helper method to insert data for a partition
     */
    private void insertDataForPartition(ColumnFamilyStore store, int partitionKey, int numRows, int rowSizeBytes)
    {
        for (int i = 0; i < numRows; i++)
        {
            new RowUpdateBuilder(store.metadata(), i, String.valueOf(partitionKey))
                .clustering(String.valueOf(i))
                .add("val", ByteBufferUtil.bytes(generateString(rowSizeBytes)))
                .build()
                .applyUnsafe();
        }
    }

    /**
     * Generate a string of specified length for testing
     */
    private String generateString(int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }

    /**
     * Context holder for SSTable reader resources that implements AutoCloseable
     */
    private static class SSTableContext implements AutoCloseable
    {
        final BackupChunkReader reader;
        final CompressionMetadata compressionMetadata;

        SSTableContext(SSTableReader sstable)
        {
            File dataFile = sstable.descriptor.fileFor(org.apache.cassandra.io.sstable.Component.DATA);
            MockObjectStoreAccess mockS3 = new MockObjectStoreAccess(dataFile);

            this.compressionMetadata = sstable.compression ? sstable.getCompressionMetadata() : null;
            assertNotNull("Compression metadata should exist", compressionMetadata);

            this.reader = new BackupChunkReader(
                mockS3,
                new EmptyRebufferer(),
                compressionMetadata,
                sstable.uncompressedLength(),
                "test-bucket",
                "test-key"
            );
        }

        @Override
        public void close()
        {
            if (reader != null)
                reader.close();
        }
    }

    /**
     * Empty Rebufferer implementation for testing
     */
    private static class EmptyRebufferer implements Rebufferer
    {
        @Override
        public BufferHolder rebuffer(long position)
        {
            return EMPTY;
        }

        @Override
        public void closeReader()
        {
            // Nothing to close
        }

        @Override
        public ChannelProxy channel()
        {
            throw new UnsupportedOperationException("No channel available");
        }

        @Override
        public long fileLength()
        {
            return 0;
        }

        @Override
        public double getCrcCheckChance()
        {
            return 0;
        }

        @Override
        public void close()
        {
            // Nothing to close
        }
    }

    /**
     * Mock ObjectStoreAccess that reads from a local file instead of S3
     */
    private static class MockObjectStoreAccess implements ObjectStoreAccess
    {
        private final File dataFile;
        private int fetchCount = 0;

        MockObjectStoreAccess(File dataFile)
        {
            this.dataFile = dataFile;
        }

        @Override
        public AsyncPromise<Void> getObjectAsFile(String bucket, String key, Path path)
        {
            AsyncPromise<Void> promise = new AsyncPromise<>();
            promise.setFailure(new UnsupportedOperationException("Not implemented"));
            return promise;
        }

        @Override
        public AsyncPromise<byte[]> getObjectAsBytes(String bucket, String key)
        {
            AsyncPromise<byte[]> promise = new AsyncPromise<>();
            promise.setFailure(new UnsupportedOperationException("Not implemented"));
            return promise;
        }

        @Override
        public AsyncPromise<Void> getObjectRangeIntoBuffer(String bucket, String key, long start, long end, ByteBuffer dest)
        {
            fetchCount++;
            AsyncPromise<Void> promise = new AsyncPromise<>();
            try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(dataFile.toJavaIOFile(), "r"))
            {
                raf.seek(start);
                long length = end - start + 1;

                // Ensure we don't read more than the buffer can hold
                int maxRead = dest.remaining();
                int toRead = (int) Math.min(length, maxRead);

                // Read data from file
                byte[] bytes = new byte[toRead];
                int bytesRead = raf.read(bytes);

                if (bytesRead <= 0)
                {
                    promise.setFailure(new IOException("No data read from file"));
                    return promise;
                }

                // Write to buffer
                if (dest.hasArray())
                {
                    // Direct array access for heap buffers
                    System.arraycopy(bytes, 0, dest.array(), dest.arrayOffset() + dest.position(), bytesRead);
                    dest.position(dest.position() + bytesRead);
                }
                else
                {
                    // Use put for direct buffers
                    dest.put(bytes, 0, bytesRead);
                }
                promise.setSuccess(null);
            }
            catch (IOException e)
            {
                e.printStackTrace();
                promise.setFailure(e);
            }
            return promise;
        }

        @Override
        public AsyncPromise<List<String>> getObjectKeys(String bucket, String prefix)
        {
            AsyncPromise<List<String>> promise = new AsyncPromise<>();
            promise.setSuccess(new ArrayList<>());
            return promise;
        }

        @Override
        public AsyncPromise<Long> getObjectSize(String bucket, String key)
        {
            AsyncPromise<Long> promise = new AsyncPromise<>();
            promise.setSuccess(0L);
            return promise;
        }
    }
}