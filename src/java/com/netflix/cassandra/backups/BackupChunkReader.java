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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.netflix.cassandra.metrics.ColdTierMetrics;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.ThreadLocalByteBufferHolder;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.vint.VIntCoding;

public class BackupChunkReader extends RandomAccessReader
{
    private static final Logger logger = LoggerFactory.getLogger(BackupChunkReader.class);
    private final ThreadLocalByteBufferHolder bufferHolder;
    private final String bucket;
    private final String key;
    private final CompressionMetadata compressionMetadata;
    private final int chunkLen;
    private final ObjectStoreAccess s3AsyncClient;
    private long srcPos = 0;
    private int currentChunk = -1;
    private final long fileLength;
    private long markedPointer = 0;

    // Context for tracking per-read metrics across threads
    private volatile ReadContext readContext;

    // Static shared Caffeine cache for chunk prefetching
    private static volatile Cache<ChunkKey, Future<CachedChunk>> SHARED_CHUNK_CACHE;
    private static final java.util.concurrent.ArrayBlockingQueue<ByteBuffer> prefetchPool;

    static
    {
        int cacheSize = DatabaseDescriptor.getObjectStoreSharedChunkCacheCount();
        prefetchPool = new java.util.concurrent.ArrayBlockingQueue<>(cacheSize);
        if (DatabaseDescriptor.getObjectStorePrefetchPartitionEnabled())
        {
            for (int i = 0; i < cacheSize; i++)
            {
                prefetchPool.offer(ByteBuffer.allocate(DatabaseDescriptor.getObjectStoreSharedChunkCacheSize()));
            }
        }
        updateSharedChunkCacheSize(cacheSize);
    }

    public BackupChunkReader(ObjectStoreAccess access,
                             Rebufferer rebufferer,
                             CompressionMetadata metadata,
                             long fileLength,
                             String bucket,
                             String key)
    {
        super(rebufferer);
        Preconditions.checkNotNull(metadata);
        this.bucket = bucket;
        this.fileLength = fileLength;
        this.key = key;
        this.compressionMetadata = metadata;
        this.s3AsyncClient = access;
        this.chunkLen = metadata.chunkLength();
        this.bufferHolder = new ThreadLocalByteBufferHolder(BufferType.ON_HEAP);
        this.buffer = ByteBuffer.allocate(chunkLen + 4);
    }


    /**
     * Set the read context for tracking per-read metrics.
     * This should be called before using this reader for a read operation.
     *
     * @param context the ReadContext to use for tracking metrics
     */
    public void setReadContext(ReadContext context)
    {
        this.readContext = context;
    }

    /**
     * Clear the read context after a read operation completes.
     */
    public void clearReadContext()
    {
        this.readContext = null;
    }

    private void checkAndRebuffer()
    {
        int chunkIndex = (int) (srcPos / chunkLen);
        if (chunkIndex != currentChunk)
        {
            reBuffer();
        }
    }

    /**
     * Helper method to create a ChunkKey for the given chunk index.
     */
    private ChunkKey createChunkKey(long chunkIndex)
    {
        return new ChunkKey(bucket, key, chunkIndex);
    }

    /**
     * Fetch and decompress a single chunk from S3.
     *
     * @param target      The compression metadata chunk with offset and length
     * @param destination @param destination The destination buffer for the uncompressed data (must be heap-backed)
     * @throws InterruptedException if fetch is interrupted
     * @throws ExecutionException   if fetch fails
     * @throws IOException          if decompression fails
     */
    private void fetchAndDecompressChunk(CompressionMetadata.Chunk target, ByteBuffer destination)
    throws InterruptedException, ExecutionException, IOException, TimeoutException
    {
        ByteBuffer compressed = bufferHolder.getBuffer(target.length);
        s3AsyncClient.getObjectRangeIntoBuffer(bucket, key, target.offset, target.offset + target.length - 1, compressed)
                     .get(DatabaseDescriptor.getObjectStoreFetchTimeoutMs(), TimeUnit.MILLISECONDS);
        compressed.flip();

        // Use byte[] decompression API - works with all compressors without direct buffer requirements (zstd)
        // Both buffers must be heap-backed with accessible arrays
        byte[] compressedBytes = compressed.array();
        int compressedOffset = compressed.arrayOffset() + compressed.position();
        int compressedLength = compressed.remaining();

        byte[] destBytes = destination.array();
        int destOffset = destination.arrayOffset() + destination.position();

        // Decompress directly into destination's backing array
        int decompressedSize = compressionMetadata.compressor()
                                                  .uncompress(compressedBytes, compressedOffset, compressedLength,
                                                              destBytes, destOffset);

        // Update destination buffer state
        destination.position(0);
        destination.limit(decompressedSize);

        // Record S3 bytes fetched
        if (readContext != null)
        {
            readContext.recordBytesFetched(target.length);
        }
    }

    @Override
    public void reBuffer()
    {
        int chunkIndex = (int) (srcPos / chunkLen);
        if (chunkIndex == currentChunk)
        {
            buffer.position((int) (srcPos % chunkLen));
            return;
        }

        // Check if we have this chunk in cache from prefetching
        ChunkKey chunkKey = createChunkKey(chunkIndex);
        Future<CachedChunk> cachedChunkFuture = SHARED_CHUNK_CACHE.getIfPresent(chunkKey);
        if (cachedChunkFuture != null)
        {
            try
            {
                CachedChunk cachedChunk = cachedChunkFuture.get(DatabaseDescriptor.getObjectStoreChunkCacheTimeoutMs(), TimeUnit.MILLISECONDS);
                if (cachedChunk != null)
                {
                    Tracing.trace("Found chunk {} in cache", chunkIndex);
                    buffer.clear();
                    // Copy cached chunk contents using System.arraycopy (safe, with bounds checking)
                    ByteBuffer chunkBuffer = cachedChunk.getBuffer();

                    // Verify both buffers have backing arrays (should always be true for heap buffers)
                    if (!chunkBuffer.hasArray() || !buffer.hasArray())
                    {
                        logger.error("Buffer without backing array found - chunkBuffer.hasArray()={}, buffer.hasArray()={}, falling back to direct fetch",
                                     chunkBuffer.hasArray(), buffer.hasArray());
                        // Fall through to normal fetching
                    }
                    else
                    {
                        int length = chunkBuffer.limit();

                        // Validate and cap length to prevent buffer overflow
                        if (length > buffer.capacity())
                            length = buffer.capacity();

                        // Use System.arraycopy for safe copying with automatic bounds checking
                        System.arraycopy(chunkBuffer.array(), chunkBuffer.arrayOffset(),
                                         buffer.array(), buffer.arrayOffset(),
                                         length);
                        buffer.position(0);
                        buffer.limit(length);

                        // Check if the cached chunk is still valid after the copy
                        // If it was invalidated during the copy, the data might be corrupted
                        if (!cachedChunk.isValid())
                        {
                            Tracing.trace("Chunk {} was invalidated during copy, falling back to direct fetch", chunkIndex);
                            // Fall through to normal fetching
                        }
                        else
                        {
                            // Record metrics for cache hit
                            if (readContext != null)
                                readContext.recordCacheHit();

                            buffer.position((int) (srcPos % chunkLen));
                            currentChunk = chunkIndex;
                            return;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                // Fall through to normal fetching if cache retrieval fails
            }
        }

        try
        {
            buffer.clear();
            CompressionMetadata.Chunk target = compressionMetadata.chunkFor(srcPos);
            Tracing.trace("Fetching {} bytes of chunk {} at {}", target.length, chunkIndex, target.offset);
            fetchAndDecompressChunk(target, buffer);

            // Record metrics for chunk read from S3
            if (readContext != null)
            {
                readContext.recordChunkRead();
            }

            // position buffer at the right offset
            buffer.position((int) (srcPos % chunkLen));
        }
        catch (InterruptedException | ExecutionException | IOException | TimeoutException e)
        {
            throw new RuntimeException("Failed to reBuffer chunk from S3", e);
        }
        currentChunk = chunkIndex;
    }

    @Override
    public void reBufferAt(long position)
    {
        srcPos = position;
        reBuffer();
    }

    @Override
    public ChannelProxy getChannel()
    {
        throw new UnsupportedOperationException("S3 reader has no FileChannel");
    }

    @Override
    public long bytesPastMark()
    {
        long bytes = srcPos - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    @Override
    public DataPosition mark()
    {
        markedPointer = srcPos;
        return new BackupChunkReaderMark(markedPointer);
    }

    @Override
    public void reset(DataPosition mark)
    {
        assert mark instanceof BackupChunkReaderMark;
        seek(((BackupChunkReaderMark) mark).pointer);
    }

    @Override
    public long bytesPastMark(DataPosition mark)
    {
        assert mark instanceof BackupChunkReaderMark;
        long bytes = srcPos - ((BackupChunkReaderMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    @Override
    public boolean isEOF()
    {
        return getFilePointer() >= fileLength;
    }

    @Override
    public long bytesRemaining()
    {
        return fileLength - getFilePointer();
    }

    @Override
    public int available() throws IOException
    {
        throw new UnsupportedOperationException("available not supported for S3 reader");
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        if (n <= 0)
            return 0;
        seek(srcPos + n);
        return n;
    }

    @Override
    public long getPosition()
    {
        return getFilePointer();
    }

    @Override
    public double getCrcCheckChance()
    {
        return 0.0; // S3 payloads not CRC-checked here
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        reBufferAt(srcPos);
        // avoid int overflow
        if (off < 0 || off > b.length || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return 0;

        int copied = 0;
        while (copied < len)
        {
            int remaining = buffer.remaining();
            if (remaining == 0)
            {
                reBuffer();
                remaining = buffer.remaining();
                if (remaining == 0)
                    return copied == 0 ? -1 : copied;
            }
            int toCopy = Math.min(len - copied, remaining);
            buffer.get(b, off + copied, toCopy);
            copied += toCopy;
            srcPos += toCopy;
            checkAndRebuffer();
        }

        return copied;
    }

    @Override
    public int read() throws IOException
    {
        reBufferAt(srcPos);
        if (buffer.remaining() == 0)
        {
            reBuffer();
            if (buffer.remaining() == 0)
                return -1;
        }
        int value = buffer.get() & 0xFF;
        srcPos++;
        checkAndRebuffer();
        return value;
    }

    @Override
    public void seek(long newPosition)
    {
        srcPos = newPosition;
        reBuffer();
    }

    @Override
    public long getFilePointer()
    {
        return srcPos;
    }

    @Override
    public long length()
    {
        return fileLength;
    }

    @Override
    public String getPath()
    {
        return "s3://" + bucket + '/' + key;
    }

    @Override
    public void close()
    {
        if (buffer != null)
        {
            buffer = null;
        }
    }

    /**
     * Equivalent to {@link #read(byte[], int, int)}, where offset is {@code dst.position()} and length is {@code dst.remaining()}
     */
    public void readFully(ByteBuffer dst) throws IOException
    {
        int offset = dst.position();
        int len = dst.limit() - offset;

        reBufferAt(srcPos);
        int copied = 0;
        while (copied < len)
        {
            int position = buffer.position();
            int remaining = buffer.remaining();

            if (remaining == 0)
            {
                reBuffer();

                position = buffer.position();
                remaining = buffer.remaining();

                if (remaining == 0)
                    throw new EOFException("EOF after " + copied + " bytes out of " + len);
            }

            int toCopy = Math.min(len - copied, remaining);

            // Safe copy with automatic bounds checking
            // Works for both heap and direct destination buffers
            if (dst.hasArray() && buffer.hasArray())
            {
                // Both heap-backed: use safe System.arraycopy
                System.arraycopy(buffer.array(), buffer.arrayOffset() + position,
                                 dst.array(), dst.arrayOffset() + offset + copied,
                                 toCopy);
            }
            else
            {
                // Direct buffer or mixed: use ByteBuffer.put() with bounds checking
                ByteBuffer srcSlice = buffer.duplicate();
                srcSlice.position(position);
                srcSlice.limit(position + toCopy);

                ByteBuffer dstSlice = dst.duplicate();
                dstSlice.position(offset + copied);
                dstSlice.put(srcSlice);
            }

            buffer.position(position + toCopy);
            srcPos += toCopy;
            copied += toCopy;
            checkAndRebuffer();
        }
    }

    @Override
    public byte readByte() throws IOException
    {
        reBufferAt(srcPos);
        if (!buffer.hasRemaining())
        {
            reBuffer();
            if (!buffer.hasRemaining())
                throw new EOFException();
        }
        byte value = buffer.get();
        srcPos++;
        checkAndRebuffer();
        return value;
    }

    @Override
    public short readShort() throws IOException
    {
        if (buffer.remaining() >= 2)
        {
            short value = buffer.getShort();
            srcPos += 2;
            checkAndRebuffer();
            return value;
        }
        else
            return (short) readPrimitiveSlowly(2);
    }

    @Override
    public char readChar() throws IOException
    {
        reBufferAt(srcPos);
        if (buffer.remaining() >= 2)
        {
            char value = buffer.getChar();
            srcPos += 2;
            checkAndRebuffer();
            return value;
        }
        else
            return (char) readPrimitiveSlowly(2);
    }

    @Override
    public int readInt() throws IOException
    {
        if (buffer.remaining() >= 4)
        {
            int value = buffer.getInt();
            srcPos += 4;
            checkAndRebuffer();
            return value;
        }
        else
            return (int) readPrimitiveSlowly(4);
    }

    @Override
    public long readLong() throws IOException
    {
        if (buffer.remaining() >= 8)
        {
            long value = buffer.getLong();
            srcPos += 8;
            checkAndRebuffer();
            return value;
        }
        else
            return readPrimitiveSlowly(8);
    }

    @Override
    public float readFloat() throws IOException
    {
        if (buffer.remaining() >= 4)
        {
            float value = buffer.getFloat();
            srcPos += 4;
            checkAndRebuffer();
            return value;
        }
        else
            return Float.intBitsToFloat((int) readPrimitiveSlowly(4));
    }

    @Override
    public double readDouble() throws IOException
    {
        if (buffer.remaining() >= 8)
        {
            double value = buffer.getDouble();
            srcPos += 8;
            checkAndRebuffer();
            return value;
        }
        else
            return Double.longBitsToDouble(readPrimitiveSlowly(8));
    }

    public long readUnsignedVInt() throws IOException
    {
        //If 9 bytes aren't available use the slow path in VIntCoding
        if (buffer.remaining() < 9)
            return VIntCoding.readUnsignedVInt(this);

        byte firstByte = buffer.get();
        srcPos++;

        //Bail out early if this is one byte, necessary, or it fails later
        if (firstByte >= 0)
            return firstByte;

        int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);

        int position = buffer.position();
        int extraBits = extraBytes * 8;

        long retval = buffer.getLong(position);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            retval = Long.reverseBytes(retval);
        buffer.position(position + extraBytes);
        srcPos += extraBytes;

        // truncate the bytes we read in excess of those we needed
        retval >>>= 64 - extraBits;
        // remove the non-value bits from the first byte
        firstByte &= (byte) VIntCoding.firstByteValueMask(extraBytes);
        // shift the first byte up to its correct position
        retval |= (long) firstByte << extraBits;

        checkAndRebuffer();
        return retval;
    }

    /**
     * Prefetch all chunks needed for a partition based on RowIndexEntry.
     * This method calculates the partition end using IndexInfo and initiates parallel chunk fetching.
     *
     * @param rie The RowIndexEntry containing partition position and index information
     */
    public void prefetchPartition(RowIndexEntry<?> rie)
    {
        long startPosition = rie.position;
        long endPosition = calculatePartitionEnd(rie);
        prefetchPartition(startPosition, endPosition);
    }

    /**
     * Prefetch all chunks needed for a partition range to reduce S3 round trips.
     * This method calculates the chunk range and initiates parallel chunk fetching.
     *
     * @param startPosition Start position of the partition in the file
     * @param endPosition   End position of the partition in the file
     */
    public void prefetchPartition(long startPosition, long endPosition)
    {
        if (!DatabaseDescriptor.getObjectStorePrefetchPartitionEnabled())
        {
            return;
        }

        long startChunk = startPosition / chunkLen;
        long endChunk = endPosition / chunkLen;

        // Cap endChunk to ensure we never try to read past the last chunk
        long maxChunk = (fileLength - 1) / chunkLen;
        if (endChunk > maxChunk)
        {
            endChunk = maxChunk;
        }

        long numChunks = endChunk - startChunk + 1;
        ColdTierMetrics.chunksPerRead.update(numChunks);

        // Don't prefetch if it's just a single chunk
        if (numChunks <= 1)
        {
            return;
        }

        // Record prefetch initiation
        if (readContext != null)
        {
            readContext.recordPrefetchInitiated();
        }

        try
        {
            Tracing.trace("Starting prefetches of chunks {}-{}", startChunk, endChunk);
            for (long chunkIndex = startChunk; chunkIndex <= endChunk; chunkIndex++)
            {
                final long finalChunkIndex = chunkIndex;
                ChunkKey chunkKey = createChunkKey(chunkIndex);

                // Create a new promise that we might insert into the cache
                AsyncPromise<CachedChunk> newPromise = new AsyncPromise<>();

                // Atomically get or create the future for this chunk
                // Use get() with a pure compute function (no side effects)
                Future<CachedChunk> existing = SHARED_CHUNK_CACHE.get(chunkKey, key -> newPromise);

                // Only start the async fetch if we created a new promise (identity check)
                if (existing == newPromise)
                {
                    Stage.NETFLIX.execute(() -> {
                        try
                        {
                            ByteBuffer buffer = preFetchChunkWithBufferPool(finalChunkIndex);
                            if (buffer != null)
                            {
                                CachedChunk cachedChunk = new CachedChunk(buffer);
                                newPromise.setSuccess(cachedChunk);
                                Tracing.trace("Prefetch complete chunk {}", finalChunkIndex);
                            }
                            else
                            {
                                newPromise.setSuccess(null);
                                SHARED_CHUNK_CACHE.invalidate(chunkKey);
                            }
                        }
                        catch (Exception e)
                        {
                            newPromise.setFailure(e);
                            // Remove failed entry from cache
                            SHARED_CHUNK_CACHE.invalidate(chunkKey);
                        }
                    });
                }
            }
        }
        catch (Exception e)
        {
            // Log error but don't fail - we can fall back to single chunk fetching
            logger.error("Prefetch failed", e);
        }
    }

    /**
     * Fetch a single chunk and return a ByteBuffer.
     */
    private ByteBuffer preFetchChunkWithBufferPool(long chunkIndex)
    {
        ByteBuffer chunkBuffer = null;
        try
        {
            long chunkPosition = chunkIndex * chunkLen;
            CompressionMetadata.Chunk target = compressionMetadata.chunkFor(chunkPosition);
            chunkBuffer = prefetchPool.poll();
            if (chunkBuffer == null) return null;
            if (chunkBuffer.capacity() < chunkLen)
            {
                chunkBuffer.position(0).limit(0);
                prefetchPool.offer(chunkBuffer);
                return null;
            }
            if (chunkBuffer.limit() > chunkLen)
                chunkBuffer.limit(chunkLen);
            chunkBuffer.clear();
            fetchAndDecompressChunk(target, chunkBuffer);
            return chunkBuffer;
        }
        catch (Exception e)
        {
            if (chunkBuffer != null) {
                chunkBuffer.position(0).limit(0);
                prefetchPool.offer(chunkBuffer);
            }
            logger.error("Failed to fetch chunk {}", chunkIndex, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculate the end position of a partition using RowIndexEntry information.
     *
     * @param rie The RowIndexEntry for the partition
     * @return The estimated end position of the partition
     */
    private long calculatePartitionEnd(RowIndexEntry<?> rie)
    {
        long startPosition = rie.position;

        // If the partition is not indexed (small partition), use a default estimate
        if (!rie.isIndexed())
        {
            // For non-indexed partitions, assume all in one chunk
            return startPosition + 1;
        }
        // Try to get the last IndexInfo entry to calculate accurate end
        int indexCount = rie.columnsIndexCount();
        if (indexCount > 0)
        {
            // Use a rough estimate based on the number of index entries
            // Each IndexInfo typically covers ~64KB of data
            long estimatedSize = (long) indexCount * DatabaseDescriptor.getColumnIndexSize();
            return startPosition + estimatedSize;
        }

        // Fallback: single chunk estimate for indexed partitions
        return startPosition + 2;
    }

    @Override
    public String toString()
    {
        int chunkIndex = (int) (srcPos / chunkLen);
        int offsetInChunk = (int) (srcPos % chunkLen);
        return String.format("BackupChunkReader{srcPos=%d, chunk=%d, offsetInChunk=%d, fileLength=%d, bucket='%s', key='%s'}",
                             srcPos, chunkIndex, offsetInChunk, fileLength, bucket, key);
    }

    /**
     * Update the shared chunk cache size and recreate the cache with the new size.
     * This method is called when the cache size configuration is changed.
     *
     * @param newSize the new maximum cache size in number of entries
     */
    public static void updateSharedChunkCacheSize(int newSize)
    {
        if (newSize < 1)
            throw new IllegalArgumentException("Cache size must be positive");
        Cache<ChunkKey, Future<CachedChunk>> old = SHARED_CHUNK_CACHE;
        // Recreate the cache with the new size
        SHARED_CHUNK_CACHE = Caffeine.newBuilder()
                                     .maximumSize(newSize)
                                     .removalListener((ChunkKey key, Future<CachedChunk> value, RemovalCause cause) -> {
                                         if (value != null)
                                         {
                                             value.addListener(() -> {
                                                 try
                                                 {
                                                     CachedChunk cachedChunk = value.get();
                                                     if (cachedChunk != null)
                                                     {
                                                         // Mark the cached chunk as invalid so readers know not to trust it
                                                         cachedChunk.invalidate();
                                                         // Return the buffer to the pool
                                                         ByteBuffer buf = cachedChunk.getBuffer();
                                                         buf.clear();
                                                         buf.position(0).limit(0);
                                                         prefetchPool.offer(buf);
                                                     }
                                                 }
                                                 catch (Exception ignored)
                                                 {
                                                     // ignore failed or cancelled futures
                                                 }
                                             });
                                         }
                                     })
                                     .build();
        if (old != null)
            old.invalidateAll();
    }

    private static class BackupChunkReaderMark implements DataPosition
    {
        final long pointer;

        private BackupChunkReaderMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    // Metrics helper methods for shared chunk cache

    /**
     * Get the current size of the shared chunk cache (number of entries).
     */
    public static long getSharedChunkCacheSize()
    {
        if (SHARED_CHUNK_CACHE == null)
            return 0;
        return SHARED_CHUNK_CACHE.estimatedSize();
    }

    /**
     * Get the maximum capacity of the shared chunk cache (number of entries).
     */
    public static long getSharedChunkCacheCapacity()
    {
        try
        {
            return DatabaseDescriptor.getObjectStoreSharedChunkCacheCount();
        }
        catch (Exception e)
        {
            return 512; // Default value
        }
    }

    /**
     * Get the utilization percentage of the shared chunk cache (0.0 to 1.0).
     */
    public static double getSharedChunkCacheUtilization()
    {
        long capacity = getSharedChunkCacheCapacity();
        if (capacity == 0)
            return 0.0;
        return (double) getSharedChunkCacheSize() / capacity;
    }

    /**
     * Get the age of the oldest entry in the shared chunk cache in milliseconds.
     * Returns 0 if the cache is empty.
     */
    public static long getSharedChunkCacheOldestEntryAgeMillis()
    {
        if (SHARED_CHUNK_CACHE == null)
            return 0;

        long oldestAge = 0;
        for (Future<CachedChunk> future : SHARED_CHUNK_CACHE.asMap().values())
        {
            try
            {
                if (future.isDone() && !future.isCancelled())
                {
                    CachedChunk chunk = future.get();
                    if (chunk != null && chunk.isValid())
                    {
                        long age = chunk.getAgeMillis();
                        if (age > oldestAge)
                            oldestAge = age;
                    }
                }
            }
            catch (Exception ignored)
            {
                // Skip failed or cancelled futures
            }
        }
        return oldestAge;
    }

    /**
     * Get the age of the newest entry in the shared chunk cache in milliseconds.
     * Returns 0 if the cache is empty.
     */
    public static long getSharedChunkCacheNewestEntryAgeMillis()
    {
        if (SHARED_CHUNK_CACHE == null)
            return 0;

        long newestAge = Long.MAX_VALUE;
        boolean foundAny = false;
        for (Future<CachedChunk> future : SHARED_CHUNK_CACHE.asMap().values())
        {
            try
            {
                if (future.isDone() && !future.isCancelled())
                {
                    CachedChunk chunk = future.get();
                    if (chunk != null && chunk.isValid())
                    {
                        long age = chunk.getAgeMillis();
                        if (age < newestAge)
                        {
                            newestAge = age;
                            foundAny = true;
                        }
                    }
                }
            }
            catch (Exception ignored)
            {
                // Skip failed or cancelled futures
            }
        }
        return foundAny ? newestAge : 0;
    }

    /**
     * Get the average age of entries in the shared chunk cache in milliseconds.
     * Returns 0 if the cache is empty.
     */
    public static long getSharedChunkCacheAverageEntryAgeMillis()
    {
        if (SHARED_CHUNK_CACHE == null)
            return 0;

        long totalAge = 0;
        int count = 0;
        for (Future<CachedChunk> future : SHARED_CHUNK_CACHE.asMap().values())
        {
            try
            {
                if (future.isDone() && !future.isCancelled())
                {
                    CachedChunk chunk = future.get();
                    if (chunk != null && chunk.isValid())
                    {
                        totalAge += chunk.getAgeMillis();
                        count++;
                    }
                }
            }
            catch (Exception ignored)
            {
                // Skip failed or cancelled futures
            }
        }
        return count > 0 ? totalAge / count : 0;
    }
}