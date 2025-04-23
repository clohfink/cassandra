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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUIDAsBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DigestTest
{
    private static final Logger logger = LoggerFactory.getLogger(DigestTest.class);

    @BeforeClass
    public static void setUp() throws Exception
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void hashEmptyBytes() throws Exception {
        Assert.assertArrayEquals(Hex.hexToBytes("d41d8cd98f00b204e9800998ecf8427e"),
                                 Digest.forReadResponse().update(ByteBufferUtil.EMPTY_BYTE_BUFFER).digest());
    }

    @Test
    public void hashBytesFromTinyDirectByteBuffer() throws Exception {
        ByteBuffer directBuf = ByteBuffer.allocateDirect(8);
        directBuf.putLong(5L).position(0);
        directBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("aaa07454fa93ed2d37b4c5da9f2f87fd"),
                                         Digest.forReadResponse().update(directBuf).digest());
    }

    @Test
    public void hashBytesFromLargerDirectByteBuffer() throws Exception {
        ByteBuffer directBuf = ByteBuffer.allocateDirect(1024);
        for (int i = 0; i < 100; i++) {
            directBuf.putInt(i);
        }
        directBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("daf10ea8894783b1b2618309494cde21"),
                          Digest.forReadResponse().update(directBuf).digest());
    }

    @Test
    public void hashBytesFromTinyOnHeapByteBuffer() throws Exception {
        ByteBuffer onHeapBuf = ByteBuffer.allocate(8);
        onHeapBuf.putLong(5L);
        onHeapBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("aaa07454fa93ed2d37b4c5da9f2f87fd"),
                          Digest.forReadResponse().update(onHeapBuf).digest());
    }

    @Test
    public void hashBytesFromLargerOnHeapByteBuffer() throws Exception {
        ByteBuffer onHeapBuf = ByteBuffer.allocate(1024);
        for (int i = 0; i < 100; i++) {
            onHeapBuf.putInt(i);
        }
        onHeapBuf.position(0);
        assertArrayEquals(Hex.hexToBytes("daf10ea8894783b1b2618309494cde21"),
                          Digest.forReadResponse().update(onHeapBuf).digest());
    }

    @Test
    public void testValidatorDigest()
    {
        Digest[] digests = new Digest[]
                           {
                           Digest.forValidator(),
                           new Digest(Hashing.murmur3_128(1000).newHasher()),
                           new Digest(Hashing.murmur3_128(2000).newHasher())
                           };
        byte [] random = nextTimeUUIDAsBytes();

        for (Digest digest : digests)
        {
            digest.updateWithByte((byte) 33)
                  .update(random, 0, random.length)
                  .update(ByteBuffer.wrap(random))
                  .update(random, 0, 3)
                  .updateWithBoolean(false)
                  .updateWithInt(77)
                  .updateWithLong(101);
        }

        long len = Byte.BYTES
                   + random.length * 2 // both the byte[] and the ByteBuffer
                   + 3 // 3 bytes from the random byte[]
                   + Byte.BYTES
                   + Integer.BYTES
                   + Long.BYTES;

        assertEquals(len, digests[0].inputBytes());
        byte[] h = digests[0].digest();
        assertArrayEquals(digests[1].digest(), Arrays.copyOfRange(h, 0, 16));
        assertArrayEquals(digests[2].digest(), Arrays.copyOfRange(h, 16, 32));
    }

    /**
     * Helper method to compute the 128-bit hash using Guava's Murmur3_128 hasher.
     *
     * @param input the byte array to hash.
     * @return the 16-byte (128-bit) hash.
     */
    private byte[] computeGuavaHash(byte[] input) {
        Hasher hasher = Hashing.murmur3_128().newHasher();
        hasher.putBytes(input);
        return hasher.hash().asBytes();
    }

    /**
     * Test that an empty input produces the same hash.
     */
    @Test
    public void testEmptyInput() {
        byte[] input = new byte[0];

        Murmur3Digest myDigest = new Murmur3Digest();
        myDigest.update(input, 0, input.length);
        byte[] myHash = myDigest.digest();

        byte[] guavaHash = computeGuavaHash(input);
        Assert.assertArrayEquals("Empty input hash mismatch", guavaHash, myHash);
    }

    /**
     * Test using a simple string input.
     */
    @Test
    public void testStringInput() {
        String inputStr = "The quick brown fox jumps over the lazy dog";
        byte[] input = inputStr.getBytes(StandardCharsets.UTF_8);

        // Test using the byte array update
        Murmur3Digest digestFromArray = new Murmur3Digest();
        digestFromArray.update(input, 0, input.length);
        byte[] hashFromArray = digestFromArray.digest();

        // Compute reference hash from Guava
        byte[] guavaHash = computeGuavaHash(input);
        Assert.assertArrayEquals("Byte array input hash mismatch", guavaHash, hashFromArray);

        // Test using an array-backed ByteBuffer update
        ByteBuffer bbArray = ByteBuffer.wrap(input);
        Murmur3Digest digestFromArrayBuffer = new Murmur3Digest();
        digestFromArrayBuffer.update(bbArray, bbArray.position(), bbArray.remaining());
        byte[] hashFromArrayBuffer = digestFromArrayBuffer.digest();
        Assert.assertArrayEquals("Array-backed ByteBuffer hash mismatch", guavaHash, hashFromArrayBuffer);
    }

    /**
     * Test that using a direct ByteBuffer produces the same hash.
     */
    @Test
    public void testDirectByteBuffer() {
        String inputStr = "Sample data for direct ByteBuffer test.";
        byte[] input = inputStr.getBytes(StandardCharsets.UTF_8);

        // Create a direct ByteBuffer and fill it.
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(input.length);
        directBuffer.put(input);
        directBuffer.flip();

        Murmur3Digest myDigest = new Murmur3Digest();
        myDigest.update(directBuffer, directBuffer.position(), directBuffer.remaining());
        byte[] myHash = myDigest.digest();

        byte[] guavaHash = computeGuavaHash(input);
        Assert.assertArrayEquals("Direct ByteBuffer hash mismatch", guavaHash, myHash);
    }

    /**
     * Test that incremental updates (splitting the input over multiple update calls)
     * produce the same hash as a single update.
     */
    @Test
    public void testIncrementalUpdates() {
        // Create random data of 1024 bytes.
        byte[] input = new byte[1024];
        new Random(42).nextBytes(input);

        // Compute hash using a single update call.
        Murmur3Digest digestSingle = new Murmur3Digest();
        digestSingle.update(input, 0, input.length);
        byte[] hashSingle = digestSingle.digest();

        // Compute hash using multiple update calls (splitting input into chunks).
        Murmur3Digest digestIncremental = new Murmur3Digest();
        int chunkSize = input.length / 10;
        int offset = 0;
        while (offset < input.length) {
            int len = Math.min(chunkSize, input.length - offset);
            digestIncremental.update(input, offset, len);
            offset += len;
        }
        byte[] hashIncremental = digestIncremental.digest();

        // Reference hash from Guava
        byte[] guavaHash = computeGuavaHash(input);

        Assert.assertArrayEquals("Incremental update hash mismatch (single vs. incremental)",
                                 hashSingle, hashIncremental);
        Assert.assertArrayEquals("Incremental update hash mismatch (Guava vs. incremental)",
                                 guavaHash, hashIncremental);
    }
}
