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
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackupMemtableLenFileTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private int fileCounter = 0;

    private Path writeRawLenFile(byte[] content) throws IOException
    {
        Path file = tempFolder.newFile("test-" + fileCounter++ + ".len").toPath();
        Files.write(file, content);
        return file;
    }

    @Test
    public void testRoundTrip() throws IOException
    {
        File file = new File(tempFolder.newFile("roundtrip.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 123456789L);
        assertEquals(123456789L, BackupMemtableContext.DataLengthFileSerializer.read(file.path().toString()));
    }

    @Test
    public void testRoundTripZero() throws IOException
    {
        File file = new File(tempFolder.newFile("zero.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 0L);
        assertEquals(0L, BackupMemtableContext.DataLengthFileSerializer.read(file.path().toString()));
    }

    @Test
    public void testRoundTripMaxValue() throws IOException
    {
        File file = new File(tempFolder.newFile("max.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, BackupMemtableContext.DataLengthFileSerializer.read(file.path().toString()));
    }

    @Test
    public void testBinaryFileSize() throws IOException
    {
        File file = new File(tempFolder.newFile("size.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 42L);
        byte[] raw = Files.readAllBytes(file.toPath());
        assertEquals(BackupMemtableContext.DataLengthFileSerializer.FILE_SIZE, raw.length);
    }

    @Test(expected = IOException.class)
    public void testBitFlipInLength() throws IOException
    {
        File file = new File(tempFolder.newFile("flip-len.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 5000000L);
        byte[] raw = Files.readAllBytes(file.toPath());
        raw[4] ^= 1;
        Path corrupted = writeRawLenFile(raw);
        BackupMemtableContext.DataLengthFileSerializer.read(corrupted.toString());
    }

    @Test(expected = IOException.class)
    public void testBitFlipInChecksum() throws IOException
    {
        File file = new File(tempFolder.newFile("flip-crc.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 5000000L);
        byte[] raw = Files.readAllBytes(file.toPath());
        raw[raw.length - 1] ^= 1;
        Path corrupted = writeRawLenFile(raw);
        BackupMemtableContext.DataLengthFileSerializer.read(corrupted.toString());
    }

    @Test(expected = IOException.class)
    public void testTruncatedFile() throws IOException
    {
        Path file = writeRawLenFile(new byte[] { 'D', 'L', 0, 0 });
        BackupMemtableContext.DataLengthFileSerializer.read(file.toString());
    }

    @Test(expected = IOException.class)
    public void testBadMagic() throws IOException
    {
        Path file = writeRawLenFile(new byte[BackupMemtableContext.DataLengthFileSerializer.FILE_SIZE]);
        BackupMemtableContext.DataLengthFileSerializer.read(file.toString());
    }

    @Test
    public void testWriteOverwritesExisting() throws IOException
    {
        File file = new File(tempFolder.newFile("overwrite.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 111L);
        BackupMemtableContext.DataLengthFileSerializer.write(file, 222L);
        assertEquals(222L, BackupMemtableContext.DataLengthFileSerializer.read(file.path().toString()));
    }

    @Test
    public void testWriteLeavesNoTmp() throws IOException
    {
        File file = new File(tempFolder.newFile("clean.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 42L);
        Path tmp = Path.of(file.path() + ".tmp");
        assertFalse("write() should not leave a .tmp behind", Files.exists(tmp));
    }

    @Test
    public void testReadOrDelete_ValidFileIsKept() throws IOException
    {
        File file = new File(tempFolder.newFile("valid-keep.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 12345L);

        long value = BackupMemtableContext.DataLengthFileSerializer.readOrDelete(file);

        assertEquals(12345L, value);
        assertTrue("Valid .len should not be deleted", file.exists());
    }

    @Test
    public void testReadOrDelete_BadMagicIsDeleted() throws IOException
    {
        File file = new File(tempFolder.newFile("bad-magic.len"));
        // 14 bytes but wrong magic — size check would pass but read() rejects
        Files.write(file.toPath(), new byte[BackupMemtableContext.DataLengthFileSerializer.FILE_SIZE]);

        try
        {
            BackupMemtableContext.DataLengthFileSerializer.readOrDelete(file);
            org.junit.Assert.fail("Expected IOException");
        }
        catch (IOException expected) { /* expected */ }

        assertFalse("Corrupt .len should be deleted on read failure", file.exists());
    }

    @Test
    public void testReadOrDelete_BadChecksumIsDeleted() throws IOException
    {
        File file = new File(tempFolder.newFile("bad-crc.len"));
        BackupMemtableContext.DataLengthFileSerializer.write(file, 5000000L);
        // Flip the last CRC byte to corrupt the checksum
        byte[] raw = Files.readAllBytes(file.toPath());
        raw[raw.length - 1] ^= 1;
        Files.write(file.toPath(), raw);

        try
        {
            BackupMemtableContext.DataLengthFileSerializer.readOrDelete(file);
            org.junit.Assert.fail("Expected IOException");
        }
        catch (IOException expected) { /* expected */ }

        assertFalse("CRC-corrupt .len should be deleted", file.exists());
    }

    @Test
    public void testReadOrDelete_TruncatedFileIsDeleted() throws IOException
    {
        File file = new File(tempFolder.newFile("truncated.len"));
        Files.write(file.toPath(), new byte[] { 'D', 'L', 0, 0 });

        try
        {
            BackupMemtableContext.DataLengthFileSerializer.readOrDelete(file);
            org.junit.Assert.fail("Expected IOException");
        }
        catch (IOException expected) { /* expected */ }

        assertFalse("Truncated .len should be deleted", file.exists());
    }

    @Test
    public void testWriteReplacesStaleTmp() throws IOException
    {
        // Simulate a leftover .tmp from a previously interrupted write.
        // The write should still succeed because it truncates the tmp before writing.
        File file = new File(tempFolder.newFile("with-stale-tmp.len"));
        Path tmp = Path.of(file.path() + ".tmp");
        Files.write(tmp, new byte[] { 1, 2, 3 });

        BackupMemtableContext.DataLengthFileSerializer.write(file, 7L);

        assertEquals(7L, BackupMemtableContext.DataLengthFileSerializer.read(file.path().toString()));
        assertFalse(Files.exists(tmp));
    }
}