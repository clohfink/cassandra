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
}