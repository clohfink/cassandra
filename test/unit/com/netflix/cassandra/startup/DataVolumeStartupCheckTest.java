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
package com.netflix.cassandra.startup;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataVolumeStartupCheckTest
{
    @Test
    public void floorIs15GiB()
    {
        assertEquals(15L * 1024L * 1024L * 1024L, DataVolumeStartupCheck.DATA_VOLUME_FLOOR_BYTES);
    }

    @Test
    public void nearestExistingAncestorReturnsPathWhenItExists() throws Exception
    {
        Path existing = Files.createTempDirectory("dvsc-test-");
        try
        {
            Path resolved = DataVolumeStartupCheck.nearestExistingAncestor(existing);
            assertNotNull(resolved);
            assertEquals(existing.toAbsolutePath().normalize(), resolved);
        }
        finally
        {
            Files.deleteIfExists(existing);
        }
    }

    @Test
    public void nearestExistingAncestorWalksUpWhenLeafIsMissing() throws Exception
    {
        Path existing = Files.createTempDirectory("dvsc-test-");
        try
        {
            Path missing = existing.resolve("does-not-exist/nor-this");
            Path resolved = DataVolumeStartupCheck.nearestExistingAncestor(missing);
            assertNotNull(resolved);
            assertEquals(existing.toAbsolutePath().normalize(), resolved);
        }
        finally
        {
            Files.deleteIfExists(existing);
        }
    }

    @Test
    public void nearestExistingAncestorFallsBackToRoot()
    {
        // Even a path with no real ancestor still resolves to "/" which exists.
        Path resolved = DataVolumeStartupCheck.nearestExistingAncestor(Path.of("/definitely/does/not/exist/anywhere"));
        assertNotNull(resolved);
        assertTrue(Files.exists(resolved));
    }
}
