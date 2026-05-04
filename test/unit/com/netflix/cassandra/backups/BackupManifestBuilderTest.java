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

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BackupManifestBuilderTest extends CQLTester
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private BackupContext ctx;

    @Before
    public void setupCtx()
    {
        ctx = new BackupContext("test", "us-east-1", "cass_test_app", "-100");
        assertTrue(ctx.isValid());
    }

    @Test
    public void composesAzFromRegionAndRack()
    {
        assertEquals("us-east-1a", BackupManifestBuilder.composeAz("us-east-1", "1a"));
        assertEquals("us-west-2b", BackupManifestBuilder.composeAz("us-west-2", "2b"));
        assertEquals("eu-west-1c", BackupManifestBuilder.composeAz("eu-west-1", "1c"));
        // region with multiple trailing digits
        assertEquals("ap-northeast-10d", BackupManifestBuilder.composeAz("ap-northeast-10", "10d"));
        // missing inputs degrade gracefully
        assertNull(BackupManifestBuilder.composeAz("us-east-1", null));
        assertNull(BackupManifestBuilder.composeAz("us-east-1", ""));
        assertEquals("1a", BackupManifestBuilder.composeAz(null, "1a"));
    }

    @Test
    public void buildsManifestWithExpectedPaths() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (id, v) VALUES (1, 1)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        assertFalse("need at least one SSTable to test", sstables.isEmpty());

        String tag = "manifest_test_" + System.nanoTime();
        cfs.snapshotWithoutMemtable(tag);

        SSTableReader first = sstables.iterator().next();
        File snapshotDir = Directories.getSnapshotDirectory(first.descriptor, tag);

        Instant snapshotInstant = Instant.ofEpochMilli(1775754001000L);
        BackupManifestBuilder.build(sstables, ctx, snapshotInstant, cfs.metadata(), snapshotDir);

        File manifestFile = new File(snapshotDir, BackupManifestBuilder.MANIFEST_FILENAME);
        assertTrue("manifest not written", manifestFile.exists());

        BackupManifest manifest = MAPPER.readValue(manifestFile.toJavaIOFile(), BackupManifest.class);
        assertNotNull(manifest.getInfo());
        assertEquals(1, manifest.getInfo().getVersion());
        assertEquals("cass_test_app", manifest.getInfo().getAppName());
        assertEquals("us-east-1", manifest.getInfo().getRegion());
        assertEquals(Collections.singletonList("-100"), manifest.getInfo().getBackupIdentifier());

        assertEquals(1, manifest.getData().size());
        BackupManifest.Data data = manifest.getData().get(0);
        assertEquals(cfs.keyspace.getName(), data.getKeyspaceName());
        assertEquals(cfs.name, data.getColumnfamilyName());

        Collection<BackupManifest.BackupSSTable> sstableEntries = data.getSstables();
        assertEquals(sstables.size(), sstableEntries.size());

        String cfSegment = cfs.name + '-' + cfs.metadata().id.toHexString();
        for (BackupManifest.BackupSSTable entry : sstableEntries)
        {
            assertFalse("should have component entries", entry.getSstableComponents().isEmpty());
            for (BackupManifest.BackupSSTableComponent component : entry.getSstableComponents())
            {
                assertFalse("isUploaded must start false", component.isUploaded());
                assertEquals("NONE", component.getCompression());
                assertEquals("PLAINTEXT", component.getEncryption());
                assertTrue("size should be > 0: " + component.getFileName(),
                           component.getFileSizeOnDisk() > 0);

                String expectedPath = ctx.sstableComponentPath(1775754001000L,
                                                               cfs.keyspace.getName(),
                                                               cfSegment,
                                                               "NONE",
                                                               "PLAINTEXT",
                                                               component.getFileName());
                assertEquals(expectedPath, component.getBackupPath());
            }
        }
    }

    @Test
    public void reusesBackupTimestampAcrossCalls() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (id, v) VALUES (1, 1)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Set<SSTableReader> sstables = new HashSet<>(cfs.getLiveSSTables());
        SSTableReader reader = sstables.iterator().next();
        String generationId = reader.descriptor.id.toString();

        String tagA = "reuse_a_" + System.nanoTime();
        String tagB = "reuse_b_" + System.nanoTime();
        cfs.snapshotWithoutMemtable(tagA);
        cfs.snapshotWithoutMemtable(tagB);

        File dirA = Directories.getSnapshotDirectory(reader.descriptor, tagA);
        File dirB = Directories.getSnapshotDirectory(reader.descriptor, tagB);

        BackupManifestBuilder.build(sstables, ctx, Instant.ofEpochMilli(1_000_000L), cfs.metadata(), dirA);
        // A later snapshot with a different candidate must not change the stored timestamp
        BackupManifestBuilder.build(sstables, ctx, Instant.ofEpochMilli(9_000_000L), cfs.metadata(), dirB);

        long stored = SystemKeyspace.getOrAssignBackupTimestamp(cfs.keyspace.getName(),
                                                                cfs.name,
                                                                generationId,
                                                                -1L);
        assertEquals("first candidate should stick", 1_000_000L, stored);

        BackupManifest manifestB = MAPPER.readValue(new File(dirB, BackupManifestBuilder.MANIFEST_FILENAME).toJavaIOFile(),
                                                    BackupManifest.class);
        BackupManifest.BackupSSTableComponent component = manifestB.getData()
                                                                   .get(0)
                                                                   .getSstables()
                                                                   .get(0)
                                                                   .getSstableComponents()
                                                                   .get(0);
        assertTrue("manifest B must still reference the original backup_ts in path: " + component.getBackupPath(),
                   component.getBackupPath().contains("/SST_V2/1000000/"));
    }
}
