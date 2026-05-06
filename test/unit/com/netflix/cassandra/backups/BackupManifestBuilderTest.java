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
import org.apache.cassandra.service.snapshot.TableSnapshot;

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
        TableSnapshot snapshot = cfs.snapshotWithoutMemtable(tag);

        SSTableReader first = sstables.iterator().next();
        File datadir = Directories.getSnapshotDirectory(first.descriptor, tag)
                                  .parent().parent().parent().parent();

        Instant snapshotInstant = Instant.ofEpochMilli(1775754001000L);
        BackupManifestBuilder builder = new BackupManifestBuilder(tag, snapshotInstant, ctx, datadir);
        builder.accept(snapshot);
        builder.write();

        File manifestFile = pendingManifest(datadir, tag);
        assertTrue("manifest not written: " + manifestFile, manifestFile.exists());

        BackupManifest manifest = MAPPER.readValue(manifestFile.toJavaIOFile(), BackupManifest.class);
        assertNotNull(manifest.getInfo());
        assertEquals(tag, manifest.getInfo().getSnapshotTag());
        assertEquals(1, manifest.getInfo().getVersion());
        assertEquals("cass_test_app", manifest.getInfo().getAppName());
        assertEquals("us-east-1", manifest.getInfo().getRegion());
        assertEquals(Collections.singletonList("-100"), manifest.getInfo().getBackupIdentifier());
        assertEquals(1775754001000L, manifest.getInfo().getSnapshotInstantMs());
        assertEquals(ctx.prefix() + "/-100", manifest.getInfo().getBackupPathPrefix());

        assertEquals(1, manifest.getData().size());
        BackupManifest.Data data = manifest.getData().get(0);
        assertEquals(cfs.keyspace.getName(), data.getKeyspaceName());
        assertEquals(cfs.name, data.getColumnfamilyName());

        Collection<BackupManifest.BackupSSTable> sstableEntries = data.getSstables();
        // One group for the SSTable's nb-1-big prefix, plus one each for the sidecars
        // Cassandra writes alongside (manifest.json, schema.cql) — both uploaded under
        // the SST_V2 layout with prefix = filename-without-extension.
        java.util.Set<String> prefixes = sstableEntries.stream()
                                                      .map(BackupManifest.BackupSSTable::getPrefix)
                                                      .collect(java.util.stream.Collectors.toSet());
        assertTrue("expected SSTable group: " + prefixes,
                   prefixes.stream().anyMatch(p -> p.startsWith("nb-")));
        assertTrue("expected manifest sidecar: " + prefixes, prefixes.contains("manifest"));
        assertTrue("expected schema sidecar: " + prefixes, prefixes.contains("schema"));

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
    public void multiTableSnapshotProducesOneManifestWithAllCfsAndSidecars() throws Throwable
    {
        String table1 = createTable("CREATE TABLE %s (id int PRIMARY KEY, v int)");
        String table2 = createTable("CREATE TABLE %s (id int PRIMARY KEY, v int)");
        execute(String.format("INSERT INTO %s.%s (id, v) VALUES (1, 1)", KEYSPACE, table1));
        execute(String.format("INSERT INTO %s.%s (id, v) VALUES (2, 2)", KEYSPACE, table2));

        ColumnFamilyStore cfs1 = getColumnFamilyStore(KEYSPACE, table1);
        ColumnFamilyStore cfs2 = getColumnFamilyStore(KEYSPACE, table2);
        cfs1.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        cfs2.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        String tag = "multi_table_snap_" + System.nanoTime();
        Instant snapshotInstant = Instant.ofEpochMilli(1777978801000L);
        BackupManifestBuilder builder = new BackupManifestBuilder(tag, snapshotInstant, ctx, datadir(cfs1));

        // Mirror StorageService's threading: feed each per-CF TableSnapshot to the same builder.
        cfs1.snapshotWithoutMemtable(tag, null, false, null, null, snapshotInstant, builder);
        cfs2.snapshotWithoutMemtable(tag, null, false, null, null, snapshotInstant, builder);
        builder.write();

        File manifestFile = pendingManifest(datadir(cfs1), tag);
        assertTrue("manifest not written: " + manifestFile, manifestFile.exists());
        BackupManifest manifest = MAPPER.readValue(manifestFile.toJavaIOFile(), BackupManifest.class);

        // Top-level fields.
        assertEquals(tag, manifest.getInfo().getSnapshotTag());
        assertEquals(ctx.prefix() + "/-100", manifest.getInfo().getBackupPathPrefix());
        assertEquals(1777978801000L, manifest.getInfo().getSnapshotInstantMs());
        assertEquals("cass_test_app", manifest.getInfo().getAppName());
        assertEquals("us-east-1", manifest.getInfo().getRegion());

        // Both CFs should appear, each with the full sidecar set.
        assertEquals(2, manifest.getData().size());
        for (String table : new String[] { table1, table2 })
        {
            BackupManifest.Data data = manifest.getData().stream()
                    .filter(d -> table.equals(d.getColumnfamilyName()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("missing CF " + table + " in manifest"));
            assertEquals(KEYSPACE, data.getKeyspaceName());

            Set<String> prefixes = new HashSet<>();
            for (BackupManifest.BackupSSTable sst : data.getSstables())
                prefixes.add(sst.getPrefix());

            assertTrue("expected SSTable group for " + table + ": " + prefixes,
                       prefixes.stream().anyMatch(p -> p.startsWith("nb-")));
            assertTrue("expected manifest sidecar for " + table + ": " + prefixes, prefixes.contains("manifest"));
            assertTrue("expected schema sidecar for " + table + ": " + prefixes, prefixes.contains("schema"));

            // Spot-check the sidecar paths and required component fields.
            BackupManifest.BackupSSTableComponent manifestSidecar = onlyComponentOf(data, "manifest");
            assertEquals("manifest.json", manifestSidecar.getFileName());
            assertEquals("NONE", manifestSidecar.getCompression());
            assertEquals("PLAINTEXT", manifestSidecar.getEncryption());
            assertFalse(manifestSidecar.isUploaded());
            assertTrue("manifest.json size should be > 0", manifestSidecar.getFileSizeOnDisk() > 0);
            assertTrue("sidecar backupPath must be SST_V2 with the snapshot ts: " + manifestSidecar.getBackupPath(),
                       manifestSidecar.getBackupPath().contains("/SST_V2/1777978801000/" + KEYSPACE + "/"));
            assertTrue("sidecar backupPath must end with the file name: " + manifestSidecar.getBackupPath(),
                       manifestSidecar.getBackupPath().endsWith("/manifest.json"));

            BackupManifest.BackupSSTableComponent schemaSidecar = onlyComponentOf(data, "schema");
            assertEquals("schema.cql", schemaSidecar.getFileName());
            assertTrue(schemaSidecar.getBackupPath().endsWith("/schema.cql"));
        }
    }

    private static File pendingManifest(File datadir, String tag)
    {
        return new File(new File(new File(datadir, BackupManifestBuilder.MANIFESTS_DIRNAME),
                                 BackupManifestBuilder.PENDING_SUBDIR),
                        tag + ".json");
    }

    private static File datadir(ColumnFamilyStore cfs)
    {
        // .../data/<ks>/<cf-uuid>/ → ascend to the data directory root.
        return cfs.getDirectories().getCFDirectories().get(0).parent().parent();
    }

    private static BackupManifest.BackupSSTableComponent onlyComponentOf(BackupManifest.Data data, String prefix)
    {
        BackupManifest.BackupSSTable group = data.getSstables().stream()
                .filter(s -> prefix.equals(s.getPrefix()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no group with prefix=" + prefix));
        assertEquals("expected exactly one component for sidecar prefix=" + prefix,
                     1, group.getSstableComponents().size());
        return group.getSstableComponents().get(0);
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
        TableSnapshot snapA = cfs.snapshotWithoutMemtable(tagA);
        TableSnapshot snapB = cfs.snapshotWithoutMemtable(tagB);

        File datadir = Directories.getSnapshotDirectory(reader.descriptor, tagA)
                                  .parent().parent().parent().parent();

        BackupManifestBuilder builderA = new BackupManifestBuilder(tagA, Instant.ofEpochMilli(1_000_000L), ctx, datadir);
        builderA.accept(snapA);
        builderA.write();
        // A later snapshot with a different candidate must not change the stored timestamp
        BackupManifestBuilder builderB = new BackupManifestBuilder(tagB, Instant.ofEpochMilli(9_000_000L), ctx, datadir);
        builderB.accept(snapB);
        builderB.write();

        long stored = SystemKeyspace.getOrAssignBackupTimestamp(cfs.keyspace.getName(),
                                                                cfs.name,
                                                                generationId,
                                                                -1L);
        assertEquals("first candidate should stick", 1_000_000L, stored);

        File manifestFileB = pendingManifest(datadir, tagB);
        BackupManifest manifestB = MAPPER.readValue(manifestFileB.toJavaIOFile(), BackupManifest.class);
        BackupManifest.BackupSSTable sstableEntry = manifestB.getData().get(0).getSstables().stream()
                .filter(s -> s.getPrefix().startsWith("nb-"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no SSTable group in manifest B"));
        BackupManifest.BackupSSTableComponent component = sstableEntry.getSstableComponents().get(0);
        assertTrue("manifest B must still reference the original backup_ts in path: " + component.getBackupPath(),
                   component.getBackupPath().contains("/SST_V2/1000000/"));
    }
}
