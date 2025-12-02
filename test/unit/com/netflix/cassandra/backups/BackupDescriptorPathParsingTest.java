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

import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BackupDescriptorPathParsingTest
{
    @Test
    public void testPrefixParsingValid()
    {
        BackupManifest.BackupSSTable backupSSTable = createValidS3SSTable();

        assertEquals("nb", BackupDescriptor.getVersion(backupSSTable));
        assertEquals("1", BackupDescriptor.getId(backupSSTable).toString());
        assertEquals(SSTableFormat.Type.BIG, BackupDescriptor.getType(backupSSTable));
    }

    @Test
    public void testGetKeyspaceName()
    {
        BackupManifest.BackupSSTable backupSSTable = createValidS3SSTable();
        
        String keyspace = BackupDescriptor.getKeyspaceName(backupSSTable);
        assertEquals("system", keyspace);
    }

    @Test
    public void testGetTableName()
    {
        BackupManifest.BackupSSTable backupSSTable = createValidS3SSTable();
        
        String tableName = BackupDescriptor.getTableName(backupSSTable);
        assertEquals("available_ranges_v2", tableName);
    }

    @Test
    public void testGetVersion()
    {
        BackupManifest.BackupSSTable backupSSTable = createValidS3SSTable();
        
        String version = BackupDescriptor.getVersion(backupSSTable);
        assertEquals("nb", version);
    }

    @Test
    public void testGetType()
    {
        BackupManifest.BackupSSTable backupSSTable = createValidS3SSTable();
        
        SSTableFormat.Type type = BackupDescriptor.getType(backupSSTable);
        assertEquals(SSTableFormat.Type.BIG, type);
    }

    @Test
    public void testGetId()
    {
        BackupManifest.BackupSSTable backupSSTable = createValidS3SSTable();
        
        SSTableId id = BackupDescriptor.getId(backupSSTable);
        assertNotNull(id);
        assertEquals("1", id.toString());
    }

    @Test
    public void testPathParsingWithDifferentVersions()
    {
        // Test with different version format
        BackupManifest.BackupSSTableComponent component = new BackupManifest.BackupSSTableComponent();
        component.setFileName("mc-42-BIG-Index.db");
        component.setBackupPath("prod_backup/-5160_cass_dgw_kv_growthidgraph_v3/-2497996591506259514/SST_V2/1736811488000/system/available_ranges_v2-4224a0882ac93d0c889dfbb5f0facda0/NONE/PLAINTEXT/mc-42-BIG-Index.db");

        BackupManifest.BackupSSTable backupSSTable = new BackupManifest.BackupSSTable();
        backupSSTable.setPrefix("mc-42-BIG");
        backupSSTable.setSstableComponents(Collections.singletonList(component));

        assertEquals("mc", BackupDescriptor.getVersion(backupSSTable));
        assertEquals("42", BackupDescriptor.getId(backupSSTable).toString());
        assertEquals(SSTableFormat.Type.BIG, BackupDescriptor.getType(backupSSTable));
    }

    private BackupManifest.BackupSSTable createValidS3SSTable()
    {
        BackupManifest.BackupSSTableComponent component = new BackupManifest.BackupSSTableComponent();
        component.setFileName("nb-1-BIG-Data.db");
        component.setBackupPath("prod_backup/-5160_cass_dgw_kv_growthidgraph_v3/-2497996591506259514/SST_V2/1736811488000/system/available_ranges_v2-4224a0882ac93d0c889dfbb5f0facda0/NONE/PLAINTEXT/nb-1-BIG-Data.db");

        BackupManifest.BackupSSTable backupSSTable = new BackupManifest.BackupSSTable();
        backupSSTable.setPrefix("nb-1-BIG");
        backupSSTable.setSstableComponents(Collections.singletonList(component));

        return backupSSTable;
    }
}