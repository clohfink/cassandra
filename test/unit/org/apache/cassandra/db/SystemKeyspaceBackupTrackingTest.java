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

import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.commitlog.CommitLog;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;

public class SystemKeyspaceBackupTrackingTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test
    public void firstCallAssignsCandidate()
    {
        String ks = "ks_" + uniq();
        long assigned = SystemKeyspace.getOrAssignBackupTimestamp(ks, "tbl", "1", 111L);
        assertEquals(111L, assigned);

        UntypedResultSet rs = executeInternal(format("SELECT backup_timestamp FROM system.%s"
                                                     + " WHERE keyspace_name=? AND table_name=? AND generation_id=?",
                                                     SystemKeyspace.BACKUP_SSTABLE_TRACKING),
                                              ks, "tbl", "1");
        assertEquals(1, rs.size());
        assertEquals(111L, rs.one().getLong("backup_timestamp"));
    }

    @Test
    public void secondCallReturnsStoredTimestamp()
    {
        String ks = "ks_" + uniq();
        long first = SystemKeyspace.getOrAssignBackupTimestamp(ks, "tbl", "7", 1000L);
        long second = SystemKeyspace.getOrAssignBackupTimestamp(ks, "tbl", "7", 2000L);
        assertEquals(1000L, first);
        assertEquals("second call must return already-assigned value", 1000L, second);
    }

    @Test
    public void differentGenerationIdsAreIndependent()
    {
        String ks = "ks_" + uniq();
        assertEquals(500L, SystemKeyspace.getOrAssignBackupTimestamp(ks, "tbl", "a", 500L));
        assertEquals(600L, SystemKeyspace.getOrAssignBackupTimestamp(ks, "tbl", "b", 600L));
    }

    private static String uniq()
    {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }
}
