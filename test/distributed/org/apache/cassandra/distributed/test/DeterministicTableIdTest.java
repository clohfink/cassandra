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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;
import org.junit.matchers.JUnitMatchers;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ShutdownException;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeterministicTableIdTest extends TestBaseImpl
{
    private static final String TABLE = "tb_deterministic_table_id";

    private static final String CREATE_TABLE_QUERY = String.format("CREATE TABLE %s.%s (key text, c1 text, c2 text, c3 text, PRIMARY KEY (key, c1));", KEYSPACE, TABLE);

    private static final String DROP_TABLE_QUERY = String.format("DROP TABLE %s.%s", KEYSPACE, TABLE);

    private static final String ALTER_TABLE_QUERY = String.format("ALTER TABLE %s.%s ADD c4 text;", KEYSPACE, TABLE);

    private static final String SELECT_TABLE_QUERY = String.format("SELECT COUNT(*) FROM %s.%s", KEYSPACE, TABLE);

    private static final String GET_CFID_QUERY = String.format("SELECT id FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s' ALLOW FILTERING", KEYSPACE, TABLE);

    @Test
    public void testTimeUUIDTableId() throws Throwable
    {
        runTest(false);
    }

    @Test
    public void testDeterministicTableId() throws Throwable
    {
        runTest(true);
    }

    @Test
    public void testDropAndCreateTableWithOfflineNode() throws Throwable
    {
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config
                                                                 .with(NETWORK)
                                                                 .with(GOSSIP)
                                                                 .set("use_deterministic_table_id", true))
                                           .start()))
        {
            // Create table while both nodes are up
            String tableId1 = createTable(cluster.get(1));

            // Shutdown the node 2
            cluster.get(2).shutdown().get();
            assertTrue(cluster.get(2).isShutdown());

            // Drop the table on node 1
            cluster.get(1).executeInternal(DROP_TABLE_QUERY);

            // Re-create the table with the same schema
            String tableId2 = createTable(cluster.get(1));

            // Verify the two table ids should be the same
            assertThat(tableId1, equalTo(tableId2));

            // Startup the node2
            startupAndWait(cluster.get(2));

            // Make another schema change to trigger the schema sync up
            cluster.schemaChange(ALTER_TABLE_QUERY);

            // Verify that table on node2 should be empty, so it won't be data resurrection
            Object[][] res = cluster.get(2).executeInternal(SELECT_TABLE_QUERY);
            assertEquals(0L, res[0][0]);
        }
    }

    private void runTest(boolean enableDeterministicTableId) throws Throwable
    {
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default

        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config
                                                                 .with(NETWORK)
                                                                 .with(GOSSIP)
                                                                 .set("use_deterministic_table_id", enableDeterministicTableId))
                                           .start()))
        {
            // Shutdown the node 2
            cluster.get(2).shutdown().get();
            assertTrue(cluster.get(2).isShutdown());

            // Create table on node 1
            String tableId1 = createTable(cluster.get(1));

            // Shutdown node 1
            cluster.get(1).shutdown().get();
            assertTrue(cluster.get(1).isShutdown());

            // Start the node 2
            startupAndWait(cluster.get(2));

            // Create table on node 2 and get cfId
            String tableId2 = createTable(cluster.get(2));

            // Start node 1
            startupAndWait(cluster.get(1));

            // Make another schema change to trigger the schema sync-up
            try
            {
                cluster.schemaChange(ALTER_TABLE_QUERY);
            }
            catch (Exception e)
            {
                if (!enableDeterministicTableId)
                {
                    assertThat(e.getMessage(), JUnitMatchers.containsString("Schema agreement not reached."));
                }
                else
                {
                    fail("Should not reach here with deterministic table id enabled.");
                }
            }

            // Get schema versions of the two nodes
            String schemaVersion1 = cluster.get(1).schemaVersion().toString();
            String schemaVersion2 = cluster.get(2).schemaVersion().toString();

            if (enableDeterministicTableId)
            {
                assertThat(tableId1, equalTo(tableId2));
                assertThat(schemaVersion1, equalTo(schemaVersion2));
            }
            else
            {
                assertThat(tableId1, not(equalTo(tableId2)));
                assertThat(schemaVersion1, not(equalTo(schemaVersion2)));
            }
        }
        catch (ShutdownException ignored) {}
        catch (Exception e)
        {
            if (!enableDeterministicTableId)
            {
                assertThat(e.getMessage(), JUnitMatchers.containsString("Column family ID mismatch"));
            }
            else
            {
                fail("Should not reach here with deterministic table id enabled.");
            }
        }
    }

    /**
     * Create table and return the table id
     * @param instance instance
     * @return The table id
     */
    private String createTable(IInstance instance)
    {
        instance.executeInternal(CREATE_TABLE_QUERY);
        Object[][] res = instance.executeInternal(GET_CFID_QUERY);

        return String.valueOf(res[0][0]);
    }

    /**
     * Startup the node and wait until the node is in the NORMAL state
     * @param instance instance
     */
    private void startupAndWait(IInvokableInstance instance)
    {
        instance.startup();
        instance.runOnInstance(() -> {
            long deadlineInMillis = System.currentTimeMillis() + Math.max(1, TimeUnit.SECONDS.toMillis(60));
            while (!StorageService.instance.getOperationMode().equals("NORMAL"))
            {
                if (System.currentTimeMillis() >= deadlineInMillis)
                {
                    throw new RuntimeException("Instance did not reach application state NORMAL before timeout");
                }
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }
        });
    }
}
