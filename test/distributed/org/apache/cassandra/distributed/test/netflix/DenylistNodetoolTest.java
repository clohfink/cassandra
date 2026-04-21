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

package org.apache.cassandra.distributed.test.netflix;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Exercises the `nodetool denylist` command group against a live in-JVM cluster:
 * add, list, refresh, remove.
 */
public class DenylistNodetoolTest extends TestBaseImpl
{
    private static final String TABLE = "denylist_nt_tbl";
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    public void testDenylistLifecycle() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("partition_denylist_enabled", true)
                                                             .set("denylist_initial_load_retry", "1s")
                                                             .set("denylist_consistency_level", "ONE"))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k int PRIMARY KEY, v int)");
            for (int i = 0; i < 5; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (k, v) VALUES (?, ?)",
                                               ConsistencyLevel.ONE, i, i);

            // list before any adds: should be empty
            NodeToolResult listEmpty = cluster.get(1).nodetoolResult("denylist", "list");
            listEmpty.asserts().success();
            assertThat("Fresh cluster should have no denylisted keys",
                       listEmpty.getStdout(), containsString("No denylisted partition keys"));

            // add a key
            NodeToolResult add = cluster.get(1).nodetoolResult("denylist", "add", KEYSPACE, TABLE, "42");
            add.asserts().success();
            assertThat(add.getStdout(), containsString("Denylisted 42 in " + KEYSPACE + '.' + TABLE));

            // list all: should include ks.table and the key
            NodeToolResult listAll = cluster.get(1).nodetoolResult("denylist", "list");
            listAll.asserts().success();
            assertThat(listAll.getStdout(), containsString(KEYSPACE + '.' + TABLE));
            assertThat(listAll.getStdout(), containsString("42"));

            // list with scoped ks/table should also show it
            NodeToolResult listScoped = cluster.get(1).nodetoolResult("denylist", "list", KEYSPACE, TABLE);
            listScoped.asserts().success();
            assertThat(listScoped.getStdout(), containsString("42"));

            // refresh: cache reload should succeed and report the key remains
            NodeToolResult refresh = cluster.get(1).nodetoolResult("denylist", "refresh");
            refresh.asserts().success();
            assertThat(refresh.getStdout(), containsString("Denylist reloaded"));

            NodeToolResult listAfterRefresh = cluster.get(1).nodetoolResult("denylist", "list", KEYSPACE, TABLE);
            listAfterRefresh.asserts().success();
            assertThat("Refresh should preserve denylisted key", listAfterRefresh.getStdout(), containsString("42"));

            // remove the key
            NodeToolResult remove = cluster.get(1).nodetoolResult("denylist", "remove", KEYSPACE, TABLE, "42");
            remove.asserts().success();
            assertThat(remove.getStdout(), containsString("Removed 42 from " + KEYSPACE + '.' + TABLE));

            // scoped list should be empty
            NodeToolResult listGone = cluster.get(1).nodetoolResult("denylist", "list", KEYSPACE, TABLE);
            listGone.asserts().success();
            assertThat(listGone.getStdout(), containsString("No denylisted partition keys for " + KEYSPACE + '.' + TABLE));

            // global list should not mention the key anymore
            NodeToolResult listAllGone = cluster.get(1).nodetoolResult("denylist", "list");
            listAllGone.asserts().success();
            assertThat(listAllGone.getStdout(), not(containsString(KEYSPACE + '.' + TABLE + "\n  42")));
        }
    }

    @Test
    public void testDenylistAddFailsForUnknownTable() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("partition_denylist_enabled", true)
                                                             .set("denylist_initial_load_retry", "1s")
                                                             .set("denylist_consistency_level", "ONE"))
                                           .start()))
        {
            NodeToolResult result = cluster.get(1).nodetoolResult("denylist", "add", KEYSPACE, "no_such_table", "1");
            assertNotEquals("Adding to an unknown table should fail", 0, result.getRc());
        }
    }

    @Test
    public void testDenylistListJsonOutput() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("partition_denylist_enabled", true)
                                                             .set("denylist_initial_load_retry", "1s")
                                                             .set("denylist_consistency_level", "ONE"))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k int PRIMARY KEY, v int)");

            // empty list in JSON: should parse to an empty object
            NodeToolResult listEmpty = cluster.get(1).nodetoolResult("denylist", "list", "-F", "json");
            listEmpty.asserts().success();
            JsonNode emptyJson = JSON.readTree(listEmpty.getStdout());
            assertTrue("Empty denylist should render as JSON object", emptyJson.isObject());
            assertTrue("Empty denylist should have no entries", emptyJson.isEmpty());

            // add a couple of keys across the same table
            cluster.get(1).nodetoolResult("denylist", "add", KEYSPACE, TABLE, "7").asserts().success();
            cluster.get(1).nodetoolResult("denylist", "add", KEYSPACE, TABLE, "99").asserts().success();

            String qualified = KEYSPACE + '.' + TABLE;

            // global JSON list should contain the ks.table key with both entries
            NodeToolResult listAll = cluster.get(1).nodetoolResult("denylist", "list", "-F", "json");
            listAll.asserts().success();
            JsonNode all = JSON.readTree(listAll.getStdout());
            assertTrue("All list should be a JSON object", all.isObject());
            assertTrue("All list should contain our ks.table", all.has(qualified));
            JsonNode keysNode = all.get(qualified);
            assertTrue("Keys should be a JSON array", keysNode.isArray());
            assertEquals("Expected two denylisted keys", 2, keysNode.size());

            // scoped JSON list should produce a single-entry object
            NodeToolResult listScoped = cluster.get(1).nodetoolResult("denylist", "list", KEYSPACE, TABLE, "-F", "json");
            listScoped.asserts().success();
            JsonNode scoped = JSON.readTree(listScoped.getStdout());
            assertTrue("Scoped list should be a JSON object", scoped.isObject());
            assertEquals("Scoped list should have a single entry", 1, scoped.size());
            assertTrue("Scoped list should contain our ks.table", scoped.has(qualified));
            assertEquals(2, scoped.get(qualified).size());

            // bad -F value should error out
            NodeToolResult bad = cluster.get(1).nodetoolResult("denylist", "list", "-F", "csv");
            assertNotEquals("Unsupported format should fail", 0, bad.getRc());

            // scoped JSON for an empty table should be an empty object
            cluster.get(1).nodetoolResult("denylist", "remove", KEYSPACE, TABLE, "7").asserts().success();
            cluster.get(1).nodetoolResult("denylist", "remove", KEYSPACE, TABLE, "99").asserts().success();
            NodeToolResult listGone = cluster.get(1).nodetoolResult("denylist", "list", KEYSPACE, TABLE, "-F", "json");
            listGone.asserts().success();
            JsonNode gone = JSON.readTree(listGone.getStdout());
            assertTrue(gone.isObject());
            assertFalse("Scoped list for empty table should not contain the ks.table key", gone.has(qualified));
        }
    }

    @Test
    public void testDenylistListArgsValidation() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP)
                                                             .set("partition_denylist_enabled", true)
                                                             .set("denylist_initial_load_retry", "1s")
                                                             .set("denylist_consistency_level", "ONE"))
                                           .start()))
        {
            // a single positional arg is invalid — list takes either none or <ks> <table>
            NodeToolResult bad = cluster.get(1).nodetoolResult("denylist", "list", KEYSPACE);
            assertNotEquals("list with 1 arg should fail", 0, bad.getRc());
        }
    }
}
