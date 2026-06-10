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

import java.nio.ByteBuffer;

import org.junit.Test;

import com.netflix.cassandra.schema.NetflixTableOptions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that Netflix custom table options (see {@link NetflixTableOptions}) behave the same way
 * across a multi-node cluster as they do in {@link com.netflix.cassandra.schema.NetflixTableOptionsTest}:
 * setting, updating and unsetting them on a coordinator is spread to every other node through ordinary
 * schema migration. Because the options live in the standard {@code extensions} map, the coordinator
 * stores them and the resulting schema mutation propagates and is applied on the remaining nodes with
 * no special handling.
 */
public class NetflixTableOptionsDistributedTest extends TestBaseImpl
{
    private static final String TABLE = "netflix_opts_tbl";

    @Test
    public void netflixOptionsPropagateOnCreateAndAlter() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            // CREATE on the coordinator; schemaChange blocks until the schema agrees across the cluster.
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE +
                                 " (k int PRIMARY KEY, v int) WITH netflix_tier = 1");

            // The option is stored in the extensions map on every node, not just the coordinator.
            for (int node = 1; node <= 2; node++)
                assertEquals("netflix_tier on node " + node, "1", option(cluster.get(node), "netflix_tier"));

            // A single ALTER updates one option and adds another; both changes spread to all nodes.
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + '.' + TABLE +
                                 " WITH netflix_tier = 3 AND netflix_immutable = true");

            for (int node = 1; node <= 2; node++)
            {
                assertEquals("netflix_tier on node " + node, "3", option(cluster.get(node), "netflix_tier"));
                assertEquals("netflix_immutable on node " + node, "true", option(cluster.get(node), "netflix_immutable"));
            }

            // The non-coordinator also surfaces them as first-class options via DESCRIBE.
            String describe = describe(cluster.get(2));
            assertTrue(describe, describe.contains("netflix_tier = 3"));
            assertTrue(describe, describe.contains("netflix_immutable = true"));
        }
    }

    @Test
    public void unsettingNetflixOptionPropagates() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE +
                                 " (k int PRIMARY KEY, v int) WITH netflix_tier = 2 AND netflix_immutable = true");

            for (int node = 1; node <= 2; node++)
                assertEquals("netflix_tier on node " + node, "2", option(cluster.get(node), "netflix_tier"));

            // Unset one option via the empty-string sentinel; the removal must spread to every node.
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + '.' + TABLE + " WITH netflix_tier = ''");

            for (int node = 1; node <= 2; node++)
            {
                assertNull("netflix_tier on node " + node, option(cluster.get(node), "netflix_tier"));
                // The untouched option still agrees everywhere.
                assertEquals("netflix_immutable on node " + node, "true", option(cluster.get(node), "netflix_immutable"));
            }
        }
    }

    /** Returns the decoded value of a Netflix option as stored in {@code TABLE}'s extensions on the given node, or null if absent. */
    private static String option(IInvokableInstance node, String name)
    {
        return node.callOnInstance(() -> {
            TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
            ByteBuffer value = metadata == null ? null : metadata.params.extensions.get(name);
            return value == null ? null : NetflixTableOptions.decode(value);
        });
    }

    /** Returns the DESCRIBE (CQL) rendering of {@code TABLE} as seen by the given node. */
    private static String describe(IInvokableInstance node)
    {
        return node.callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, TABLE).toCqlString(false, false));
    }
}
