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

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class RepairNoPurgeTombstonesTest extends TestBaseImpl
{
    /**
     * A tombstone that is past gc_grace_seconds and present on only one replica (with no matching data on
     * the other) is dropped during validation, so a regular repair sees matching merkle trees and never
     * streams it. The missing tombstone lets an older write resurrect on the replica that lacks it.
     * <p>
     * --no-purge-tombstones keeps the tombstone in the merkle tree so the divergence is detected and the
     * tombstone is streamed to the other replica, where it correctly shadows the stale write.
     */
    @Test
    public void testNoPurgeTombstonesPropagatesExpiredTombstone() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start()))
        {
            // gc_grace_seconds=0 makes the tombstone immediately purgeable during validation. The "control"
            // table is repaired normally, the "feature" table is repaired with --no-purge-tombstones.
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".control (k INT PRIMARY KEY, v INT) WITH gc_grace_seconds = 0 AND read_repair = 'NONE'");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".feature (k INT PRIMARY KEY, v INT) WITH gc_grace_seconds = 0 AND read_repair = 'NONE'");

            // disable autocompaction so the only purging that can happen is during repair validation
            cluster.forEach(i -> i.runOnInstance(() -> {
                Keyspace keyspace = Keyspace.open(KEYSPACE);
                keyspace.getColumnFamilyStore("control").disableAutoCompaction();
                keyspace.getColumnFamilyStore("feature").disableAutoCompaction();
            }));

            // a partition tombstone exists on node1 only, with no matching data on node2; it is past gc_grace
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".control USING TIMESTAMP 2000 WHERE k = 1");
            cluster.get(1).executeInternal("DELETE FROM " + KEYSPACE + ".feature USING TIMESTAMP 2000 WHERE k = 1");
            cluster.get(1).flush(KEYSPACE);

            // gc_grace_seconds=0 makes a tombstone purgeable once its local deletion time is strictly in the
            // past; wait a couple of seconds so validation (which compares localDeletionTime < now) will purge it
            Thread.sleep(2000);

            // a regular repair purges the tombstone during validation, so the trees match and nothing is streamed
            cluster.get(1).nodetoolResult("repair", "--full", KEYSPACE, "control").asserts().success();

            // --no-purge-tombstones keeps the tombstone in the tree, so node1 and node2 differ and it is streamed to node2
            cluster.get(1).nodetoolResult("repair", "--full", "--no-purge-tombstones", KEYSPACE, "feature").asserts().success();

            // a stale write (older than the tombstone) lands on node2 only
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".control (k, v) VALUES (1, 99) USING TIMESTAMP 1000");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".feature (k, v) VALUES (1, 99) USING TIMESTAMP 1000");

            // control: the tombstone was not propagated, so the stale write resurrects on node2
            assertRows(cluster.get(2).executeInternal("SELECT k, v FROM " + KEYSPACE + ".control WHERE k = 1"), row(1, 99));
            // feature: the tombstone was propagated, so it shadows the stale write on node2
            assertRows(cluster.get(2).executeInternal("SELECT k, v FROM " + KEYSPACE + ".feature WHERE k = 1"));
        }
    }
}
