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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.fail;

public class HLLTableTest extends TestBaseImpl
{
    public static final Logger logger = LoggerFactory.getLogger(HLLTableTest.class);

    @Test
    public void testMultiSSTables() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY, v int)");
            for (int i = 0; i < 100; i++)
            {
                if (i % 10 == 0)
                    cluster.get(1).flush(KEYSPACE);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, v) VALUES (?, ?)", ConsistencyLevel.QUORUM, i, i);
            }
            cluster.get(1).flush(KEYSPACE);
            SimpleQueryResult result = cluster.get(1).executeInternalWithResult("SELECT * FROM netflix_views.internal_table_hll WHERE " +
                                                                                "keyspace_name = '" + KEYSPACE + "' AND table_name = 'tbl'");
            while(result.hasNext())
            {
                Row row = result.next();
                HyperLogLogPlus hll = HyperLogLogPlus.Builder.build(((ByteBuffer) row.get(2)).array());
                Assert.assertEquals(100, hll.cardinality());
            }
        }
    }

    @Test
    public void testDistributed() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(c -> c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl2 (pk int PRIMARY KEY, v int)");
            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl2 (pk, v) VALUES (?, ?)", ConsistencyLevel.QUORUM, i, i);
            }
            cluster.get(1).flush(KEYSPACE);
            SimpleQueryResult result = cluster.get(1).executeInternalWithResult("SELECT * FROM netflix_views.partition_count WHERE " +
                                                                                "keyspace_name = '" + KEYSPACE + "' AND table_name = 'tbl2'");
            while(result.hasNext())
            {
                Row row = result.next();
                Assert.assertEquals(100L, row.getLong("count").longValue());
            }
            // same when hitting cache
            result = cluster.get(1).executeInternalWithResult("SELECT * FROM netflix_views.partition_count WHERE " +
                                                                                "keyspace_name = '" + KEYSPACE + "' AND table_name = 'tbl2'");
            while(result.hasNext())
            {
                Row row = result.next();
                Assert.assertEquals(100L, row.getLong("count").longValue());
            }

            try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
                 Session s = c.connect())
            {
                try
                {
                    s.execute("SELECT * FROM netflix_views.partition_count WHERE keyspace_name = '" + KEYSPACE + "' AND table_name = 'notexits'");
                    fail("Should have thrown InvalidQueryException");
                }
                catch (InvalidQueryException e)
                {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("Table notexits does not exist in keyspace"));
                }
                try
                {
                    s.execute("SELECT * FROM netflix_views.partition_count WHERE keyspace_name = 'netflix_views' AND table_name = 'notexits'");
                    fail("Should have thrown InvalidQueryException");
                }
                catch (InvalidQueryException e)
                {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("Table notexits does not exist in keyspace"));
                }
                try
                {
                    s.execute("SELECT * FROM netflix_views.partition_count WHERE keyspace_name = 'none' AND table_name = 'notexits'");
                    fail("Should have thrown InvalidQueryException");
                }
                catch (InvalidQueryException e)
                {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("Keyspace none does not exist"));
                }
            }
        }
    }
}
