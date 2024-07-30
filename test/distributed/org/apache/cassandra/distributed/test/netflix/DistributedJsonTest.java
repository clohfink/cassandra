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

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;


public class DistributedJsonTest extends TestBaseImpl
{
    public static final Logger logger = LoggerFactory.getLogger(DistributedJsonTest.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testMultiplePartitionKeys() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            SimpleQueryResult result = cluster.get(1).executeInternalWithResult("SELECT * FROM netflix_views.cluster_view WHERE " +
                                                                                "keyspace_name = 'system_views' AND table_name = 'partition_count'");
            while(result.hasNext())
            {
                Row row = result.next();
                // verify its parsable
                mapper.readTree(row.getString("value"));
            }
        }
    }

    @Test
    public void testSinglePartitionKeys() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            SimpleQueryResult result = cluster.get(1).executeInternalWithResult("SELECT * FROM netflix_views.cluster_view WHERE " +
                                                                                "keyspace_name = 'system_views' AND table_name = 'system_properties'");
            while(result.hasNext())
            {
                Row row = result.next();
                // verify its parsable
                mapper.readTree(row.getString("value"));
            }
        }
    }

    @Test
    public void testComplexDataTypes() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            SimpleQueryResult result = cluster.get(1).executeInternalWithResult("SELECT * FROM netflix_views.cluster_view WHERE " +
                                                                                "keyspace_name = 'system' AND table_name = 'peers_v2'");
            while(result.hasNext())
            {
                Row row = result.next();
                // verify its parsable
                mapper.readTree(row.getString("value"));
            }
        }
    }

    @Test
    public void testCompositeClusteringKeys() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).start()))
        {
            SimpleQueryResult result = cluster.get(1).executeInternalWithResult("SELECT * FROM netflix_views.cluster_view WHERE " +
                                                                                "keyspace_name = 'system_schema' AND table_name = 'columns'");
            while(result.hasNext())
            {
                Row row = result.next();
                // verify its parsable
                mapper.readTree(row.getString("value"));
            }
        }
    }
}
