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

package com.netflix.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public abstract class ScopedTable implements VirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(ScopedTable.class);

    protected final TableMetadata metadata;

    protected ScopedTable(TableMetadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Updates are not supported by table " + metadata);
    }

    public abstract UnfilteredRowIterator select(DecoratedKey partitionKey, String keyspace, String table);

    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter)
    {
        ByteBuffer[] key = ((CompositeType) this.metadata.partitionKeyType).split(partitionKey.getKey());
        String keyspace = UTF8Type.instance.getString(key[0]);
        String table = UTF8Type.instance.getString(key[1]);
        // verify keyspace and table exists
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        if (ksm == null)
        {
            throw invalidRequest("Keyspace %s does not exist", keyspace);
        }
        TableMetadata metadata = ksm.getTableOrViewNullable(table);
        if (metadata == null)
        {
            throw invalidRequest("Table %s does not exist in keyspace %s", table, keyspace);
        }
        return new SingletonUnfilteredPartitionIterator(select(partitionKey, keyspace, table));
    }

    @Override
    public UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        throw new InvalidRequestException("Range queries are not supported by table, keyspace_name and table_name must be included in query" + metadata);
    }


    protected Map<InetAddressAndPort, Future<Message<ReadResponse>>> sendReadCommandToAllEndpoints(ReadCommand read) {
        Set<InetAddressAndPort> allEndpoints = StorageService.instance.getLiveRingMembers(true);
        Map<InetAddressAndPort, Future<Message<ReadResponse>>> results = Maps.newHashMap();
        for (InetAddressAndPort endpoint : allEndpoints) {
            Message<ReadCommand> m = Message.out(read.verb(), read);
            results.put(endpoint, MessagingService.instance().<ReadResponse>sendWithResult(m, endpoint));
        }
        return results;
    }

    protected void waitForFutures(Map<InetAddressAndPort, Future<Message<ReadResponse>>> results, long startTime, long timeout) {
        boolean allDone;
        do {
            allDone = true;
            for (Future<?> future : results.values()) {
                if (!future.isDone() && Clock.Global.currentTimeMillis() - startTime < timeout) {
                    allDone = false;
                    Uninterruptibles.sleepUninterruptibly(30, TimeUnit.MILLISECONDS);
                    break;
                }
            }
        } while (!allDone && Clock.Global.currentTimeMillis() - startTime < timeout);
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException("Truncation is not supported by table " + metadata);
    }
}
