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
package org.apache.cassandra.db.virtual;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

final class SSTableDumpTable extends AbstractIteratingTable
{
    private static final String KEYSPACE_NAME = "keyspace_name";
    private final static String TABLE_NAME = "table_name";
    private static final String ID = "id";
    private static final String PK = "partition_key";
    private static final String CK = "clustering";
    private static final IPartitioner partitioner = new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance));

    private static final String RAW = "raw";

    SSTableDumpTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "sstable_dump")
                           .comment("current sstables and debug dump of their data")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE_NAME, UTF8Type.instance)
                           .partitioner(partitioner)
                           .addClusteringColumn(ID, Int32Type.instance)
                           .addClusteringColumn(PK, UTF8Type.instance)
                           .addClusteringColumn(CK, UTF8Type.instance)
                           .addRegularColumn(RAW, UTF8Type.instance)
                           .build());
    }

    private String getKeyspace(DecoratedKey key)
    {
        CompositeType type = (CompositeType) metadata.partitionKeyType;
        ByteBuffer[] bb = type.split(key.getKey());

        return type.types.get(0).getString(bb[0]);
    }

    private String getTable(DecoratedKey key)
    {
        CompositeType type = (CompositeType) metadata.partitionKeyType;
        ByteBuffer[] bb = type.split(key.getKey());
        return type.types.get(1).getString(bb[1]);
    }

    private DecoratedKey makeKey(String keyspace, String table)
    {
        ByteBuffer partitionKey = ((CompositeType) metadata.partitionKeyType).decompose(keyspace, table);
        return metadata.partitioner.decorateKey(partitionKey);
    }

    protected boolean hasKey(DecoratedKey key)
    {
        return Schema.instance.getTableMetadata(getKeyspace(key), getTable(key)) != null;
    }

    public Iterator<DecoratedKey> getPartitionKeys(DataRange range)
    {
        Iterator<ColumnFamilyStore> cfs = Ordering.natural().sortedCopy(ColumnFamilyStore.all()).iterator();
        return new AbstractIterator<DecoratedKey>()
        {
            protected DecoratedKey computeNext()
            {
                if (!cfs.hasNext())
                    return endOfData();
                ColumnFamilyStore next = cfs.next();
                return makeKey(next.keyspace.getName(), next.name);
            }
        };
    }

    protected Iterator<Row> getRows(boolean isReversed, DecoratedKey key, RegularAndStaticColumns columns)
    {

        ColumnFamilyStore cfs = Schema.instance.getKeyspaceInstance(getKeyspace(key))
                                               .getColumnFamilyStore(getTable(key));
        List<SSTableReader> generations = Lists.newArrayList(cfs.getSSTables(SSTableSet.CANONICAL));
        Collections.sort(generations, Comparator.comparingInt(s -> s.descriptor.generation));
        if (isReversed)
            Collections.reverse(generations);
        Iterator<SSTableReader> iter = generations.iterator();
        return new AbstractIterator<Row>()
        {
            SSTableReader current = null;
            ISSTableScanner scanner = null;
            UnfilteredRowIterator partition = null;
            boolean checkedStatic = false;

            private String pk()
            {
                Preconditions.checkNotNull(current, "Current sstable cannot be null");
                Preconditions.checkNotNull(partition, "Partition cannot be null");

                return current.metadata().partitionKeyType.getString(partition.partitionKey().getKey());
            }

            protected Row computeNext()
            {
                // return any static rows for partition first
                if (!checkedStatic && partition != null && !partition.staticRow().isEmpty())
                {
                    checkedStatic = true;
                    Unfiltered row = partition.staticRow();
                    return row(current.descriptor.generation, pk(), "")
                           .add(RAW, row.toString(current.metadata(), false, true))
                           .build(columns);
                }

                // return any rows left in the partition
                if (partition != null && partition.hasNext())
                {
                    Unfiltered row = partition.next();
                    String ck = row.clustering().toString(current.metadata());
                    return row(current.descriptor.generation, pk(), ck)
                           .add(RAW, row.toString(current.metadata(), false, true))
                           .build(columns);
                }

                // find next partition
                if (scanner != null && scanner.hasNext())
                {
                    partition = scanner.next();
                    checkedStatic = false;

                    // if its a partition level deletion return that first
                    if (!partition.partitionLevelDeletion().isLive())
                    {
                        String raw = partition.partitionLevelDeletion().toString();
                        return row(current.descriptor.generation, pk(), "")
                               .add(RAW, raw)
                               .build(columns);
                    }
                    return computeNext();
                }

                // find next sstable
                if (iter.hasNext())
                {
                    current = iter.next();
                    scanner = current.getScanner();
                    return computeNext();
                }
                return endOfData();
            }
        };
    }
}
