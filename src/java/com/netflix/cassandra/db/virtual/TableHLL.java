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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class TableHLL extends ScopedTable
{
    private static final Logger logger = LoggerFactory.getLogger(TableHLL.class);

    public static final String NAME = "internal_table_hll";
    protected TableHLL(String keyspace)
    {
        super(TableMetadata.builder(keyspace, NAME)
                           .comment("Internal table for HLL cardinality estimation")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn("keyspace_name", UTF8Type.instance)
                           .addPartitionKeyColumn("table_name", UTF8Type.instance)
                           .addRegularColumn("value", BytesType.instance)
                           .build());
    }

    @Override
    public UnfilteredRowIterator select(DecoratedKey partitionKey, String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        // see MetadataCollector.cardinality
        ICardinality base = new HyperLogLogPlus(13, 25);
        try (ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
        {
            for (SSTableReader sstable : refViewFragment.sstables)
            {
                if (sstable.openReason == SSTableReader.OpenReason.EARLY)
                    continue;
                CompactionMetadata metadata = (CompactionMetadata) sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, MetadataType.COMPACTION);
                if (metadata != null)
                {
                    base = base.merge(metadata.cardinalityEstimator);
                }
            }
        }
        catch (IOException | CardinalityMergeException e)
        {
            throw new RuntimeException(e);
        }
        final ICardinality baseFinal = base;
        AtomicBoolean done = new AtomicBoolean(false);
        return new AbstractUnfilteredRowIterator(metadata, partitionKey, DeletionTime.LIVE,
                                                 metadata.regularAndStaticColumns(), Rows.EMPTY_STATIC_ROW,
                                                 false, EncodingStats.NO_STATS)
        {
            @Override
            protected Unfiltered computeNext()
            {
                if (!done.get())
                {
                    done.set(true);
                    Row.Builder row = BTreeRow.sortedBuilder();
                    row.newRow(Clustering.EMPTY);
                    ColumnMetadata def = metadata.regularColumns().getSimple(0);
                    ByteBuffer bb = null;
                    try
                    {
                        bb = ByteBuffer.wrap(baseFinal.getBytes());
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                    BufferCell buf = new BufferCell(def, 1L, BufferCell.NO_TTL, BufferCell.NO_DELETION_TIME, bb, null);
                    row.addCell(buf);
                    return row.build();
                }
                else
                {
                    return endOfData();
                }
            }
        };
    }
}
