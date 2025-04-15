package com.netflix.cassandra.db.virtual;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;

public class PartitionHistogramTable extends AbstractVirtualTable
{
    private static final String PARTITION_HISTOGRAM = "partition_histogram";
    private static final String MIN = "min_bytes";
    private static final String P50 = "p50_bytes";
    private static final String P75 = "p75_bytes";
    private static final String P95 = "p95_bytes";
    private static final String P98 = "p98_bytes";
    private static final String P99 = "p99_bytes";
    private static final String MAX = "max_bytes";

    public PartitionHistogramTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, PARTITION_HISTOGRAM)
                .comment("Estimates of partition storage footprints in the cluster")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(UTF8Type.instance))
                .addPartitionKeyColumn("keyspace_name", UTF8Type.instance)
                .addClusteringColumn("table_name", UTF8Type.instance)
                .addRegularColumn(MIN, LongType.instance)
                .addRegularColumn(P50, LongType.instance)
                .addRegularColumn(P75, LongType.instance)
                .addRegularColumn(P95, LongType.instance)
                .addRegularColumn(P98, LongType.instance)
                .addRegularColumn(P99, LongType.instance)
                .addRegularColumn(MAX, LongType.instance)
                .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (String keyspace : Schema.instance.getKeyspaces())
        {
            populateData(result, keyspace);
        }
        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        populateData(result, UTF8Type.instance.compose(partitionKey.getKey()));
        return result;
    }

    private void populateData(SimpleDataSet data, String keyspace)
    {
        KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(keyspace);
        if (null == ks || ks.isVirtual())
            return;
        for (TableMetadata tableMetadata : ks.tables)
        {
            ColumnFamilyStore table = Keyspace.openAndGetStore(tableMetadata);
            Long minPartitionSize = table.metric.minPartitionSize.getValue();
            Long maxPartitionSize = table.metric.maxPartitionSize.getValue();
            long[] partitionHistogramData = table.metric.estimatedPartitionSizeHistogram.getValue();

            if (partitionHistogramData == null || partitionHistogramData.length == 0)
                // Skip this table if there is no data for it
                continue;

            EstimatedHistogram partitionHistogram = new EstimatedHistogram(table.metric.estimatedPartitionSizeHistogram.getValue());

            data.row(tableMetadata.keyspace, tableMetadata.name)
                .column(MIN, minPartitionSize)
                .column(P50, partitionHistogram.percentile(.50))
                .column(P75, partitionHistogram.percentile(.75))
                .column(P95, partitionHistogram.percentile(.95))
                .column(P98, partitionHistogram.percentile(.98))
                .column(P99, partitionHistogram.percentile(.99))
                .column(MAX, maxPartitionSize);
        }
    }
}
