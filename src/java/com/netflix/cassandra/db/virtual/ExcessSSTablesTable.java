package com.netflix.cassandra.db.virtual;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static java.util.stream.Collectors.toList;

public class ExcessSSTablesTable extends AbstractVirtualTable
{
    private static final String EXCESS_SSTABLES = "excess_sstables";
    private static final String KEYSPACE_NAME = "keyspace_name";
    private static final String TABLE_NAME = "table_name";
    private static final String LEVEL = "level";

    ExcessSSTablesTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, EXCESS_SSTABLES)
                           .comment("Excess sstables by table and level")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addClusteringColumn(LEVEL, Int32Type.instance)
                           .addRegularColumn(EXCESS_SSTABLES, Int32Type.instance)
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
        for (TableMetadata table : ks.tables)
        {
            List<Integer> excessSSTables = getExcessSSTableCountByLevel(table);
            IntStream.range(0, excessSSTables.size())
                     .filter(i -> excessSSTables.get(i) > 0)
                     .forEach(
                     i -> data.row(table.keyspace, table.name, i).column(EXCESS_SSTABLES, excessSSTables.get(i)));
        }
    }

    private List<Integer> getExcessSSTableCountByLevel(TableMetadata tableMetadata)
    {
        ColumnFamilyStore table = Keyspace.openAndGetStore(tableMetadata);
        int[] leveledSStables = table.getSSTableCountPerLevel();
        if (leveledSStables == null)
            return new ArrayList<>();
        return IntStream.range(0, leveledSStables.length)
                        .mapToObj(level -> {
                            int maxCount = level == 0 ? 4 : (int) Math.pow(table.getLevelFanoutSize(), level);
                            return Math.max(0, leveledSStables[level] - maxCount);
                        })
                        .collect(toList());
    }
}