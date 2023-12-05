package com.netflix.cassandra.db.virtual;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyIterator;
import static org.apache.cassandra.dht.AbstractBounds.strictlyWrapsAround;

public class PartitionCountTable implements VirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionCountTable.class);

    protected TableMetadata metadata;
    private static final String TABLE = "table_name";
    private static final String KEYSPACE = "keyspace_name";
    private static final String START = "range_start";
    private static final String END = "range_end";
    private static final String VALUE = "value";

    public static final String TABLE_NAME = "partition_count";

    private static final CompositeType PARTITION_TYPE = CompositeType.getInstance(
            UTF8Type.instance, UTF8Type.instance, LongType.instance, LongType.instance
    );

    PartitionCountTable(String keyspace)
    {
        this.metadata = TableMetadata.builder(keyspace, TABLE_NAME)
                .comment("partition count of ranges in table")
                .kind(TableMetadata.Kind.VIRTUAL)
                .partitioner(new LocalPartitioner(PARTITION_TYPE))
                .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                .addPartitionKeyColumn(TABLE, UTF8Type.instance)
                .addPartitionKeyColumn(START, LongType.instance)
                .addPartitionKeyColumn(END, LongType.instance)
                .addRegularColumn(VALUE, LongType.instance)
                .build();
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public void truncate()
    {
        throw new InvalidRequestException("Truncation is not supported by table " + metadata);
    }


    @Override
    public UnfilteredPartitionIterator select(DecoratedKey partitionKey, ClusteringIndexFilter clusteringIndexFilter, ColumnFilter columnFilter)
    {
        if (!DatabaseDescriptor.getPartitioner().equals(Murmur3Partitioner.instance))
        {
            throw new InvalidRequestException("Only Murmur3 partitioner is supported");
        }
        ByteBuffer[] keys = PARTITION_TYPE.split(partitionKey.getKey());
        String keyspace = UTF8Type.instance.compose(keys[0]);
        String table = UTF8Type.instance.compose(keys[1]);

        PartitionPosition startKey = new BufferDecoratedKey(new Murmur3Partitioner.LongToken(LongType.instance.compose(keys[2])), ByteBufferUtil.EMPTY_BYTE_BUFFER);
        PartitionPosition endKey = new BufferDecoratedKey(new Murmur3Partitioner.LongToken(LongType.instance.compose(keys[3])), ByteBufferUtil.EMPTY_BYTE_BUFFER);

        if (strictlyWrapsAround(startKey, endKey))
        {
            throw new InvalidRequestException("range_start must be < range_end");
        }

        Bounds bounds = new Bounds(startKey, endKey);

        ColumnFamilyStore cf = ColumnFamilyStore.getIfExists(keyspace, table);
        if (cf == null)
        {
            throw new InvalidRequestException("Unknown keyspace/table " + keyspace + "/" + table);
        }
        ICardinality cardinality = new HyperLogLogPlus(14, 25);
        try (ColumnFamilyStore.RefViewFragment view = cf.selectAndReference(View.selectLive(bounds)))
        {
            for (SSTableReader sstable: view.sstables)
            {
                if (sstable.getIndexFile() == null || sstable.openReason == SSTableReader.OpenReason.EARLY)
                    continue;

                long sampledPosition = sstable.getIndexScanPosition(startKey);
                byte[] buffer = new byte[1024];
                ByteBuffer indexKey = ByteBuffer.wrap(buffer);
                String path = null;
                try (FileDataInput in = sstable.getIndexFile().createReader(sampledPosition))
                {
                    path = in.getPath();
                    while (!in.isEOF())
                    {
                        int len = in.readUnsignedShort();
                        if (len > buffer.length)
                        {
                            buffer = new byte[len];
                            indexKey = ByteBuffer.wrap(buffer);
                        }
                        in.readFully(buffer, 0, len);
                        indexKey.position(0);
                        indexKey.limit(len);
                        DecoratedKey indexDecoratedKey = sstable.decorateKey(indexKey);
                        if (indexDecoratedKey.compareTo(startKey) >= 0
                                && indexDecoratedKey.compareTo(endKey) <= 0)
                        {
                            cardinality.offerHashed(((Murmur3Partitioner.LongToken) indexDecoratedKey.getToken()).token);
                        }
                        RowIndexEntry.Serializer.skip(in, sstable.descriptor.version);
                    }
                }
                catch (IOException e)
                {
                    throw new CorruptSSTableException(e, path);
                }
            }
        }

        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        RegularAndStaticColumns columns = columnFilter.queriedColumns();
        columns.regulars.forEach(cm -> {
            builder.addCell(BufferCell.live(cm, 1, LongType.instance.decompose(cardinality.cardinality())));
        });

        return new SingletonUnfilteredPartitionIterator(UnfilteredRowIterators.singleton(
                builder.build(),
                metadata,
                partitionKey,
                DeletionTime.LIVE,
                columnFilter.queriedColumns(),
                Rows.EMPTY_STATIC_ROW,
                false,
                EncodingStats.NO_STATS
        ));
    }

    @Override
    public UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        throw new InvalidRequestException("Range queries not supported on " + metadata);
    }
}

