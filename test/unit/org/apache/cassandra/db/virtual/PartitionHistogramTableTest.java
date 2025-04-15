package org.apache.cassandra.db.virtual;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.netflix.cassandra.db.virtual.PartitionHistogramTable;
import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class PartitionHistogramTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @SuppressWarnings("FieldCanBeLocal")
    private PartitionHistogramTable table;

    private String dataTable;

    @Before
    public void before() throws Throwable
    {
        table = new PartitionHistogramTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        // Populate test table with sample data to read histogram of
        dataTable = createTable("CREATE TABLE %s (key blob PRIMARY KEY, value blob)");
        for (int i = 0; i < 1000; i++)
        {
            ByteBuffer value = ByteBuffer.wrap(new byte[i]);
            ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(i);
            execute("INSERT INTO %s (key, value) VALUES (?, ?)", key, value);
        }
        Util.flushTable(KEYSPACE, dataTable);

        disablePreparedReuseForTest();
    }

    @Test
    public void testPartitionHistogram()
    {
        ResultSet rs = executeNetWithPaging("SELECT * FROM vts.partition_histogram",
                10);
        List<Row> all = rs.all();
        for (Row row : all)
        {
            if (row.get("table_name", String.class).equalsIgnoreCase(dataTable))
            {
                long minBytes = row.get("min_bytes", Long.class);
                assertEquals(36, minBytes);

                long p50Bytes = row.get("p50_bytes", Long.class);
                assertEquals(642, p50Bytes);

                long p75Bytes = row.get("p75_bytes", Long.class);
                assertEquals(924, p75Bytes);

                long p95Bytes = row.get("p95_bytes", Long.class);
                assertEquals(1109, p95Bytes);

                long p98Bytes = row.get("p98_bytes", Long.class);
                assertEquals(1109, p98Bytes);

                long p99Bytes = row.get("p99_bytes", Long.class);
                assertEquals(1109, p99Bytes);

                long maxBytes = row.get("max_bytes", Long.class);
                assertEquals(1109, maxBytes);
            }
        }
    }
}
