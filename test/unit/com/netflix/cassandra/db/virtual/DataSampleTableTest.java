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
import java.nio.charset.StandardCharsets;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class DataSampleTableTest extends CQLTester
{
    private static final String VT_KS = "datasample_unit_vt";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    private DataSampleTable createVirtualTable()
    {
        return new DataSampleTable(VT_KS);
    }

    @Test
    public void testAccumulatorIsFull()
    {
        DataSampleTable table = createVirtualTable();
        SimpleDataSet ds = new SimpleDataSet(table.metadata());
        DataSampleTable.SampleAccumulator acc = new DataSampleTable.SampleAccumulator(
            ds, 100, "ks", "tbl", "100B", "1d");

        assertFalse(acc.isFull());
        assertEquals(0, acc.accumulatedBytes);
        assertEquals(0, acc.valueIndex);

        // Add a value under the limit
        acc.addValue(ByteBuffer.wrap(new byte[50]));
        assertFalse(acc.isFull());
        assertEquals(50, acc.accumulatedBytes);
        assertEquals(1, acc.valueIndex);

        // Push past the limit
        acc.addValue(ByteBuffer.wrap(new byte[60]));
        assertTrue(acc.isFull());
        assertEquals(110, acc.accumulatedBytes);
        assertEquals(2, acc.valueIndex);
    }

    @Test
    public void testAccumulatorZeroBytesLimit()
    {
        DataSampleTable table = createVirtualTable();
        SimpleDataSet ds = new SimpleDataSet(table.metadata());
        DataSampleTable.SampleAccumulator acc = new DataSampleTable.SampleAccumulator(
            ds, 0, "ks", "tbl", "0B", "1d");

        assertTrue("Zero limit should be immediately full", acc.isFull());
    }

    @Test
    public void testCollectCellValuesFromPartition() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 'hello')");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 'world')");
        flush();

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        DecoratedKey dk = cfs.decorateKey(ByteBuffer.allocate(4).putInt(0, 1));

        DataSampleTable vtable = createVirtualTable();
        SimpleDataSet ds = new SimpleDataSet(vtable.metadata());
        DataSampleTable.SampleAccumulator acc = new DataSampleTable.SampleAccumulator(
            ds, 1024, "ks", "tbl", "1KiB", "1d");

        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), nowInSec, dk);
        try (ReadExecutionController controller = cmd.executionController();
             PartitionIterator partitions = UnfilteredPartitionIterators.filter(cmd.executeLocally(controller), nowInSec))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    vtable.collectCellValues(partition, acc);
                }
            }
        }

        // We inserted 2 rows, each with 1 regular column (v), so expect 2 values
        assertEquals("Should collect 2 cell values", 2, acc.valueIndex);
        assertTrue(acc.accumulatedBytes > 0);

        // Verify the actual values are "hello" and "world"
        long helloBytes = "hello".getBytes(StandardCharsets.UTF_8).length;
        long worldBytes = "world".getBytes(StandardCharsets.UTF_8).length;
        assertEquals(helloBytes + worldBytes, acc.accumulatedBytes);
    }

    @Test
    public void testCollectCellValuesRespectsLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        for (int ck = 0; ck < 100; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (1, ?, ?)", ck, "value-" + ck);
        flush();

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        DecoratedKey dk = cfs.decorateKey(ByteBuffer.allocate(4).putInt(0, 1));

        DataSampleTable vtable = createVirtualTable();
        SimpleDataSet ds = new SimpleDataSet(vtable.metadata());
        // Set a very small limit — should stop before collecting all 100 values
        DataSampleTable.SampleAccumulator acc = new DataSampleTable.SampleAccumulator(
            ds, 30, "ks", "tbl", "30B", "1d");

        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), nowInSec, dk);
        try (ReadExecutionController controller = cmd.executionController();
             PartitionIterator partitions = UnfilteredPartitionIterators.filter(cmd.executeLocally(controller), nowInSec))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    vtable.collectCellValues(partition, acc);
                }
            }
        }

        assertTrue("Should have collected some values", acc.valueIndex > 0);
        assertTrue("Should not have collected all 100 values", acc.valueIndex < 100);
        assertTrue(acc.isFull());
    }

    @Test
    public void testCollectCellValuesSkipsEmptyPartition() throws Throwable
    {
        // Table with no regular columns — rows exist but have no cell values
        createTable("CREATE TABLE %s (pk int PRIMARY KEY)");
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        DecoratedKey dk = cfs.decorateKey(ByteBuffer.allocate(4).putInt(0, 1));

        DataSampleTable vtable = createVirtualTable();
        SimpleDataSet ds = new SimpleDataSet(vtable.metadata());
        DataSampleTable.SampleAccumulator acc = new DataSampleTable.SampleAccumulator(
            ds, 1024, "ks", "tbl", "1KiB", "1d");

        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(cfs.metadata(), nowInSec, dk);
        try (ReadExecutionController controller = cmd.executionController();
             PartitionIterator partitions = UnfilteredPartitionIterators.filter(cmd.executeLocally(controller), nowInSec))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    vtable.collectCellValues(partition, acc);
                }
            }
        }

        assertEquals("No values should be collected from a table with no regular columns", 0, acc.valueIndex);
        assertEquals(0, acc.accumulatedBytes);
        assertFalse(acc.isFull());
    }
}
