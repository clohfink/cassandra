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
package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.junit.Assert.*;

public class DataSampleTest extends TestBaseImpl
{
    private static Cluster CLUSTER;
    private static final String TEST_KEYSPACE = "datasample_test_ks";
    private static final String TEST_TABLE = "test_table";

    /** All values we inserted, as raw UTF-8 bytes for comparison with sampled BLOBs */
    private static final Set<ByteBuffer> INSERTED_VALUES = new HashSet<>();

    @BeforeClass
    public static void setup() throws IOException
    {
        CLUSTER = init(Cluster.build(1).start());

        CLUSTER.schemaChange("CREATE KEYSPACE " + TEST_KEYSPACE +
                             " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        CLUSTER.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE +
                             " (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        for (int pk = 0; pk < 50; pk++)
        {
            for (int ck = 0; ck < 5; ck++)
            {
                String value = "value-" + pk + "-" + ck;
                CLUSTER.coordinator(1).execute(
                    "INSERT INTO " + TEST_KEYSPACE + "." + TEST_TABLE + " (pk, ck, v) VALUES (?, ?, ?)",
                    ConsistencyLevel.ALL, pk, ck, value);
                INSERTED_VALUES.add(ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
            }
        }

        CLUSTER.get(1).nodetoolResult("flush", TEST_KEYSPACE).asserts().success();
    }

    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testEveryReturnedValueWasInserted()
    {
        Object[][] rows = CLUSTER.get(1).executeInternal(
            "SELECT * FROM netflix_views.data_sample" +
            " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
            TEST_KEYSPACE, TEST_TABLE, "1MiB", "10d");

        assertNotNull("Should return results", rows);
        assertTrue("Should return at least one row", rows.length > 0);

        for (Object[] row : rows)
        {
            // Row layout: keyspace_name, table_name, bytes_limit, lookback, value_index, data
            ByteBuffer data = (ByteBuffer) row[5];
            assertNotNull("data column should not be null", data);
            assertTrue("Every returned value must be a value that was inserted, got: " +
                       StandardCharsets.UTF_8.decode(data.duplicate()),
                       INSERTED_VALUES.contains(data));
        }
    }

    @Test
    public void testDataSampleRespectsLookback()
    {
        Object[][] rows = CLUSTER.get(1).executeInternal(
            "SELECT * FROM netflix_views.data_sample" +
            " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
            TEST_KEYSPACE, TEST_TABLE, "1MiB", "0s");

        assertTrue("Should return no rows for 0s lookback",
                   rows == null || rows.length == 0);
    }

    @Test
    public void testDataSampleRespectsBytesLimit()
    {
        // Use a very small limit — should get only a few values
        Object[][] smallRows = CLUSTER.get(1).executeInternal(
            "SELECT * FROM netflix_views.data_sample" +
            " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
            TEST_KEYSPACE, TEST_TABLE, "64B", "10d");

        Object[][] largeRows = CLUSTER.get(1).executeInternal(
            "SELECT * FROM netflix_views.data_sample" +
            " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
            TEST_KEYSPACE, TEST_TABLE, "1MiB", "10d");

        assertNotNull(smallRows);
        assertNotNull(largeRows);
        assertTrue("Small limit should return fewer rows than large limit",
                   smallRows.length < largeRows.length);
    }

    @Test(timeout = 30_000)
    public void testMaxAttemptsTerminatesWhenNoValuesToSample()
    {
        // Create a table with only primary key columns and no regular columns.
        // Rows exist on disk but have zero cell values to emit, so accumulatedBytes
        // can never reach bytesLimit. Without the max-attempts cap this would loop forever.
        String noValuesTable = "no_values_table";
        CLUSTER.schemaChange("CREATE TABLE " + TEST_KEYSPACE + "." + noValuesTable +
                             " (pk int PRIMARY KEY)");

        for (int pk = 0; pk < 100; pk++)
        {
            CLUSTER.coordinator(1).execute(
                "INSERT INTO " + TEST_KEYSPACE + "." + noValuesTable + " (pk) VALUES (?)",
                ConsistencyLevel.ALL, pk);
        }
        CLUSTER.get(1).nodetoolResult("flush", TEST_KEYSPACE, noValuesTable).asserts().success();

        // The important assertion is that this returns at all (no infinite loop).
        // The @Test(timeout = 30_000) ensures the test fails if we hang.
        Object[][] rows = CLUSTER.get(1).executeInternal(
            "SELECT * FROM netflix_views.data_sample" +
            " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
            TEST_KEYSPACE, noValuesTable, "1MiB", "10d");

        assertTrue("Should return no values from a table with no regular columns",
                   rows == null || rows.length == 0);
    }

    @Test
    public void testDataSampleRejectsVirtualKeyspace()
    {
        try
        {
            CLUSTER.get(1).executeInternal(
                "SELECT * FROM netflix_views.data_sample" +
                " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
                "netflix_views", "data_sample", "1MiB", "10d");
            fail("Expected InvalidRequestException for virtual keyspace");
        }
        catch (Exception e)
        {
            assertTrue("Expected InvalidRequestException but got " + e.getClass().getName(),
                       e.getClass().getName().endsWith("InvalidRequestException"));
            assertTrue(e.getMessage().contains("virtual"));
        }
    }

    @Test
    public void testDataSampleInvalidKeyspace()
    {
        try
        {
            CLUSTER.get(1).executeInternal(
                "SELECT * FROM netflix_views.data_sample" +
                " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
                "nonexistent_ks", TEST_TABLE, "1MiB", "10d");
            fail("Expected InvalidRequestException");
        }
        catch (Exception e)
        {
            assertTrue("Expected InvalidRequestException but got " + e.getClass().getName(),
                       e.getClass().getName().endsWith("InvalidRequestException"));
        }
    }

    @Test
    public void testDataSampleInvalidTable()
    {
        try
        {
            CLUSTER.get(1).executeInternal(
                "SELECT * FROM netflix_views.data_sample" +
                " WHERE keyspace_name = ? AND table_name = ? AND bytes_limit = ? AND lookback = ?",
                TEST_KEYSPACE, "nonexistent_table", "1MiB", "10d");
            fail("Expected InvalidRequestException");
        }
        catch (Exception e)
        {
            assertTrue("Expected InvalidRequestException but got " + e.getClass().getName(),
                       e.getClass().getName().endsWith("InvalidRequestException"));
        }
    }
}
