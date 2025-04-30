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

package org.apache.cassandra.schema;

import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class DeterministicTableIdTest extends CQLTester
{
    private static final String KEYSPACE = "ks_deterministic_table_id";
    private static final String TABLE = "tb_deterministic_table_id";

    private static final String CREATE_TABLE_QUERY_1 = String.format("CREATE TABLE %s.%s (key text, c1 text, c2 text, c3 text, PRIMARY KEY (key, c1));", KEYSPACE, TABLE);

    private static final String CREATE_TABLE_QUERY_1_DEFAULT_PARAMS = String.format("CREATE TABLE %s.%s (KEY text, C1 text, C2 text, C3 text, PRIMARY KEY (KEY, C1))" +
                                                                                    " WITH compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}", KEYSPACE, TABLE);

    private static final String CREATE_TABLE_QUERY_2 = String.format("CREATE TABLE %s.%s (key text, c1 text, c2 text, c3 text, PRIMARY KEY (key, c1))" +
                                                                     " WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'};", KEYSPACE, TABLE);

    private static final String DROP_TABLE_QUERY = String.format("DROP TABLE IF EXISTS %s.%s;", KEYSPACE, TABLE);


    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        DatabaseDescriptor.useDeterministicTableID(true);
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
    }

    @AfterClass
    public static void afterClass()
    {
        schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    @After
    public void afterTest()
    {
        schemaChange(DROP_TABLE_QUERY);
    }

    @Test
    public void knownIdTest()
    {
        UUID tableId1 = createTableAndGetTableId(CREATE_TABLE_QUERY_1);
        // generated off v4.1.50, if this is different it means we open ourselves to data loss in mixed mode
        assertEquals("Table ID should be 7728cc44-1aa5-3b51-838e-22f67773f03f", UUID.fromString("7728cc44-1aa5-3b51-838e-22f67773f03f"), tableId1);
    }

    @Test
    public void createTableWithSameQueryTest()
    {
        UUID tableId1 = createTableAndGetTableId(CREATE_TABLE_QUERY_1);

        // Drop and create the same table and get the table id
        schemaChange(DROP_TABLE_QUERY);
        UUID tableId2 = createTableAndGetTableId(CREATE_TABLE_QUERY_1);

        assertThat(tableId1, equalTo(tableId2));
    }

    @Test
    public void createTableWithSameQueryTest2()
    {
        UUID tableId1 = createTableAndGetTableId(CREATE_TABLE_QUERY_1);

        schemaChange(DROP_TABLE_QUERY);

        // Create the same table with syntacically different but semantically the same quries.
        // It's expected the two CREATE TABLE queries generate the same table id
        UUID tableId2 = createTableAndGetTableId(CREATE_TABLE_QUERY_1_DEFAULT_PARAMS);

        assertThat(tableId1, equalTo(tableId2));
    }

    @Test
    public void createTableWithDifferentQueriesTest()
    {
        UUID tableId1 = createTableAndGetTableId(CREATE_TABLE_QUERY_1);

        schemaChange(DROP_TABLE_QUERY);
        UUID tableId2 = createTableAndGetTableId(CREATE_TABLE_QUERY_2);

        assertThat(tableId1, not(equalTo(tableId2)));
    }

    @Test
    public void createTableWithTimeUUIDTest()
    {
        try
        {
            DatabaseDescriptor.useDeterministicTableID(false);

            UUID tableId1 = createTableAndGetTableId(CREATE_TABLE_QUERY_1);

            schemaChange(DROP_TABLE_QUERY);
            UUID tableId2 = createTableAndGetTableId(CREATE_TABLE_QUERY_1);

            assertThat(tableId1, not(equalTo(tableId2)));
        }
        finally
        {
            DatabaseDescriptor.useDeterministicTableID(true);
        }
    }

    @Test
    public void persistTableIdTest() throws Throwable
    {
        UUID tableId = createTableAndGetTableId(CREATE_TABLE_QUERY_1);
        String query = String.format("SELECT id FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s' ALLOW FILTERING", KEYSPACE, TABLE);

        // table id should be the same for both in-memory CFMetaData and persisted in system_schema.tables
        assertRows(execute(query), row(tableId));
    }

    private UUID createTableAndGetTableId(String createTableQuery)
    {
        schemaChange(createTableQuery);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        return cfs.metadata.id.asUUID();
    }
}
