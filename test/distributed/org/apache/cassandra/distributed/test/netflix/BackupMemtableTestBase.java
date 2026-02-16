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

package org.apache.cassandra.distributed.test.netflix;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import com.netflix.cassandra.backups.AwsAsyncS3FakeBackup;
import com.netflix.cassandra.backups.BackupMemtableParams;
import com.netflix.cassandra.backups.ObjectStoreAccess;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.util.File;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertEquals;

public abstract class BackupMemtableTestBase extends TestBaseImpl
{
    protected static final String[] CLUSTERING_ORDERS = { "ASC", "DESC" };

    protected static final Map<String, String> envVars = new HashMap<>(Map.of(
        BackupMemtableParams.NETFLIX_ENVIRONMENT, "test",
        BackupMemtableParams.NETFLIX_REGION, "us-east-1",
        BackupMemtableParams.NETFLIX_APP, "testapp"
    ));

    protected static final Map<String, String> configParameters = new HashMap<>(Map.of(
        BackupMemtableParams.TOKEN, "-1"
    ));

    protected static void assertQueryResults(Object[][] actual, Object[][] expected)
    {
        assertEquals("Number of rows", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++)
        {
            assertEquals("Row " + i, expected[i].length, actual[i].length);
            for (int j = 0; j < expected[i].length; j++)
            {
                assertEquals("Row " + i + ", Column " + j, expected[i][j], actual[i][j]);
            }
        }
    }

    protected static String getBackups(Cluster cluster)
    {
        cluster.get(1).nodetool("flush", "test");
        return cluster.get(1).callOnInstance(
        () -> ColumnFamilyStore.getIfExists("test", "test_table")
                               .snapshot("s3backup")
                               .getDirectories()
                               .stream()
                               .map(File::toString)
                               .findFirst().orElseThrow(() -> new RuntimeException("No snapshot directory found"))
        );
    }

    /**
     * ID - partition key, sub_id - clustering key. Both form a primary key and are associated with the value.
     */
    public static void setupTable(Cluster cluster, String order)
    {
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS test.test_table (id int, sub_id int, value text, PRIMARY KEY (id, sub_id)) WITH CLUSTERING ORDER BY (sub_id " + order + ')');
    }

    public static void insertTestData(Cluster cluster)
    {
        // Insert some data
        for (int i = 0; i < 100; i++)
        {
            for(int j = 0; j < 5; j++) {
                cluster.coordinator(1).execute("INSERT INTO test.test_table (id, sub_id, value) VALUES (?, ?, ?)", ConsistencyLevel.ALL, i, j, "test " + i + '-' + j);
            }
        }
        cluster.get(1).nodetool("flush", "test");
    }

    public static void setupBackupMemtable(Cluster cluster, AwsAsyncS3FakeBackup.Injection<?> ... injections) throws IOException
    {
        final String snapshotDir = getBackups(cluster);
        mockS3Access(snapshotDir, cluster, cluster.size(), injections);
        cluster.schemaChange("ALTER TABLE test.test_table WITH memtable = 'backupmemtable:bucket=testbucket,NETFLIX_REGION=us-east-1,NETFLIX_ENVIRONMENT=test,token=-1,NETFLIX_APP=testapp'");
        cluster.get(1).coordinator().execute("TRUNCATE test.test_table", ConsistencyLevel.ALL);
        // Delete backups to reduce chance of interference.
        FileUtils.deleteDirectory(new File(snapshotDir).toJavaIOFile());
    }

    public static void mockS3Access(String snapshotDir, Cluster cluster, int nodes, AwsAsyncS3FakeBackup.Injection<?> ... injections)
    {
        AwsAsyncS3FakeBackup fakeBackup = new AwsAsyncS3FakeBackup(envVars, Region.US_EAST_1);
        fakeBackup.initializeFileSystemState(configParameters, "testbucket", "test", "test_table", snapshotDir);
        for(AwsAsyncS3FakeBackup.Injection<?> injection : injections) {
            fakeBackup.injectBehavior(injection);
        }

        for(int i = 1; i <= nodes; i++)
        {
            cluster.get(i).callOnInstance(() -> ObjectStoreAccess.set(Region.US_EAST_1, fakeBackup));
        }
    }

    protected static String generateRandomStringOfByteLength(int byteLength) {
        byte[] bytes = new byte[byteLength];
        new Random().nextBytes(bytes);
        // Use ISO-8859-1 so every byte maps to a single character
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
}