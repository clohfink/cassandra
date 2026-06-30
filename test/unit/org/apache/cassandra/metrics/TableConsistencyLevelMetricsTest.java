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

package org.apache.cassandra.metrics;

import java.lang.management.ManagementFactory;
import javax.management.ObjectName;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.cql3.CQLTester;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that coordinator CQL requests are counted per (keyspace, table, commit CL, serial CL) by the
 * {@code type=Table,...,consistencyLevel=...,serialConsistencyLevel=...,name=Requests} metric registered
 * lazily in {@link TableMetrics}. The serial dimension is what lets a serial/commit consistency mismatch
 * (e.g. an LWT using SERIAL with a LOCAL_QUORUM commit) be flagged per table.
 */
public class TableConsistencyLevelMetricsTest extends CQLTester
{
    @BeforeClass
    public static void startNetwork()
    {
        requireNetwork();
    }

    private long requestCount(String table, String consistencyLevel, String serialConsistencyLevel)
    {
        String name = String.format("org.apache.cassandra.metrics.Table.Requests.%s.%s.%s.%s",
                                     keyspace(), table, consistencyLevel, serialConsistencyLevel);
        Counter counter = CassandraMetricsRegistry.Metrics.getCounters().get(name);
        return counter == null ? 0 : counter.getCount();
    }

    private void writeAt(String table, ConsistencyLevel cl)
    {
        SimpleStatement statement = new SimpleStatement(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", keyspace(), table));
        statement.setConsistencyLevel(cl);
        sessionNet().execute(statement);
    }

    private void readAt(String table, ConsistencyLevel cl)
    {
        SimpleStatement statement = new SimpleStatement(String.format("SELECT * FROM %s.%s WHERE k = 1", keyspace(), table));
        statement.setConsistencyLevel(cl);
        sessionNet().execute(statement);
    }

    @Test
    public void readsAndWritesShareOneCounterPerConsistencyLevel()
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        assertThat(requestCount(table, "QUORUM", "NONE")).isZero();
        assertThat(requestCount(table, "ONE", "NONE")).isZero();

        writeAt(table, ConsistencyLevel.QUORUM);
        writeAt(table, ConsistencyLevel.QUORUM);
        writeAt(table, ConsistencyLevel.ONE);
        readAt(table, ConsistencyLevel.QUORUM);
        readAt(table, ConsistencyLevel.ONE);
        readAt(table, ConsistencyLevel.ONE);

        // Reads and writes are not distinguished; non-LWT requests record a NONE serial consistency.
        assertThat(requestCount(table, "QUORUM", "NONE")).isEqualTo(3); // 2 writes + 1 read
        assertThat(requestCount(table, "ONE", "NONE")).isEqualTo(3);    // 1 write + 2 reads
    }

    @Test
    public void conditionalWritesRecordBothCommitAndSerialConsistency()
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        // Plain write: no Paxos, so the serial consistency is recorded as NONE.
        writeAt(table, ConsistencyLevel.QUORUM);

        // LWT write: QUORUM commit + SERIAL Paxos. Same commit CL as the plain write but a distinct serial
        // CL, which is exactly what makes a serial/commit scope mismatch visible per table.
        SimpleStatement lwt = new SimpleStatement(String.format("INSERT INTO %s.%s (k, v) VALUES (2, 2) IF NOT EXISTS", keyspace(), table));
        lwt.setConsistencyLevel(ConsistencyLevel.QUORUM);
        lwt.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        sessionNet().execute(lwt);

        assertThat(requestCount(table, "QUORUM", "NONE")).isEqualTo(1);   // plain write
        assertThat(requestCount(table, "QUORUM", "SERIAL")).isEqualTo(1); // LWT write
    }

    @Test
    public void requestCounterIsRegisteredWithKeyspaceScopeAndConsistencyTags() throws Exception
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        writeAt(table, ConsistencyLevel.QUORUM);

        ObjectName objectName = new ObjectName(String.format(
            "org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,consistencyLevel=QUORUM,serialConsistencyLevel=NONE,name=Requests",
            keyspace(), table));

        assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(objectName)).isTrue();
    }

    @Test
    public void batchesAreCountedOncePerTableAtTheBatchConsistencyLevel()
    {
        String first = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String second = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.add(new SimpleStatement(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", keyspace(), first)));
        batch.add(new SimpleStatement(String.format("INSERT INTO %s.%s (k, v) VALUES (2, 2)", keyspace(), first)));
        batch.add(new SimpleStatement(String.format("INSERT INTO %s.%s (k, v) VALUES (1, 1)", keyspace(), second)));
        batch.setConsistencyLevel(ConsistencyLevel.QUORUM);
        sessionNet().execute(batch);

        // Each distinct table is counted once regardless of how many statements target it.
        assertThat(requestCount(first, "QUORUM", "NONE")).isEqualTo(1);
        assertThat(requestCount(second, "QUORUM", "NONE")).isEqualTo(1);
    }

    @Test
    public void requestCountersAreRemovedWhenTableIsDropped()
    {
        String table = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        writeAt(table, ConsistencyLevel.ONE);
        assertThat(requestCount(table, "ONE", "NONE")).isEqualTo(1);

        sessionNet().execute(String.format("DROP TABLE %s.%s", keyspace(), table));

        boolean anyLeft = CassandraMetricsRegistry.Metrics.getNames().stream()
                              .anyMatch(n -> n.contains(table) && n.contains("Requests"));
        assertThat(anyLeft).isFalse();
    }
}
