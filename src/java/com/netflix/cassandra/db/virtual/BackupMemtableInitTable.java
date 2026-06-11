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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.backups.BackupMemtable;
import com.netflix.cassandra.backups.BackupMemtableContext;
import com.netflix.cassandra.backups.BackupMemtableParams;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Virtual table exposing the initialization state of every {@link BackupMemtable} on the
 * local node. There is one row per (keyspace, table) currently using a BackupMemtable.
 * <p>
 * Columns:
 *  - keyspace_name, table_name (partition key)
 *  - timestamp: backup timestamp the memtable is reading
 *  - state: current init phase (PENDING / DOWNLOADING_MANIFEST / VALIDATING_CACHE /
 *           DOWNLOADING_COMPONENTS / BUILDING_READERS / READY / FAILED)
 *  - expected_descriptors: number of SSTable descriptors from the manifest
 *  - ready_descriptors: number of SSTables with all required components present locally
 *  - pending_downloads: number of in-flight S3 downloads (only meaningful during DOWNLOADING_COMPONENTS)
 *  - attempt_count: number of times initialize() has run for this memtable
 *  - last_state_change_ms / last_attempt_started_ms / last_attempt_completed_ms: wall-clock timestamps
 *  - bucket, prefix: S3 location being read from
 *  - last_error: last failure message, null on success
 */
public class BackupMemtableInitTable extends AbstractVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(BackupMemtableInitTable.class);

    public static final String TABLE_NAME = "backup_memtable_init";

    private static final String KEYSPACE = "keyspace_name";
    private static final String TABLE = "table_name";
    private static final String TIMESTAMP = "timestamp";
    private static final String STATE = "state";
    private static final String EXPECTED_DESCRIPTORS = "expected_descriptors";
    private static final String READY_DESCRIPTORS = "ready_descriptors";
    private static final String PENDING_DOWNLOADS = "pending_downloads";
    private static final String ATTEMPT_COUNT = "attempt_count";
    private static final String LAST_STATE_CHANGE_MS = "last_state_change_ms";
    private static final String LAST_ATTEMPT_STARTED_MS = "last_attempt_started_ms";
    private static final String LAST_ATTEMPT_COMPLETED_MS = "last_attempt_completed_ms";
    private static final String BUCKET = "bucket";
    private static final String PREFIX = "prefix";
    private static final String LAST_ERROR = "last_error";

    BackupMemtableInitTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Initialization state of BackupMemtables on this node")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE, UTF8Type.instance)
                           .addRegularColumn(TIMESTAMP, LongType.instance)
                           .addRegularColumn(STATE, UTF8Type.instance)
                           .addRegularColumn(EXPECTED_DESCRIPTORS, Int32Type.instance)
                           .addRegularColumn(READY_DESCRIPTORS, Int32Type.instance)
                           .addRegularColumn(PENDING_DOWNLOADS, Int32Type.instance)
                           .addRegularColumn(ATTEMPT_COUNT, Int32Type.instance)
                           .addRegularColumn(LAST_STATE_CHANGE_MS, LongType.instance)
                           .addRegularColumn(LAST_ATTEMPT_STARTED_MS, LongType.instance)
                           .addRegularColumn(LAST_ATTEMPT_COMPLETED_MS, LongType.instance)
                           .addRegularColumn(BUCKET, UTF8Type.instance)
                           .addRegularColumn(PREFIX, UTF8Type.instance)
                           .addRegularColumn(LAST_ERROR, UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        for (Keyspace ks : Keyspace.all())
        {
            for (ColumnFamilyStore cfs : ks.getColumnFamilyStores())
            {
                Memtable mt;
                try
                {
                    mt = cfs.getCurrentMemtable();
                }
                catch (Throwable t)
                {
                    logger.debug("Skipping {}.{}, no current memtable", ks.getName(), cfs.name, t);
                    continue;
                }
                if (!(mt instanceof BackupMemtable))
                    continue;
                addRow(result, ks.getName(), cfs.name, (BackupMemtable) mt);
            }
        }
        return result;
    }

    private void addRow(SimpleDataSet result, String ksName, String tableName, BackupMemtable mt)
    {
        BackupMemtableContext ctx = mt.getContext();
        BackupMemtableParams params = ctx.getParams();
        result.row(ksName, tableName)
              .column(TIMESTAMP, params.getTimestamp())
              .column(STATE, ctx.getState().name())
              .column(EXPECTED_DESCRIPTORS, ctx.getExpectedDescriptors())
              .column(READY_DESCRIPTORS, ctx.getDescriptors().size())
              .column(PENDING_DOWNLOADS, ctx.getPendingDownloads())
              .column(ATTEMPT_COUNT, ctx.getAttemptCount())
              .column(LAST_STATE_CHANGE_MS, ctx.getLastStateChangeMs())
              .column(LAST_ATTEMPT_STARTED_MS, ctx.getLastAttemptStartedMs())
              .column(LAST_ATTEMPT_COMPLETED_MS, ctx.getLastAttemptCompletedMs())
              .column(BUCKET, params.getBucket())
              .column(PREFIX, params.getPrefix())
              .column(LAST_ERROR, ctx.getLastError());
    }
}
