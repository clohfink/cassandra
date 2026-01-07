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

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.cassandra.importing.ImportJob;
import com.netflix.cassandra.importing.ImportJobManager;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

public class LocalImport extends AbstractVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(LocalImport.class);

    public static final String TABLE_NAME = "local_import";

    private static final String ID = "id";
    private static final String KEYSPACE = "target_keyspace";
    private static final String TABLE = "target_table";
    private static final String STATUS = "status";

    LocalImport(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Import sstables from S3")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(UUIDType.instance, UTF8Type.instance, UTF8Type.instance)))
                           .addPartitionKeyColumn(ID, UUIDType.instance)
                           .addPartitionKeyColumn(KEYSPACE, UTF8Type.instance)
                           .addPartitionKeyColumn(TABLE, UTF8Type.instance)
                           .addRegularColumn(STATUS, MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        Map<UUID, ImportJob> jobs = ImportJobManager.getInstance().getAllJobs();
        for (ImportJob job : jobs.values())
        {
            result.row(job.id, job.targetKeyspace, job.targetTable)
                  .column(STATUS, job.getStatusMap());
        }
        return result;
    }
}