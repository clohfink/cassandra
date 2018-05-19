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
package org.apache.cassandra.db.virtual;

import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.TableMetadata;

final class CompactionStats extends AbstractVirtualTable
{
    private final static String COMPACTION_ID = "compaction_id";
    private final static String TASK_TYPE = "task_type";
    private final static String TABLE = "table";
    private final static String CURRENT = "progress_current";
    private final static String TOTAL = "progress_total";
    private final static String UNIT = "progress_unit";

    CompactionStats(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "compaction_stats")
                           .comment("compactions state")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(COMPACTION_ID, UUIDType.instance)
                           .addRegularColumn(TASK_TYPE, UTF8Type.instance)
                           .addRegularColumn(TABLE, UTF8Type.instance)
                           .addRegularColumn(CURRENT, UTF8Type.instance)
                           .addRegularColumn(TOTAL, UTF8Type.instance)
                           .addRegularColumn(UNIT, UTF8Type.instance)
                           .build());
    }

    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map<String, String> c : CompactionManager.instance.getCompactions())
        {
            result.row(UUID.fromString(c.get(CompactionInfo.COMPACTION_ID)))
                  .column(TASK_TYPE, c.get(CompactionInfo.TASK_TYPE))
                  .column(TABLE, c.get(CompactionInfo.KEYSPACE) + '.' + c.get(CompactionInfo.COLUMNFAMILY))
                  .column(CURRENT, c.get(CompactionInfo.COMPLETED))
                  .column(TOTAL, c.get(CompactionInfo.TOTAL))
                  .column(UNIT, c.get(CompactionInfo.UNIT));
        }

        return result;
    }
}