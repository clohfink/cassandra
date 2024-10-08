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

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.metrics.AutoRepairMetrics;
import org.apache.cassandra.metrics.AutoRepairMetricsManager;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.schema.TableMetadata;

final class RepairMetricsTable extends AbstractVirtualTable
{
    private static final String TABLE_NAME = "auto_repair_metrics";
    private static final String REPAIR_TYPE = "repair_type";
    private static final String REPAIRS_IN_PROGRESS = "repairs_in_progress";
    private static final String NODE_REPAIR_TIME = "node_repair_time_sec";
    private static final String CLUSTER_REPAIR_TIME = "cluster_repair_time_sec";
    private static final String LONGEST_UNREPAIRED = "longest_unrepaired_sec";
    private static final String SUCCEEDED_RANGES = "succeeded_token_ranges_count";
    private static final String FAILED_RANGES = "failed_token_ranges_count";
    private static final String SKIPPED_RANGES = "skipped_token_ranges_count";
    private static final String REPAIR_TURN_MY_TURN = "my_turn_count";
    private static final String REPAIR_TURN_PRIORITY = "my_turn_due_to_priority";
    private static final String REPAIR_TURN_FORCE = "my_turn_force_repair";
    private static final String MV_TABLES_CONSIDERED = "total_considered_mv_tables";
    private static final String DISABLED_REPAIR_TABLES = "total_disabled";

    RepairMetricsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment("Lists current auto repair state by RepairType")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(REPAIR_TYPE, UTF8Type.instance)
                           .addRegularColumn(REPAIRS_IN_PROGRESS, Int32Type.instance)
                           .addRegularColumn(NODE_REPAIR_TIME, LongType.instance)
                           .addRegularColumn(CLUSTER_REPAIR_TIME, LongType.instance)
                           .addRegularColumn(LONGEST_UNREPAIRED, LongType.instance)
                           .addRegularColumn(SUCCEEDED_RANGES, LongType.instance)
                           .addRegularColumn(FAILED_RANGES, LongType.instance)
                           .addRegularColumn(SKIPPED_RANGES, LongType.instance)
                           .addRegularColumn(REPAIR_TURN_MY_TURN, LongType.instance)
                           .addRegularColumn(REPAIR_TURN_PRIORITY, LongType.instance)
                           .addRegularColumn(REPAIR_TURN_FORCE, LongType.instance)
                           .addRegularColumn(MV_TABLES_CONSIDERED, LongType.instance)
                           .addRegularColumn(DISABLED_REPAIR_TABLES, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (AutoRepairConfig.RepairType repairType : AutoRepairConfig.RepairType.values())
        {
            AutoRepairMetrics metrics = AutoRepairMetricsManager.getMetrics(repairType);

            result.row(repairType.name())
                  .column(REPAIRS_IN_PROGRESS, metrics.repairsInProgress.getValue())
                  .column(NODE_REPAIR_TIME, metrics.nodeRepairTimeInSec.getValue())
                  .column(CLUSTER_REPAIR_TIME, metrics.clusterRepairTimeInSec.getValue())
                  .column(LONGEST_UNREPAIRED, metrics.longestUnrepairedSec.getValue())
                  .column(SUCCEEDED_RANGES, metrics.succeededTokenRangesCount.getValue())
                  .column(FAILED_RANGES, metrics.failedTokenRangesCount.getValue())
                  .column(SKIPPED_RANGES, metrics.skippedTokenRangesCount.getValue())
                  .column(REPAIR_TURN_MY_TURN, metrics.repairTurnMyTurn.getCount())
                  .column(REPAIR_TURN_PRIORITY, metrics.repairTurnMyTurnDueToPriority.getCount())
                  .column(REPAIR_TURN_FORCE, metrics.repairTurnMyTurnForceRepair.getCount())
                  .column(MV_TABLES_CONSIDERED, metrics.totalMVTablesConsideredForRepair.getValue())
                  .column(DISABLED_REPAIR_TABLES, metrics.totalDisabledRepairTables.getValue());
        }

        return result;
    }
}
