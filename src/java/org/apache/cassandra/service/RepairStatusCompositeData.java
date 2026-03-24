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
package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.management.openmbean.*;

public class RepairStatusCompositeData
{
    private static final String[] TOP_LEVEL_NAMES = {
        "autoRepairMetrics", "incrementalRepairStats", "consistentSessions", "activeRepairs", "tableRepairConfigs"
    };

    private static final String[] TOP_LEVEL_DESCRIPTIONS = {
        "Auto repair metrics per repair type",
        "Incremental repair stats per table",
        "Consistent repair sessions",
        "Active repairs",
        "Table repair configurations"
    };

    public static final CompositeType TYPE;

    static
    {
        try
        {
            TYPE = new CompositeType(
                "RepairStatus",
                "Comprehensive repair status",
                TOP_LEVEL_NAMES,
                TOP_LEVEL_DESCRIPTIONS,
                new OpenType<?>[] {
                    new ArrayType<>(1, AutoRepairMetric.TYPE),
                    new ArrayType<>(1, IncrementalRepairStat.TYPE),
                    new ArrayType<>(1, ConsistentSession.TYPE),
                    new ArrayType<>(1, ActiveRepair.TYPE),
                    new ArrayType<>(1, TableRepairConfig.TYPE)
                }
            );
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final List<AutoRepairMetric> autoRepairMetrics;
    private final List<IncrementalRepairStat> incrementalRepairStats;
    private final List<ConsistentSession> consistentSessions;
    private final List<ActiveRepair> activeRepairs;
    private final List<TableRepairConfig> tableRepairConfigs;

    private RepairStatusCompositeData(Builder builder)
    {
        this.autoRepairMetrics = builder.autoRepairMetrics;
        this.incrementalRepairStats = builder.incrementalRepairStats;
        this.consistentSessions = builder.consistentSessions;
        this.activeRepairs = builder.activeRepairs;
        this.tableRepairConfigs = builder.tableRepairConfigs;
    }

    public CompositeData toCompositeData()
    {
        try
        {
            return new CompositeDataSupport(TYPE, TOP_LEVEL_NAMES, new Object[] {
                toArray(autoRepairMetrics, AutoRepairMetric.TYPE),
                toArray(incrementalRepairStats, IncrementalRepairStat.TYPE),
                toArray(consistentSessions, ConsistentSession.TYPE),
                toArray(activeRepairs, ActiveRepair.TYPE),
                toArray(tableRepairConfigs, TableRepairConfig.TYPE)
            });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static RepairStatusCompositeData fromCompositeData(CompositeData cd)
    {
        Builder builder = builder();

        for (CompositeData item : (CompositeData[]) cd.get("autoRepairMetrics"))
            builder.addAutoRepairMetric(AutoRepairMetric.fromCompositeData(item));

        for (CompositeData item : (CompositeData[]) cd.get("incrementalRepairStats"))
            builder.addIncrementalRepairStat(IncrementalRepairStat.fromCompositeData(item));

        for (CompositeData item : (CompositeData[]) cd.get("consistentSessions"))
            builder.addConsistentSession(ConsistentSession.fromCompositeData(item));

        for (CompositeData item : (CompositeData[]) cd.get("activeRepairs"))
            builder.addActiveRepair(ActiveRepair.fromCompositeData(item));

        for (CompositeData item : (CompositeData[]) cd.get("tableRepairConfigs"))
            builder.addTableRepairConfig(TableRepairConfig.fromCompositeData(item));

        return builder.build();
    }

    public Map<String, Object> toMap()
    {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("autoRepairMetrics", toMapList(autoRepairMetrics));
        result.put("incrementalRepairStats", toMapList(incrementalRepairStats));
        result.put("consistentSessions", toMapList(consistentSessions));
        result.put("activeRepairs", toMapList(activeRepairs));
        result.put("tableRepairConfigs", toMapList(tableRepairConfigs));
        return result;
    }

    private static <T extends Mappable> List<Map<String, Object>> toMapList(List<T> items)
    {
        List<Map<String, Object>> list = new ArrayList<>(items.size());
        for (T item : items)
            list.add(item.toMap());
        return list;
    }

    private interface Mappable
    {
        Map<String, Object> toMap();
    }

    private interface CompositeDataConvertible
    {
        CompositeData toCompositeData();
    }

    private static <T extends CompositeDataConvertible> CompositeData[] toArray(List<T> items, CompositeType type)
    {
        CompositeData[] arr = new CompositeData[items.size()];
        for (int i = 0; i < items.size(); i++)
            arr[i] = items.get(i).toCompositeData();
        return arr;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    // ---- Inner classes ----

    public static class AutoRepairMetric implements Mappable, CompositeDataConvertible
    {
        private static final String[] NAMES = {
            "repairType", "repairsInProgress", "nodeRepairTimeSec", "clusterRepairTimeSec",
            "longestUnrepairedSec", "succeededTokenRangesCount", "failedTokenRangesCount",
            "skippedTokenRangesCount", "myTurnCount", "myTurnDueToPriority", "myTurnForceRepair",
            "totalConsideredMvTables", "totalDisabled"
        };
        private static final String[] DESCRIPTIONS = NAMES;

        public static final CompositeType TYPE;
        static
        {
            try
            {
                TYPE = new CompositeType("AutoRepairMetric", "Auto repair metrics for one repair type", NAMES, DESCRIPTIONS,
                    new OpenType<?>[] {
                        SimpleType.STRING, SimpleType.INTEGER, SimpleType.LONG, SimpleType.LONG,
                        SimpleType.LONG, SimpleType.LONG, SimpleType.LONG,
                        SimpleType.LONG, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG,
                        SimpleType.INTEGER, SimpleType.INTEGER
                    });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public final String repairType;
        public final int repairsInProgress;
        public final long nodeRepairTimeSec;
        public final long clusterRepairTimeSec;
        public final long longestUnrepairedSec;
        public final long succeededTokenRangesCount;
        public final long failedTokenRangesCount;
        public final long skippedTokenRangesCount;
        public final long myTurnCount;
        public final long myTurnDueToPriority;
        public final long myTurnForceRepair;
        public final int totalConsideredMvTables;
        public final int totalDisabled;

        public AutoRepairMetric(String repairType, int repairsInProgress, long nodeRepairTimeSec,
                                long clusterRepairTimeSec, long longestUnrepairedSec,
                                long succeededTokenRangesCount, long failedTokenRangesCount,
                                long skippedTokenRangesCount, long myTurnCount,
                                long myTurnDueToPriority, long myTurnForceRepair,
                                int totalConsideredMvTables, int totalDisabled)
        {
            this.repairType = repairType;
            this.repairsInProgress = repairsInProgress;
            this.nodeRepairTimeSec = nodeRepairTimeSec;
            this.clusterRepairTimeSec = clusterRepairTimeSec;
            this.longestUnrepairedSec = longestUnrepairedSec;
            this.succeededTokenRangesCount = succeededTokenRangesCount;
            this.failedTokenRangesCount = failedTokenRangesCount;
            this.skippedTokenRangesCount = skippedTokenRangesCount;
            this.myTurnCount = myTurnCount;
            this.myTurnDueToPriority = myTurnDueToPriority;
            this.myTurnForceRepair = myTurnForceRepair;
            this.totalConsideredMvTables = totalConsideredMvTables;
            this.totalDisabled = totalDisabled;
        }

        public CompositeData toCompositeData()
        {
            try
            {
                return new CompositeDataSupport(TYPE, NAMES, new Object[] {
                    repairType, repairsInProgress, nodeRepairTimeSec, clusterRepairTimeSec,
                    longestUnrepairedSec, succeededTokenRangesCount, failedTokenRangesCount,
                    skippedTokenRangesCount, myTurnCount, myTurnDueToPriority, myTurnForceRepair,
                    totalConsideredMvTables, totalDisabled
                });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static AutoRepairMetric fromCompositeData(CompositeData cd)
        {
            return new AutoRepairMetric(
                (String) cd.get("repairType"),
                (Integer) cd.get("repairsInProgress"),
                (Long) cd.get("nodeRepairTimeSec"),
                (Long) cd.get("clusterRepairTimeSec"),
                (Long) cd.get("longestUnrepairedSec"),
                (Long) cd.get("succeededTokenRangesCount"),
                (Long) cd.get("failedTokenRangesCount"),
                (Long) cd.get("skippedTokenRangesCount"),
                (Long) cd.get("myTurnCount"),
                (Long) cd.get("myTurnDueToPriority"),
                (Long) cd.get("myTurnForceRepair"),
                (Integer) cd.get("totalConsideredMvTables"),
                (Integer) cd.get("totalDisabled")
            );
        }

        public Map<String, Object> toMap()
        {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("repairType", repairType);
            m.put("repairsInProgress", repairsInProgress);
            m.put("nodeRepairTimeSec", nodeRepairTimeSec);
            m.put("clusterRepairTimeSec", clusterRepairTimeSec);
            m.put("longestUnrepairedSec", longestUnrepairedSec);
            m.put("succeededTokenRangesCount", succeededTokenRangesCount);
            m.put("failedTokenRangesCount", failedTokenRangesCount);
            m.put("skippedTokenRangesCount", skippedTokenRangesCount);
            m.put("myTurnCount", myTurnCount);
            m.put("myTurnDueToPriority", myTurnDueToPriority);
            m.put("myTurnForceRepair", myTurnForceRepair);
            m.put("totalConsideredMvTables", totalConsideredMvTables);
            m.put("totalDisabled", totalDisabled);
            return m;
        }
    }

    public static class IncrementalRepairStat implements Mappable, CompositeDataConvertible
    {
        private static final String[] NAMES = {
            "keyspace", "table", "bytesRepaired", "bytesUnrepaired", "bytesPendingRepair",
            "sstablesRepaired", "sstablesUnrepaired", "sstablesPendingRepair"
        };
        private static final String[] DESCRIPTIONS = NAMES;

        public static final CompositeType TYPE;
        static
        {
            try
            {
                TYPE = new CompositeType("IncrementalRepairStat", "Incremental repair stats for one table", NAMES, DESCRIPTIONS,
                    new OpenType<?>[] {
                        SimpleType.STRING, SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.LONG,
                        SimpleType.INTEGER, SimpleType.INTEGER, SimpleType.INTEGER
                    });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public final String keyspace;
        public final String table;
        public final long bytesRepaired;
        public final long bytesUnrepaired;
        public final long bytesPendingRepair;
        public final int sstablesRepaired;
        public final int sstablesUnrepaired;
        public final int sstablesPendingRepair;

        public IncrementalRepairStat(String keyspace, String table, long bytesRepaired, long bytesUnrepaired,
                                     long bytesPendingRepair, int sstablesRepaired, int sstablesUnrepaired,
                                     int sstablesPendingRepair)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.bytesRepaired = bytesRepaired;
            this.bytesUnrepaired = bytesUnrepaired;
            this.bytesPendingRepair = bytesPendingRepair;
            this.sstablesRepaired = sstablesRepaired;
            this.sstablesUnrepaired = sstablesUnrepaired;
            this.sstablesPendingRepair = sstablesPendingRepair;
        }

        public CompositeData toCompositeData()
        {
            try
            {
                return new CompositeDataSupport(TYPE, NAMES, new Object[] {
                    keyspace, table, bytesRepaired, bytesUnrepaired, bytesPendingRepair,
                    sstablesRepaired, sstablesUnrepaired, sstablesPendingRepair
                });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static IncrementalRepairStat fromCompositeData(CompositeData cd)
        {
            return new IncrementalRepairStat(
                (String) cd.get("keyspace"),
                (String) cd.get("table"),
                (Long) cd.get("bytesRepaired"),
                (Long) cd.get("bytesUnrepaired"),
                (Long) cd.get("bytesPendingRepair"),
                (Integer) cd.get("sstablesRepaired"),
                (Integer) cd.get("sstablesUnrepaired"),
                (Integer) cd.get("sstablesPendingRepair")
            );
        }

        public Map<String, Object> toMap()
        {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("keyspace", keyspace);
            m.put("table", table);
            m.put("bytesRepaired", bytesRepaired);
            m.put("bytesUnrepaired", bytesUnrepaired);
            m.put("bytesPendingRepair", bytesPendingRepair);
            m.put("sstablesRepaired", sstablesRepaired);
            m.put("sstablesUnrepaired", sstablesUnrepaired);
            m.put("sstablesPendingRepair", sstablesPendingRepair);
            return m;
        }
    }

    public static class ConsistentSession implements Mappable, CompositeDataConvertible
    {
        private static final String[] NAMES = {
            "SESSION_ID", "STATE", "STARTED", "LAST_UPDATE", "COORDINATOR",
            "PARTICIPANTS", "PARTICIPANTS_WP", "TABLES"
        };
        private static final String[] DESCRIPTIONS = NAMES;

        public static final CompositeType TYPE;
        static
        {
            try
            {
                TYPE = new CompositeType("ConsistentSession", "A consistent repair session", NAMES, DESCRIPTIONS,
                    new OpenType<?>[] {
                        SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
                        SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING
                    });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public final String sessionId;
        public final String state;
        public final String started;
        public final String lastUpdate;
        public final String coordinator;
        public final String participants;
        public final String participantsWithPort;
        public final String tables;

        public ConsistentSession(String sessionId, String state, String started, String lastUpdate,
                                 String coordinator, String participants, String participantsWithPort,
                                 String tables)
        {
            this.sessionId = sessionId;
            this.state = state;
            this.started = started;
            this.lastUpdate = lastUpdate;
            this.coordinator = coordinator;
            this.participants = participants;
            this.participantsWithPort = participantsWithPort;
            this.tables = tables;
        }

        public static ConsistentSession fromSessionMap(Map<String, String> m)
        {
            return new ConsistentSession(
                m.get("SESSION_ID"), m.get("STATE"), m.get("STARTED"), m.get("LAST_UPDATE"),
                m.get("COORDINATOR"), m.get("PARTICIPANTS"), m.get("PARTICIPANTS_WP"), m.get("TABLES")
            );
        }

        public CompositeData toCompositeData()
        {
            try
            {
                return new CompositeDataSupport(TYPE, NAMES, new Object[] {
                    sessionId, state, started, lastUpdate, coordinator, participants,
                    participantsWithPort, tables
                });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static ConsistentSession fromCompositeData(CompositeData cd)
        {
            return new ConsistentSession(
                (String) cd.get("SESSION_ID"),
                (String) cd.get("STATE"),
                (String) cd.get("STARTED"),
                (String) cd.get("LAST_UPDATE"),
                (String) cd.get("COORDINATOR"),
                (String) cd.get("PARTICIPANTS"),
                (String) cd.get("PARTICIPANTS_WP"),
                (String) cd.get("TABLES")
            );
        }

        public Map<String, Object> toMap()
        {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("SESSION_ID", sessionId);
            m.put("STATE", state);
            m.put("STARTED", started);
            m.put("LAST_UPDATE", lastUpdate);
            m.put("COORDINATOR", coordinator);
            m.put("PARTICIPANTS", participants);
            m.put("PARTICIPANTS_WP", participantsWithPort);
            m.put("TABLES", tables);
            return m;
        }
    }

    public static class ActiveRepair implements Mappable, CompositeDataConvertible
    {
        private static final String[] NAMES = {
            "id", "keyspaceName", "type", "status", "durationMillis",
            "sessions", "participants", "ranges", "failureCause"
        };
        private static final String[] DESCRIPTIONS = NAMES;

        public static final CompositeType TYPE;
        static
        {
            try
            {
                TYPE = new CompositeType("ActiveRepair", "An active repair operation", NAMES, DESCRIPTIONS,
                    new OpenType<?>[] {
                        SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.STRING,
                        SimpleType.LONG, SimpleType.INTEGER, SimpleType.STRING, SimpleType.INTEGER,
                        SimpleType.STRING
                    });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public final String id;
        public final String keyspaceName;
        public final String type;
        public final String status;
        public final long durationMillis;
        public final int sessions;
        public final String participants;
        public final int ranges;
        public final String failureCause;

        public ActiveRepair(String id, String keyspaceName, String type, String status,
                            long durationMillis, int sessions, String participants,
                            int ranges, String failureCause)
        {
            this.id = id;
            this.keyspaceName = keyspaceName;
            this.type = type;
            this.status = status;
            this.durationMillis = durationMillis;
            this.sessions = sessions;
            this.participants = participants;
            this.ranges = ranges;
            this.failureCause = failureCause;
        }

        public CompositeData toCompositeData()
        {
            try
            {
                return new CompositeDataSupport(TYPE, NAMES, new Object[] {
                    id, keyspaceName, type, status, durationMillis, sessions, participants,
                    ranges, failureCause
                });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static ActiveRepair fromCompositeData(CompositeData cd)
        {
            return new ActiveRepair(
                (String) cd.get("id"),
                (String) cd.get("keyspaceName"),
                (String) cd.get("type"),
                (String) cd.get("status"),
                (Long) cd.get("durationMillis"),
                (Integer) cd.get("sessions"),
                (String) cd.get("participants"),
                (Integer) cd.get("ranges"),
                (String) cd.get("failureCause")
            );
        }

        public Map<String, Object> toMap()
        {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", id);
            m.put("keyspaceName", keyspaceName);
            m.put("type", type);
            m.put("status", status);
            m.put("durationMillis", durationMillis);
            m.put("sessions", sessions);
            m.put("participants", participants);
            m.put("ranges", ranges);
            m.put("failureCause", failureCause);
            return m;
        }
    }

    public static class TableRepairConfig implements Mappable, CompositeDataConvertible
    {
        private static final String[] NAMES = {
            "keyspace", "table", "incrementalEnabled", "previewEnabled", "fullEnabled", "priority"
        };
        private static final String[] DESCRIPTIONS = NAMES;

        public static final CompositeType TYPE;
        static
        {
            try
            {
                TYPE = new CompositeType("TableRepairConfig", "Repair configuration for one table", NAMES, DESCRIPTIONS,
                    new OpenType<?>[] {
                        SimpleType.STRING, SimpleType.STRING, SimpleType.BOOLEAN, SimpleType.BOOLEAN,
                        SimpleType.BOOLEAN, SimpleType.INTEGER
                    });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public final String keyspace;
        public final String table;
        public final boolean incrementalEnabled;
        public final boolean previewEnabled;
        public final boolean fullEnabled;
        public final int priority;

        public TableRepairConfig(String keyspace, String table, boolean incrementalEnabled,
                                 boolean previewEnabled, boolean fullEnabled, int priority)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.incrementalEnabled = incrementalEnabled;
            this.previewEnabled = previewEnabled;
            this.fullEnabled = fullEnabled;
            this.priority = priority;
        }

        public CompositeData toCompositeData()
        {
            try
            {
                return new CompositeDataSupport(TYPE, NAMES, new Object[] {
                    keyspace, table, incrementalEnabled, previewEnabled, fullEnabled, priority
                });
            }
            catch (OpenDataException e)
            {
                throw new RuntimeException(e);
            }
        }

        public static TableRepairConfig fromCompositeData(CompositeData cd)
        {
            return new TableRepairConfig(
                (String) cd.get("keyspace"),
                (String) cd.get("table"),
                (Boolean) cd.get("incrementalEnabled"),
                (Boolean) cd.get("previewEnabled"),
                (Boolean) cd.get("fullEnabled"),
                (Integer) cd.get("priority")
            );
        }

        public Map<String, Object> toMap()
        {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("keyspace", keyspace);
            m.put("table", table);
            m.put("incrementalEnabled", incrementalEnabled);
            m.put("previewEnabled", previewEnabled);
            m.put("fullEnabled", fullEnabled);
            m.put("priority", priority);
            return m;
        }
    }

    // ---- Builder ----

    public static class Builder
    {
        private final List<AutoRepairMetric> autoRepairMetrics = new ArrayList<>();
        private final List<IncrementalRepairStat> incrementalRepairStats = new ArrayList<>();
        private final List<ConsistentSession> consistentSessions = new ArrayList<>();
        private final List<ActiveRepair> activeRepairs = new ArrayList<>();
        private final List<TableRepairConfig> tableRepairConfigs = new ArrayList<>();

        public Builder addAutoRepairMetric(AutoRepairMetric metric)
        {
            autoRepairMetrics.add(metric);
            return this;
        }

        public Builder addIncrementalRepairStat(IncrementalRepairStat stat)
        {
            incrementalRepairStats.add(stat);
            return this;
        }

        public Builder addConsistentSession(ConsistentSession session)
        {
            consistentSessions.add(session);
            return this;
        }

        public Builder addActiveRepair(ActiveRepair repair)
        {
            activeRepairs.add(repair);
            return this;
        }

        public Builder addTableRepairConfig(TableRepairConfig config)
        {
            tableRepairConfigs.add(config);
            return this;
        }

        public RepairStatusCompositeData build()
        {
            return new RepairStatusCompositeData(this);
        }
    }
}
