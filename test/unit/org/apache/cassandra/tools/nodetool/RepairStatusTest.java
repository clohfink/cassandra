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
package org.apache.cassandra.tools.nodetool;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import javax.management.openmbean.CompositeData;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.cassandra.service.RepairStatusCompositeData;
import org.apache.cassandra.tools.nodetool.stats.RepairStatusHolder;
import org.apache.cassandra.tools.nodetool.stats.RepairStatusPrinter;
import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RepairStatusTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static CompositeData buildTestCompositeData()
    {
        return RepairStatusCompositeData.builder()
            .addAutoRepairMetric(new RepairStatusCompositeData.AutoRepairMetric(
                "INCREMENTAL", 0, 120L, 600L, 3600L, 100L, 2L, 0L, 50L, 5L, 1L, 3, 0))
            .addIncrementalRepairStat(new RepairStatusCompositeData.IncrementalRepairStat(
                "my_ks", "my_table", 1000000L, 500000L, 100000L, 10, 5, 2))
            .addConsistentSession(new RepairStatusCompositeData.ConsistentSession(
                "abc-123", "FINALIZED", "1711180800", "1711180900", "10.0.0.1", "10.0.0.1,10.0.0.2", "", ""))
            .addActiveRepair(new RepairStatusCompositeData.ActiveRepair(
                "repair-uuid-1", "my_ks", "incremental", "REPAIR_START", 5000L, 3, "10.0.0.1:7000,10.0.0.2:7000", 4, null))
            .addTableRepairConfig(new RepairStatusCompositeData.TableRepairConfig(
                "my_ks", "my_table", true, false, false, 0))
            .build()
            .toCompositeData();
    }

    private static CompositeData buildEmptyCompositeData()
    {
        return RepairStatusCompositeData.builder().build().toCompositeData();
    }

    @Test
    public void testHolderParsesAllSections() throws Exception
    {
        CompositeData cd = buildTestCompositeData();
        RepairStatusHolder holder = new RepairStatusHolder(cd, Collections.emptyList());
        Map<String, Object> map = holder.convert2Map();

        assertEquals(5, map.size());
        assertTrue(map.containsKey("autoRepairMetrics"));
        assertTrue(map.containsKey("incrementalRepairStats"));
        assertTrue(map.containsKey("consistentSessions"));
        assertTrue(map.containsKey("activeRepairs"));
        assertTrue(map.containsKey("tableRepairConfigs"));
    }

    @Test
    public void testHolderFiltersSections() throws Exception
    {
        CompositeData cd = buildTestCompositeData();
        RepairStatusHolder holder = new RepairStatusHolder(cd, Arrays.asList("activeRepairs", "autoRepairMetrics"));
        Map<String, Object> map = holder.convert2Map();

        assertEquals(2, map.size());
        assertTrue(map.containsKey("autoRepairMetrics"));
        assertTrue(map.containsKey("activeRepairs"));
        assertFalse(map.containsKey("incrementalRepairStats"));
    }

    @Test
    public void testJsonPrinterRoundTrips() throws Exception
    {
        CompositeData cd = buildTestCompositeData();
        RepairStatusHolder holder = new RepairStatusHolder(cd, Collections.emptyList());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        StatsPrinter<RepairStatusHolder> printer = RepairStatusPrinter.from("json");
        printer.print(holder, out);
        out.flush();

        String output = baos.toString();
        assertNotNull(output);
        // Verify it's valid JSON by parsing it
        Map<String, Object> parsed = MAPPER.readValue(output, new TypeReference<Map<String, Object>>() {});
        assertEquals(5, parsed.size());
        assertTrue(parsed.containsKey("autoRepairMetrics"));
    }

    @Test
    public void testDefaultPrinterContainsSections() throws Exception
    {
        CompositeData cd = buildTestCompositeData();
        RepairStatusHolder holder = new RepairStatusHolder(cd, Collections.emptyList());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        StatsPrinter<RepairStatusHolder> printer = RepairStatusPrinter.from("");
        printer.print(holder, out);
        out.flush();

        String output = baos.toString();
        assertTrue(output.contains("-- Auto Repair Metrics --"));
        assertTrue(output.contains("-- Incremental Repair Stats --"));
        assertTrue(output.contains("-- Consistent Repair Sessions --"));
        assertTrue(output.contains("-- Active Repairs --"));
        assertTrue(output.contains("-- Table Repair Configs --"));
        assertTrue(output.contains("INCREMENTAL"));
        assertTrue(output.contains("my_ks"));
        assertTrue(output.contains("my_table"));
        assertTrue(output.contains("REPAIR_START"));
    }

    @Test
    public void testDefaultPrinterEmptySections() throws Exception
    {
        CompositeData cd = buildEmptyCompositeData();
        RepairStatusHolder holder = new RepairStatusHolder(cd, Collections.emptyList());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        StatsPrinter<RepairStatusHolder> printer = RepairStatusPrinter.from("");
        printer.print(holder, out);
        out.flush();

        String output = baos.toString();
        assertTrue(output.contains("-- Auto Repair Metrics --"));
        assertTrue(output.contains("none"));
    }

    @Test
    public void testYamlPrinterProducesOutput() throws Exception
    {
        CompositeData cd = buildTestCompositeData();
        RepairStatusHolder holder = new RepairStatusHolder(cd, Collections.emptyList());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(baos);
        StatsPrinter<RepairStatusHolder> printer = RepairStatusPrinter.from("yaml");
        printer.print(holder, out);
        out.flush();

        String output = baos.toString();
        assertTrue(output.contains("autoRepairMetrics:"));
        assertTrue(output.contains("incrementalRepairStats:"));
    }
}
