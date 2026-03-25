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

import java.io.File;
import java.io.FileWriter;
import java.net.URL;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.*;

public class ConfigCheckNodetoolTest extends TestBaseImpl
{
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private Cluster buildCluster() throws Throwable
    {
        return init(Cluster.build(1)
                           .withConfig(c -> c.with(NETWORK, GOSSIP))
                           .start());
    }

    private void loadConfig(Cluster cluster, File tempConfig)
    {
        String configPath;
        try
        {
            configPath = tempConfig.toURI().toURL().toString();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        cluster.get(1).runOnInstance(() -> {
            try
            {
                new org.apache.cassandra.config.YamlConfigurationLoader().loadConfig(new URL(configPath));
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private File writeConfig(String content) throws Exception
    {
        File tempConfig = File.createTempFile("cassandra-test", ".yaml");
        tempConfig.deleteOnExit();
        try (FileWriter w = new FileWriter(tempConfig))
        {
            w.write(content);
        }
        return tempConfig;
    }

    @Test
    public void testConfigCheckUnchanged() throws Throwable
    {
        try (Cluster cluster = buildCluster())
        {
            File tempConfig = writeConfig("cluster_name: TestCluster\n");
            loadConfig(cluster, tempConfig);

            NodeToolResult result = cluster.get(1).nodetoolResult("configcheck");
            assertEquals(0, result.getRc());
            String stdout = result.getStdout();

            assertTrue("Should contain loaded hash, got: " + stdout, stdout.contains("Loaded config hash:"));
            assertTrue("Should contain file hash, got: " + stdout, stdout.contains("File config hash:"));
            assertTrue("Should report unchanged, got: " + stdout, stdout.contains("Config is unchanged."));
        }
    }

    @Test
    public void testConfigCheckChanged() throws Throwable
    {
        try (Cluster cluster = buildCluster())
        {
            File tempConfig = writeConfig("cluster_name: TestCluster\n");
            loadConfig(cluster, tempConfig);

            try (FileWriter w = new FileWriter(tempConfig))
            {
                w.write("cluster_name: ModifiedCluster\n");
            }

            NodeToolResult result = cluster.get(1).nodetoolResult("configcheck");
            assertEquals(0, result.getRc());
            String stdout = result.getStdout();

            assertTrue("Should report modified, got: " + stdout, stdout.contains("Config has been MODIFIED"));
        }
    }

    @Test
    public void testConfigCheckDiffFlag() throws Throwable
    {
        try (Cluster cluster = buildCluster())
        {
            File tempConfig = writeConfig("cluster_name: TestCluster\n");
            loadConfig(cluster, tempConfig);

            try (FileWriter w = new FileWriter(tempConfig))
            {
                w.write("cluster_name: ModifiedCluster\n");
            }

            NodeToolResult result = cluster.get(1).nodetoolResult("configcheck", "--diff");
            assertEquals(0, result.getRc());
            String stdout = result.getStdout();

            assertTrue("Should report modified, got: " + stdout, stdout.contains("Config has been MODIFIED"));
            assertTrue("Should show diff entry, got: " + stdout, stdout.contains("cluster_name: TestCluster -> ModifiedCluster"));
        }
    }

    @Test
    public void testConfigCheckJsonUnchanged() throws Throwable
    {
        try (Cluster cluster = buildCluster())
        {
            File tempConfig = writeConfig("cluster_name: TestCluster\n");
            loadConfig(cluster, tempConfig);

            NodeToolResult result = cluster.get(1).nodetoolResult("configcheck", "-F", "json");
            assertEquals(0, result.getRc());
            String stdout = result.getStdout();

            JsonNode json = JSON_MAPPER.readTree(stdout);
            assertNotNull("Should be valid JSON", json);
            assertFalse("changed should be false", json.get("changed").asBoolean());
            assertNotNull("Should have loaded_hash", json.get("loaded_hash"));
            assertNotNull("Should have file_hash", json.get("file_hash"));
            assertEquals("Hashes should match", json.get("loaded_hash").asText(), json.get("file_hash").asText());
            assertNull("Should not have diff when unchanged", json.get("diff"));
        }
    }

    @Test
    public void testConfigCheckJsonChanged() throws Throwable
    {
        try (Cluster cluster = buildCluster())
        {
            File tempConfig = writeConfig("cluster_name: TestCluster\n");
            loadConfig(cluster, tempConfig);

            try (FileWriter w = new FileWriter(tempConfig))
            {
                w.write("cluster_name: ModifiedCluster\n");
            }

            NodeToolResult result = cluster.get(1).nodetoolResult("configcheck", "-F", "json");
            assertEquals(0, result.getRc());
            String stdout = result.getStdout();

            JsonNode json = JSON_MAPPER.readTree(stdout);
            assertNotNull("Should be valid JSON", json);
            assertTrue("changed should be true", json.get("changed").asBoolean());
            assertNotEquals("Hashes should differ", json.get("loaded_hash").asText(), json.get("file_hash").asText());
            assertNull("Should not include diff without --diff flag", json.get("diff"));
        }
    }

    @Test
    public void testConfigCheckJsonDiff() throws Throwable
    {
        try (Cluster cluster = buildCluster())
        {
            File tempConfig = writeConfig("cluster_name: TestCluster\n");
            loadConfig(cluster, tempConfig);

            try (FileWriter w = new FileWriter(tempConfig))
            {
                w.write("cluster_name: ModifiedCluster\n");
            }

            NodeToolResult result = cluster.get(1).nodetoolResult("configcheck", "-F", "json", "--diff");
            assertEquals(0, result.getRc());
            String stdout = result.getStdout();

            JsonNode json = JSON_MAPPER.readTree(stdout);
            assertNotNull("Should be valid JSON", json);
            assertTrue("changed should be true", json.get("changed").asBoolean());
            JsonNode diff = json.get("diff");
            assertNotNull("Should have diff object", diff);
            assertTrue("diff should be an object", diff.isObject());

            JsonNode clusterNameDiff = diff.get("cluster_name");
            assertNotNull("diff should contain cluster_name", clusterNameDiff);
            assertEquals("TestCluster", clusterNameDiff.get("loaded").asText());
            assertEquals("ModifiedCluster", clusterNameDiff.get("file").asText());
        }
    }
}
