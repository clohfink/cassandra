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

import java.io.*;
import java.util.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.netflix.cassandra.importing.ImportJob;
import com.netflix.cassandra.importing.ImportJobManager;
import com.netflix.cassandra.importing.ImportStatus;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.Assert;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.marshal.Int32Type;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Base class for remote import tests providing shared infrastructure and helper methods.
 *
 * IMPORTANT: This test suite is split into multiple test classes to manage resource constraints.
 * Distributed tests (dtests) using in-JVM clusters leak some memory on each cluster creation,
 * which can lead to OutOfMemoryErrors if too many tests run in a single test class.
 *
 * By splitting tests into smaller classes with 3-4 tests each:
 * - Each test class creates fewer clusters overall
 * - JVM can garbage collect between test class executions
 * - Tests are more stable and less likely to encounter resource exhaustion
 * - Tests can be run in parallel more effectively
 *
 */
public abstract class RemoteImportTestBase extends TestBaseImpl
{
    public static final Logger logger = LoggerFactory.getLogger(RemoteImportTestBase.class);
    protected static final ObjectMapper mapper = new ObjectMapper();

    // Test constants
    protected static final String TEST_KEYSPACE = "testk";
    protected static final String TEST_TABLE = "testt";
    protected static final String REPLICATION_STRATEGY = "SimpleStrategy";

    protected HttpServer httpServer;
    protected HttpServer httpServer2;
    protected List<HttpServer> additionalHttpServers = new ArrayList<>();
    protected final int serverPort = 8989;
    protected final int serverPort2 = 8990;
    protected Path tempSSTableZip;
    protected Path tempSSTableZip2;

    @After
    public void tearDown() throws Exception
    {
        if (httpServer != null)
        {
            httpServer.stop(0);
        }
        if (httpServer2 != null)
        {
            httpServer2.stop(0);
        }
        for (HttpServer server : additionalHttpServers)
        {
            if (server != null)
            {
                server.stop(0);
            }
        }
        additionalHttpServers.clear();
        if (tempSSTableZip != null && Files.exists(tempSSTableZip))
        {
            Files.delete(tempSSTableZip);
        }
        if (tempSSTableZip2 != null && Files.exists(tempSSTableZip2))
        {
            Files.delete(tempSSTableZip2);
        }
    }

    protected void setupHttpServer() throws Exception
    {
        setupSingleHttpServer(serverPort, tempSSTableZip, "HTTP server", true);
    }

    protected void setupHttpServers() throws Exception
    {
        setupSingleHttpServer(serverPort, tempSSTableZip, "HTTP server", true);
        setupSingleHttpServer(serverPort2, tempSSTableZip2, "HTTP server 2", false);
    }

    protected void setupSingleHttpServer(int port, Path zipPath, String serverName, boolean isPrimary) throws Exception
    {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/sstable.zip", new SSTableHandler(zipPath));
        server.setExecutor(Executors.newFixedThreadPool(1));
        server.start();

        if (isPrimary) {
            httpServer = server;
        } else if (httpServer2 == null) {
            httpServer2 = server;
        } else {
            additionalHttpServers.add(server);
        }

        logger.info("{} started on port {}", serverName, port);
    }

    protected static class SSTableConfig {
        final String keyspaceName;
        final String partitioner;
        final TableMetadata.Builder tableBuilder;
        final int numRows;

        SSTableConfig(String keyspaceName, String partitioner, TableMetadata.Builder tableBuilder, int numRows) {
            this.keyspaceName = keyspaceName;
            this.partitioner = partitioner;
            this.tableBuilder = tableBuilder;
            this.numRows = numRows;
        }
    }

    protected static class SSTableZipResult {
        final Path zipPath;
        final String startToken;
        final String endToken;

        SSTableZipResult(Path zipPath, String startToken, String endToken) {
            this.zipPath = zipPath;
            this.startToken = startToken;
            this.endToken = endToken;
        }
    }

    protected void setupDatabase(String partitioner) {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.getRawConfig().partitioner = partitioner;
        DatabaseDescriptor.applyPartitioner();
        SchemaLoader.prepareServer();
    }

    protected ColumnFamilyStore createKeyspaceAndTable(SSTableConfig config) throws Exception {
        SchemaLoader.createKeyspace(config.keyspaceName, KeyspaceParams.simple(1), config.tableBuilder.build());
        Keyspace keyspace = Keyspace.open(config.keyspaceName);
        return keyspace.getColumnFamilyStore("testt");
    }

    protected List<DecoratedKey> generateSortedKeys(ColumnFamilyStore cfs, int count) {
        List<DecoratedKey> sortedKeys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            UUID uuid = UUID.randomUUID();
            DecoratedKey key = cfs.metadata().partitioner.decorateKey(UUIDType.instance.decompose(uuid));
            sortedKeys.add(key);
        }
        sortedKeys.sort(DecoratedKey::compareTo);
        return sortedKeys;
    }

    @FunctionalInterface
    protected interface DataFormatter {
        String format(int index);
    }

    protected void writeRowsToSSTable(SSTableWriter writer, List<DecoratedKey> keys, ColumnFamilyStore cfs,
                                       boolean includeExtraField, DataFormatter dataFormatter) throws Exception {
        for (int i = 0; i < keys.size(); i++) {
            DecoratedKey decoratedKey = keys.get(i);
            UUID uuid = UUIDType.instance.compose(decoratedKey.getKey());
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), uuid).withTimestamp(System.currentTimeMillis());
            String dataValue = dataFormatter != null ? dataFormatter.format(i) : "test data " + i;
            if (includeExtraField) {
                builder.newRow().add("data", dataValue).add("extra_field", i * 100);
            } else {
                builder.newRow().add("data", dataValue);
            }
            writer.append(builder.build().unfilteredIterator());
        }
    }

    protected void writeRowsToSSTable(SSTableWriter writer, List<DecoratedKey> keys, ColumnFamilyStore cfs, boolean includeExtraField) throws Exception {
        writeRowsToSSTable(writer, keys, cfs, includeExtraField, null);
    }

    protected Path createZipFromDirectory(File dir) throws Exception {
        Path zipPath = Files.createTempFile("sstable", ".zip");
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipPath))) {
            for (File file : dir.tryList()) {
                if (file.name().endsWith(".db") || file.name().endsWith(".txt")) {
                    ZipEntry entry = new ZipEntry(file.name());
                    zos.putNextEntry(entry);
                    Files.copy(file.toPath(), zos);
                    zos.closeEntry();
                }
            }
        }
        return zipPath;
    }

    protected SSTableZipResult createSSTableZipWithConfig(SSTableConfig config, boolean includeExtraField) throws Exception {
        setupDatabase(config.partitioner);
        ColumnFamilyStore cfs = createKeyspaceAndTable(config);

        File dir = new File(Files.createTempDirectory("sstable").toString());
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

        List<SSTableWriter> writers = new ArrayList<>();
        List<SSTableReader> readers = new ArrayList<>();
        boolean allFinished = false;

        try {
            List<DecoratedKey> allKeys = generateSortedKeys(cfs, config.numRows);
            String startToken = allKeys.get(0).getToken().toString();
            String endToken = allKeys.get(allKeys.size() - 1).getToken().toString();

            // Determine if we need multiple SSTables (for two SSTable test)
            boolean createTwoSSTables = "testk_two_".equals(config.keyspaceName.substring(0, Math.min(config.keyspaceName.length(), 9)));

            if (createTwoSSTables) {
                // Create two SSTables
                for (int tableNum = 0; tableNum < 2; tableNum++) {
                    SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(dir), 100, 0, null, false,
                                                              new SerializationHeader(true, cfs.metadata(),
                                                                                    cfs.metadata().regularAndStaticColumns(),
                                                                                    EncodingStats.NO_STATS),
                                                              cfs.indexManager.listIndexes(), txn);
                    writers.add(writer);

                    int startIdx = tableNum * (config.numRows / 2);
                    int endIdx = (tableNum + 1) * (config.numRows / 2);
                    List<DecoratedKey> keysForTable = allKeys.subList(startIdx, endIdx);

                    writeRowsToSSTable(writer, keysForTable, cfs, includeExtraField);
                    readers.add(writer.finish(true));
                }
            } else {
                // Create single SSTable
                SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(dir), 100, 0, null, false,
                                                          new SerializationHeader(true, cfs.metadata(),
                                                                                cfs.metadata().regularAndStaticColumns(),
                                                                                EncodingStats.NO_STATS),
                                                          cfs.indexManager.listIndexes(), txn);
                writers.add(writer);
                writeRowsToSSTable(writer, allKeys, cfs, includeExtraField);
                readers.add(writer.finish(true));
            }

            allFinished = true;
            Path zipPath = createZipFromDirectory(dir);
            return new SSTableZipResult(zipPath, startToken, endToken);
        } finally {
            for (SSTableReader reader : readers) {
                if (reader != null) reader.selfRef().release();
            }
            for (int i = 0; i < writers.size(); i++) {
                SSTableWriter writer = writers.get(i);
                if (writer != null && !allFinished) writer.abort();
            }
            txn.abort();
        }
    }

    protected TableMetadata.Builder createBaseTableBuilder(String keyspaceName) {
        return TableMetadata.builder(keyspaceName, TEST_TABLE)
                            .addPartitionKeyColumn("id", UUIDType.instance)
                            .addRegularColumn("data", UTF8Type.instance);
    }

    protected SSTableConfig createSSTableConfig(String namePrefix, String partitioner, int numRows, boolean includeExtraField) {
        String keyspaceName = namePrefix + System.currentTimeMillis();
        TableMetadata.Builder builder = createBaseTableBuilder(keyspaceName);
        if (includeExtraField) {
            builder.addRegularColumn("extra_field", Int32Type.instance);
        }
        return new SSTableConfig(keyspaceName, partitioner, builder, numRows);
    }

    protected SSTableZipResult createSSTableZip() throws Exception {
        SSTableConfig config = createSSTableConfig("testk_", "Murmur3Partitioner", 1000, false);
        return createSSTableZipWithConfig(config, false);
    }

    protected SSTableZipResult createSSTableZipWithPartitioner(String partitionerClass) throws Exception {
        SSTableConfig config = createSSTableConfig("testk_part_", partitionerClass, 10, false);
        return createSSTableZipWithConfig(config, false);
    }

    protected SSTableZipResult createSSTableZipWithDifferentSchema() throws Exception {
        SSTableConfig config = createSSTableConfig("testk_schema_", "Murmur3Partitioner", 10, true);
        return createSSTableZipWithConfig(config, true);
    }

    protected SSTableZipResult createTwoSSTablesZip() throws Exception {
        SSTableConfig config = createSSTableConfig("testk_two_", "Murmur3Partitioner", 1000, false);
        return createSSTableZipWithConfig(config, false);
    }

    protected SSTableZipResult createSSTableZipWithData(String dataPrefix, int startId, int numRows) throws Exception {
        SSTableConfig config = createSSTableConfig("testk_dup_", "Murmur3Partitioner", numRows, false);
        return createSSTableZipWithDataConfig(config, dataPrefix, startId);
    }

    protected SSTableZipResult createSSTableZipSinglePartition() throws Exception {
        SSTableConfig config = createSSTableConfig("testk_single_", "Murmur3Partitioner", 1, false);
        return createSSTableZipWithConfig(config, false);
    }

    protected Cluster setupTestCluster(int nodeCount, int replicationFactor) throws Exception {
        Cluster cluster = init(Cluster.build(nodeCount)
                                     .withConfig(c -> c.with(NATIVE_PROTOCOL, NETWORK, GOSSIP)
                                                       .set("import_cleanup_initial_delay", "5s")
                                                       .set("import_cleanup_period", "5s")
                                                       .set("import_cleanup_min_age", "1s"))
                                     .start());
        cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + TEST_KEYSPACE + " WITH replication = {'class': '" + REPLICATION_STRATEGY + "', 'replication_factor': '" + replicationFactor + "'}");
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + TEST_KEYSPACE + '.' + TEST_TABLE + " (id uuid PRIMARY KEY, data text)");
        return cluster;
    }

    protected void startImportJob(Cluster cluster, UUID importId, String keyspace, String table,
                                   String sourceUrl, String startToken, String endToken) throws Exception {
        cluster.coordinator(1).execute(String.format(
            "INSERT INTO system_distributed.remote_import (id, target_keyspace, target_table, source, source_type, start_token, end_token) " +
            "VALUES (%s, '%s', '%s', '%s', 'url', '%s', '%s')",
            importId, keyspace, table, sourceUrl, startToken, endToken),
            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE);
        cluster.coordinator(1).execute(String.format(
            "UPDATE system_distributed.remote_import SET state = 'staging' " +
            "WHERE id = %s AND target_keyspace = '%s' AND target_table = '%s'",
            importId, keyspace, table),
            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE);
    }

    protected void startImportJobWithDcFilter(Cluster cluster, UUID importId, String keyspace, String table,
                                               String sourceUrl, String startToken, String endToken, String dcFilter) throws Exception {
        cluster.coordinator(1).execute(String.format(
            "INSERT INTO system_distributed.remote_import (id, target_keyspace, target_table, source, source_type, start_token, end_token, dc_filter) " +
            "VALUES (%s, '%s', '%s', '%s', 'url', '%s', '%s', '%s')",
            importId, keyspace, table, sourceUrl, startToken, endToken, dcFilter),
            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE);
    }

    protected long getRowCount(Cluster cluster, String keyspace, String table) throws Exception {
        Object[][] rs = cluster.coordinator(1).execute(
            String.format("SELECT COUNT(*) FROM %s.%s", keyspace, table),
            org.apache.cassandra.distributed.api.ConsistencyLevel.ONE);
        return rs.length > 0 ? (Long) rs[0][0] : 0L;
    }

    /**
     * Waits for an import job to reach ERROR state by checking the internal ImportJob status.
     * This is useful for failure test cases where the import might fail before reaching STAGED.
     */
    protected void waitForImportError(Cluster cluster, UUID importId, int timeoutSeconds) throws Exception {
        logger.info("Waiting for import {} to reach ERROR state", importId);
        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        while (System.currentTimeMillis() < deadline) {
            String status = cluster.get(1).callOnInstance(() -> {
                ImportJob job = ImportJobManager.getInstance().getJob(importId);
                if (job != null) {
                    return job.status.get().toString();
                }
                return null;
            });

            logger.info("Current import status: {}", status);

            if ("ERROR".equals(status)) {
                logger.info("Import {} reached ERROR state as expected", importId);
                return;
            }

            Thread.sleep(1000);
        }

        throw new AssertionError(String.format("Import %s did not reach ERROR state within %d seconds", importId, timeoutSeconds));
    }

    protected void executeBasicImportTest(SSTableZipResult zipResult, UUID testId, int nodeCount, int replicationFactor, int expectedRows) throws Throwable {
        tempSSTableZip = zipResult.zipPath;
        setupHttpServer();
        String localUrl = "http://localhost:" + serverPort + "/sstable.zip";

        try (Cluster cluster = setupTestCluster(nodeCount, replicationFactor)) {
            runFullImportTest(testId, TEST_KEYSPACE, TEST_TABLE, localUrl, zipResult.startToken, zipResult.endToken, Files.size(tempSSTableZip), expectedRows);
        }
    }

    protected void executeFailureImportTest(SSTableZipResult zipResult, UUID testId, String expectedErrorSubstring) throws Throwable {
        tempSSTableZip = zipResult.zipPath;
        setupHttpServer();
        String localUrl = "http://localhost:" + serverPort + "/sstable.zip";

        try (Cluster cluster = setupTestCluster(1, 1)) {
            runFailureImportTest(testId, TEST_KEYSPACE, TEST_TABLE, localUrl, Files.size(tempSSTableZip), expectedErrorSubstring);
        }
    }

    protected SSTableZipResult createSSTableZipWithDataConfig(SSTableConfig config, String dataPrefix, int startId) throws Exception {
        setupDatabase(config.partitioner);
        ColumnFamilyStore cfs = createKeyspaceAndTable(config);

        File dir = new File(Files.createTempDirectory("sstable").toString());
        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE);

        List<SSTableWriter> writers = new ArrayList<>();
        List<SSTableReader> readers = new ArrayList<>();
        boolean allFinished = false;

        try {
            List<DecoratedKey> allKeys = generateSortedKeys(cfs, config.numRows);
            String startToken = allKeys.get(0).getToken().toString();
            String endToken = allKeys.get(allKeys.size() - 1).getToken().toString();

            SSTableWriter writer = SSTableWriter.create(cfs.newSSTableDescriptor(dir), 100, 0, null, false,
                                                      new SerializationHeader(true, cfs.metadata(),
                                                                            cfs.metadata().regularAndStaticColumns(),
                                                                            EncodingStats.NO_STATS),
                                                      cfs.indexManager.listIndexes(), txn);
            writers.add(writer);
            writeRowsToSSTable(writer, allKeys, cfs, false, i -> dataPrefix + ' ' + (startId + i));
            readers.add(writer.finish(true));

            allFinished = true;
            Path zipPath = createZipFromDirectory(dir);
            return new SSTableZipResult(zipPath, startToken, endToken);
        } finally {
            for (SSTableReader reader : readers) {
                if (reader != null) reader.selfRef().release();
            }
            for (int i = 0; i < writers.size(); i++) {
                SSTableWriter writer = writers.get(i);
                if (writer != null && !allFinished) writer.abort();
            }
            txn.abort();
        }
    }

    protected void insertImportJob(Session s, UUID id, String keyspace, String table, String url,
                               String startToken, String endToken, long size) throws Exception {
        if (startToken != null && endToken != null) {
            s.execute(
                "INSERT INTO system_distributed.remote_import " +
                "(id, target_keyspace, target_table, source, source_type, start_token, end_token, size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                id, keyspace, table, url, "url", startToken, endToken, size
            );
        } else {
            s.execute(
                "INSERT INTO system_distributed.remote_import " +
                "(id, target_keyspace, target_table, source, source_type, size) " +
                "VALUES (?, ?, ?, ?, ?, ?)",
                id, keyspace, table, url, "url", size
            );
        }
    }

    protected void setImportState(Session s, UUID id, String keyspace, String table, String state) throws Exception {
        s.execute(
            "UPDATE system_distributed.remote_import " +
            "SET state = ? " +
            "WHERE id = ? AND target_keyspace = ? AND target_table = ?",
            state, id, keyspace, table
        );
    }

    protected boolean waitForStagingCompletion(Session s, UUID id, int maxWaitSeconds) throws Exception {
        logger.info("Waiting for staging to complete...");

        for (int i = 0; i < maxWaitSeconds; i++) {
            Thread.sleep(1000);
            var stagingResult = s.execute("SELECT * FROM netflix_views.cluster_view WHERE keyspace_name = ? AND table_name = ?",
                                         "netflix_views", "local_import");

            int totalNodes = 0;
            int stagedNodes = 0;

            for (var row : stagingResult) {
                String json = row.getString("value");
                try {
                    var jsonArray = mapper.readTree(json);
                    if (jsonArray.isArray() && !jsonArray.isEmpty()) {
                        for (var importJob : jsonArray) {
                            String snapshotId = importJob.get("id").asText();
                            if (id.toString().equals(snapshotId)) {
                                totalNodes++;
                                var statusNode = importJob.get("status");
                                logger.info("Node staging status: {}", statusNode);

                                if (statusNode != null && statusNode.has("step")) {
                                    String currentStep = statusNode.get("step").asText();
                                    if ("STAGED".equals(currentStep)) {
                                        stagedNodes++;
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to parse cluster_view JSON: {}", json, e);
                }
            }

            logger.info("Staging progress: {}/{} nodes staged", stagedNodes, totalNodes);

            if (totalNodes > 0 && stagedNodes == totalNodes) {
                return true;
            }
        }
        return false;
    }

    protected static class ImportResult {
        final boolean completed;
        final boolean hasError;
        final String errorMessage;

        ImportResult(boolean completed, boolean hasError, String errorMessage) {
            this.completed = completed;
            this.hasError = hasError;
            this.errorMessage = errorMessage;
        }

        static ImportResult success() {
            return new ImportResult(true, false, null);
        }

        static ImportResult error(String message) {
            return new ImportResult(true, true, message);
        }

        static ImportResult inProgress() {
            return new ImportResult(false, false, null);
        }
    }

    protected ImportResult waitForImportCompletion(Session s, UUID id, String keyspace, String table, int maxWaitSeconds) throws Exception {
        logger.info("Waiting for import to complete...");

        for (int i = 0; i < maxWaitSeconds; i++) {
            Thread.sleep(1000);
            var importResult = s.execute("SELECT * FROM netflix_views.local_import WHERE id = ? AND target_keyspace = ? AND target_table = ?",
                                       id, keyspace, table);
            for (var row : importResult) {
                var status = row.getMap("status", String.class, String.class);
                logger.info("Import status: {}", status);
                if (status != null && status.containsKey("step")) {
                    String state = status.get("step").toLowerCase();
                    if ("done".equals(state)) {
                        return ImportResult.success();
                    } else if ("error".equals(state)) {
                        String errorMessage = status.get("description");
                        logger.error("Import failed with status: {}", status);
                        return ImportResult.error(errorMessage);
                    }
                }
            }
        }
        return ImportResult.inProgress();
    }

    /**
     * Helper method for distributed API tests that automatically transitions through both stages:
     * 1. Waits for STAGED state after initial 'staging' state is set
     * 2. Automatically transitions to 'importing' state
     * 3. Waits for DONE or ERROR state
     *
     * This is designed for the distributed Cluster API where we use coordinator().execute()
     * instead of the DataStax Session API.
     */
    protected String waitForImportCompletionWithAutoTransition(Cluster cluster, UUID importId,
                                                                String keyspace, String table,
                                                                int timeoutSeconds) throws Exception {
        logger.info("Waiting for import {} to reach STAGED state", importId);
        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        // Wait for STAGED state
        while (System.currentTimeMillis() < deadline) {
            // Check if job has reached STAGED or ERROR via the ImportJob status
            String jobStatus = cluster.get(1).callOnInstance(() -> {
                ImportJob job = ImportJobManager.getInstance().getJob(importId);
                if (job != null) {
                    return job.status.get().toString();
                }
                return null;
            });

            logger.info("Current import {} job status: {}", importId, jobStatus);

            // If import failed before reaching STAGED, return ERROR immediately
            if ("ERROR".equals(jobStatus)) {
                logger.info("Import {} failed before reaching STAGED state", importId);
                return "ERROR";
            }

            boolean allStaged = "STAGED".equals(jobStatus);

            if (allStaged) {
                logger.info("Import {} reached STAGED state, transitioning to 'importing'", importId);
                // Transition to importing
                cluster.coordinator(1).execute(String.format(
                    "UPDATE system_distributed.remote_import SET state = 'importing' " +
                    "WHERE id = %s AND target_keyspace = '%s' AND target_table = '%s'",
                    importId, keyspace, table),
                    org.apache.cassandra.distributed.api.ConsistencyLevel.ONE);
                break;
            }

            Thread.sleep(1000);
        }

        // Now wait for DONE or ERROR
        logger.info("Waiting for import {} to reach DONE or ERROR state", importId);
        while (System.currentTimeMillis() < deadline) {
            boolean isDone = cluster.get(1).callOnInstance(() -> {
                ImportJob job =
                    ImportJobManager.getInstance().getJob(importId);
                if (job != null) {
                    ImportStatus status = job.status.get();
                    return status == ImportStatus.DONE ||
                           status == ImportStatus.ERROR;
                }
                return false;
            });

            if (isDone) {
                String finalStatus = cluster.get(1).callOnInstance(() -> {
                    ImportJob job =
                        ImportJobManager.getInstance().getJob(importId);
                    return job != null ? job.status.get().toString() : "UNKNOWN";
                });
                logger.info("Import {} completed with final status: {}", importId, finalStatus);
                return finalStatus;
            }

            Thread.sleep(1000);
        }

        throw new AssertionError(String.format("Import %s did not complete within %d seconds",
                                              importId, timeoutSeconds));
    }

    protected static class DataVerificationResult {
        final int rowCount;
        final boolean foundExpectedData;

        DataVerificationResult(int rowCount, boolean foundExpectedData) {
            this.rowCount = rowCount;
            this.foundExpectedData = foundExpectedData;
        }
    }

    protected DataVerificationResult verifyImportedData(Session s, String keyspace, String table, int expectedRows) throws Exception {
        logger.info("Verifying imported data...");

        Statement statement = new SimpleStatement("SELECT * FROM " + keyspace + '.' + table);
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        var dataResult = s.execute(statement);

        int rowCount = 0;
        boolean foundExpectedData = false;

        for (var row : dataResult) {
            rowCount++;
            String data = row.getString("data");

            if (data != null && data.startsWith("test data ")) {
                foundExpectedData = true;
            }
        }

        logger.info("Verification complete: {} rows imported, foundExpectedData={}", rowCount, foundExpectedData);

        if (expectedRows > 0) {
            assertEquals(expectedRows, rowCount);
        }

        return new DataVerificationResult(rowCount, foundExpectedData);
    }

    protected DataVerificationResult verifyImportedDataWithPrefixes(Session s, String keyspace, String table,
                                                                String[] expectedPrefixes, int expectedRows) throws Exception {
        logger.info("Verifying imported data with prefixes: {}", String.join(", ", expectedPrefixes));

        Statement statement = new SimpleStatement("SELECT * FROM " + keyspace + '.' + table);
        statement.setConsistencyLevel(ConsistencyLevel.ONE);
        var dataResult = s.execute(statement);

        int rowCount = 0;
        boolean[] foundPrefixes = new boolean[expectedPrefixes.length];

        for (var row : dataResult) {
            rowCount++;
            UUID rowId = row.getUUID("id");
            String data = row.getString("data");
            logger.info("Found imported row: id={}, data={}", rowId, data);

            for (int i = 0; i < expectedPrefixes.length; i++) {
                if (data != null && data.startsWith(expectedPrefixes[i])) {
                    foundPrefixes[i] = true;
                }
            }
        }

        boolean allPrefixesFound = true;
        for (int i = 0; i < foundPrefixes.length; i++) {
            if (!foundPrefixes[i]) {
                logger.error("Missing data with prefix: {}", expectedPrefixes[i]);
                allPrefixesFound = false;
            }
        }

        logger.info("Verification complete: {} rows imported, all prefixes found: {}", rowCount, allPrefixesFound);

        if (expectedRows > 0) {
            assertEquals(expectedRows, rowCount);
        }

        assertTrue("All expected data prefixes should be found", allPrefixesFound);

        return new DataVerificationResult(rowCount, allPrefixesFound);
    }

    protected void runFullImportTest(UUID id, String keyspace, String table, String url,
                                 String startToken, String endToken, long zipSize, int expectedRows) throws Exception {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect()) {

            insertImportJob(s, id, keyspace, table, url, startToken, endToken, zipSize);
            setImportState(s, id, keyspace, table, "staging");

            boolean stagingCompleted = waitForStagingCompletion(s, id, 60);
            if (!stagingCompleted) {
                Assert.fail("Staging did not complete within 60 seconds");
            }

            logger.info("Staging completed, now triggering import...");
            setImportState(s, id, keyspace, table, "importing");

            ImportResult importResult = waitForImportCompletion(s, id, keyspace, table, 120);
            if (!importResult.completed) {
                Assert.fail("Import did not complete within 120 seconds");
            }
            if (importResult.hasError) {
                throw new RuntimeException("Import failed: " + importResult.errorMessage);
            }

            logger.info("Import completed successfully");
            DataVerificationResult verificationResult = verifyImportedData(s, keyspace, table, expectedRows);

            if (verificationResult.rowCount == 0) {
                throw new RuntimeException("No data was imported into " + keyspace + '.' + table);
            }

            if (!verificationResult.foundExpectedData) {
                throw new RuntimeException("Expected test data pattern not found in imported rows");
            }

            logger.info("Remote import test completed successfully with {} imported rows", verificationResult.rowCount);
        }
    }

    protected void runFailureImportTest(UUID id, String keyspace, String table, String url, long zipSize,
                                    String expectedErrorSubstring) throws Exception {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect()) {

            insertImportJob(s, id, keyspace, table, url, null, null, zipSize);
            setImportState(s, id, keyspace, table, "staging");

            boolean stagingCompleted = waitForStagingCompletion(s, id, 60);
            if (!stagingCompleted) {
                Assert.fail("Staging did not complete within 60 seconds");
            }

            logger.info("Staging completed, now triggering import...");
            setImportState(s, id, keyspace, table, "importing");

            ImportResult importResult = waitForImportCompletion(s, id, keyspace, table, 120);
            if (!importResult.completed) {
                Assert.fail("Expected error was not detected within 120 seconds");
            }
            if (!importResult.hasError) {
                Assert.fail("Expected import to fail but it succeeded");
            }

            assertTrue("Error message should contain expected substring: " + importResult.errorMessage,
                      importResult.errorMessage != null && importResult.errorMessage.contains(expectedErrorSubstring));

            logger.info("Expected error detected: {}", importResult.errorMessage);
        }
    }

    protected void runMultiUrlImportTest(UUID id, String keyspace, String table, String[] urls,
                                     String[] startTokens, String[] endTokens, long[] zipSizes,
                                     String[] expectedPrefixes, int expectedTotalRows) throws Exception {
        try (com.datastax.driver.core.Cluster c = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
             Session s = c.connect()) {

            // Insert multiple import job entries with the same UUID but different URLs
            for (int i = 0; i < urls.length; i++) {
                insertImportJob(s, id, keyspace, table, urls[i], startTokens[i], endTokens[i], zipSizes[i]);
            }
            setImportState(s, id, keyspace, table, "staging");

            boolean stagingCompleted = waitForStagingCompletion(s, id, 60);
            if (!stagingCompleted) {
                Assert.fail("Staging did not complete within 60 seconds");
            }

            logger.info("Staging completed, now triggering import...");
            setImportState(s, id, keyspace, table, "importing");

            ImportResult importResult = waitForImportCompletion(s, id, keyspace, table, 120);
            if (!importResult.completed) {
                Assert.fail("Import did not complete within 120 seconds");
            }
            if (importResult.hasError) {
                throw new RuntimeException("Import failed: " + importResult.errorMessage);
            }

            logger.info("Import completed successfully");
            DataVerificationResult verificationResult = verifyImportedDataWithPrefixes(s, keyspace, table, expectedPrefixes, expectedTotalRows);

            if (verificationResult.rowCount == 0) {
                throw new RuntimeException("No data was imported into " + keyspace + '.' + table);
            }

            if (!verificationResult.foundExpectedData) {
                throw new RuntimeException("Expected test data pattern not found in imported rows");
            }

            logger.info("Multi-URL remote import test completed successfully with {} imported rows", verificationResult.rowCount);
        }
    }

    protected static class SSTableHandler implements HttpHandler
    {
        private final Path zipPath;

        public SSTableHandler(Path zipPath)
        {
            this.zipPath = zipPath;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException
        {
            if ("GET".equals(exchange.getRequestMethod()))
            {
                try
                {
                    if (zipPath == null || !Files.exists(zipPath))
                    {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }

                    byte[] zipBytes = Files.readAllBytes(zipPath);
                    exchange.getResponseHeaders().set("Content-Type", "application/zip");
                    exchange.getResponseHeaders().set("Content-Disposition", "attachment; filename=sstable.zip");
                    exchange.sendResponseHeaders(200, zipBytes.length);

                    try (OutputStream os = exchange.getResponseBody())
                    {
                        os.write(zipBytes);
                    }

                    logger.info("Served SSTable zip {} of size {} bytes", zipPath.getFileName(), zipBytes.length);
                }
                catch (Exception e)
                {
                    logger.error("Error serving SSTable zip", e);
                    exchange.sendResponseHeaders(500, -1);
                }
            }
            else
            {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }
}
