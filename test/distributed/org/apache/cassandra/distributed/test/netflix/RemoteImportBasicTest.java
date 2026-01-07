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

import java.nio.file.Files;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

/**
 * Tests for basic remote import functionality covering happy-path scenarios.
 */
public class RemoteImportBasicTest extends RemoteImportTestBase
{
    @Test
    public void testRemoteImport() throws Throwable {
        SSTableZipResult zipResult = createSSTableZip();
        executeBasicImportTest(zipResult, UUID.randomUUID(), 2, 1, 1000);
    }

    @Test
    public void testRemoteImportWithTwoSSTables() throws Throwable {
        SSTableZipResult zipResult = createTwoSSTablesZip();
        executeBasicImportTest(zipResult, UUID.randomUUID(), 2, 1, 1000);
    }

    @Test
    public void testRemoteImportWithDuplicateSSTableNamesFromDifferentUrls() throws Throwable {
        // Create two SSTable zips with same name but different data
        SSTableZipResult zipResult1 = createSSTableZipWithData("dataset1", 1, 500);
        SSTableZipResult zipResult2 = createSSTableZipWithData("dataset2", 501, 500);

        tempSSTableZip = zipResult1.zipPath;
        tempSSTableZip2 = zipResult2.zipPath;
        setupHttpServers();

        String localUrl1 = "http://localhost:" + serverPort + "/sstable.zip";
        String localUrl2 = "http://localhost:" + serverPort2 + "/sstable.zip";

        try (Cluster cluster = setupTestCluster(2, 1)) {
            String[] urls = {localUrl1, localUrl2};
            String[] startTokens = {zipResult1.startToken, zipResult2.startToken};
            String[] endTokens = {zipResult1.endToken, zipResult2.endToken};
            long[] zipSizes = {Files.size(tempSSTableZip), Files.size(tempSSTableZip2)};
            String[] expectedPrefixes = {"dataset1", "dataset2"};
            int expectedTotalRows = 1000; // 500 + 500

            runMultiUrlImportTest(UUID.randomUUID(), TEST_KEYSPACE, TEST_TABLE, urls, startTokens, endTokens, zipSizes, expectedPrefixes, expectedTotalRows);
        }
    }
}
