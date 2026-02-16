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

package com.netflix.cassandra.backups;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BackupMemtableParamsTest
{
    private Map<String, String> envVars;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        envVars = new HashMap<>();
        envVars.put("NETFLIX_REGION", "us-east-1");
        envVars.put("NETFLIX_APP", "testapp");
        envVars.put("NETFLIX_ENVIRONMENT", "test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_BackupMemtableParams_InitializationNoPrefix()
    {
        BackupMemtableParams params = new BackupMemtableParams("");
    }

    @Test
    public void test_BackupMemtableParams_ValidInitialization_PrefixOverride()
    {
        String config = "backupmemtable:prefix=test/prefix,bucket=test-bucket,keyspace=testks,token=3,table=testtable,timestamp=1234567890";
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);
        
        assertEquals("test-bucket", params.getBucket());
        assertEquals("test/prefix", params.getPrefix());
        assertEquals("testks", params.getKeyspace());
        assertEquals("testtable", params.getTable());
        assertEquals(1234567890L, params.getTimestamp());
        assertEquals("3", params.getToken());
    }

    @Test
    public void test_BackupMemtableParams_PrefixDerived()
    {
        String config = "backupmemtable:bucket=test-bucket,keyspace=testks,token=3,table=testtable,timestamp=1234567890";
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);

        assertEquals("test_backup/-3681_testapp", params.getPrefix());
    }

    @Test
    public void test_BackupMemtableParams_BackupAccessSettingsParsed() {
        String config = "backupmemtable:prefix=test/prefix,bucket=test-bucket,keyspace=testks,token=0,table=testtable,timestamp=1234567890";
        envVars.put("BACKUP_MEMTABLE_ACCESS_SETTINGS", "maxRetries=5");
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);

        ObjectStoreConfiguration configuration = params.getAsyncS3AccessConfiguration();
        assertNotNull(configuration);
        assertEquals(5, configuration.maxRetries);
        assertEquals("test-bucket", params.getBucket());
    }

    @Test
    public void test_BackupMemtableParams_BackupAccessSettings_DefaultRetries() {
        String config = "backupmemtable:prefix=test/prefix,bucket=test-bucket,keyspace=testks,token=0,table=testtable,timestamp=1234567890";
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);

        ObjectStoreConfiguration configuration = params.getAsyncS3AccessConfiguration();
        assertNotNull(configuration);
        assertEquals(2, configuration.maxRetries);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_BackupMemtableParams_InvalidParameter()
    {
        String config = "backupmemtable:invalidparam=value";
        new BackupMemtableParams(config, envVars);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_BackupMemtableParams_MissingRequiredEnvVars()
    {
        Map<String, String> emptyEnvVars = new HashMap<>();
        String config = "backupmemtable:prefix=test/prefix";
        new BackupMemtableParams(config, emptyEnvVars);
    }

    @Test
    public void test_BackupMemtableParams_DerivedBucketFromEnvVars()
    {
        String config = "backupmemtable:prefix=test/prefix,token=0";
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);
        
        assertEquals("useast1-cass-test-1", params.getBucket());
    }

    @Test
    public void test_BackupMemtableParams_DerivedPrefixFromEnvVars()
    {
        String config = "backupmemtable:bucket=test-bucket,token=0";
        BackupMemtableParams params = new BackupMemtableParams(config, envVars);
        
        assertNotNull(params.getPrefix());
        assertEquals("test_backup/" + String.format("%d_%s", "testapp".hashCode() % 10000, "testapp"), params.getPrefix());
    }

    @Test
    public void test_loadManifest_SuccessfulLoad() throws Exception
    {
        // Create a temporary file with valid JSON manifest
        File tempFile = FileUtils.createTempFile("manifest", ".json");
        String validJson = "{\"data\":[{\"keyspaceName\":\"testks\",\"columnfamilyName\":\"testtable\"}]}";
        Files.write(tempFile.toPath(), validJson.getBytes());

        BackupMemtableParams params = new BackupMemtableParams("backupmemtable:token=0,bucket=test-bucket", envVars);
        BackupManifest manifest = params.loadManifest(tempFile);

        assertNotNull(manifest);
        assertEquals(1, manifest.getData().size());
        assertEquals("testks", manifest.getData().get(0).getKeyspaceName());
        assertEquals("testtable", manifest.getData().get(0).getColumnfamilyName());

        tempFile.delete();
    }

    @Test(expected = RuntimeException.class)
    public void test_loadManifest_InvalidJson() throws Exception
    {
        // Create a temporary file with invalid JSON
        File tempFile = FileUtils.createTempFile("manifest", ".json");
        String invalidJson = "{invalid json content}";
        Files.write(tempFile.toPath(), invalidJson.getBytes());

        BackupMemtableParams params = new BackupMemtableParams("backupmemtable:token=0,bucket=test-bucket", envVars);
        params.loadManifest(tempFile);

        tempFile.delete();
    }

    @Test(expected = RuntimeException.class)
    public void test_loadManifest_EmptyFile() throws Exception
    {
        // Create an empty temporary file
        File tempFile = FileUtils.createTempFile("manifest", ".json");

        BackupMemtableParams params = new BackupMemtableParams("backupmemtable:token=0,bucket=test-bucket", envVars);
        params.loadManifest(tempFile);

        tempFile.delete();
    }
}