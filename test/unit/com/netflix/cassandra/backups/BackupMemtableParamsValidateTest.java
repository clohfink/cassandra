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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import software.amazon.awssdk.regions.Region;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackupMemtableParamsValidateTest
{
    private Map<String, String> envVars;
    private AwsAsyncS3FakeBackup fakeS3;

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

        fakeS3 = new AwsAsyncS3FakeBackup(envVars, Region.US_EAST_1);
        ObjectStoreAccess.set(Region.US_EAST_1, fakeS3);
    }

    @After
    public void cleanup()
    {
        ObjectStoreAccess.regionToClientMap.clear();
    }

    // === Happy path ===

    @Test
    public void testValidateSuccess()
    {
        injectPrefixKeys("some/backup/data");
        injectMetaKeys("some/meta/meta_v2_123.json");

        BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket,prefix=test/prefix");
        params.validate(); // should not throw
    }

    @Test
    public void testValidateSuccessWithBucketOverride()
    {
        injectPrefixKeys("data/file");
        injectMetaKeys("meta/file.json");

        BackupMemtableParams params = createParams("backupmemtable:token=42,bucket=custom-bucket,prefix=custom/prefix");
        params.validate();
    }

    @Test
    public void testValidateSuccessWithDerivedBucketAndPrefix()
    {
        injectPrefixKeys("data/file");
        injectMetaKeys("meta/file.json");

        BackupMemtableParams params = createParams("backupmemtable:token=42");
        params.validate();
    }

    @Test
    public void testValidateSuccessWithMultiplePrefixKeys()
    {
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Value<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            Arrays.asList("key1", "key2", "key3")));
        injectMetaKeys("meta/file.json");

        BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket,prefix=test/prefix");
        params.validate();
    }

    @Test
    public void testValidateSuccessWithMultipleMetaKeys()
    {
        injectPrefixKeys("data/file");
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Value<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            Arrays.asList("meta1.json", "meta2.json")));

        BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket,prefix=test/prefix");
        params.validate();
    }

    // === Invalid BackupContext ===

    @Test
    public void testValidateInvalidContext_BlankToken()
    {
        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=,bucket=test-bucket");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("Invalid backup memtable configuration"));
            assertTrue(e.getMessage().contains("token=''"));
        }
    }

    @Test
    public void testValidateInvalidContext_InvalidEnvironment()
    {
        envVars.put("NETFLIX_ENVIRONMENT", "staging");

        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("Invalid backup memtable configuration"));
            assertTrue(e.getMessage().contains("env='staging'"));
        }
    }

    @Test
    public void testValidateInvalidContext_InvalidEnvironmentViaConfigParam()
    {
        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket,NETFLIX_ENVIRONMENT=dev");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("Invalid backup memtable configuration"));
            assertTrue(e.getMessage().contains("env='dev'"));
        }
    }

    @Test
    public void testValidateInvalidContext_InvalidRegion()
    {
        envVars.put("NETFLIX_REGION", "us-fake-1");
        ObjectStoreAccess.set(Region.of("us-fake-1"), fakeS3);

        try
        {
            BackupMemtableParams params = new BackupMemtableParams("backupmemtable:token=123,bucket=test-bucket", envVars);
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("Invalid backup memtable configuration"));
            assertTrue(e.getMessage().contains("region='us-fake-1'"));
        }
    }

    @Test
    public void testValidateValidContextProdEnvironment()
    {
        envVars.put("NETFLIX_ENVIRONMENT", "prod");
        injectPrefixKeys("data/file");
        injectMetaKeys("meta/file.json");

        BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket");
        params.validate(); // prod is valid, should not throw
    }

    // === No backup data at prefix ===

    @Test
    public void testValidateNoPrefixData()
    {
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Value<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            new ArrayList<>()));

        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket,prefix=test/prefix");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("No backup data found"));
            assertTrue(e.getMessage().contains("test-bucket"));
            assertTrue(e.getMessage().contains("test/prefix"));
        }
    }

    // === No meta files for token ===

    @Test
    public void testValidateNoMetaFiles()
    {
        injectPrefixKeys("some/backup/data");
        // Meta prefix returns empty
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Value<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            new ArrayList<>()));

        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=999,bucket=test-bucket,prefix=test/prefix");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("No backup manifests found"));
            assertTrue(e.getMessage().contains("token '999'"));
        }
    }

    // === S3 errors ===

    @Test
    public void testValidateS3ErrorOnPrefixCheck()
    {
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Failure<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            new RuntimeException("Access Denied")));

        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket,prefix=test/prefix");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("Failed to access backup data"));
            assertTrue(e.getMessage().contains("test-bucket"));
            assertTrue(e.getMessage().contains("test/prefix"));
        }
    }

    @Test
    public void testValidateS3ErrorOnMetaCheck()
    {
        injectPrefixKeys("some/backup/data");
        // Meta check fails
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Failure<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            new RuntimeException("Connection timeout")));

        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket,prefix=test/prefix");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            assertTrue(e.getMessage().contains("Failed to list backup manifests"));
            assertTrue(e.getMessage().contains("token '123'"));
        }
    }

    @Test
    public void testValidateS3ErrorPreservesOriginalException()
    {
        RuntimeException cause = new RuntimeException("S3 bucket not found");
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Failure<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS, cause));

        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=bad-bucket,prefix=test/prefix");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            // Verify the original exception is preserved in the chain
            assertTrue(e.getCause() != null);
        }
    }

    // === Context validation checked before S3 calls ===

    @Test
    public void testValidateContextCheckedBeforeS3()
    {
        // No S3 injections - if context check doesn't short-circuit, the S3 call would
        // fail with a different error (no fakeS3RootDir set, no injections)
        envVars.put("NETFLIX_ENVIRONMENT", "staging");

        try
        {
            BackupMemtableParams params = createParams("backupmemtable:token=123,bucket=test-bucket");
            params.validate();
            fail("Expected ConfigurationException");
        }
        catch (ConfigurationException e)
        {
            // Should be the context validation error, not an S3 error
            assertTrue(e.getMessage().contains("Invalid backup memtable configuration"));
        }
    }

    // === Helpers ===

    private BackupMemtableParams createParams(String config)
    {
        return new BackupMemtableParams(config, envVars);
    }

    private void injectPrefixKeys(String... keys)
    {
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Value<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            Arrays.asList(keys)));
    }

    private void injectMetaKeys(String... keys)
    {
        fakeS3.injectBehavior(new AwsAsyncS3FakeBackup.Injection.Value<>(
            AwsAsyncS3FakeBackup.Method.GET_OBJECT_KEYS,
            Arrays.asList(keys)));
    }
}
