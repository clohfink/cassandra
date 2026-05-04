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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackupContextTest
{
    @Test
    public void testSstV2PrefixAndPath()
    {
        BackupContext ctx = new BackupContext("test", "us-east-1", "cass_dgw_ts_ntl_logblob",
                                              "-6148914689427941605");

        assertTrue(ctx.isValid());
        String expectedPrefix = ctx.prefix() + "/-6148914689427941605/SST_V2/";
        assertEquals(expectedPrefix, ctx.sstV2Prefix());

        String path = ctx.sstableComponentPath(1775754001000L,
                                               "system",
                                               "IndexInfo-9f5c6374d48532299a0a5094af9ad1e3",
                                               "NONE",
                                               "PLAINTEXT",
                                               "nb-1-BIG-Data.db");
        assertEquals(expectedPrefix
                     + "1775754001000/system/IndexInfo-9f5c6374d48532299a0a5094af9ad1e3/NONE/PLAINTEXT/nb-1-BIG-Data.db",
                     path);
    }

    @Test
    public void testInvalidContextFailsValidation()
    {
        BackupContext missingEnv = new BackupContext(null, "us-east-1", "app", "0");
        assertFalse(missingEnv.isValid());

        BackupContext badEnv = new BackupContext("staging", "us-east-1", "app", "0");
        assertFalse(badEnv.isValid());

        BackupContext badRegion = new BackupContext("test", "not-a-region", "app", "0");
        assertFalse(badRegion.isValid());
    }
}
