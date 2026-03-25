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
package org.apache.cassandra.config;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;

public class ConfigCheckCompositeData
{
    private static final String[] ITEM_NAMES = { "changed", "loaded_hash", "file_hash", "loaded_config", "file_config" };
    private static final String[] ITEM_DESCS = { "whether the on-disk config differs from the loaded config",
                                                  "hash of cassandra.yaml at load time",
                                                  "hash of cassandra.yaml currently on disk",
                                                  "config file content at load time",
                                                  "config file content currently on disk" };
    private static final OpenType<?>[] ITEM_TYPES = { SimpleType.BOOLEAN, SimpleType.STRING, SimpleType.STRING,
                                                       SimpleType.STRING, SimpleType.STRING };

    private static final CompositeType TYPE;

    static
    {
        try
        {
            TYPE = new CompositeType("ConfigCheck", "Config file check result", ITEM_NAMES, ITEM_DESCS, ITEM_TYPES);
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static CompositeType getType()
    {
        return TYPE;
    }

    public static CompositeData build(String loadedHash, String fileHash, String loadedConfig, String fileConfig)
    {
        boolean changed = loadedHash == null || !loadedHash.equals(fileHash);
        try
        {
            return new CompositeDataSupport(TYPE, ITEM_NAMES,
                                            new Object[]{ changed, loadedHash, fileHash, loadedConfig, fileConfig });
        }
        catch (OpenDataException e)
        {
            throw new RuntimeException("Failed to build ConfigCheck CompositeData", e);
        }
    }
}
