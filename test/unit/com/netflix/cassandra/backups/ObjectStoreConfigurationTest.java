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
import static org.junit.Assert.assertSame;

public class ObjectStoreConfigurationTest
{
    @Test
    public void testBuilderWithCustomValues()
    {
        ObjectStoreConfiguration config = ObjectStoreConfiguration.builder()
                                                                  .withMaxRetries(5)
                                                                  .build();
        
        assertEquals(5, config.maxRetries);
    }

    @Test
    public void testBuilderChaining()
    {
        ObjectStoreConfiguration.Builder builder = ObjectStoreConfiguration.builder();
        ObjectStoreConfiguration.Builder returnedBuilder = builder
            .withMaxRetries(3);
        
        assertSame(builder, returnedBuilder);
    }

    @Test
    public void testFromKeyValueStringWithMaxRetries()
    {
        String configString = "maxRetries=5";
        
        ObjectStoreConfiguration config = ObjectStoreConfiguration.fromKeyValueString(configString);
        
        assertEquals(5, config.maxRetries);
    }

    @Test
    public void testFromKeyValueStringWithCustomMaxRetries()
    {
        String configString = "maxRetries=3";
        
        ObjectStoreConfiguration config = ObjectStoreConfiguration.fromKeyValueString(configString);
        
        assertEquals(3, config.maxRetries);
    }

    @Test
    public void testFromKeyValueStringWithEmptyString()
    {
        ObjectStoreConfiguration config = ObjectStoreConfiguration.fromKeyValueString("");
        
        // Should use default
        assertEquals(2, config.maxRetries);
    }

    @Test
    public void testFromKeyValueStringWithWhitespace()
    {
        String configString = "  maxRetries = 4  ";
        
        ObjectStoreConfiguration config = ObjectStoreConfiguration.fromKeyValueString(configString);
        
        assertEquals(4, config.maxRetries);
    }

    @Test
    public void testFromKeyValueStringWithInvalidPairs()
    {
        String configString = "maxRetries=4,invalidPair";
        
        ObjectStoreConfiguration config = ObjectStoreConfiguration.fromKeyValueString(configString);
        
        // Should ignore invalid pair and process valid ones
        assertEquals(4, config.maxRetries);
    }

    @Test
    public void testFromKeyValueStringWithMultipleEquals()
    {
        String configString = "maxRetries=4,description=test=value";
        
        ObjectStoreConfiguration config = ObjectStoreConfiguration.fromKeyValueString(configString);
        
        // Should handle split with limit 2, ignoring description parameter
        assertEquals(4, config.maxRetries);
    }

    @Test(expected = NumberFormatException.class)
    public void testFromKeyValueStringWithInvalidMaxRetries()
    {
        String configString = "maxRetries=invalid";
        ObjectStoreConfiguration.fromKeyValueString(configString);
    }



    @Test
    public void testZeroMaxRetries()
    {
        ObjectStoreConfiguration config = ObjectStoreConfiguration.builder()
                                                                  .withMaxRetries(0)
                                                                  .build();
        
        assertEquals(0, config.maxRetries);
    }

    @Test
    public void testLargeMaxRetries()
    {
        ObjectStoreConfiguration config = ObjectStoreConfiguration.builder()
                                                                  .withMaxRetries(Integer.MAX_VALUE)
                                                                  .build();
        
        assertEquals(Integer.MAX_VALUE, config.maxRetries);
    }

    @Test
    public void testNegativeMaxRetries()
    {
        ObjectStoreConfiguration config = ObjectStoreConfiguration.builder()
                                                                  .withMaxRetries(-1)
                                                                  .build();
        
        assertEquals(-1, config.maxRetries);
    }

    @Test
    public void testFromKeyValueStringWithNegativeMaxRetries()
    {
        String configString = "maxRetries=-1";
        
        ObjectStoreConfiguration config = ObjectStoreConfiguration.fromKeyValueString(configString);
        
        assertEquals(-1, config.maxRetries);
    }

    @Test
    public void testDefaultConfiguration()
    {
        ObjectStoreConfiguration config = ObjectStoreConfiguration.builder().build();
        
        assertEquals(2, config.maxRetries);
    }
}