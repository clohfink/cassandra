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

package org.apache.cassandra;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.io.ByteArrayInputStream;

import com.netflix.cassandra.LocateService;
import org.apache.cassandra.locator.InetAddressAndPort;

public class LocateServiceTest {

    private HttpURLConnection mockConnection;
    private LocateService service;

    @Before
    public void setUp() throws Exception {
        mockConnection = mock(HttpURLConnection.class);
        service = new LocateService() {
            protected HttpURLConnection getConnection(String host)
            {
                return mockConnection;
            }
        };

        when(mockConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    }

    @Test
    public void testGetDatacenterWithRegionEndingIn2() throws Exception {
        mockResponse("[{\"attrs\": {\"region\": \"us-east-2\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        assertEquals("us-east-2", service.getDatacenter(endpoint));
    }

    @Test
    public void testGetDatacenterWithRegionEndingIn1() throws Exception {
        mockResponse("[{\"attrs\": {\"region\": \"us-east-1\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        assertEquals("us-east", service.getDatacenter(endpoint));
    }

    @Test
    public void testGetRack() throws Exception {
        mockResponse("[{\"attrs\": {\"zone\": \"us-west-2b\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        assertEquals("2b", service.getRack(endpoint));
    }

    private void mockResponse(String json) throws Exception {
        when(mockConnection.getInputStream()).thenReturn(new ByteArrayInputStream(json.getBytes()));
    }

    @Test
    public void testTimeoutDuringHttpRequest() throws Exception {
        // Simulate a timeout exception when attempting to get the HTTP response code
        when(mockConnection.getResponseCode()).thenThrow(new SocketTimeoutException("Connection timed out"));

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");

        // Execute the test
        try {
            String datacenter = service.getDatacenter(endpoint);
            assertNull("Datacenter should be null on timeout", datacenter);
        } catch (Exception e) {
            e.printStackTrace();
            fail("The method should handle timeouts gracefully without throwing exceptions");
        }
    }

    @After
    public void tearDown() throws Exception {
        // Clean up code if necessary
    }
}