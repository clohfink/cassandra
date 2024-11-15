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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import org.junit.Before;
import org.junit.Test;

import com.netflix.cassandra.NetflixInstance;
import com.netflix.cassandra.TokenService;

import static org.antlr.tool.ErrorManager.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TokenServiceTest
{
    private TokenService tokenService;
    private HttpsURLConnection mockConnection;

    @Before
    public void setUp()
    {
        // Mock the HttpsURLConnection
        mockConnection = mock(HttpsURLConnection.class);

        // Instantiate TokenService and override getConnection to return the mocked connection
        tokenService = new TokenService("test-app", "us-east-1", "test-env", "i-123456789")
        {
            @Override
            public HttpsURLConnection getConnection(String urlStr)
            {
                return mockConnection;
            }
        };
    }

    // Mock the getConnection method to return the mocked connection
    private void mockConnectionWithResponse(String response, int responseCode) throws Exception
    {
        when(mockConnection.getResponseCode()).thenReturn(responseCode);

        // Mock input stream from connection
        InputStream inputStream = new InputStream()
        {
            private final String data = response;
            private int pos = 0;

            @Override
            public int read()
            {
                if (pos >= data.length()) return -1;
                return data.charAt(pos++);
            }
        };

        when(mockConnection.getInputStream()).thenReturn(inputStream);
        when(mockConnection.getErrorStream()).thenReturn(null);
    }

    @Test
    public void testFetchServiceResponseSuccess() throws Exception
    {
        // Prepare mock response
        String expectedResponse = "{\"updateTime\": 1728398107613, \"createdTime\": 1728397893251, \"app\": \"test-app\"}";
        mockConnectionWithResponse(expectedResponse, 200);

        // Call fetchDataFromService and assert the result
        String response = tokenService.fetchDataFromService("/v1/cluster/test-env/test-app");
        assertEquals(expectedResponse, response);
    }

    @Test(expected = IOException.class)
    public void testFetchServiceResponseTimeout() throws Exception
    {
        // Simulate timeout exception
        doThrow(new IOException("Connection timed out")).when(mockConnection).connect();

        // Attempt to fetch data should result in a timeout exception
        tokenService.fetchDataFromService("/v1/cluster/test-env/test-app");
    }

    @Test(expected = IOException.class)
    public void testFetchServiceResponseFailure() throws Exception
    {
        // Prepare a non-200 response code to simulate a failure
        mockConnectionWithResponse("", 500);

        // Fetch should fail due to 500 error
        tokenService.fetchDataFromService("/v1/cluster/test-env/test-app");
    }

    @Test
    public void testFetchServiceResponseMultipleRegions() throws Exception
    {
        // Prepare mock response only for the second region
        String expectedResponse = "{\"updateTime\": 1728398107613, \"createdTime\": 1728397893251, \"app\": \"test-app\"}";

        // First region will fail
        HttpsURLConnection firstRegionConnection = mock(HttpsURLConnection.class);
        when(firstRegionConnection.getResponseCode()).thenThrow(new IOException("Failed to connect"));

        // Second region will succeed
        HttpsURLConnection secondRegionConnection = mock(HttpsURLConnection.class);
        mockConnectionWithResponse(expectedResponse, 200);
        when(secondRegionConnection.getResponseCode()).thenReturn(200);
        when(secondRegionConnection.getInputStream()).thenReturn(new InputStream()
        {
            private final String data = expectedResponse;
            private int pos = 0;

            @Override
            public int read()
            {
                if (pos >= data.length()) return -1;
                return data.charAt(pos++);
            }
        });

        // Override getConnection to return the first and then the second connection
        TokenService tokenServiceSpy = spy(tokenService);
        doReturn(firstRegionConnection).doReturn(secondRegionConnection).when(tokenServiceSpy).getConnection(anyString());

        // Call fetchDataFromService and assert the result
        String response = tokenServiceSpy.fetchDataFromService("/v1/cluster/test-env/test-app");
        assertEquals(expectedResponse, response);
    }

    @Test
    public void testFetchServiceResponseHandlesInvalidJSON() throws Exception
    {
        // Prepare an invalid JSON response
        String invalidJson = "INVALID_JSON";
        mockConnectionWithResponse(invalidJson, 200);

        // Try to parse the invalid JSON and expect an error
        try
        {
            tokenService.getInstances();
            fail("Expected an IOException due to invalid JSON format");
        }
        catch (IOException e)
        {
            // Exception is expected
            assertTrue(e.getMessage().contains("Unrecognized token"), "unexpected exception " + e.getMessage());
        }
    }

    @Test
    public void testGetInstancesSuccess() throws Exception
    {
        // Mock a successful JSON response
        String jsonResponse = "[{\"updateTime\": 1728398107613, \"createdTime\": 1728397893251, \"app\": \"test-app\", " +
                              "\"instanceId\": \"i-123456789\", \"availabilityZone\": \"us-east-1a\", \"token\": \"-7173733804634027806\", " +
                              "\"region\": \"us-east-1\", \"id\": -1670265060, \"hostIP\": \"100.107.12.161\", \"hostName\": \"ip-100-107-12-161.ec2.internal\"}]";

        mockConnectionWithResponse(jsonResponse, 200);

        // Call the getInstances method
        List<NetflixInstance> instances = tokenService.getInstances();

        // Verify the result
        assertNotNull(instances);
        assertEquals(1, instances.size());

        NetflixInstance instance = instances.get(0);
        assertEquals("test-app", instance.getApp());
        assertEquals("i-123456789", instance.getInstanceId());
        assertEquals("us-east-1a", instance.getAvailabilityZone());
        assertEquals("100.107.12.161", instance.getHostIP());
    }


    @Test
    public void testGetInstancesMultipleInstances() throws Exception
    {
        // Mock a JSON response with multiple instances
        String jsonResponse = "[{\"updateTime\": 1728398107613, \"createdTime\": 1728397893251, \"app\": \"test-app\", " +
                              "\"instanceId\": \"i-123456789\", \"availabilityZone\": \"us-east-1a\", \"token\": \"-7173733804634027806\", " +
                              "\"region\": \"us-east-1\", \"id\": -1670265060, \"hostIP\": \"100.107.12.161\", \"hostName\": \"ip-100-107-12-161.ec2.internal\"}, " +
                              "{\"updateTime\": 1728398107614, \"createdTime\": 1728397893252, \"app\": \"test-app\", " +
                              "\"instanceId\": \"i-987654321\", \"availabilityZone\": \"us-east-1b\", \"token\": \"-7173733804634027807\", " +
                              "\"region\": \"us-east-1\", \"id\": -1670265061, \"hostIP\": \"100.107.12.162\", \"hostName\": \"ip-100-107-12-162.ec2.internal\"}]";

        mockConnectionWithResponse(jsonResponse, 200);

        // Call the getInstances method
        List<NetflixInstance> instances = tokenService.getInstances();

        // Verify the result
        assertNotNull(instances);
        assertEquals(2, instances.size());

        NetflixInstance firstInstance = instances.get(0);
        assertEquals("test-app", firstInstance.getApp());
        assertEquals("i-123456789", firstInstance.getInstanceId());
        assertEquals("us-east-1a", firstInstance.getAvailabilityZone());
        assertEquals("100.107.12.161", firstInstance.getHostIP());

        NetflixInstance secondInstance = instances.get(1);
        assertEquals("test-app", secondInstance.getApp());
        assertEquals("i-987654321", secondInstance.getInstanceId());
        assertEquals("us-east-1b", secondInstance.getAvailabilityZone());
        assertEquals("100.107.12.162", secondInstance.getHostIP());
    }

    @Test(expected = IOException.class)
    public void testGetInstancesInvalidResponseCode() throws Exception
    {
        // Simulate a non-200 response code
        mockConnectionWithResponse("", 500);

        // Call the getInstances method, which should throw an IOException
        tokenService.getInstances();
    }

    @Test
    public void testGetInstancesHandlesEmptyResponse() throws Exception
    {
        // Mock an empty JSON array response
        String jsonResponse = "[]";
        mockConnectionWithResponse(jsonResponse, 200);

        // Call the getInstances method
        List<NetflixInstance> instances = tokenService.getInstances();

        // Verify the result (it should return an empty list)
        assertNotNull(instances);
        assertTrue(instances.isEmpty(), "instances was not empty " + instances);
    }

    @Test(expected = IOException.class)
    public void testGetInstancesInvalidJSON() throws Exception
    {
        // Mock an invalid JSON response
        String invalidJson = "INVALID_JSON";
        mockConnectionWithResponse(invalidJson, 200);

        // Call the getInstances method, expecting it to throw an IOException due to invalid JSON
        tokenService.getInstances();
    }
}
