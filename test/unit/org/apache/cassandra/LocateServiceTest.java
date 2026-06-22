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
import static org.junit.Assert.assertTrue;
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

public class LocateServiceTest
{

    private HttpURLConnection mockConnection;
    private LocateService service;

    @Before
    public void setUp() throws Exception
    {
        mockConnection = mock(HttpURLConnection.class);
        service = new LocateService()
        {
            protected HttpURLConnection getConnection(String host)
            {
                return mockConnection;
            }
        };

        when(mockConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    }

    @Test
    public void testGetDatacenterWithRegionEndingIn2() throws Exception
    {
        mockResponse("[{\"attrs\": {\"region\": \"us-east-2\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        assertEquals("us-east-2", service.getDatacenter(endpoint));
    }

    @Test
    public void testGetDatacenterWithRegionEndingIn1() throws Exception
    {
        mockResponse("[{\"attrs\": {\"region\": \"us-east-1\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        assertEquals("us-east", service.getDatacenter(endpoint));
    }

    @Test
    public void testGetRack() throws Exception
    {
        mockResponse("[{\"attrs\": {\"zone\": \"us-west-2b\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        assertEquals("2b", service.getRack(endpoint));
    }

    private void mockResponse(String json) throws Exception
    {
        when(mockConnection.getInputStream()).thenReturn(new ByteArrayInputStream(json.getBytes()));
    }

    @Test
    public void testTimeoutDuringHttpRequest() throws Exception
    {
        // Simulate a timeout exception when attempting to get the HTTP response code
        when(mockConnection.getResponseCode()).thenThrow(new SocketTimeoutException("Connection timed out"));

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");

        // Execute the test
        try
        {
            String datacenter = service.getDatacenter(endpoint);
            assertNull("Datacenter should be null on timeout", datacenter);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("The method should handle timeouts gracefully without throwing exceptions");
        }
    }

    @Test
    public void testGetId() throws Exception
    {
        // Mock the response for the getId method
        mockResponse("[{\"attrs\": {\"eddaUri\": \"http://some-host/api/v2/view/instances/i-0693202c7eab828e6\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        assertEquals("i-0693202c7eab828e6", service.getId(endpoint));
    }

    @Test
    public void testGetIdBadString() throws Exception
    {
        // Simulate a response where eddaUri does not match the expected pattern
        mockResponse("[{\"attrs\": {\"eddaUri\": \"http://some-host/api/v2/view/instances/invalid-id\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        // Expect null because the instance id format is invalid
        assertNull("Should return null for a bad eddaUri format", service.getId(endpoint));
    }

    /**
     * Regression test for the production bug where {@code nt ring --ids} reported wrong
     * instance ids cluster-wide. The locate service returns both an ENI entry and an
     * instance entry for the same IP; the ENI entry comes first and has an {@code eddaUri}
     * ending in {@code /networkInterfaces/eni-...}. The previous {@code .*(i-...)$} regex
     * captured the trailing {@code i-...} from inside the {@code eni-...} id and returned
     * that as if it were an instance id. The fix must instead return the real instance id.
     */
    @Test
    public void testGetIdPicksInstanceWhenEniEntryComesFirst() throws Exception
    {
        String json = "[" +
                      "  {\"id\":\"eni-0a92f0c8811ce0ec7\",\"attrs\":{" +
                      "      \"type\":\"eni\"," +
                      "      \"attachment.instanceId\":\"i-0247a5dbd43691118\"," +
                      "      \"eddaUri\":\"http://edda/api/v2/aws/networkInterfaces/eni-0a92f0c8811ce0ec7\"" +
                      "  }}," +
                      "  {\"id\":\"i-0247a5dbd43691118\",\"attrs\":{" +
                      "      \"type\":\"instance\"," +
                      "      \"eddaUri\":\"http://edda/api/v2/view/instances/i-0247a5dbd43691118\"" +
                      "  }}" +
                      "]";
        mockResponse(json);

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.176.97");
        assertEquals("i-0247a5dbd43691118", service.getId(endpoint));
    }

    /**
     * Even when the ENI entry does not expose {@code attachment.instanceId}, the strict
     * {@link com.netflix.cassandra.LocateService#ID_PATTERN} must not match the trailing
     * {@code i-...} of an {@code eni-...} id, and we must fall through to the instance entry's
     * {@code eddaUri}.
     */
    @Test
    public void testGetIdFallsThroughEniEddaUri() throws Exception
    {
        String json = "[" +
                      "  {\"attrs\":{" +
                      "      \"eddaUri\":\"http://edda/api/v2/aws/networkInterfaces/eni-0a92f0c8811ce0ec7\"" +
                      "  }}," +
                      "  {\"attrs\":{" +
                      "      \"eddaUri\":\"http://edda/api/v2/view/instances/i-0247a5dbd43691118\"" +
                      "  }}" +
                      "]";
        mockResponse(json);

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.176.97");
        assertEquals("i-0247a5dbd43691118", service.getId(endpoint));
    }

    /**
     * If locate returns only an ENI entry and we have no authoritative
     * {@code attachment.instanceId}, the strict regex must reject the {@code eni-...}
     * uri rather than incorrectly extracting an "instance id" from inside it.
     */
    @Test
    public void testGetIdReturnsNullForEniOnlyResponseWithoutAttachment() throws Exception
    {
        mockResponse("[{\"attrs\":{\"eddaUri\":\"http://edda/api/v2/aws/networkInterfaces/eni-0a92f0c8811ce0ec7\"}}]");

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.176.97");
        assertNull("Must not extract an instance id from an eni-... eddaUri",
                   service.getId(endpoint));
    }

    /**
     * When the ENI entry exposes {@code attachment.instanceId} we should use it directly
     * and never need to inspect the {@code eddaUri} of any entry.
     */
    @Test
    public void testGetIdPrefersAttachmentInstanceId() throws Exception
    {
        String json = "[" +
                      "  {\"attrs\":{" +
                      "      \"attachment.instanceId\":\"i-aaaaaaaaaaaaaaaaa\"," +
                      "      \"eddaUri\":\"http://edda/api/v2/view/instances/i-bbbbbbbbbbbbbbbbb\"" +
                      "  }}" +
                      "]";
        mockResponse(json);

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.176.97");
        assertEquals("i-aaaaaaaaaaaaaaaaa", service.getId(endpoint));
    }

    @Test
    public void testGetIdWithLongDelay() throws Exception
    {
        // Simulate a long delay in the HTTP response for getId
        doAnswer(invocation -> {
            Thread.sleep(3000); // simulate a 3-second delay
            return new ByteArrayInputStream(
            "[{\"attrs\": {\"eddaUri\": \"http://some-host/api/v2/view/instances/i-0693202c7eab828e6\"}}]".getBytes()
            );
        }).when(mockConnection).getInputStream();

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        long start = System.currentTimeMillis();
        String instanceId = service.getId(endpoint);
        long elapsed = System.currentTimeMillis() - start;
        assertTrue("Should delay at least 3 seconds", elapsed >= 3000);
        assertEquals("i-0693202c7eab828e6", instanceId);
    }

    @Test
    public void testGetIdIOException() throws Exception
    {
        // Simulate an I/O exception during the HTTP request for getId
        when(mockConnection.getInputStream()).thenThrow(new SocketTimeoutException("Connection timed out"));

        InetAddressAndPort endpoint = InetAddressAndPort.getByName("100.91.199.247");
        try
        {
            String instanceId = service.getId(endpoint);
            // Expecting null when an exception occurs
            assertNull("Should return null if an exception occurs while reading the response", instanceId);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("getId should handle IO exceptions gracefully and not propagate the exception");
        }
    }

    @After
    public void tearDown() throws Exception
    {
        // Clean up code if necessary
    }
}
