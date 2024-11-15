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

package com.netflix.cassandra;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.metatron.ipc.security.MetatronSslContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

public class TokenService
{
    private static final Logger logger = LoggerFactory.getLogger(TokenService.class);
    private static final List<String> DEFAULT_REGIONS = Arrays.asList("us-east-1", "us-east-2", "us-west-2", "eu-west-1");
    private static final String REGION_PROPERTY = System.getProperty("netflix.tokenservice.regions");

    // Override the regions with a system property, or use the default list
    private static final List<String> REGIONS = (REGION_PROPERTY != null && !REGION_PROPERTY.isEmpty())
                                                ? Arrays.stream(REGION_PROPERTY.split(","))
                                                        .map(String::trim)
                                                        .collect(Collectors.toList())
                                                : DEFAULT_REGIONS;

    private static final int TIMEOUT_SECONDS = Integer.parseInt(System.getProperty("netflix.tokenservice.timeout", "10"));
    private static final String BASE_URL_TEMPLATE = System.getProperty(
    "netflix.tokenservice.url",
    "https://nfcassandratokens.cluster.{region}.{environment}.cloud.netflix.net:7004"
    );
    private static final String TOKEN_SERVICE_APP_NAME = System.getProperty("netflix.tokenservice.name", "nfcassandratokens");

    public final String app;
    public final String region;
    public final String env;
    public final String instanceId;

    public TokenService()
    {
        this.region = System.getenv("NETFLIX_REGION");
        this.env = System.getenv("NETFLIX_ENVIRONMENT");
        this.app = System.getenv("NETFLIX_APP");
        this.instanceId = System.getenv("NETFLIX_INSTANCE_ID");

        if (region == null || env == null || app == null || instanceId == null)
            throw new IllegalStateException("Missing environment variables: NETFLIX_*");
    }

    public TokenService(String app, String region, String env, String instanceId)
    {
        this.app = app;
        this.region = region;
        this.env = env;
        this.instanceId = instanceId;
    }

    /**
     * Fetches the response from a URL, trying the local region first and falling back to other regions.
     *
     * @param endpoint the endpoint to use, e.g., "/v1/cluster/prod/app".
     * @return JSON response as a String.
     * @throws IOException if all attempts to fetch the data fail.
     */
    public String fetchDataFromService(String endpoint) throws IOException
    {
        List<String> regionsToTry = new ArrayList<>();
        regionsToTry.add(region); // Always try the local region first
        for (String currentRegion : REGIONS)
        {
            if (!currentRegion.equals(region))
                regionsToTry.add(currentRegion);
        }

        for (String currentRegion : regionsToTry)
        {
            try
            {
                String urlStr = BASE_URL_TEMPLATE.replace("{region}", currentRegion)
                                                 .replace("{environment}", env);
                logger.info("Fetching service response from region: {}", currentRegion);
                return fetchServiceResponse(urlStr);
            }
            catch (Exception e)
            {
                logger.error("Failed to fetch data from region: {}, trying next region...", currentRegion, e);
            }
        }

        throw new IOException("Error retrieving data from all available regions");
    }

    public List<NetflixInstance> getInstances() throws IOException
    {
        List<NetflixInstance> instances = new ArrayList<>();
        // Fetch the JSON response from the service
        String response = fetchDataFromService("/v1/cluster/" + env + '/' + app);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response);

        if (root.isArray())
        {
            for (JsonNode node : root)
            {
                // Convert each JSON node into a NetflixInstance object
                NetflixInstance instance = mapper.treeToValue(node, NetflixInstance.class);
                instances.add(instance);
            }
        }
        return instances;
    }

    @VisibleForTesting
    public HttpsURLConnection getConnection(String urlStr) throws IOException
    {
        URL url = new URL(urlStr);
        logger.info("Connecting to service at URL: {}", urlStr);
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
        conn.setSSLSocketFactory(MetatronSslContext.forClient(TOKEN_SERVICE_APP_NAME).getSocketFactory());
        conn.setHostnameVerifier((hostname, session) -> true);
        conn.setRequestMethod("GET");
        conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        return conn;
    }

    /**
     * Fetches the response from the given URL.
     *
     * @param urlStr the URL to connect to.
     * @return the response from the service as a String.
     * @throws Exception if there is an error fetching the response.
     */
    private String fetchServiceResponse(String urlStr) throws Exception
    {
        HttpsURLConnection conn = getConnection(urlStr);

        // Check the response code to ensure the request was successful
        int responseCode = conn.getResponseCode();
        if (responseCode != 200)
        {
            logger.error("Failed to get data from service: HTTP error code {}", responseCode);
            throw new RuntimeException("Failed to get data from service: HTTP error code " + responseCode);
        }

        // Read the response from the service
        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null)
            response.append(inputLine);
        in.close();
        return response.toString();
    }
}
