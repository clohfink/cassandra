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

import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

public class TokenService
{
    public static class ServiceResponse
    {
        public final int statusCode;
        public final String body;

        public ServiceResponse(int statusCode, String body)
        {
            this.statusCode = statusCode;
            this.body = body;
        }
    }

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
    "https://odscasstokens.cluster.{region}.{environment}.cloud.netflix.net:7004"
    );
    private static final String TOKEN_SERVICE_APP_NAME = System.getProperty("netflix.tokenservice.name", "odscasstokens");

    public final String app;
    public final String region;
    public final String env;
    @Nullable public final String instanceId;

    public TokenService()
    {
        Map<String, String> envVars = System.getenv();
        this.region = envVars.getOrDefault("NETFLIX_REGION", "us-east-1");
        this.env = envVars.getOrDefault("NETFLIX_ENVIRONMENT", "test");
        this.app = envVars.getOrDefault("NETFLIX_APP", "cass_local");
        this.instanceId = envVars.get("NETFLIX_INSTANCE_ID");
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
     * On connection errors, falls back to the next region. On any HTTP response (including non-200),
     * returns immediately so callers can inspect the status code.
     *
     * @param endpoint the endpoint to use, e.g., "/v1/cluster/prod/app".
     * @return a ServiceResponse containing the HTTP status code and response body.
     * @throws IOException if all attempts to connect fail.
     */
    public ServiceResponse fetchDataFromService(String endpoint) throws IOException
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
                                                 .replace("{environment}", env) + endpoint;
                logger.info("Fetching {}", urlStr);
                return fetchServiceResponse(urlStr);
            }
            catch (Exception e)
            {
                logger.error("Failed to fetch {} from region: {}, trying next region...", endpoint, currentRegion, e);
            }
        }

        throw new IOException("Error retrieving data from all available regions");
    }

    /**
     * Fetches the current instance from the token service at /v1/token/current.
     *
     * @return the current NetflixInstance, or null if the service returns 404 (no token assigned).
     * @throws IOException if the service is unreachable or returns an unexpected error.
     */
    @Nullable
    public NetflixInstance getCurrentInstance() throws IOException
    {
        ServiceResponse response = fetchDataFromService("/v1/token/current");

        if (response.statusCode == 404)
        {
            logger.info("No current token assignment found (404)");
            return null;
        }

        if (response.statusCode != 200)
            throw new IOException("Failed to get current token: HTTP " + response.statusCode + " - " + response.body);

        return new ObjectMapper().readValue(response.body, NetflixInstance.class);
    }

    public List<NetflixInstance> getInstances() throws IOException
    {
        ServiceResponse response = fetchDataFromService("/v1/cluster/" + env + '/' + app);

        if (response.statusCode != 200)
            throw new IOException("Failed to get instances: HTTP " + response.statusCode + " - " + response.body);

        List<NetflixInstance> instances = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(response.body);

        if (root.isArray())
        {
            for (JsonNode node : root)
            {
                NetflixInstance instance = mapper.treeToValue(node, NetflixInstance.class);
                instances.add(instance);
            }
        }
        return instances;
    }

    @VisibleForTesting
    public HttpURLConnection getConnection(String urlStr) throws IOException
    {
        URL url = new URL(urlStr);
        logger.info("Connecting to service at URL: {}", urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        // Managed (DGW) endpoints are https and authenticate with a Metatron client cert.
        // A local token server (see RUNNING_LOCALLY.md) is plain http and needs no SSL
        // setup, so only configure Metatron mTLS when we actually have an https connection.
        // This keeps production behavior unchanged while allowing a local http token server.
        if (conn instanceof HttpsURLConnection)
        {
            HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
            httpsConn.setSSLSocketFactory(MetatronSslContext.forClient(TOKEN_SERVICE_APP_NAME).getSocketFactory());
            httpsConn.setHostnameVerifier((hostname, session) -> true);
        }
        conn.setRequestMethod("GET");
        conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        return conn;
    }

    /**
     * Fetches the response from the given URL.
     *
     * @param urlStr the URL to connect to.
     * @return a ServiceResponse containing the HTTP status code and response body.
     * @throws Exception if there is a connection error.
     */
    private ServiceResponse fetchServiceResponse(String urlStr) throws Exception
    {
        HttpURLConnection conn = getConnection(urlStr);
        int responseCode = conn.getResponseCode();

        StringBuilder body = new StringBuilder();
        if (responseCode >= 200 && responseCode < 300)
        {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream())))
            {
                String line;
                while ((line = in.readLine()) != null)
                    body.append(line);
            }
        }
        else
        {
            try (BufferedReader errorReader = new BufferedReader(new InputStreamReader(conn.getErrorStream())))
            {
                String line;
                while ((line = errorReader.readLine()) != null)
                    body.append(line);
            }
            catch (Exception e)
            {
                // Error stream might be null or unreadable
            }
        }

        return new ServiceResponse(responseCode, body.toString());
    }
}
