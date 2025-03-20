package com.netflix.cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.locator.InetAddressAndPort;

public class LocateService
{
    public static final Pattern ID_PATTERN = Pattern.compile(".*(i-[0-9a-zA-Z]+)$");
    public static final int TIMEOUT =  Integer.parseInt(System.getProperty("netflix.locate.timeout", "2000"));
    public static final int MAX_CACHE_SIZE =  Integer.parseInt(System.getProperty("netflix.locate.cache", "2000"));
    private static final Logger logger = LoggerFactory.getLogger(LocateService.class);
    private static final String URL_TEMPLATE = "http://locate.prod.netflix.net/api/v1/locate/%s";
    private static final ObjectMapper MAPPER = new ObjectMapper();


    public static final LocateService instance = new LocateService();

    // keyed off host address, netflix locate service does not have ip based endpoints so dont use InetAddressAndPort
    private final LoadingCache<String, String> datacenterCache;
    private final LoadingCache<String, String> rackCache;
    private final LoadingCache<String, String> idCache;

    public LocateService()
    {
        datacenterCache = Caffeine.newBuilder().maximumSize(MAX_CACHE_SIZE)
                .build(endpoint ->
                       getAttribute(endpoint, "region")
                       // hack for CASSANDRA-4026. e.g., "us-east-1" --> "us-east, "us-west-2" --> "us-west-2
                       .map(region -> region.endsWith("1") ? region.substring(0, region.length() - 2) : region)
                       .orElse(null));

        rackCache = Caffeine.newBuilder().maximumSize(MAX_CACHE_SIZE)
                .build(endpoint ->
                       getAttribute(endpoint, "zone")
                       .map(zone -> {
                           String[] parts = zone.split("-");
                           return parts.length > 0 ? parts[parts.length - 1] : null;
                       })
                       .orElse(null));

        idCache = Caffeine.newBuilder().maximumSize(MAX_CACHE_SIZE)
                          .build(endpoint -> {
                              Optional<String> optionalUrl = getAttribute(endpoint, "eddaUri");
                              if (optionalUrl.isPresent()) {
                                  String url = optionalUrl.get();
                                  Matcher matcher = ID_PATTERN.matcher(url);
                                  if (matcher.find()) {
                                      return matcher.group(1);
                                  }
                              }
                              return null;
                          });
    }

    public String getDatacenter(InetAddressAndPort endpoint)
    {
        return datacenterCache.get(endpoint.getHostAddress(false));
    }

    public String getRack(InetAddressAndPort endpoint)
    {
        return rackCache.get(endpoint.getHostAddress(false));
    }

    public String getId(InetAddressAndPort endpoint)
    {
        return idCache.get(endpoint.getHostAddress(false));
    }

    private Optional<String> getAttribute(String host, String attributeName)
    {
        try
        {
            String jsonString = locateCall(host);
            if (jsonString == null)
            {
                return Optional.empty();
            }

            ArrayNode jsonArray = (ArrayNode) MAPPER.readTree(jsonString);
            for (JsonNode jsonObject : jsonArray)
            {
                JsonNode attrs = jsonObject.get("attrs");
                if (attrs.has(attributeName))
                {
                    return Optional.ofNullable(attrs.get(attributeName).asText());
                }
            }
        }
        catch (IOException e)
        {
            logger.debug("Failed to get attribute for endpoint {}", host, e);
        }
        return Optional.empty();
    }

    // This is broken out for mocking in tests
    protected HttpURLConnection getConnection(String host)
    {
        String url = String.format(URL_TEMPLATE, host);
        try
        {
            return (HttpURLConnection) new URL(url).openConnection();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String locateCall(String host)
    {
        HttpURLConnection conn = null;
        try
        {
            conn = getConnection(host);
            conn.setRequestMethod("GET");

            conn.setConnectTimeout(TIMEOUT);
            conn.setReadTimeout(TIMEOUT);

            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK)
            {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream())))
                {
                    StringBuilder content = new StringBuilder();
                    String inputLine;
                    while ((inputLine = in.readLine()) != null)
                    {
                        content.append(inputLine);
                    }
                    return content.toString();
                }
            }
        }
        catch (Throwable e)
        {
            logger.debug("Failed to get attribute for endpoint {}", host, e);
        }
        finally
        {
            if (conn != null)
            {
                conn.disconnect();
            }
        }
        return null;
    }
}
