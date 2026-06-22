package com.netflix.cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
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
    // Match instance ids only when preceded by a path separator. Without the leading '/',
    // this pattern would also match the trailing characters of an ENI id like
    // "/networkInterfaces/eni-0a92f0c8811ce0ec7", incorrectly extracting
    // "i-0a92f0c8811ce0ec7" (the eni id minus its "en" prefix) as if it were an instance id.
    public static final Pattern ID_PATTERN = Pattern.compile(".*/(i-[0-9a-zA-Z]+)$");
    public static final int TIMEOUT =  Integer.parseInt(System.getProperty("netflix.locate.timeout", "2000"));
    public static final int MAX_CACHE_SIZE =  Integer.parseInt(System.getProperty("netflix.locate.cache", "2000"));
    // TTL on the (ip -> instance id) cache. Unlike (ip -> region) and (ip -> rack), which are
    // stable because IPs are subnet-bound and subnets are AZ/region-bound, an IP can be
    // reassigned to a different EC2 instance via the standard replace/recycle flow. The cache
    // must expire entries periodically rather than holding them for the life of the JVM,
    // otherwise getId() will keep returning the predecessor's instance id forever.
    public static final Duration CACHE_TTL = Duration.ofMinutes(Integer.parseInt(System.getProperty("netflix.locate.cache.ttlMinutes", "10")));
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
                          .expireAfterWrite(CACHE_TTL)
                          .build(this::lookupInstanceId);
    }

    /**
     * Resolve the EC2 instance id for an IP by parsing the locate service response.
     *
     * The response is a JSON array that typically contains both an ENI entry and an instance
     * entry for the IP. The ENI entry exposes the authoritative instance id directly as
     * {@code attrs.attachment.instanceId}; prefer that. Otherwise, fall back to extracting the
     * id from {@code attrs.eddaUri} using the strict {@link #ID_PATTERN} which requires a
     * '/' before the {@code i-} prefix so we cannot accidentally match an eni-... id.
     *
     * Returns null if no instance id can be resolved.
     */
    private String lookupInstanceId(String host)
    {
        String jsonString = locateCall(host);
        if (jsonString == null)
        {
            return null;
        }
        try
        {
            JsonNode parsed = MAPPER.readTree(jsonString);
            if (parsed == null || !parsed.isArray())
            {
                logger.debug("Locate response for endpoint {} is not a JSON array, ignoring", host);
                return null;
            }
            // Prefer the explicit attachment.instanceId attribute when locate provides it
            // (typically present on the ENI entry). This avoids URI parsing entirely.
            for (JsonNode jsonObject : parsed)
            {
                JsonNode attrs = jsonObject.get("attrs");
                if (attrs != null && attrs.has("attachment.instanceId"))
                {
                    String id = attrs.get("attachment.instanceId").asText();
                    if (id.startsWith("i-"))
                    {
                        return id;
                    }
                }
            }
            // Fall back to extracting from eddaUri. Iterate all entries so we don't return
            // the first eddaUri (which may belong to the ENI) when a subsequent entry holds
            // the instance view URL.
            for (JsonNode jsonObject : parsed)
            {
                JsonNode attrs = jsonObject.get("attrs");
                if (attrs == null || !attrs.has("eddaUri"))
                {
                    continue;
                }
                Matcher matcher = ID_PATTERN.matcher(attrs.get("eddaUri").asText());
                if (matcher.find())
                {
                    return matcher.group(1);
                }
            }
        }
        catch (IOException e)
        {
            logger.debug("Failed to parse locate response for endpoint {}", host, e);
        }
        return null;
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
