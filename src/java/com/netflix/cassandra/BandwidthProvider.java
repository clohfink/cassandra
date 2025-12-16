package com.netflix.cassandra;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;
import software.amazon.awssdk.services.ec2.model.NetworkCardInfo;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;

public class BandwidthProvider implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(BandwidthProvider.class);

    // Conversion factor from Gbps to MiB/s: 1 Gbps = 1,000,000,000 bits/s / 8 / 1024^2 ~= 119.21 MiB/s
    private static final double MIBS_PER_GBPS = 1_000_000_000.0 / 8.0 / (1024.0 * 1024.0);
    
    private final Ec2Client ec2Client;
    private ApacheHttpClient httpClient = null;
    private final String instanceType;
    
    public BandwidthProvider()
    {
        this(System.getenv());
    }
    
    public BandwidthProvider(Map<String, String> envVars)
    {
        this(envVars, null);
    }
    
    public BandwidthProvider(Map<String, String> envVars, Ec2Client ec2Client)
    {
        String regionEnv = envVars.get("NETFLIX_REGION");
        String region = StringUtils.isEmpty(regionEnv) ? Region.US_EAST_1.id() : regionEnv;
        
        String instanceTypeEnv = envVars.get("EC2_INSTANCE_TYPE");
        this.instanceType = StringUtils.isEmpty(instanceTypeEnv) ? null : instanceTypeEnv;
        
        if (ec2Client != null)
        {
            this.ec2Client = ec2Client;
        }
        else
        {
            Ec2Client client = null;
            try
            {
                httpClient = (ApacheHttpClient) ApacheHttpClient.builder()
                                           .useIdleConnectionReaper(false)
                                           .build();
                client = Ec2Client.builder()
                        .region(Region.of(region))
                        .httpClient(httpClient)
                        .build();
            }
            catch (Exception e)
            {
                logger.warn("Failed to create EC2 client for region '{}': {}", region, e.getMessage());
            }
            this.ec2Client = client;
        }
    }

    public long getBaselineBandwidthInMiB()
    {
        long totalBandwidthMiB = 0;
        try
        {
            DescribeInstanceTypesRequest request = DescribeInstanceTypesRequest.builder()
                                                                               .instanceTypes(InstanceType.fromValue(instanceType))
                                                                               .build();

            DescribeInstanceTypesResponse result = ec2Client.describeInstanceTypes(request);
            List<InstanceTypeInfo> instanceTypes = result.instanceTypes();
            List<NetworkCardInfo> nics = instanceTypes.get(0).networkInfo().networkCards();
            for (NetworkCardInfo nic : nics)
            {
                Double baselineBandwidthGbps = nic.baselineBandwidthInGbps();
                if (baselineBandwidthGbps != null)
                    totalBandwidthMiB += (long) (baselineBandwidthGbps * MIBS_PER_GBPS);
            }
        }
        catch (Exception e)
        {
            logger.error("Exception determining baseline bandwidth", e);
            return 0L;
        }

        return totalBandwidthMiB;
    }

    @Override
    public void close()
    {
        if (ec2Client != null)
        {
            try
            {
                ec2Client.close();
            }
            catch (Exception e)
            {
                logger.warn("Error closing EC2 client", e);
            }
        }
        if (httpClient != null)
        {
            try
            {
                httpClient.close();
            }
            catch (Exception e)
            {
                logger.warn("Error closing http client", e);
            }
        }
    }
}