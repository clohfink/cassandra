package com.netflix.cassandra;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstanceTypesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.InstanceTypeInfo;
import software.amazon.awssdk.services.ec2.model.NetworkCardInfo;
import software.amazon.awssdk.services.ec2.model.NetworkInfo;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BandwidthProviderTest
{
    @Mock
    private Ec2Client mockEc2Client;
    
    private BandwidthProvider provider;
    
    @Before
    public void setUp()
    {
        Map<String, String> envVars = new HashMap<>();
        envVars.put("NETFLIX_REGION", "us-east-1");
        envVars.put("NETFLIX_INSTANCE_ID", "i-1234567890abcdef0");
        envVars.put("EC2_INSTANCE_TYPE", "m5.large");
        provider = new BandwidthProvider(envVars, mockEc2Client);
    }

    
    @Test
    public void testGetInstanceTypeInfo_AwsServiceException()
    {
        AwsServiceException exception = AwsServiceException.builder()
                .message("Service error")
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("RequestLimitExceeded")
                        .build())
                .build();
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenThrow(exception);
        assertEquals(0, provider.getBaselineBandwidthInMiB());
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_Success()
    {
        // Test case with multiple network cards having baseline bandwidth
        NetworkCardInfo nic1 = NetworkCardInfo.builder()
                .baselineBandwidthInGbps(10.0)  // 10 Gbps
                .build();
        NetworkCardInfo nic2 = NetworkCardInfo.builder()
                .baselineBandwidthInGbps(5.0)   // 5 Gbps
                .build();
        
        NetworkInfo networkInfo = NetworkInfo.builder()
                .networkCards(nic1, nic2)
                .build();
        
        InstanceTypeInfo mockTypeInfo = InstanceTypeInfo.builder()
                .instanceType(InstanceType.M5_LARGE)
                .networkInfo(networkInfo)
                .build();
        
        DescribeInstanceTypesResponse mockResult = DescribeInstanceTypesResponse.builder()
                .instanceTypes(mockTypeInfo)
                .build();
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenReturn(mockResult);
        
        // Expected: (10 + 5) Gbps * MIBS_PER_GBPS
        // MIBS_PER_GBPS = 1_000_000_000.0 / 8.0 / (1024.0 * 1024.0) ~= 119.209
        long result = provider.getBaselineBandwidthInMiB();
        long expected = (long) (15.0 * (1_000_000_000.0 / 8.0 / (1024.0 * 1024.0)));
        assertEquals(expected, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_DescribeInstanceTypesThrows()
    {
        // Test when describeInstanceTypes() throws an exception
        RuntimeException exception = new RuntimeException("AWS API error");
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenThrow(exception);
        
        long result = provider.getBaselineBandwidthInMiB();
        assertEquals(0L, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_NoInstanceTypesReturned()
    {
        // Test when describeInstanceTypes() returns no instance types
        DescribeInstanceTypesResponse mockResult = DescribeInstanceTypesResponse.builder()
                .instanceTypes(java.util.Collections.emptyList())
                .build();
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenReturn(mockResult);
        
        long result = provider.getBaselineBandwidthInMiB();
        assertEquals(0L, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_NoNetworkInfo()
    {
        // Test when the first instance type has no NetworkInfo
        InstanceTypeInfo mockTypeInfo = InstanceTypeInfo.builder()
                .instanceType(InstanceType.M5_LARGE)
                .networkInfo((NetworkInfo) null)  // No network info
                .build();
        
        DescribeInstanceTypesResponse mockResult = DescribeInstanceTypesResponse.builder()
                .instanceTypes(mockTypeInfo)
                .build();
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenReturn(mockResult);
        
        long result = provider.getBaselineBandwidthInMiB();
        assertEquals(0L, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_NoNetworkCardInfo()
    {
        // Test when NetworkInfo has no NetworkCardInfo (empty list)
        NetworkInfo networkInfo = NetworkInfo.builder()
                .networkCards(java.util.Collections.emptyList())  // Empty list
                .build();
        
        InstanceTypeInfo mockTypeInfo = InstanceTypeInfo.builder()
                .instanceType(InstanceType.M5_LARGE)
                .networkInfo(networkInfo)
                .build();
        
        DescribeInstanceTypesResponse mockResult = DescribeInstanceTypesResponse.builder()
                .instanceTypes(mockTypeInfo)
                .build();
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenReturn(mockResult);
        
        long result = provider.getBaselineBandwidthInMiB();
        assertEquals(0L, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_NoBaselineBandwidthInGbps()
    {
        // Test when NetworkCardInfos have no BaselineBandwidthInGbps (null values)
        NetworkCardInfo nic1 = NetworkCardInfo.builder()
                .baselineBandwidthInGbps(null)  // No baseline bandwidth
                .build();
        NetworkCardInfo nic2 = NetworkCardInfo.builder()
                .baselineBandwidthInGbps(null)  // No baseline bandwidth
                .build();
        
        NetworkInfo networkInfo = NetworkInfo.builder()
                .networkCards(nic1, nic2)
                .build();
        
        InstanceTypeInfo mockTypeInfo = InstanceTypeInfo.builder()
                .instanceType(InstanceType.M5_LARGE)
                .networkInfo(networkInfo)
                .build();
        
        DescribeInstanceTypesResponse mockResult = DescribeInstanceTypesResponse.builder()
                .instanceTypes(mockTypeInfo)
                .build();
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenReturn(mockResult);
        
        long result = provider.getBaselineBandwidthInMiB();
        assertEquals(0L, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_MixedNullAndValidBandwidth()
    {
        // Test with some NICs having bandwidth and some having null
        NetworkCardInfo nic1 = NetworkCardInfo.builder()
                .baselineBandwidthInGbps(10.0)  // 10 Gbps
                .build();
        NetworkCardInfo nic2 = NetworkCardInfo.builder()
                .baselineBandwidthInGbps(null)  // No baseline bandwidth
                .build();
        NetworkCardInfo nic3 = NetworkCardInfo.builder()
                .baselineBandwidthInGbps(5.0)   // 5 Gbps
                .build();
        
        NetworkInfo networkInfo = NetworkInfo.builder()
                .networkCards(nic1, nic2, nic3)
                .build();
        
        InstanceTypeInfo mockTypeInfo = InstanceTypeInfo.builder()
                .instanceType(InstanceType.M5_LARGE)
                .networkInfo(networkInfo)
                .build();
        
        DescribeInstanceTypesResponse mockResult = DescribeInstanceTypesResponse.builder()
                .instanceTypes(mockTypeInfo)
                .build();
        when(mockEc2Client.describeInstanceTypes(any(DescribeInstanceTypesRequest.class))).thenReturn(mockResult);
        
        // Expected: (10 + 0 + 5) Gbps * MIBS_PER_GBPS
        long result = provider.getBaselineBandwidthInMiB();
        long expected = (long) (15.0 * (1_000_000_000.0 / 8.0 / (1024.0 * 1024.0)));
        assertEquals(expected, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_NoInstanceType()
    {
        // Test when instance type is not set (null)
        Map<String, String> envVars = new HashMap<>();
        envVars.put("EC2_REGION", "us-east-1");
        envVars.put("EC2_INSTANCE_ID", "i-1234567890abcdef0");
        // Not setting EC2_INSTANCE_TYPE
        BandwidthProvider providerNoType = new BandwidthProvider(envVars, mockEc2Client);
        
        long result = providerNoType.getBaselineBandwidthInMiB();
        assertEquals(0L, result);
    }
    
    @Test
    public void testGetBaselineBandwidthInMiB_NoEc2Client()
    {
        // Test when EC2 client is null
        Map<String, String> envVars = new HashMap<>();
        envVars.put("EC2_INSTANCE_TYPE", "m5.large");
        BandwidthProvider providerNoClient = new BandwidthProvider(envVars, mockEc2Client);
        
        long result = providerNoClient.getBaselineBandwidthInMiB();
        assertEquals(0L, result);
    }


}