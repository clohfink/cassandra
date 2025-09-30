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

import java.util.Objects;


public class NetflixInstance
{

    private long updateTime;
    private long createdTime;
    private String app;
    private String instanceId;
    private String availabilityZone;
    private String token;
    private String region;
    private int id;
    private String hostIP;
    private String hostName;
    private String key;

    // Constructors
    public NetflixInstance()
    {
    }

    public NetflixInstance(long updateTime, long createdTime, String app, String instanceId, String availabilityZone,
                           String token, String region, int id, String hostIP, String hostName, String key)
    {
        this.updateTime = updateTime;
        this.createdTime = createdTime;
        this.app = app;
        this.instanceId = instanceId;
        this.availabilityZone = availabilityZone;
        this.token = token;
        this.region = region;
        this.id = id;
        this.hostIP = hostIP;
        this.hostName = hostName;
        this.key = key;
    }

    // Getters and Setters
    public long getUpdateTime()
    {
        return updateTime;
    }

    public void setUpdateTime(long updateTime)
    {
        this.updateTime = updateTime;
    }

    public long getCreatedTime()
    {
        return createdTime;
    }

    public void setCreatedTime(long createdTime)
    {
        this.createdTime = createdTime;
    }

    public String getApp()
    {
        return app;
    }

    public void setApp(String app)
    {
        this.app = app;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public void setInstanceId(String instanceId)
    {
        this.instanceId = instanceId;
    }

    public String getAvailabilityZone()
    {
        return availabilityZone;
    }

    public void setAvailabilityZone(String availabilityZone)
    {
        this.availabilityZone = availabilityZone;
    }

    public String getToken()
    {
        return token;
    }

    public void setToken(String token)
    {
        this.token = token;
    }

    public String getRegion()
    {
        return region;
    }

    public void setRegion(String region)
    {
        this.region = region;
    }

    public int getId()
    {
        return id;
    }

    public void setId(int id)
    {
        this.id = id;
    }

    public String getHostIP()
    {
        return hostIP;
    }

    public void setHostIP(String hostIP)
    {
        this.hostIP = hostIP;
    }

    public String getHostName()
    {
        return hostName;
    }

    public void setHostName(String hostName)
    {
        this.hostName = hostName;
    }

    public String getKey()
    {
        return key;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    // equals(), hashCode() and toString() methods
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NetflixInstance that = (NetflixInstance) o;
        return updateTime == that.updateTime && createdTime == that.createdTime && id == that.id &&
               Objects.equals(app, that.app) && Objects.equals(instanceId, that.instanceId) &&
               Objects.equals(availabilityZone, that.availabilityZone) && Objects.equals(token, that.token) &&
               Objects.equals(region, that.region) && Objects.equals(hostIP, that.hostIP) &&
               Objects.equals(hostName, that.hostName) && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(updateTime, createdTime, app, instanceId, availabilityZone, token, region, id, hostIP, hostName, key);
    }

    @Override
    public String toString()
    {
        return "NetflixInstance{" +
               "updateTime=" + updateTime +
               ", createdTime=" + createdTime +
               ", app='" + app + '\'' +
               ", instanceId='" + instanceId + '\'' +
               ", availabilityZone='" + availabilityZone + '\'' +
               ", token='" + token + '\'' +
               ", region='" + region + '\'' +
               ", id=" + id +
               ", hostIP='" + hostIP + '\'' +
               ", hostName='" + hostName + '\'' +
               ", key='" + key + '\'' +
               '}';
    }
}
