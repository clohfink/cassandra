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

package com.netflix.cassandra.backups;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BackupManifest {
    private Info info;
    private List<Data> data;

    // For Jackson deserialization
    public BackupManifest() {}

    private BackupManifest(Builder builder) {
        this.info = builder.info;
        this.data = builder.data;
    }

    public Info getInfo() { return info; }
    public void setInfo(Info info) { this.info = info; }

    public List<Data> getData() { return data; }
    public List<Data> getData(String keyspace, String table) {
        return data == null
               ? new ArrayList<>()
               : data.stream()
                     .filter(data -> data.getKeyspaceName().equals(keyspace))
                     .filter(data -> data.getColumnfamilyName().equals(table))
                     .collect(Collectors.toList());
    }
    public void setData(List<Data> data) { this.data = data; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Info info;
        private List<Data> data = new ArrayList<>();

        public Builder info(Info info) {
            this.info = info;
            return this;
        }

        public Builder data(List<Data> data) {
            this.data = data;
            return this;
        }

        public Builder addData(Data data) {
            this.data.add(data);
            return this;
        }

        public BackupManifest build() {
            return new BackupManifest(this);
        }
    }

    // ----------------------------------------------------
    // static inner classes
    // ----------------------------------------------------

    public static class Info {
        private int version;
        private String appName;
        private String region;
        private String rack;
        private List<String> backupIdentifier;

        // For Jackson deserialization
        public Info() {}

        private Info(Builder builder) {
            this.version = builder.version;
            this.appName = builder.appName;
            Validate.notBlank(builder.appName, "appName cannot be blank");
            this.region = builder.region;
            this.rack = builder.rack;
            this.backupIdentifier = builder.backupIdentifier;
        }

        public int getVersion() { return version; }
        public void setVersion(int version) { this.version = version; }

        public String getAppName() { return appName; }
        public void setAppName(String appName) { this.appName = appName; }

        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }

        public String getRack() { return rack; }
        public void setRack(String rack) { this.rack = rack; }

        public List<String> getBackupIdentifier() { return backupIdentifier; }
        public void setBackupIdentifier(List<String> backupIdentifier) {
            this.backupIdentifier = backupIdentifier;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private int version;
            private String appName;
            private String region;
            private String rack;
            private List<String> backupIdentifier = new ArrayList<>();

            public Builder version(int version) {
                this.version = version;
                return this;
            }

            public Builder appName(String appName) {
                this.appName = appName;
                return this;
            }

            public Builder region(String region) {
                this.region = region;
                return this;
            }

            public Builder rack(String rack) {
                this.rack = rack;
                return this;
            }

            public Builder backupIdentifier(List<String> backupIdentifier) {
                this.backupIdentifier = backupIdentifier;
                return this;
            }

            public Builder addBackupIdentifier(String identifier) {
                this.backupIdentifier.add(identifier);
                return this;
            }

            public Info build() {
                return new Info(this);
            }
        }
    }

    public static class Data {
        private String keyspaceName;
        private String columnfamilyName;
        private List<BackupSSTable> sstables;

        // For Jackson deserialization
        public Data() {}

        private Data(Builder builder) {
            Validate.notBlank(builder.keyspaceName, "keyspaceName cannot be blank");
            this.keyspaceName = builder.keyspaceName;
            Validate.notBlank(builder.columnfamilyName, "columnfamilyName cannot be blank");
            this.columnfamilyName = builder.columnfamilyName;
            this.sstables = builder.sstables;
        }

        public String getKeyspaceName() { return keyspaceName; }
        public void setKeyspaceName(String keyspaceName) {
            this.keyspaceName = keyspaceName;
        }

        public String getColumnfamilyName() { return columnfamilyName; }
        public void setColumnfamilyName(String columnfamilyName) {
            this.columnfamilyName = columnfamilyName;
        }

        public List<BackupSSTable> getSstables() { return sstables == null ? new ArrayList<>() : sstables; }
        public void setSstables(List<BackupSSTable> sstables) {
            this.sstables = sstables;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String keyspaceName;
            private String columnfamilyName;
            private List<BackupSSTable> sstables = new ArrayList<>();

            public Builder keyspaceName(String keyspaceName) {
                this.keyspaceName = keyspaceName;
                return this;
            }

            public Builder columnfamilyName(String columnfamilyName) {
                this.columnfamilyName = columnfamilyName;
                return this;
            }

            public Builder sstables(List<BackupSSTable> sstables) {
                this.sstables = sstables;
                return this;
            }

            public Builder addSstable(BackupSSTable sstable) {
                this.sstables.add(sstable);
                return this;
            }

            public Data build() {
                return new Data(this);
            }
        }
    }

    public static class BackupSSTable
    {
        private String prefix;
        private List<BackupSSTableComponent> sstableComponents;

        // For Jackson deserialization
        public BackupSSTable() {}

        private BackupSSTable(Builder builder) {
            Validate.notBlank(builder.prefix, "prefix cannot be blank");
            this.prefix = builder.prefix;
            this.sstableComponents = builder.sstableComponents;
        }

        public String getPrefix() { return prefix; }
        public void setPrefix(String prefix) { this.prefix = prefix; }

        public List<BackupSSTableComponent> getSstableComponents() {
            return sstableComponents == null ? new ArrayList<>() : sstableComponents;
        }
        public void setSstableComponents(List<BackupSSTableComponent> sstableComponents) {
            this.sstableComponents = sstableComponents;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String prefix;
            private List<BackupSSTableComponent> sstableComponents = new ArrayList<>();

            public Builder prefix(String prefix) {
                this.prefix = prefix;
                return this;
            }

            public Builder sstableComponents(List<BackupSSTableComponent> sstableComponents) {
                this.sstableComponents = sstableComponents;
                return this;
            }

            public Builder addSstableComponent(BackupSSTableComponent component) {
                this.sstableComponents.add(component);
                return this;
            }

            public BackupSSTable build() {
                return new BackupSSTable(this);
            }
        }
    }

    public static class BackupSSTableComponent
    {
        private String fileName;
        private long lastModifiedTime;
        private long fileCreationTime;
        private long fileSizeOnDisk;
        private String compression;
        private String encryption;

        @JsonProperty("isUploaded")
        private boolean isUploaded;

        private String backupPath;

        // For Jackson deserialization
        public BackupSSTableComponent() {}

        private BackupSSTableComponent(Builder builder) {
            this.fileName = builder.fileName;
            this.lastModifiedTime = builder.lastModifiedTime;
            this.fileCreationTime = builder.fileCreationTime;
            this.fileSizeOnDisk = builder.fileSizeOnDisk;
            this.compression = builder.compression;
            this.encryption = builder.encryption;
            this.isUploaded = builder.isUploaded;
            this.backupPath = builder.backupPath;
        }

        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }

        public long getLastModifiedTime() { return lastModifiedTime; }
        public void setLastModifiedTime(long lastModifiedTime) {
            this.lastModifiedTime = lastModifiedTime;
        }

        public long getFileCreationTime() { return fileCreationTime; }
        public void setFileCreationTime(long fileCreationTime) {
            this.fileCreationTime = fileCreationTime;
        }

        public long getFileSizeOnDisk() { return fileSizeOnDisk; }
        public void setFileSizeOnDisk(long fileSizeOnDisk) {
            this.fileSizeOnDisk = fileSizeOnDisk;
        }

        public String getCompression() { return compression; }
        public void setCompression(String compression) {
            this.compression = compression;
        }

        public String getEncryption() { return encryption; }
        public void setEncryption(String encryption) {
            this.encryption = encryption;
        }

        public boolean isUploaded() { return isUploaded; }
        public void setUploaded(boolean uploaded) { isUploaded = uploaded; }

        public String getBackupPath() { return backupPath; }
        public void setBackupPath(String backupPath) {
            this.backupPath = backupPath;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String fileName;
            private long lastModifiedTime;
            private long fileCreationTime;
            private long fileSizeOnDisk;
            private String compression;
            private String encryption;
            private boolean isUploaded;
            private String backupPath;

            public Builder fileName(String fileName) {
                this.fileName = fileName;
                return this;
            }

            public Builder lastModifiedTime(long lastModifiedTime) {
                this.lastModifiedTime = lastModifiedTime;
                return this;
            }

            public Builder fileCreationTime(long fileCreationTime) {
                this.fileCreationTime = fileCreationTime;
                return this;
            }

            public Builder fileSizeOnDisk(long fileSizeOnDisk) {
                this.fileSizeOnDisk = fileSizeOnDisk;
                return this;
            }

            public Builder compression(String compression) {
                this.compression = compression;
                return this;
            }

            public Builder encryption(String encryption) {
                this.encryption = encryption;
                return this;
            }

            public Builder isUploaded(boolean isUploaded) {
                this.isUploaded = isUploaded;
                return this;
            }

            public Builder backupPath(String backupPath) {
                this.backupPath = backupPath;
                return this;
            }

            public BackupSSTableComponent build() {
                return new BackupSSTableComponent(this);
            }
        }
    }
}