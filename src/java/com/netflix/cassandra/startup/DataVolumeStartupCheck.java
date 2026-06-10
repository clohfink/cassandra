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
package com.netflix.cassandra.startup;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.service.StartupCheck;
import org.apache.cassandra.service.StartupChecks.StartupCheckType;

/**
 * Refuses to start Cassandra when the configured data directories are not backed
 * by a real data volume (a dedicated EBS volume or a second ephemeral drive).
 *
 * <p>At Netflix the root volume is 12GB. Without this check, if no data volume is
 * mounted at {@code /mnt/data/cassandra}, the default {@code checkDataDirs}
 * startup check happily creates the directory on the root volume and Cassandra
 * silently fills it up.
 *
 * <p>The check examines each configured data, commitlog, and saved-caches
 * directory. For each, it walks up to the nearest existing ancestor (since the
 * configured path itself may not exist yet) and inspects the {@link FileStore}
 * backing it. If the total filesystem size is at or below
 * {@link #DATA_VOLUME_FLOOR_BYTES} (15GB, exclusive floor — chosen because the
 * root volume is 12GB so anything larger must be a dedicated data volume),
 * startup is aborted.
 *
 * <p>This check is configurable via {@code cassandra.yaml}'s {@code startup_checks}
 * block under the key {@code check_data_volume_size} and may be disabled by
 * operators in environments where the heuristic does not apply.
 */
public class DataVolumeStartupCheck implements StartupCheck
{
    private static final Logger logger = LoggerFactory.getLogger(DataVolumeStartupCheck.class);

    /**
     * Exclusive floor: filesystems backing data directories must be strictly
     * larger than this. 15GB is chosen because the Netflix root volume is 12GB,
     * so any filesystem larger than 15GB must be a dedicated data volume (EBS
     * or a second ephemeral drive).
     */
    @VisibleForTesting
    static final long DATA_VOLUME_FLOOR_BYTES = 15L * 1024L * 1024L * 1024L;

    @Override
    public void execute(StartupChecksOptions options) throws StartupException
    {
        if (options.isDisabled(getStartupCheckType()))
            return;

        Set<String> dirs = new LinkedHashSet<>();
        dirs.addAll(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()));
        dirs.add(DatabaseDescriptor.getCommitLogLocation());
        dirs.add(DatabaseDescriptor.getSavedCachesLocation());

        for (String dir : dirs)
            verifyDataVolume(dir);
    }

    private void verifyDataVolume(String dir) throws StartupException
    {
        Path path = Path.of(dir);
        Path existing = nearestExistingAncestor(path);
        if (existing == null)
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Cannot resolve filesystem for data directory %s: no ancestor exists.",
                                                     dir));

        FileStore store;
        try
        {
            store = Files.getFileStore(existing);
        }
        catch (IOException e)
        {
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Cannot read filesystem info for %s (resolved via %s): %s",
                                                     dir, existing, e.getMessage()));
        }

        long totalBytes;
        try
        {
            totalBytes = store.getTotalSpace();
        }
        catch (IOException e)
        {
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Cannot read total size of filesystem %s backing %s: %s",
                                                     store.name(), dir, e.getMessage()));
        }

        if (totalBytes <= DATA_VOLUME_FLOOR_BYTES)
        {
            // Path may not exist yet (checkDataDirs would create it on startup).
            // Report whichever ancestor we actually resolved so the operator
            // sees that the data dir is about to land on the root volume.
            String resolvedNote = path.equals(existing) ? "" : String.format(" (resolved via existing ancestor %s)", existing);
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                       String.format("Filesystem %s backing data directory %s%s is %.1fGB " +
                                                     "(need >%.0fGB). This looks like the 12GB root volume, " +
                                                     "not a dedicated data volume. Refusing to start to avoid " +
                                                     "writing data to the root volume. Mount an EBS volume or a " +
                                                     "second ephemeral drive and retry.",
                                                     store.name(), dir, resolvedNote,
                                                     totalBytes / (double) (1024L * 1024L * 1024L),
                                                     DATA_VOLUME_FLOOR_BYTES / (double) (1024L * 1024L * 1024L)));
        }

        logger.info("Data volume check passed for {}: filesystem {} has {} bytes total",
                    dir, store.name(), totalBytes);
    }

    /**
     * Walks up the path looking for the first ancestor that exists. Returns
     * {@code null} only if no ancestor (including the root) exists, which should
     * be impossible on any real filesystem.
     */
    @VisibleForTesting
    static Path nearestExistingAncestor(Path path)
    {
        List<Path> candidates = new ArrayList<>();
        for (Path p = path.toAbsolutePath().normalize(); p != null; p = p.getParent())
            candidates.add(p);
        for (Path candidate : candidates)
        {
            if (Files.exists(candidate))
                return candidate;
        }
        return null;
    }

    @Override
    public StartupCheckType getStartupCheckType()
    {
        return StartupCheckType.check_data_volume_size;
    }
}
