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

package com.netflix.cassandra.metrics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.utils.ExpiringMemoizingSupplier;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Exposes metrics about JVM crash artifacts (hs_err_pid*.log files).
 * <p>
 * When the JVM crashes due to a fatal error (e.g. SIGSEGV, internal VM error) it writes an
 * {@code hs_err_pid<pid>.log} file. By default HotSpot writes it to the JVM working directory;
 * the path can be overridden by passing {@code -XX:ErrorFile=...} to the JVM.
 * <p>
 * Two gauges are exposed:
 * <ul>
 *   <li>{@code HsErrLogCount} — number of {@code hs_err_pid*.log} files in the scan directory</li>
 *   <li>{@code HsErrLogBytes} — total bytes consumed by those files</li>
 * </ul>
 * <p>
 * The scan directory is derived from the running JVM's {@code -XX:ErrorFile} option (via
 * {@link HotSpotDiagnosticMXBean#getVMOption(String)}). When {@code -XX:ErrorFile} is unset, has
 * no parent component, or cannot be read, we fall back to {@link #FALLBACK_HS_ERR_DIR} — the
 * Netflix Cassandra data volume mount point.
 * <p>
 * Both gauges read from a single directory scan that is memoized for {@link #CACHE_TTL_SECONDS}.
 * The metrics collector polls each gauge separately, so without sharing we would scan the
 * directory twice per collection cycle; the short cache window collapses that into one scan
 * while staying well below the collection interval, so each cycle still reflects a fresh scan.
 */
public class JvmCrashMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(JvmCrashMetrics.class);
    private static final MetricNameFactory factory = new DefaultNameFactory("Jvm");

    @VisibleForTesting
    static final String HS_ERR_GLOB = "hs_err_pid*.log";

    /**
     * Fallback scan directory when {@code -XX:ErrorFile} doesn't yield a usable parent.
     */
    @VisibleForTesting
    static final String FALLBACK_HS_ERR_DIR = "/mnt/data/cassandra";

    /**
     * How long a single directory scan is shared between the two gauges. Kept comfortably below
     * the metrics collection interval so each cycle still reflects a fresh scan, yet long enough
     * that the collector's two near-simultaneous gauge reads collapse into one scan.
     */
    @VisibleForTesting
    static final long CACHE_TTL_SECONDS = 30;

    private JvmCrashMetrics()
    {
        // utility class; not instantiable
    }

    /** Single scan shared by both gauges; see the class javadoc and {@link #CACHE_TTL_SECONDS}. */
    private static final Supplier<Snapshot> snapshotCache = newSnapshotCache(() -> snapshot(hsErrDir()));

    public static final Gauge<Long> hsErrLogCount = Metrics.register(
        factory.createMetricName("HsErrLogCount"),
        () -> snapshotCache.get().count
    );

    public static final Gauge<Long> hsErrLogBytes = Metrics.register(
        factory.createMetricName("HsErrLogBytes"),
        () -> snapshotCache.get().bytes
    );

    /**
     * Wrap a directory-scanning supplier with the short-lived memoization that lets the two gauges
     * share one scan per collection cycle.
     */
    @VisibleForTesting
    static Supplier<Snapshot> newSnapshotCache(Supplier<Snapshot> scan)
    {
        return ExpiringMemoizingSupplier.memoizeWithExpiration(
            () -> new ExpiringMemoizingSupplier.Memoized<>(scan.get()),
            CACHE_TTL_SECONDS, TimeUnit.SECONDS
        );
    }

    /**
     * Resolve the directory to scan from the running JVM's {@code -XX:ErrorFile} option.
     * Returns the parent of the configured path if available, otherwise {@link #FALLBACK_HS_ERR_DIR}.
     */
    @VisibleForTesting
    static String hsErrDir()
    {
        String errorFile = readErrorFileVmOption();
        if (errorFile != null && !errorFile.isEmpty())
        {
            try
            {
                Path parent = Path.of(errorFile).getParent();
                if (parent != null)
                    return parent.toString();
            }
            catch (RuntimeException e)
            {
                logger.warn("Could not parse -XX:ErrorFile value {}", errorFile, e);
            }
        }
        return FALLBACK_HS_ERR_DIR;
    }

    private static String readErrorFileVmOption()
    {
        try
        {
            HotSpotDiagnosticMXBean mbean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            if (mbean == null)
                return null;
            VMOption opt = mbean.getVMOption("ErrorFile");
            return opt == null ? null : opt.getValue();
        }
        catch (Throwable t)
        {
            logger.warn("Could not read -XX:ErrorFile via HotSpotDiagnosticMXBean", t);
            return null;
        }
    }

    /**
     * Scan the given directory for {@code hs_err_pid*.log} files and return their count and
     * total size in bytes. Returns an empty snapshot if the directory is missing or unreadable;
     * never throws.
     */
    @VisibleForTesting
    static Snapshot snapshot(String dir)
    {
        if (dir == null)
            return Snapshot.EMPTY;

        Path path = Path.of(dir);
        if (!Files.isDirectory(path))
            return Snapshot.EMPTY;

        long count = 0L;
        long bytes = 0L;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, HS_ERR_GLOB))
        {
            for (Path p : stream)
            {
                count++;
                try
                {
                    bytes += Files.size(p);
                }
                catch (IOException e)
                {
                    // file may have been removed concurrently; skip its size
                    logger.debug("Could not stat {}", p, e);
                }
            }
        }
        catch (IOException e)
        {
            logger.warn("Failed to enumerate {} files in {}", HS_ERR_GLOB, dir, e);
            return Snapshot.EMPTY;
        }
        return new Snapshot(count, bytes);
    }

    /** Result of a single directory scan: file count and aggregated size in bytes. */
    @VisibleForTesting
    static final class Snapshot
    {
        static final Snapshot EMPTY = new Snapshot(0L, 0L);

        final long count;
        final long bytes;

        Snapshot(long count, long bytes)
        {
            this.count = count;
            this.bytes = bytes;
        }
    }
}
