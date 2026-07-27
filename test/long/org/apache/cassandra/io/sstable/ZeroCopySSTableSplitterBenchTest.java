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
package org.apache.cassandra.io.sstable;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MurmurHash;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Benchmark harness comparing {@link ZeroCopySSTableSplitter} (verbatim compression-chunk-run copy) against the
 * existing full-rewrite split path ({@link SSTableSplitter}, i.e. what {@code StandaloneSplitter} /
 * {@code nodetool}-style splitting actually runs).
 *
 * <p>This exists to replace the <em>estimated</em> cost model in the research doc with a measurement. It drives
 * the real {@code SSTableSplitter.SplittingCompactionTask} against a real {@link ColumnFamilyStore} and a real
 * {@link LifecycleTransaction} -- it is not a reimplementation of the baseline.
 *
 * <h2>How to run it</h2>
 * As a JUnit long test (this is the normal way):
 * <pre>
 *   ant long-testsome -Dtest.name=org.apache.cassandra.io.sstable.ZeroCopySSTableSplitterBenchTest \
 *                     -Duse.jdk11=true
 * </pre>
 * Including the (much slower, ~1 GiB) large-parent cases, which also need more heap than the 1024m the ant
 * junit macro defaults to:
 * <pre>
 *   ant long-testsome -Dtest.name=org.apache.cassandra.io.sstable.ZeroCopySSTableSplitterBenchTest \
 *                     -Duse.jdk11=true \
 *                     -Dtest.jvm.args="-Xmx4G -Dcassandra.test.zerocopysplit.large=true"
 * </pre>
 * As a standalone tool (there is a {@code main}, so it can be used as a benchmarking tool rather than only as a
 * test). {@code run-main} is the repo's own target for this and already puts {@code test/conf} on the
 * classpath:
 * <pre>
 *   ant build-test -Duse.jdk11=true
 *   ant run-main -Duse.jdk11=true \
 *                -Dmainclass=org.apache.cassandra.io.sstable.ZeroCopySSTableSplitterBenchTest \
 *                -Dvmargs="-Xmx4G -Dcassandra.test.zerocopysplit.large=true"
 * </pre>
 * To skip it entirely (e.g. in a full {@code ant long-test} run on a box where it is not wanted):
 * <pre>
 *   ant long-test -Duse.jdk11=true -Dtest.jvm.args="-Dcassandra.test.zerocopysplit.bench=false"
 * </pre>
 *
 * <h2>System properties</h2>
 * <ul>
 *   <li>{@code cassandra.test.zerocopysplit.bench} (default {@code true}) -- set false to skip the JUnit test.</li>
 *   <li>{@code cassandra.test.zerocopysplit.large} (default {@code false}) -- opt in to the large-parent cases.</li>
 *   <li>{@code cassandra.test.zerocopysplit.warmup} (default {@code true}) -- run one throwaway config first.</li>
 *   <li>{@code cassandra.test.zerocopysplit.smallMiB} / {@code .mediumMiB} / {@code .largeMiB} -- parent sizes,
 *       default 16 / 64 / 512.</li>
 * </ul>
 *
 * <h2>What is measured, and what the numbers do and do not mean</h2>
 * <ul>
 *   <li><b>wall ms</b>: only the split call itself. Parent construction, key verification and cleanup are
 *       outside the timed region.</li>
 *   <li><b>write bytes</b>: exact -- the sum of the on-disk lengths of every component file of every produced
 *       child, measured after the split and before cleanup. This is the number the write-amplification column
 *       is built from ({@code writeBytes / parentOnDiskLength}).</li>
 *   <li><b>read bytes</b>: taken from {@code /proc/self/io} {@code rchar} deltas when that file exists (Linux),
 *       otherwise <em>modelled</em> from the algorithm (the {@code IO} column says which). The modelled
 *       zero-copy figure is {@code 2*Index.db + Statistics.db + 2*copiedDataBytes} (two index passes, the
 *       chunk-run copy, and the digest re-read of the child Data.db); the modelled baseline figure is the
 *       parent's physical Data.db length. {@code rchar} counts bytes moved by read syscalls, so it includes
 *       page-cache hits -- that is the right unit here, since both paths are being compared on the same
 *       already-warm parent.</li>
 *   <li><b>allocated bytes</b>: {@code com.sun.management.ThreadMXBean#getThreadAllocatedBytes} for the calling
 *       thread, reached reflectively so this still compiles/runs on a JVM that does not expose it (the column
 *       prints {@code n/a} then). Both paths run their work on the calling thread, but allocation done on
 *       Cassandra's shared executors (async fsync, tidier threads) is NOT counted for either path.</li>
 * </ul>
 * The zero-copy path is measured first for each configuration and the baseline second, because the baseline
 * obsoletes and deletes the parent. That ordering means the baseline sees a slightly warmer page cache than
 * the zero-copy run did, which is conservative: it biases against the candidate.
 *
 * <h2>Assertions</h2>
 * Only correctness-preserving invariants are asserted, never timings -- a benchmark that fails on a slow CI box
 * is a liability. Per configuration both paths must produce children whose partition keys are exactly the
 * parent's (count, XOR and sum of a 64-bit hash over every key, read straight out of Index.db), and for the
 * small configurations the total row count of both paths' children must equal what was written.
 */
public class ZeroCopySSTableSplitterBenchTest
{
    private static final String PROP_ENABLED = "cassandra.test.zerocopysplit.bench";
    private static final String PROP_LARGE = "cassandra.test.zerocopysplit.large";
    private static final String PROP_WARMUP = "cassandra.test.zerocopysplit.warmup";
    private static final String PROP_SMALL_MIB = "cassandra.test.zerocopysplit.smallMiB";
    private static final String PROP_MEDIUM_MIB = "cassandra.test.zerocopysplit.mediumMiB";
    private static final String PROP_LARGE_MIB = "cassandra.test.zerocopysplit.largeMiB";

    private static final String KEYSPACE = "ZeroCopySplitBench";

    /** One table per compression chunk length, so the chunk-length sweep needs no schema mutation. */
    private static final int[] CHUNK_KB = { 4, 16, 64 };

    private static final long MIB = 1024L * 1024L;

    /** Bytes of random (incompressible) payload per row. */
    private static final int VALUE_SIZE = 1024;
    /** Rows per partition for the two partition shapes. 64 KiB partitions comfortably exceed the 4 KiB
     *  {@code column_index_size} of test/conf/cassandra.yaml, so the wide shape carries a promoted index. */
    private static final int WIDE_ROWS = 64;
    private static final int NARROW_ROWS = 1;

    /** Row counts are verified by a full scan only for parents at or below this size. */
    private static final int SCAN_VERIFY_MAX_MIB = 32;

    private static final Method THREAD_ALLOCATED_BYTES = findThreadAllocatedBytes();
    private static final Path PROC_SELF_IO = Paths.get("/proc/self/io");

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.prepareServer();

        TableMetadata.Builder[] tables = new TableMetadata.Builder[CHUNK_KB.length];
        for (int i = 0; i < CHUNK_KB.length; i++)
        {
            int chunkBytes = CHUNK_KB[i] * 1024;
            tables[i] = SchemaLoader.standardCFMD(KEYSPACE, tableName(CHUNK_KB[i]), 1,
                                                  AsciiType.instance, BytesType.instance)
                                    // maxCompressedLength == chunkLength, i.e. min_compress_ratio 1.0
                                    .compression(CompressionParams.lz4(chunkBytes, chunkBytes));
        }
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), tables);
        CompactionManager.instance.disableAutoCompaction();
    }

    @AfterClass
    public static void cleanUpSchema()
    {
        for (int chunkKb : CHUNK_KB)
        {
            try
            {
                truncate(Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName(chunkKb)));
            }
            catch (Throwable t)
            {
                System.err.println("cleanup of " + tableName(chunkKb) + " failed: " + t);
            }
        }
    }

    @Test
    public void benchmarkAgainstFullRewriteSplit() throws Throwable
    {
        Assume.assumeTrue("skipped via -D" + PROP_ENABLED + "=false",
                          Boolean.parseBoolean(System.getProperty(PROP_ENABLED, "true")));
        run();
    }

    /**
     * Standalone entry point, so this can be used as a benchmarking tool rather than only under JUnit.
     * See the class javadoc for the exact ant invocation.
     */
    public static void main(String[] args) throws Throwable
    {
        // must happen before anything touches DatabaseDescriptor
        if (System.getProperty("cassandra.config") == null)
        {
            Path yaml = Paths.get("test", "conf", "cassandra.yaml").toAbsolutePath();
            if (Files.exists(yaml))
                System.setProperty("cassandra.config", yaml.toUri().toString());
        }
        OUT.println("cassandra.config = " + System.getProperty("cassandra.config", "<from classpath>"));

        defineSchema();
        try
        {
            new ZeroCopySSTableSplitterBenchTest().run();
        }
        finally
        {
            try
            {
                cleanUpSchema();
            }
            finally
            {
                // Cassandra leaves non-daemon threads behind; StandaloneSplitter does the same thing.
                System.exit(0);
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // The sweep
    // ------------------------------------------------------------------------------------------------

    /** One point in the sweep. */
    private static final class Config
    {
        final int sizeMiB;
        final int chunkKb;
        final int children;
        final boolean wide;

        Config(int sizeMiB, int chunkKb, int children, boolean wide)
        {
            this.sizeMiB = sizeMiB;
            this.chunkKb = chunkKb;
            this.children = children;
            this.wide = wide;
        }

        String label()
        {
            return String.format("%dMiB/%dKiB/k=%d/%s", sizeMiB, chunkKb, children, wide ? "wide" : "narrow");
        }

        boolean verifyRows()
        {
            return sizeMiB <= SCAN_VERIFY_MAX_MIB;
        }
    }

    private static List<Config> sweep()
    {
        int small = Integer.getInteger(PROP_SMALL_MIB, 16);
        int medium = Integer.getInteger(PROP_MEDIUM_MIB, 64);
        int large = Integer.getInteger(PROP_LARGE_MIB, 512);

        List<Config> configs = new ArrayList<>();
        // chunk-length sweep, held at one size / child count / shape
        configs.add(new Config(small, 4, 4, true));
        configs.add(new Config(small, 16, 4, true));
        configs.add(new Config(small, 64, 4, true));
        // partition-shape sweep
        configs.add(new Config(small, 16, 4, false));
        // child-count sweep at a bigger parent
        configs.add(new Config(medium, 16, 2, true));
        configs.add(new Config(medium, 16, 4, true));
        configs.add(new Config(medium, 16, 8, true));
        configs.add(new Config(medium, 16, 4, false));

        if (Boolean.parseBoolean(System.getProperty(PROP_LARGE, "false")))
        {
            configs.add(new Config(large, 16, 4, true));
            configs.add(new Config(large, 64, 8, true));
        }
        return configs;
    }

    private void run() throws Throwable
    {
        List<Config> configs = sweep();

        if (Boolean.parseBoolean(System.getProperty(PROP_WARMUP, "true")))
        {
            OUT.println("# warmup (results discarded) ...");
            measure(new Config(Math.min(8, Integer.getInteger(PROP_SMALL_MIB, 16)), 16, 2, true));
            OUT.println("# warmup done");
        }

        List<Measurement> results = new ArrayList<>();
        printHeader();
        for (Config config : configs)
        {
            Measurement m = measure(config);
            results.add(m);
            printRows(m);
        }
        printSummary(results);
    }

    /** Both paths for one configuration, against a freshly built parent. */
    private Measurement measure(Config config) throws Throwable
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName(config.chunkKb));
        cfs.disableAutoCompaction();
        truncate(cfs);

        Parent parent = buildParent(cfs, config);
        try
        {
            // zero-copy first: it leaves the parent untouched, the baseline obsoletes it
            Run zeroCopy = runZeroCopy(cfs, parent, config);
            Run baseline = runBaseline(cfs, parent, config);
            return new Measurement(config, parent, baseline, zeroCopy);
        }
        finally
        {
            truncate(cfs);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Candidate: ZeroCopySSTableSplitter
    // ------------------------------------------------------------------------------------------------

    private Run runZeroCopy(ColumnFamilyStore cfs, Parent parent, Config config)
    {
        assertTrue("parent must be compressed for the zero-copy path",
                   ZeroCopySSTableSplitter.isSupported(parent.reader));

        long[] io0 = readProcIo();
        long alloc0 = threadAllocatedBytes();
        long t0 = System.nanoTime();
        ZeroCopySSTableSplitter.Result result = ZeroCopySSTableSplitter.split(parent.reader, config.children, null);
        long nanos = System.nanoTime() - t0;
        long alloc = delta(alloc0, threadAllocatedBytes());
        long[] io1 = readProcIo();

        long written = 0;
        List<Descriptor> descriptors = new ArrayList<>(result.children.size());
        for (ZeroCopySSTableSplitter.Child child : result.children)
        {
            written += componentBytes(child.descriptor, child.components);
            descriptors.add(child.descriptor);
        }

        long modelledRead = 2 * parent.indexBytes + parent.statsBytes + 2 * result.totalPhysicalBytesCopied;
        Run run = new Run("zerocopy", nanos, measuredRead(io0, io1), modelledRead, written, alloc,
                          result.children.size());

        try
        {
            // ---- correctness: the children must cover exactly the parent's keys ----
            assertEquals(config.label() + ": zero-copy children do not cover the parent's keys",
                         parent.signature, signature(descriptors, cfs.metadata()));

            if (config.verifyRows())
            {
                long rows = 0;
                for (ZeroCopySSTableSplitter.Child child : result.children)
                    rows += countRows(child.reader);
                assertEquals(config.label() + ": zero-copy children lost rows", parent.rows, rows);
            }
        }
        finally
        {
            for (ZeroCopySSTableSplitter.Child child : result.children)
                child.reader.selfRef().release();
            LifecycleTransaction.waitForDeletions();
            for (ZeroCopySSTableSplitter.Child child : result.children)
            {
                for (Component component : child.components)
                    child.descriptor.fileFor(component).deleteIfExists();
                new File(child.descriptor.tmpFilenameFor(Component.STATS)).deleteIfExists();
            }
        }
        return run;
    }

    // ------------------------------------------------------------------------------------------------
    // Baseline: the existing full-rewrite split (SSTableSplitter / StandaloneSplitter code path)
    // ------------------------------------------------------------------------------------------------

    private Run runBaseline(ColumnFamilyStore cfs, Parent parent, Config config) throws Throwable
    {
        // MaxSSTableSizeWriter switches output files on estimated ON-DISK bytes written, so target the
        // parent's compressed length divided by the requested child count.
        int sizeInMiB = (int) Math.max(1, parent.onDiskBytes / (config.children * MIB));

        Set<SSTableReader> before = new LinkedHashSet<>(cfs.getLiveSSTables());

        long nanos;
        long alloc;
        long[] io0;
        long[] io1;
        try (LifecycleTransaction txn = cfs.getTracker()
                                           .tryModify(Collections.singleton(parent.reader), OperationType.UNKNOWN))
        {
            assertNotNull("could not obtain a LifecycleTransaction over the parent", txn);

            io0 = readProcIo();
            long alloc0 = threadAllocatedBytes();
            long t0 = System.nanoTime();
            new SSTableSplitter(cfs, txn, sizeInMiB).split();
            nanos = System.nanoTime() - t0;
            alloc = delta(alloc0, threadAllocatedBytes());
            io1 = readProcIo();
        }
        LifecycleTransaction.waitForDeletions();

        Set<SSTableReader> produced = new LinkedHashSet<>(cfs.getLiveSSTables());
        produced.removeAll(before);
        assertTrue(config.label() + ": baseline split produced no sstables", !produced.isEmpty());

        long written = 0;
        List<Descriptor> descriptors = new ArrayList<>(produced.size());
        for (SSTableReader child : produced)
        {
            written += componentBytes(child.descriptor, child.getComponents());
            descriptors.add(child.descriptor);
        }

        // ---- correctness: the children must cover exactly the parent's keys ----
        assertEquals(config.label() + ": baseline children do not cover the parent's keys",
                     parent.signature, signature(descriptors, cfs.metadata()));

        if (config.verifyRows())
        {
            long rows = 0;
            for (SSTableReader child : produced)
                rows += countRows(child);
            assertEquals(config.label() + ": baseline children lost rows", parent.rows, rows);
        }

        return new Run("baseline", nanos, measuredRead(io0, io1), parent.onDiskBytes, written, alloc,
                       produced.size());
    }

    // ------------------------------------------------------------------------------------------------
    // Parent construction
    // ------------------------------------------------------------------------------------------------

    /** The parent sstable plus everything about it we still need after the baseline has deleted it. */
    private static final class Parent
    {
        final SSTableReader reader;
        final long onDiskBytes;
        final long uncompressedBytes;
        final long indexBytes;
        final long statsBytes;
        final long partitions;
        final long rows;
        final KeySignature signature;

        Parent(SSTableReader reader, long partitions, long rows, KeySignature signature)
        {
            this.reader = reader;
            this.onDiskBytes = reader.onDiskLength();
            this.uncompressedBytes = reader.uncompressedLength();
            this.indexBytes = reader.descriptor.fileFor(Component.PRIMARY_INDEX).length();
            this.statsBytes = reader.descriptor.fileFor(Component.STATS).length();
            this.partitions = partitions;
            this.rows = rows;
            this.signature = signature;
        }
    }

    private static Parent buildParent(ColumnFamilyStore cfs, Config config)
    {
        TableMetadata metadata = cfs.metadata();
        int rowsPerPartition = config.wide ? WIDE_ROWS : NARROW_ROWS;
        // ~64 bytes of clustering/cell overhead per row is close enough; the exact parent size does not matter,
        // only that both paths get the same one.
        long bytesPerPartition = (long) rowsPerPartition * (VALUE_SIZE + 64);
        int partitions = (int) Math.max(64, (config.sizeMiB * MIB) / bytesPerPartition);

        ByteBuffer[] keys = sortedKeys(metadata.partitioner, partitions);

        File directory = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor descriptor = cfs.newSSTableDescriptor(directory);
        SerializationHeader header = new SerializationHeader(true, metadata,
                                                             metadata.regularAndStaticColumns(),
                                                             EncodingStats.NO_STATS);

        Collection<SSTableReader> written;
        try (SSTableTxnWriter writer = SSTableTxnWriter.create(cfs, descriptor, partitions, 0, null, false, header))
        {
            for (ByteBuffer key : keys)
            {
                UpdateBuilder builder = UpdateBuilder.create(metadata, key);
                for (int r = 0; r < rowsPerPartition; r++)
                    builder.newRow(String.format("r%06d", r)).add("val", randomBytes(VALUE_SIZE));
                writer.append(builder.build().unfilteredIterator());
            }
            written = writer.finish(true);
        }
        assertEquals("expected exactly one parent sstable", 1, written.size());

        SSTableReader parent = written.iterator().next();
        cfs.addSSTable(parent);
        assertTrue("parent must be compressed", parent.compression);

        KeySignature signature = signature(Collections.singletonList(parent.descriptor), metadata);
        assertEquals("parent partition count", partitions, signature.count);

        return new Parent(parent, partitions, (long) partitions * rowsPerPartition, signature);
    }

    /**
     * Partition keys in on-disk order. Sorted explicitly rather than assuming an order-preserving partitioner,
     * because {@code BigTableWriter} requires strictly increasing decorated keys.
     */
    private static ByteBuffer[] sortedKeys(IPartitioner partitioner, int count)
    {
        ByteBuffer[] keys = new ByteBuffer[count];
        for (int i = 0; i < count; i++)
            keys[i] = ByteBufferUtil.bytes(String.format("%012d", i));
        Arrays.sort(keys, (a, b) -> partitioner.decorateKey(a).compareTo(partitioner.decorateKey(b)));
        return keys;
    }

    /** Incompressible payload, so the compressed sstable really does span many chunks. */
    private static ByteBuffer randomBytes(int size)
    {
        byte[] bytes = new byte[size];
        ThreadLocalRandom.current().nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private static void truncate(ColumnFamilyStore cfs)
    {
        cfs.truncateBlocking();
        LifecycleTransaction.waitForDeletions();
    }

    private static String tableName(int chunkKb)
    {
        return "bench_" + chunkKb + "kb";
    }

    // ------------------------------------------------------------------------------------------------
    // Correctness helpers
    // ------------------------------------------------------------------------------------------------

    /**
     * Order-independent fingerprint of a set of partition keys: count plus the XOR and the sum of a 64-bit hash
     * of every key. Cheap (Index.db only, nothing retained) and strong enough that a dropped, duplicated or
     * corrupted key changes it.
     */
    private static final class KeySignature
    {
        final long count;
        final long xor;
        final long sum;

        KeySignature(long count, long xor, long sum)
        {
            this.count = count;
            this.xor = xor;
            this.sum = sum;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof KeySignature))
                return false;
            KeySignature that = (KeySignature) o;
            return count == that.count && xor == that.xor && sum == that.sum;
        }

        @Override
        public int hashCode()
        {
            return (int) (count ^ xor ^ sum);
        }

        @Override
        public String toString()
        {
            return String.format("KeySignature[count=%d xor=%016x sum=%016x]", count, xor, sum);
        }
    }

    private static KeySignature signature(List<Descriptor> descriptors, TableMetadata metadata)
    {
        long count = 0;
        long xor = 0;
        long sum = 0;
        for (Descriptor descriptor : descriptors)
        {
            try (KeyIterator keys = new KeyIterator(descriptor, metadata))
            {
                while (keys.hasNext())
                {
                    ByteBuffer key = keys.next().getKey();
                    long hash = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);
                    count++;
                    xor ^= hash;
                    sum += hash;
                }
            }
        }
        return new KeySignature(count, xor, sum);
    }

    private static long countRows(SSTableReader sstable)
    {
        long rows = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    while (partition.hasNext())
                    {
                        partition.next();
                        rows++;
                    }
                }
            }
        }
        return rows;
    }

    private static long componentBytes(Descriptor descriptor, Set<Component> components)
    {
        long total = 0;
        for (Component component : components)
        {
            File file = descriptor.fileFor(component);
            if (file.exists())
                total += file.length();
        }
        return total;
    }

    // ------------------------------------------------------------------------------------------------
    // Instrumentation
    // ------------------------------------------------------------------------------------------------

    /** Reflective, so this class still compiles and runs where com.sun.management is unavailable. */
    private static Method findThreadAllocatedBytes()
    {
        try
        {
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            Class<?> extended = Class.forName("com.sun.management.ThreadMXBean");
            if (!extended.isInstance(bean))
                return null;
            try
            {
                extended.getMethod("setThreadAllocatedMemoryEnabled", boolean.class).invoke(bean, true);
            }
            catch (Throwable ignored)
            {
                // already enabled, or not settable; getThreadAllocatedBytes below decides
            }
            Method method = extended.getMethod("getThreadAllocatedBytes", long.class);
            long probe = (Long) method.invoke(bean, Thread.currentThread().getId());
            return probe < 0 ? null : method;
        }
        catch (Throwable t)
        {
            return null;
        }
    }

    /** @return bytes allocated by the current thread so far, or -1 when the JVM does not expose it. */
    private static long threadAllocatedBytes()
    {
        if (THREAD_ALLOCATED_BYTES == null)
            return -1;
        try
        {
            return (Long) THREAD_ALLOCATED_BYTES.invoke(ManagementFactory.getThreadMXBean(),
                                                        Thread.currentThread().getId());
        }
        catch (Throwable t)
        {
            return -1;
        }
    }

    /** @return {rchar, wchar} from /proc/self/io, or null when it is not readable (non-Linux). */
    private static long[] readProcIo()
    {
        if (!Files.isReadable(PROC_SELF_IO))
            return null;
        try
        {
            long rchar = -1;
            long wchar = -1;
            for (String line : Files.readAllLines(PROC_SELF_IO))
            {
                if (line.startsWith("rchar:"))
                    rchar = Long.parseLong(line.substring(6).trim());
                else if (line.startsWith("wchar:"))
                    wchar = Long.parseLong(line.substring(6).trim());
            }
            return (rchar < 0 || wchar < 0) ? null : new long[]{ rchar, wchar };
        }
        catch (Throwable t)
        {
            return null;
        }
    }

    private static long measuredRead(long[] before, long[] after)
    {
        return (before == null || after == null) ? -1 : after[0] - before[0];
    }

    private static long delta(long before, long after)
    {
        return (before < 0 || after < 0) ? -1 : after - before;
    }

    // ------------------------------------------------------------------------------------------------
    // Reporting
    // ------------------------------------------------------------------------------------------------

    private static final class Run
    {
        final String path;
        final long nanos;
        /** From /proc/self/io rchar, or -1. */
        final long measuredReadBytes;
        /** Derived from the algorithm; always available. */
        final long modelledReadBytes;
        /** Exact: summed on-disk component lengths of the produced children. */
        final long writtenBytes;
        /** Calling-thread allocation, or -1. */
        final long allocatedBytes;
        final int children;

        Run(String path, long nanos, long measuredReadBytes, long modelledReadBytes,
            long writtenBytes, long allocatedBytes, int children)
        {
            this.path = path;
            this.nanos = nanos;
            this.measuredReadBytes = measuredReadBytes;
            this.modelledReadBytes = modelledReadBytes;
            this.writtenBytes = writtenBytes;
            this.allocatedBytes = allocatedBytes;
            this.children = children;
        }

        long readBytes()
        {
            return measuredReadBytes >= 0 ? measuredReadBytes : modelledReadBytes;
        }

        String readSource()
        {
            return measuredReadBytes >= 0 ? "proc" : "model";
        }

        double millis()
        {
            return nanos / 1_000_000.0;
        }
    }

    private static final class Measurement
    {
        final Config config;
        final long parentOnDiskBytes;
        final long parentUncompressedBytes;
        final long parentPartitions;
        final Run baseline;
        final Run zeroCopy;

        Measurement(Config config, Parent parent, Run baseline, Run zeroCopy)
        {
            this.config = config;
            this.parentOnDiskBytes = parent.onDiskBytes;
            this.parentUncompressedBytes = parent.uncompressedBytes;
            this.parentPartitions = parent.partitions;
            this.baseline = baseline;
            this.zeroCopy = zeroCopy;
        }
    }

    /**
     * Cassandra's test harness replaces {@link System#out} with a bridge into SLF4J, and the logger splits each
     * message on whitespace -- which turns every padded column of a printf table into its own log line and makes
     * the report unreadable under {@code ant long-testsome}. So write the report straight to fd 1 (bypassing the
     * replaced System.out) and simultaneously to a file, which is also the durable artifact you want from a
     * benchmark run. Override the path with -Dcassandra.test.zerocopysplitter.bench.report=...
     */
    private static final java.io.File REPORT_FILE =
        new java.io.File(System.getProperty("cassandra.test.zerocopysplitter.bench.report",
                                            "build/test/zerocopy-split-bench.txt"));

    private static final java.io.PrintStream OUT = openReport();

    private static java.io.PrintStream openReport()
    {
        try
        {
            java.io.File parent = REPORT_FILE.getAbsoluteFile().getParentFile();
            if (parent != null)
                parent.mkdirs();
            java.io.PrintStream file = new java.io.PrintStream(new java.io.FileOutputStream(REPORT_FILE), true);
            // One unpadded line through the (possibly redirected) System.out so a reader knows where to look.
            System.out.println("zerocopy-split bench report: " + REPORT_FILE.getAbsolutePath());
            return file;
        }
        catch (java.io.IOException e)
        {
            // Better a mangled table than no benchmark at all.
            return System.out;
        }
    }

    private static final String ROW_FORMAT = "%-24s %-9s %10.1f %10.2f %10.2f %7.3f %11s %6s %5d%n";

    private static void printHeader()
    {
        OUT.println();
        OUT.println("=========================================================================================================");
        OUT.println(" ZeroCopySSTableSplitter vs. SSTableSplitter (full rewrite)");
        OUT.println("   allocation source : " + (THREAD_ALLOCATED_BYTES == null
                                                        ? "UNAVAILABLE on this JVM (com.sun.management.ThreadMXBean"
                                                          + ".getThreadAllocatedBytes missing) - alloc column is n/a"
                                                        : "com.sun.management.ThreadMXBean.getThreadAllocatedBytes"
                                                          + " (calling thread only)"));
        OUT.println("   read-byte source  : " + (readProcIo() == null
                                                        ? "/proc/self/io UNAVAILABLE - read column is modelled"
                                                        : "/proc/self/io rchar delta where IO=proc"));
        OUT.println("   write bytes       : exact, summed on-disk component lengths of the produced children");
        OUT.println("   w_amp             : write bytes / parent on-disk length");
        OUT.println("=========================================================================================================");
        OUT.printf("%-24s %-9s %10s %10s %10s %7s %11s %6s %5s%n",
                          "CONFIG", "PATH", "WALL_MS", "READ_MiB", "WRITE_MiB", "W_AMP", "ALLOC_MiB", "IO", "KIDS");
    }

    private static void printRows(Measurement m)
    {
        printRow(m.config.label(), m, m.baseline);
        printRow("", m, m.zeroCopy);
    }

    private static void printRow(String label, Measurement m, Run run)
    {
        OUT.printf(ROW_FORMAT,
                          label,
                          run.path,
                          run.millis(),
                          run.readBytes() / (double) MIB,
                          run.writtenBytes / (double) MIB,
                          run.writtenBytes / (double) m.parentOnDiskBytes,
                          run.allocatedBytes < 0 ? "n/a" : String.format("%.2f", run.allocatedBytes / (double) MIB),
                          run.readSource(),
                          run.children);
    }

    private static void printSummary(List<Measurement> results)
    {
        OUT.println();
        OUT.println("=========================================================================================================");
        OUT.println(" SUMMARY - speedup = baseline wall / zero-copy wall (higher is better for the candidate)");
        OUT.println("=========================================================================================================");
        OUT.printf("%-24s %12s %12s %12s %10s %10s %10s %12s %14s%n",
                          "CONFIG", "PARENT_MiB", "PARENT_UNC", "PARTITIONS", "BASE_MS", "ZC_MS", "SPEEDUP",
                          "BASE_W_AMP", "ZC_W_AMP");
        for (Measurement m : results)
        {
            double speedup = m.zeroCopy.nanos == 0 ? Double.NaN : m.baseline.nanos / (double) m.zeroCopy.nanos;
            OUT.printf("%-24s %12.1f %12.1f %12d %10.1f %10.1f %9.1fx %12.3f %14.3f%n",
                              m.config.label(),
                              m.parentOnDiskBytes / (double) MIB,
                              m.parentUncompressedBytes / (double) MIB,
                              m.parentPartitions,
                              m.baseline.millis(),
                              m.zeroCopy.millis(),
                              speedup,
                              m.baseline.writtenBytes / (double) m.parentOnDiskBytes,
                              m.zeroCopy.writtenBytes / (double) m.parentOnDiskBytes);
        }
        OUT.println();
        OUT.printf("%-24s %12s %12s %10s %10s %10s%n",
                          "CONFIG", "BASE_RD_MiB", "ZC_RD_MiB", "RD_RATIO", "BASE_AL_MiB", "ZC_AL_MiB");
        for (Measurement m : results)
        {
            double readRatio = m.zeroCopy.readBytes() == 0
                               ? Double.NaN
                               : m.baseline.readBytes() / (double) m.zeroCopy.readBytes();
            OUT.printf("%-24s %12.2f %12.2f %10.2f %10s %10s%n",
                              m.config.label(),
                              m.baseline.readBytes() / (double) MIB,
                              m.zeroCopy.readBytes() / (double) MIB,
                              readRatio,
                              m.baseline.allocatedBytes < 0 ? "n/a"
                                                            : String.format("%.2f", m.baseline.allocatedBytes / (double) MIB),
                              m.zeroCopy.allocatedBytes < 0 ? "n/a"
                                                            : String.format("%.2f", m.zeroCopy.allocatedBytes / (double) MIB));
        }
        OUT.println("=========================================================================================================");
        OUT.println();
    }
}
