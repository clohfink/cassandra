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

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.SSTableSplitter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Reflink;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.NativeLibrary;

/**
 * Large-scale benchmark for {@link ZeroCopySSTableSplitter} against the existing full-rewrite split path
 * ({@link SSTableSplitter}, i.e. what {@code nodetool}/{@code sstablesplit} actually run).
 *
 * <p>{@link ZeroCopySSTableSplitterBenchTest} answers "is the algorithm faster" on 16-512 MiB parents that fit
 * comfortably in page cache. This class answers the question that actually matters in production: what happens
 * on a multi-GiB (or multi-TiB) parent that is <em>not</em> resident in memory. Three things are different here:
 *
 * <ul>
 *   <li><b>The parent is built with the sorted sstable writer</b> ({@link CQLSSTableWriter#builder()}
 *       {@code .sorted()}, i.e. {@code SSTableSimpleWriter}), which streams straight to disk and buffers
 *       nothing. Partition keys are produced in strictly increasing token order without ever materialising or
 *       sorting the key set, by <em>inverting</em> murmur3:
 *       {@link Murmur3Partitioner.LongToken#keyForToken(long)} runs
 *       {@link MurmurHash#inv_hash3_x64_128(long[])} to hand back a 16-byte key that hashes to a chosen token.
 *       Walk the token ring in even steps and the keys come out pre-sorted, so corpus size is bounded by disk,
 *       not by heap.</li>
 *   <li><b>The parent is reused across runs.</b> Generating a terabyte takes hours; it is written once into a
 *       corpus directory keyed by its shape and then reused verbatim by every later invocation. Each measured
 *       run gets its own <em>hard links</em> to the corpus files, so the baseline (which obsoletes and deletes
 *       its parent) destroys only links. See {@link #link}.</li>
 *   <li><b>Page cache is dropped before every timed run</b> via {@code posix_fadvise(POSIX_FADV_DONTNEED)}
 *       ({@link NativeLibrary#trySkipCache}), and the report carries {@code read_bytes}/{@code write_bytes}
 *       from {@code /proc/self/io} -- bytes that really crossed the block layer -- alongside
 *       {@code rchar}/{@code wchar}. That is what makes a cold-cache claim checkable rather than asserted.</li>
 * </ul>
 *
 * <h2>Running it</h2>
 * As a standalone tool, which is the point of it. {@code ant zerocopy-split-bench-jar} builds a single
 * self-contained executable jar that can be copied to a production host:
 * <pre>
 *   ant zerocopy-split-bench-jar -Duse.jdk11=true
 *   scp build/tools/lib/zerocopy-split-bench.jar host:/mnt/data/
 *
 *   # synthetic 64 GiB parent, generated once into /mnt/data/zcsplit/corpus and reused afterwards
 *   java -Xmx8G -jar zerocopy-split-bench.jar --scratch /mnt/data/zcsplit --size 64GiB --children 8
 *
 *   # a real sstable (a snapshot, ideally), split into 8 -- the original is only ever hard-linked, never written
 *   java -Xmx16G -jar zerocopy-split-bench.jar --scratch /mnt/data/zcsplit \
 *        --parent /data/ks/tbl-abcd/nb-12345-big-Data.db --schema-file tbl.cql --children 8
 * </pre>
 * As a JUnit long test, see {@link LargeSSTableSplitBenchTest} (opt-in, and much smaller by default).
 *
 * <p>{@code --help} prints the full option list.
 *
 * <h2>What it needs</h2>
 * <ul>
 *   <li><b>Disk.</b> The corpus is one parent-sized file set that persists. On top of that each measured run
 *       needs room for one full set of children, so budget {@code 2x} the parent size under {@code --scratch}.
 *       This is checked before anything is generated.</li>
 *   <li><b>Heap.</b> {@code ZeroCopySSTableSplitter} holds one {@code long} per partition during its Index.db
 *       pass, so a parent with P partitions needs roughly {@code 12 * P} bytes of heap (the array plus its
 *       growth copy). The projected figure is printed up front and refused if it does not fit
 *       {@code Runtime.maxMemory()}; raise {@code -Xmx} or {@code --partition-size}.</li>
 *   <li><b>Linux</b> for the cold-cache machinery. {@code posix_fadvise} and {@code /proc/self/io} do not exist
 *       elsewhere; on other platforms the tool still runs and says so, but the numbers are warm-cache numbers.</li>
 * </ul>
 *
 * <h2>Safety</h2>
 * Every Cassandra directory (data, commitlog, hints, saved caches) is rooted under {@code --scratch}; the tool
 * writes nothing anywhere else. In {@code --parent} mode the supplied sstable is opened read-only and hard
 * linked into the scratch tree -- the destructive baseline path only ever unlinks its own link. It is still a
 * benchmark and not something to point at a live node's active data directory: use a snapshot.
 */
public class LargeSSTableSplitBench
{
    private static final long KIB = 1024L;
    private static final long MIB = 1024L * KIB;
    private static final long GIB = 1024L * MIB;

    /** Keyspace/table used for a generated corpus. Also the schema registered for {@code --parent} mode. */
    static final String DEFAULT_KEYSPACE = "zcsplit";
    static final String DEFAULT_TABLE = "parent";

    /**
     * Rough peak heap the splitter needs per partition <em>of a single child</em>.
     *
     * <p>{@code ZeroCopySSTableSplitter} used to materialise one {@code long} per partition of the parent for
     * its Index.db pass, which was the binding constraint on how large an sstable could be split at all --
     * 16-24 bytes per partition, so tens of gigabytes for a terabyte of small partitions. That is gone; both
     * passes now stream, and the run bookkeeping is O(numChildren).
     *
     * <p>What is left is inherent to the output rather than to the algorithm, and is per child rather than per
     * parent, because children are built one at a time: the child's bloom filter (~1.25 bytes per key at the
     * default 0.01 fp chance, more at a lower one) and its {@code IndexSummaryBuilder} (one sampled entry per
     * {@code min_index_interval} keys). Call it ~2.5 bytes per partition of the largest child and round up;
     * under-reporting here means the run dies hours in.
     */
    private static final int HEAP_BYTES_PER_CHILD_PARTITION = 4;

    private static final Method THREAD_ALLOCATED_BYTES = findThreadAllocatedBytes();
    private static final Path PROC_SELF_IO = Paths.get("/proc/self/io");

    private final Options options;
    private final Report report;

    public LargeSSTableSplitBench(Options options, Report report)
    {
        this.options = options;
        this.report = report;
    }

    // ================================================================================================
    // Entry points
    // ================================================================================================

    public static void main(String[] args)
    {
        // The jar's manifest carries the Add-Opens/Add-Exports half of what bin/cassandra normally passes;
        // these two are plain system properties, which a manifest cannot set. Both are read lazily by classes
        // that have not been loaded yet at this point.
        System.setProperty("io.netty.tryReflectionSetAccessible", "true");
        System.setProperty("jdk.attach.allowAttachSelf", "true");

        int status = 0;
        try
        {
            Options options = Options.parse(args);
            if (options == null)
                return; // --help, already printed

            try (Report report = Report.open(options.reportFile))
            {
                new LargeSSTableSplitBench(options, report).run();
            }
        }
        catch (IllegalArgumentException e)
        {
            System.err.println("error: " + e.getMessage());
            System.err.println("run with --help for usage");
            status = 2;
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
            status = 1;
        }
        finally
        {
            // Cassandra leaves non-daemon threads behind; every offline tool in the tree does the same thing.
            System.out.flush();
            System.err.flush();
            System.exit(status);
        }
    }

    /**
     * The whole benchmark. Bootstraps the server, makes sure a parent exists, then measures each requested path
     * once per iteration against a freshly linked, freshly evicted copy of that parent.
     */
    public void run() throws Throwable
    {
        prepareServer(options);
        report.reflinkPossible = Reflink.isPossibleIn(options.scratch);
        report.preamble(options);

        Parent parent = options.parentDataFile != null ? adoptExistingParent() : ensureCorpus();
        report.parent(parent);

        checkHeadroom(parent);

        ColumnFamilyStore cfs = Keyspace.open(parent.keyspace).getColumnFamilyStore(parent.table);
        cfs.disableAutoCompaction();

        List<Measurement> measurements = new ArrayList<>();
        report.tableHeader();
        try (Csv csv = Csv.open(options.csvFile))
        {
            for (int iteration = 1; iteration <= options.iterations; iteration++)
            {
                for (String path : options.paths)
                {
                    Measurement m = measure(cfs, parent, path, iteration);
                    measurements.add(m);
                    report.row(m, parent);
                    if (csv != null)
                        csv.row(options, parent, m);
                }
            }
        }
        report.summary(measurements, parent, options);
    }

    // ================================================================================================
    // Server bootstrap
    // ================================================================================================

    /**
     * Bring up just enough of Cassandra to own a {@link ColumnFamilyStore}: config, directories, the schema
     * machinery. Deliberately not {@code SchemaLoader.prepareServer()} -- that lives in the unit-test tree and
     * would drag it into the shipped jar, and its first act is to wipe the data directories, which is the
     * opposite of what a reusable corpus wants.
     *
     * <p>Idempotent: when the JUnit wrapper has already initialised the daemon, only the benchmark-specific
     * settings below are (re)applied.
     */
    static void prepareServer(Options options) throws IOException
    {
        if (!DatabaseDescriptor.isDaemonInitialized())
        {
            options.scratch.tryCreateDirectories();
            System.setProperty("cassandra.storagedir", options.scratch.absolutePath());
            if (System.getProperty("cassandra.config") == null)
                System.setProperty("cassandra.config", writeConfig(options.scratch, options));

            DatabaseDescriptor.daemonInitialization();
            requireDirectoriesUnder(options.scratch);
            // Before anything materialises schema: key generation inverts murmur3, so under any other
            // partitioner the keys would not be in DecoratedKey order and the sorted writer would (correctly)
            // reject them. The tables this tool creates also pin Murmur3 individually, so an already
            // initialised daemon on some other partitioner still produces a usable parent.
            if (!(DatabaseDescriptor.getPartitioner() instanceof Murmur3Partitioner))
                DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

            DatabaseDescriptor.createAllDirectories();
            CommitLog.instance.start();
            Keyspace.setInitialized();
            SystemKeyspace.persistLocalMetadata();
        }

        // The baseline is a real CompactionTask and would otherwise be rate limited to compaction_throughput,
        // which would make the comparison a measurement of the throttle rather than of the two algorithms.
        DatabaseDescriptor.setCompactionThroughputMebibytesPerSec(0);
        // Children get dropped between runs; an auto snapshot would hard link them back into existence and
        // silently fill the scratch filesystem over a long sweep.
        DatabaseDescriptor.setAutoSnapshot(false);
        DatabaseDescriptor.setColumnIndexSize((int) (options.columnIndexSize / KIB));
        DatabaseDescriptor.setZeroCopySplitReflinkEnabled(options.reflink);
        DatabaseDescriptor.setZeroCopySplitDigestEnabled(options.digest);
        CompactionManager.instance.disableAutoCompaction();
    }

    /**
     * Write the benchmark's own cassandra.yaml into the scratch directory and return its URL.
     *
     * <p>Deliberately generated rather than found. Picking up whichever {@code cassandra.yaml} happens to be
     * on the classpath or in the working directory is a real hazard for a tool meant to be run on a
     * production host: a node's own yaml names its <em>live</em> data directories explicitly, which would
     * override {@code cassandra.storagedir} and quietly have the benchmark write its children into production
     * data. Everything here is either a directory pinned under {@code --scratch} or a Cassandra default; the
     * settings that would distort the comparison (compaction throughput, auto snapshot) are pinned too.
     *
     * <p>{@code disk_access_mode} is set rather than left to {@code auto}, which resolves to {@code mmap} on a
     * 64-bit JVM. Mapping Data.db would defeat the cold cache measurement outright:
     * {@code POSIX_FADV_DONTNEED} does not evict pages that are currently mapped, so the second and later
     * runs would silently be warm. {@code --disk-access-mode} exists for anyone who wants to measure the
     * mapped case anyway.
     */
    private static String writeConfig(File scratch, Options options) throws IOException
    {
        File yaml = new File(scratch, "cassandra.yaml");
        String text =
            "# Generated by " + LargeSSTableSplitBench.class.getSimpleName() + ". Every directory is pinned\n"
            + "# under --scratch so nothing can be written outside it. Delete it to have it regenerated.\n"
            + "cluster_name: zerocopy-split-bench\n"
            + "partitioner: org.apache.cassandra.dht.Murmur3Partitioner\n"
            + "endpoint_snitch: org.apache.cassandra.locator.SimpleSnitch\n"
            + "dynamic_snitch: false\n"
            + "listen_address: 127.0.0.1\n"
            + "storage_port: 7012\n"
            + "ssl_storage_port: 17012\n"
            + "start_native_transport: false\n"
            + "seed_provider:\n"
            + "    - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n"
            + "      parameters:\n"
            + "          - seeds: \"127.0.0.1:7012\"\n"
            + "commitlog_sync: periodic\n"
            + "commitlog_sync_period: 10s\n"
            + "commitlog_segment_size: 32MiB\n"
            + "memtable_allocation_type: heap_buffers\n"
            + "cdc_enabled: false\n"
            + "auto_snapshot: false\n"
            + "incremental_backups: false\n"
            + "compaction_throughput: 0MiB/s\n"
            + "key_cache_size: 0MiB\n"
            + "disk_access_mode: " + options.diskAccessMode + '\n'
            + "zero_copy_split_reflink_enabled: " + options.reflink + '\n'
            + "zero_copy_split_digest_enabled: " + options.digest + '\n'
            + "data_file_directories:\n"
            + "    - " + quote(scratch, "data")
            + "commitlog_directory: " + quote(scratch, "commitlog")
            + "cdc_raw_directory: " + quote(scratch, "cdc_raw")
            + "hints_directory: " + quote(scratch, "hints")
            + "saved_caches_directory: " + quote(scratch, "saved_caches");

        Files.write(yaml.toPath(), text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return yaml.toPath().toUri().toString();
    }

    /** Quoted so a scratch path containing a space or a colon does not turn into invalid YAML. */
    private static String quote(File scratch, String child)
    {
        return '"' + new File(scratch, child).absolutePath().replace("\\", "\\\\").replace("\"", "\\\"") + "\"\n";
    }

    /**
     * Last line of defence. Whatever config was actually loaded -- ours, or one the caller forced with
     * {@code -Dcassandra.config} -- refuse to run if it would have Cassandra write anywhere but the scratch
     * directory. The failure mode being guarded against is writing benchmark children into a live node's data
     * directory, which is not something to discover afterwards.
     */
    private static void requireDirectoriesUnder(File scratch)
    {
        String root = scratch.absolutePath();
        List<String> directories = new ArrayList<>(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()));
        directories.add(DatabaseDescriptor.getCommitLogLocation());
        directories.add(DatabaseDescriptor.getSavedCachesLocation());
        directories.add(DatabaseDescriptor.getHintsDirectory().absolutePath());

        for (String directory : directories)
        {
            if (directory != null && !new File(directory).absolutePath().startsWith(root))
            {
                throw new IllegalStateException(
                    "refusing to run: the loaded configuration puts " + directory + " outside --scratch ("
                    + root + "). This tool must never write into a live node's directories. Unset "
                    + "-Dcassandra.config so the benchmark generates its own, or point --scratch elsewhere.");
            }
        }
    }

    // ================================================================================================
    // The parent sstable: corpus generation and reuse
    // ================================================================================================

    /** Everything about the parent that has to outlive the baseline run, which deletes its copy of it. */
    static final class Parent
    {
        final Descriptor descriptor;
        final Set<Component> components;
        final String keyspace;
        final String table;
        final long onDiskBytes;
        final long uncompressedBytes;
        final long indexBytes;
        final long partitions;
        final long rows;
        final int chunkLength;
        /** null when {@code --verify none}. */
        final KeySignature signature;
        /** Where this parent came from, for the report: generated now, reused, or somebody else's sstable. */
        final String origin;

        Parent(Descriptor descriptor, Set<Component> components, long onDiskBytes, long uncompressedBytes,
               long indexBytes, long partitions, long rows, int chunkLength, KeySignature signature,
               String origin)
        {
            this.descriptor = descriptor;
            this.components = components;
            this.keyspace = descriptor.ksname;
            this.table = descriptor.cfname;
            this.onDiskBytes = onDiskBytes;
            this.uncompressedBytes = uncompressedBytes;
            this.indexBytes = indexBytes;
            this.partitions = partitions;
            this.rows = rows;
            this.chunkLength = chunkLength;
            this.signature = signature;
            this.origin = origin;
        }
    }

    /**
     * Locate or build the synthetic parent. The corpus directory is keyed by the shape of the data
     * ({@link Options#shapeLabel}), so several sizes/chunk lengths can coexist and each one is generated at
     * most once, ever.
     */
    private Parent ensureCorpus() throws IOException
    {
        File shapeDir = new File(options.corpus, options.shapeLabel());
        File manifestFile = new File(shapeDir, "manifest.properties");
        // Cassandra's <keyspace>/<table>/ layout, so Descriptor.fromFilename can recover ksname/cfname and so
        // sstabledump/sstablemetadata work against the corpus directly.
        File tableDir = new File(new File(shapeDir, DEFAULT_KEYSPACE), DEFAULT_TABLE);

        if (options.regenerate && shapeDir.exists())
        {
            report.line("# --regenerate: discarding existing corpus " + shapeDir);
            shapeDir.deleteRecursive();
        }

        registerSchema(options.schemaCql());

        if (manifestFile.exists())
        {
            Parent reused = loadCorpus(manifestFile, tableDir);
            if (reused != null)
            {
                report.line("# reusing corpus " + shapeDir + " (generated earlier)");
                return reused;
            }
            report.line("# corpus manifest at " + manifestFile + " is stale or incomplete; regenerating");
            shapeDir.deleteRecursive();
        }

        requireFreeSpace(options.scratch, 2 * options.targetBytes,
                         "corpus generation plus one set of children");

        tableDir.tryCreateDirectories();
        Parent generated = generate(tableDir);
        storeCorpus(manifestFile, generated);
        return generated;
    }

    /** @return the parent described by an existing manifest, or null when it no longer matches what is on disk. */
    private Parent loadCorpus(File manifestFile, File tableDir)
    {
        Properties properties = new Properties();
        try (java.io.InputStream in = Files.newInputStream(manifestFile.toPath()))
        {
            properties.load(in);
        }
        catch (IOException e)
        {
            return null;
        }

        String dataFile = properties.getProperty("dataFile");
        if (dataFile == null)
            return null;

        File data = new File(tableDir, dataFile);
        if (!data.exists())
            return null;

        Descriptor descriptor;
        try
        {
            descriptor = Descriptor.fromFilename(data);
        }
        catch (RuntimeException e)
        {
            return null;
        }

        Set<Component> components = SSTable.componentsFor(descriptor);
        if (!components.contains(Component.DATA) || !components.contains(Component.PRIMARY_INDEX)
            || !components.contains(Component.COMPRESSION_INFO) || !components.contains(Component.STATS))
        {
            return null;
        }
        if (data.length() != Long.parseLong(properties.getProperty("onDiskBytes", "-1")))
            return null;

        KeySignature signature = options.verifyKeys()
                                 ? KeySignature.parse(properties.getProperty("signature"))
                                 : null;
        if (options.verifyKeys() && signature == null)
            signature = signatureOf(Collections.singletonList(descriptor), descriptor.ksname, descriptor.cfname);

        return new Parent(descriptor,
                          components,
                          data.length(),
                          Long.parseLong(properties.getProperty("uncompressedBytes", "0")),
                          descriptor.fileFor(Component.PRIMARY_INDEX).length(),
                          Long.parseLong(properties.getProperty("partitions", "0")),
                          Long.parseLong(properties.getProperty("rows", "0")),
                          Integer.parseInt(properties.getProperty("chunkLength", "0")),
                          signature,
                          "reused from a previous run's corpus");
    }

    private void storeCorpus(File manifestFile, Parent parent) throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("dataFile", parent.descriptor.fileFor(Component.DATA).name());
        properties.setProperty("onDiskBytes", Long.toString(parent.onDiskBytes));
        properties.setProperty("uncompressedBytes", Long.toString(parent.uncompressedBytes));
        properties.setProperty("partitions", Long.toString(parent.partitions));
        properties.setProperty("rows", Long.toString(parent.rows));
        properties.setProperty("chunkLength", Integer.toString(parent.chunkLength));
        properties.setProperty("shape", options.shapeLabel());
        if (parent.signature != null)
            properties.setProperty("signature", parent.signature.encode());

        // Written last and only on success, so an interrupted generation leaves no manifest and the next run
        // regenerates instead of measuring a truncated parent.
        try (java.io.OutputStream out = Files.newOutputStream(manifestFile.toPath()))
        {
            properties.store(out, "zerocopy split bench corpus; delete this file to force regeneration");
        }
    }

    /**
     * Write one parent sstable of the requested size with the sorted writer.
     *
     * <p>The i-th partition key is {@code keyForToken(MIN_TOKEN + 1 + i * stride)}: murmur3 run backwards. The
     * map {@code u -> MIN_VALUE + u} from unsigned offsets to signed longs is order preserving, so evenly
     * spaced unsigned offsets give strictly increasing tokens, hence strictly increasing decorated keys, hence
     * input the sorted writer accepts -- with no key set held in memory and no sort at any point. That is what
     * makes a terabyte corpus possible on a machine with a small heap.
     */
    private Parent generate(File tableDir) throws IOException
    {
        long partitions = Math.max(1, options.targetBytes / options.partitionSize);
        int rowsPerPartition = Math.max(1, (int) (options.partitionSize / options.valueSize));
        long rows = partitions * rowsPerPartition;
        long stride = Long.divideUnsigned(-1L, partitions);

        report.line(String.format("# generating %s parent: %,d partitions x %d rows x %s value "
                                  + "(chunk %d KiB, %s, %s payload)",
                                  bytes(options.targetBytes), partitions, rowsPerPartition,
                                  bytes(options.valueSize), options.chunkKb, options.compressor,
                                  options.payload));
        report.line("#   into " + tableDir);

        List<Descriptor> produced = new ArrayList<>();
        Filler filler = new Filler(options.payload, 42);
        Progress progress = new Progress(report, options.targetBytes);
        long logicalBytes = 0;

        long t0 = System.nanoTime();
        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                       .inDirectory(tableDir)
                                                       .forTable(options.schemaCql())
                                                       .using(options.insertCql())
                                                       .withPartitioner(Murmur3Partitioner.instance)
                                                       .sorted()
                                                       .openSSTableOnProduced()
                                                       .withSSTableProducedListener(sstables -> {
                                                           for (SSTableReader sstable : sstables)
                                                           {
                                                               produced.add(sstable.descriptor);
                                                               sstable.selfRef().release();
                                                           }
                                                       })
                                                       .build())
        {
            int[] uuidOrder = options.isUuidKey() ? uuidKeysInTokenOrder(partitions) : null;
            long emitted = uuidOrder != null ? uuidOrder.length : partitions;

            for (long p = 0; p < emitted; p++)
            {
                long partitionBytes = 0;
                if (uuidOrder != null)
                {
                    int k = uuidOrder[(int) p];
                    UUID k1 = uuidFor(k, 0);
                    UUID k2 = uuidFor(k, 1);
                    for (int r = 0; r < rowsPerPartition; r++)
                    {
                        ByteBuffer value = filler.next(jitteredValueSize());
                        partitionBytes += value.remaining();
                        writer.addRow(k1, k2, uuidFor(k, 2 + r), value);
                    }
                }
                else
                {
                    long token = Long.MIN_VALUE + 1 + p * stride;
                    ByteBuffer key = Murmur3Partitioner.LongToken.keyForToken(token);
                    assertInverse(key, token);
                    for (int r = 0; r < rowsPerPartition; r++)
                    {
                        ByteBuffer value = filler.next(jitteredValueSize());
                        partitionBytes += value.remaining();
                        writer.addRow(key.duplicate(), r, value);
                    }
                }
                logicalBytes += partitionBytes;
                progress.advance(partitionBytes);
            }
            partitions = emitted;
            rows = emitted * rowsPerPartition;
        }
        progress.done(System.nanoTime() - t0);

        if (produced.size() != 1)
        {
            throw new IllegalStateException("expected the sorted writer to produce exactly one sstable, got "
                                            + produced.size() + ": " + produced);
        }
        Descriptor descriptor = produced.get(0);
        Set<Component> components = rebuildBloomFilter(descriptor, SSTable.componentsFor(descriptor));

        // POSIX_FADV_DONTNEED only drops clean pages, so the corpus has to be on the platter before the first
        // eviction can mean anything.
        for (Component component : components)
            fsync(descriptor.fileFor(component));

        long onDisk = descriptor.fileFor(Component.DATA).length();
        SSTableReader reader = SSTableReader.open(descriptor, components,
                                                  Schema.instance.getTableMetadataRef(descriptor.ksname,
                                                                                      descriptor.cfname));
        long uncompressed = reader.uncompressedLength();
        reader.selfRef().release();

        // A random payload that compressed is a payload that was not random. Cheap guard against the shared
        // buffer class of bug, which silently shrinks the corpus and inflates every throughput number.
        double ratio = onDisk == 0 ? 1 : uncompressed / (double) onDisk;
        if (!"compressible".equals(options.payload) && ratio > 1.5)
        {
            report.format("# WARNING: --payload random but the parent compressed %.2fx. The corpus is not "
                          + "incompressible and is %s smaller on disk than asked for.", ratio, bytes(uncompressed - onDisk));
        }

        KeySignature signature = options.verifyKeys()
                                 ? signatureOf(Collections.singletonList(descriptor), descriptor.ksname, descriptor.cfname)
                                 : null;
        if (signature != null && signature.count != partitions)
        {
            throw new IllegalStateException("generated parent has " + signature.count + " partitions, expected "
                                            + partitions);
        }

        report.format("# logical payload written: %s over %,d partitions (%,d rows)",
                      bytes(logicalBytes), partitions, rows);

        return new Parent(descriptor, components, onDisk, uncompressed,
                          descriptor.fileFor(Component.PRIMARY_INDEX).length(),
                          partitions, rows, options.chunkKb * (int) KIB, signature,
                          "generated by this run");
    }

    /**
     * Give the freshly generated parent the bloom filter a real sstable of its size would have.
     *
     * <p>{@code AbstractSSTableSimpleWriter} has no way to be told how many partitions are coming, so it
     * always passes a key count of 0 and the parent lands with a 16-byte {@code AlwaysPresent} Filter.db. At a
     * terabyte the difference is tens of megabytes -- a component the split has to rebuild for every child --
     * and a corpus that is not a faithful sstable is not worth benchmarking against. Deleting the stub and
     * reopening online makes {@code SSTableReaderBuilder} rebuild the filter from Index.db at the table's
     * configured fp chance and persist it.
     *
     * @return the component set including the rebuilt filter
     */
    private Set<Component> rebuildBloomFilter(Descriptor descriptor, Set<Component> components)
    {
        descriptor.fileFor(Component.FILTER).deleteIfExists();

        Set<Component> withoutFilter = new LinkedHashSet<>(components);
        withoutFilter.remove(Component.FILTER);

        SSTableReader reader = SSTableReader.open(descriptor, withoutFilter,
                                                  Schema.instance.getTableMetadataRef(descriptor.ksname,
                                                                                      descriptor.cfname));
        reader.selfRef().release();

        if (!descriptor.fileFor(Component.FILTER).exists())
        {
            report.line("# note: no Filter.db was rebuilt (bloom_filter_fp_chance is probably 1.0); "
                        + "the parent carries no bloom filter");
            return withoutFilter;
        }
        report.format("# rebuilt Filter.db from Index.db: %s",
                      bytes(descriptor.fileFor(Component.FILTER).length()));
        return components;
    }

    /** Beyond this, the generation-time token sort would want more heap than it is worth. */
    private static final int MAX_UUID_PARTITIONS = 400_000_000;

    /**
     * Partition indices ordered by the token of their composite key.
     *
     * <p>The blob shape gets its keys in token order for free by inverting murmur3, but that inverse only
     * yields 16-byte preimages and a serialised {@code ((uuid, uuid))} partition key is 38 bytes, so there is
     * no way to ask for a key that hashes to a chosen token. The keys therefore have to be generated first and
     * ordered afterwards. Keys are a pure function of their index, so only the tokens and a permutation are
     * held -- 16 bytes per partition, during generation only, and the corpus is reused forever after.
     *
     * @return the partition indices in strictly increasing token order, with token collisions dropped
     */
    private int[] uuidKeysInTokenOrder(long partitions)
    {
        if (partitions > MAX_UUID_PARTITIONS)
        {
            throw new IllegalStateException("--key-type uuid needs a token sort over " + partitions
                                            + " partitions, which is past what this tool will hold in heap. "
                                            + "Raise --partition-size, or use --key-type blob, whose keys come "
                                            + "out in token order without sorting.");
        }
        int n = (int) partitions;
        long needed = 16L * n;
        report.format("# --key-type uuid: sorting %,d composite keys by token (%s of heap, generation only)",
                      n, bytes(needed));
        if (needed > Runtime.getRuntime().maxMemory() / 2)
        {
            throw new IllegalStateException(String.format(
                "the token sort needs about %s but -Xmx is only %s; raise -Xmx or --partition-size",
                bytes(needed), bytes(Runtime.getRuntime().maxMemory())));
        }

        long[] tokens = new long[n];
        int[] order = new int[n];
        for (int i = 0; i < n; i++)
        {
            tokens[i] = tokenOfUuidKey(i);
            order[i] = i;
        }
        sortByToken(tokens, order);

        // The writer needs strictly increasing decorated keys. Two distinct keys sharing a token is a ~1e-7
        // event at these counts, and dropping one partition is cheaper than ordering on the key bytes too.
        int kept = 0;
        long previous = 0;
        boolean first = true;
        for (int i = 0; i < n; i++)
        {
            long token = tokens[order[i]];
            if (first || token > previous)
            {
                order[kept++] = order[i];
                previous = token;
                first = false;
            }
        }
        if (kept != n)
            report.format("# dropped %d partition(s) whose composite keys collided on a token", n - kept);
        return kept == n ? order : Arrays.copyOf(order, kept);
    }

    /** Bottom-up merge sort of {@code order} by {@code tokens[order[i]]}; stable and allocation-bounded. */
    private static void sortByToken(long[] tokens, int[] order)
    {
        int n = order.length;
        int[] buffer = new int[n];
        for (int width = 1; width < n; width *= 2)
        {
            for (int lo = 0; lo < n; lo += 2 * width)
            {
                int mid = Math.min(lo + width, n);
                int hi = Math.min(lo + 2 * width, n);
                int a = lo;
                int b = mid;
                int o = lo;
                while (a < mid && b < hi)
                    buffer[o++] = tokens[order[a]] <= tokens[order[b]] ? order[a++] : order[b++];
                while (a < mid)
                    buffer[o++] = order[a++];
                while (b < hi)
                    buffer[o++] = order[b++];
            }
            System.arraycopy(buffer, 0, order, 0, n);
        }
    }

    /** The token of the composite {@code (k1, k2)} partition key of partition {@code index}. */
    private static long tokenOfUuidKey(int index)
    {
        ByteBuffer k1 = uuidBytes(index, 0);
        ByteBuffer k2 = uuidBytes(index, 1);
        ByteBuffer key = CompositeType.build(ByteBufferAccessor.instance, k1, k2);
        return Murmur3Partitioner.instance.getToken(key).token;
    }

    /** Component {@code which} of partition {@code index}, as a UUID. Pure function of its arguments. */
    private static UUID uuidFor(int index, int which)
    {
        long msb = mix(((long) index << 8) | which);
        return new UUID(msb, mix(msb));
    }

    private static ByteBuffer uuidBytes(int index, int which)
    {
        UUID uuid = uuidFor(index, which);
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        buffer.flip();
        return buffer;
    }

    /** splitmix64 finaliser: cheap, deterministic, and well distributed enough for uniform tokens. */
    private static long mix(long z)
    {
        z += 0x9E3779B97F4A7C15L;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    private long jitterState = 0x9E3779B97F4A7C15L;

    /**
     * A value size a little under {@code --value-size}, varying per row.
     *
     * <p>Not cosmetic. With uniform partitions and a power-of-two partition count, every split point
     * {@code chooseByByteShare} picks is at record {@code k * 2^j}, and {@code k * partitionSize} is a multiple
     * of the chunk length whenever {@code partitionSize} is even -- so <em>every</em> child came out exactly
     * chunk aligned, with a dead prefix of zero and no chunk shared between two children. That is the
     * best case for the algorithm and it never happens on a real table. Since the dead prefix and the
     * duplicated boundary chunk are the whole reason the zero-copy split is a chunk-run copy rather than a
     * byte-exact cut, a corpus that cannot produce them is not measuring the thing under test.
     *
     * <p>Deterministic (xorshift from a fixed seed), so a corpus is still reproducible.
     */
    private int jitteredValueSize()
    {
        int size = (int) options.valueSize;
        if (options.jitterDenominator <= 0)
            return size;
        int range = Math.max(1, size / options.jitterDenominator);
        jitterState ^= jitterState >>> 12;
        jitterState ^= jitterState << 25;
        jitterState ^= jitterState >>> 27;
        long r = (jitterState * 0x2545F4914F6CDD1DL) >>> 1;
        return Math.max(1, size - (int) (r % range));
    }

    /** Use an sstable that already exists -- the reason this tool can be pointed at a production-sized table. */
    private Parent adoptExistingParent()
    {
        File data = new File(options.parentDataFile);
        if (!data.exists())
            throw new IllegalArgumentException("no such file: " + data);

        Descriptor descriptor = Descriptor.fromFilename(data);
        registerSchema(options.externalSchemaCql(descriptor.ksname, descriptor.cfname));

        // Descriptor takes the keyspace/table from the directory the file sits in, and the children have to be
        // written against metadata that actually describes this sstable. Catch the mismatch here rather than
        // as an NPE several steps later.
        if (Schema.instance.getTableMetadata(descriptor.ksname, descriptor.cfname) == null)
        {
            throw new IllegalArgumentException(
                "the supplied CREATE TABLE does not define " + descriptor.ksname + '.' + descriptor.cfname
                + ", which is what the sstable's own directory says it belongs to. Either fix the DDL or copy "
                + "the sstable into a <keyspace>/<table>/ directory matching it.");
        }

        Set<Component> components = SSTable.componentsFor(descriptor);
        if (!components.contains(Component.COMPRESSION_INFO))
        {
            throw new IllegalArgumentException(descriptor + " has no CompressionInfo.db. "
                                               + "ZeroCopySSTableSplitter only supports compressed sstables.");
        }

        report.line("# adopting existing parent " + descriptor);
        report.line("#   the original files are only ever read and hard linked; nothing writes to them");

        SSTableReader reader =
            SSTableReader.openNoValidation(descriptor, Schema.instance.getTableMetadataRef(descriptor.ksname,
                                                                                           descriptor.cfname));
        long partitions = reader.estimatedKeys();
        long uncompressed = reader.uncompressedLength();
        int chunkLength = reader.getCompressionMetadata().chunkLength();
        reader.selfRef().release();

        KeySignature signature = options.verifyKeys()
                                 ? signatureOf(Collections.singletonList(descriptor), descriptor.ksname, descriptor.cfname)
                                 : null;
        if (signature != null)
            partitions = signature.count;

        return new Parent(descriptor, components, data.length(), uncompressed,
                          descriptor.fileFor(Component.PRIMARY_INDEX).length(),
                          partitions, -1, chunkLength, signature, "adopted from disk (--parent)");
    }

    /**
     * Make {@code keyspace.table} exist. Both the generated and the adopted case need a real
     * {@link ColumnFamilyStore}, because the baseline is a real {@code CompactionTask}.
     */
    private static void registerSchema(String createTable)
    {
        TableMetadata.Builder builder = CreateTableStatement.parse(createTable, keyspaceOf(createTable))
                                                            .partitioner(Murmur3Partitioner.instance);
        TableMetadata table = builder.build();

        Schema.instance.transform(SchemaTransformations.addKeyspace(
            KeyspaceMetadata.create(table.keyspace, KeyspaceParams.simple(1), Tables.none()), true));
        Schema.instance.transform(SchemaTransformations.addTable(table, true));
    }

    /** {@code CREATE TABLE ks.tbl (...)} -> {@code ks}. The DDL is always keyspace qualified here. */
    private static String keyspaceOf(String createTable)
    {
        java.util.regex.Matcher matcher =
            java.util.regex.Pattern.compile("CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?\"?([\\w]+)\"?\\s*\\.",
                                            java.util.regex.Pattern.CASE_INSENSITIVE)
                                   .matcher(createTable);
        if (!matcher.find())
            throw new IllegalArgumentException("the CREATE TABLE statement must be keyspace qualified: " + createTable);
        return matcher.group(1);
    }

    // ================================================================================================
    // One measured run
    // ================================================================================================

    private Measurement measure(ColumnFamilyStore cfs, Parent parent, String path, int iteration) throws Throwable
    {
        clearWorkingDirectory(cfs);
        requireFreeSpace(options.scratch, (long) (parent.onDiskBytes * 1.1),
                         "one set of children for the " + path + " run");

        // Link, then evict, then open -- in that order. Evicting after the open would be a no-op for
        // Index.db, which SSTableReader maps: POSIX_FADV_DONTNEED will not drop pages that are mapped, and
        // the Index.db pass is most of what the zero-copy path does.
        try
        {
            Descriptor target = link(cfs, parent);
            if (options.evict)
                evict(target, parent.components);

            Linked linked = open(cfs, target);
            return "zerocopy".equals(path)
                   ? runZeroCopy(cfs, parent, linked, iteration)
                   : runBaseline(cfs, parent, linked, iteration);
        }
        finally
        {
            clearWorkingDirectory(cfs);
        }
    }

    /** The parent's hard-linked stand-in for one run, plus the opened reader over it. */
    private static final class Linked
    {
        final Descriptor descriptor;
        final Set<Component> components;
        final SSTableReader reader;

        Linked(Descriptor descriptor, Set<Component> components, SSTableReader reader)
        {
            this.descriptor = descriptor;
            this.components = components;
            this.reader = reader;
        }
    }

    /**
     * Hard link every component of the corpus parent into the table's live data directory under a fresh
     * generation, and open it there.
     *
     * <p>Links, not copies: linking a terabyte is free and costs no space, and it is what makes the corpus
     * reusable -- the baseline path finishes by unlinking its parent, which drops this link and leaves the
     * corpus file itself with its remaining link intact. Nothing in an sstable's on-disk format depends on the
     * generation in its filename, so re-generationing on the way in is safe (TOC.txt lists component names,
     * Digest.crc32 covers Data.db content).
     */
    private Descriptor link(ColumnFamilyStore cfs, Parent parent)
    {
        File directory = cfs.getDirectories().getDirectoryForNewSSTables();
        Descriptor target = cfs.newSSTableDescriptor(directory);

        for (Component component : parent.components)
        {
            File from = parent.descriptor.fileFor(component);
            if (!from.exists())
                continue;
            try
            {
                FileUtils.createHardLinkWithConfirm(from, target.fileFor(component));
            }
            catch (RuntimeException e)
            {
                throw new IllegalStateException(
                    "could not hard link " + from + " into " + directory + ". Hard links do not cross "
                    + "filesystems, so --scratch has to live on the same filesystem as the parent sstable. "
                    + "Copying instead is deliberately not offered: at these sizes it would dominate the "
                    + "measurement and fill the disk.", e);
            }
        }
        return target;
    }

    /** Open the linked parent. {@code openNoValidation} so an adopted production sstable is never rewritten. */
    private static Linked open(ColumnFamilyStore cfs, Descriptor target)
    {
        Set<Component> components = SSTable.componentsFor(target);
        return new Linked(target, components, SSTableReader.openNoValidation(target, components, cfs));
    }

    /** Drop the parent and any children from the tracker and wipe the table's data directories. */
    private static void clearWorkingDirectory(ColumnFamilyStore cfs)
    {
        cfs.truncateBlockingWithoutSnapshot();
        LifecycleTransaction.waitForDeletions();
        for (File directory : cfs.getDirectories().getCFDirectories())
        {
            File[] files = directory.tryList();
            if (files == null)
                continue;
            for (File file : files)
            {
                if (!file.isDirectory())
                    file.deleteIfExists();
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Candidate
    // ------------------------------------------------------------------------------------------------

    private Measurement runZeroCopy(ColumnFamilyStore cfs, Parent parent, Linked linked, int iteration)
    {
        if (!ZeroCopySSTableSplitter.isSupported(linked.reader))
            throw new IllegalStateException("parent is not a compressed BIG-format sstable: " + linked.descriptor);

        ProcIo io0 = ProcIo.read();
        long alloc0 = threadAllocatedBytes();
        long t0 = System.nanoTime();
        ZeroCopySSTableSplitter.Result result = ZeroCopySSTableSplitter.split(linked.reader, options.children, null);
        long nanos = System.nanoTime() - t0;
        long alloc = delta(alloc0, threadAllocatedBytes());
        ProcIo io1 = ProcIo.read();

        long written = 0;
        List<Descriptor> descriptors = new ArrayList<>(result.children.size());
        for (ZeroCopySSTableSplitter.Child child : result.children)
        {
            written += componentBytes(child.descriptor, child.components);
            descriptors.add(child.descriptor);
        }

        Measurement measurement = new Measurement("zerocopy", iteration, nanos, ProcIo.delta(io0, io1),
                                                  written, alloc, result.children.size());
        measurement.deadPrefixBytes = result.totalDeadPrefixBytes;
        measurement.duplicatedChunkBytes = result.duplicatedChunkBytes;
        measurement.clonedBytes = result.totalBytesCloned;
        measurement.headPadBytes = result.totalHeadPadBytes;
        measurement.childDataBytes = result.totalPhysicalBytesCopied + result.totalHeadPadBytes;

        try
        {
            verify(measurement, parent, descriptors, readersOf(result));
        }
        finally
        {
            linked.reader.selfRef().release();
            for (ZeroCopySSTableSplitter.Child child : result.children)
                child.reader.selfRef().release();
            LifecycleTransaction.waitForDeletions();
        }
        return measurement;
    }

    private static List<SSTableReader> readersOf(ZeroCopySSTableSplitter.Result result)
    {
        List<SSTableReader> readers = new ArrayList<>(result.children.size());
        for (ZeroCopySSTableSplitter.Child child : result.children)
            readers.add(child.reader);
        return readers;
    }

    // ------------------------------------------------------------------------------------------------
    // Baseline: the full rewrite that sstablesplit/nodetool actually run
    // ------------------------------------------------------------------------------------------------

    private Measurement runBaseline(ColumnFamilyStore cfs, Parent parent, Linked linked, int iteration) throws Throwable
    {
        // MaxSSTableSizeWriter rolls over on estimated ON-DISK bytes written, so target the parent's compressed
        // length divided by the requested child count.
        int sizeInMiB = (int) Math.max(1, parent.onDiskBytes / (options.children * MIB));

        cfs.addSSTable(linked.reader);
        Set<SSTableReader> before = new LinkedHashSet<>(cfs.getLiveSSTables());

        ProcIo io0;
        ProcIo io1;
        long nanos;
        long alloc;
        try (LifecycleTransaction txn = cfs.getTracker()
                                           .tryModify(Collections.singleton(linked.reader), OperationType.UNKNOWN))
        {
            if (txn == null)
                throw new IllegalStateException("could not obtain a LifecycleTransaction over " + linked.descriptor);

            io0 = ProcIo.read();
            long alloc0 = threadAllocatedBytes();
            long t0 = System.nanoTime();
            new SSTableSplitter(cfs, txn, sizeInMiB).split();
            nanos = System.nanoTime() - t0;
            alloc = delta(alloc0, threadAllocatedBytes());
            io1 = ProcIo.read();
        }
        LifecycleTransaction.waitForDeletions();

        Set<SSTableReader> produced = new LinkedHashSet<>(cfs.getLiveSSTables());
        produced.removeAll(before);
        if (produced.isEmpty())
            throw new IllegalStateException("baseline split produced no sstables");

        long written = 0;
        List<Descriptor> descriptors = new ArrayList<>(produced.size());
        for (SSTableReader child : produced)
        {
            written += componentBytes(child.descriptor, child.getComponents());
            descriptors.add(child.descriptor);
        }

        Measurement measurement = new Measurement("baseline", iteration, nanos, ProcIo.delta(io0, io1),
                                                  written, alloc, produced.size());
        verify(measurement, parent, descriptors, new ArrayList<>(produced));
        return measurement;
    }

    // ================================================================================================
    // Verification
    // ================================================================================================

    /**
     * Correctness only, never timing -- a benchmark that fails on a slow box is a liability, but one that
     * reports a number for an algorithm that lost data is worse. Runs outside the timed region.
     */
    private void verify(Measurement measurement, Parent parent, List<Descriptor> children,
                        List<SSTableReader> readers)
    {
        if (!options.verifyKeys())
            return;

        long t0 = System.nanoTime();
        KeySignature actual = signatureOf(children, parent.keyspace, parent.table);
        if (!actual.equals(parent.signature))
        {
            throw new AssertionError(measurement.path + ": the children do not cover exactly the parent's keys."
                                     + " parent=" + parent.signature + " children=" + actual);
        }

        if (options.verifyRows)
        {
            long rows = 0;
            for (SSTableReader reader : readers)
                rows += countRows(reader);
            if (parent.rows >= 0 && rows != parent.rows)
            {
                throw new AssertionError(measurement.path + ": children hold " + rows + " rows, parent held "
                                         + parent.rows);
            }
            measurement.verifiedRows = rows;
        }
        measurement.verifyNanos = System.nanoTime() - t0;
        measurement.verifiedPartitions = actual.count;
    }

    /**
     * Order independent fingerprint of a set of partition keys: count plus the XOR and the sum of a 64-bit hash
     * of every key. Read straight out of Index.db, so it costs one sequential pass over ~1% of the data and
     * retains nothing, yet a dropped, duplicated or corrupted key changes it.
     */
    static final class KeySignature
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

        static KeySignature parse(String encoded)
        {
            if (encoded == null)
                return null;
            String[] parts = encoded.split(":");
            if (parts.length != 3)
                return null;
            try
            {
                return new KeySignature(Long.parseLong(parts[0]),
                                        Long.parseUnsignedLong(parts[1], 16),
                                        Long.parseUnsignedLong(parts[2], 16));
            }
            catch (NumberFormatException e)
            {
                return null;
            }
        }

        String encode()
        {
            return count + ":" + Long.toHexString(xor) + ':' + Long.toHexString(sum);
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

    private static KeySignature signatureOf(List<Descriptor> descriptors, String keyspace, String table)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
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

    // ================================================================================================
    // Payload generation
    // ================================================================================================

    /**
     * Fills row values. {@code random} is xorshift64* output, which LZ4 cannot compress at all, so the parent's
     * on-disk size tracks its logical size and it really does span the expected number of chunks.
     * {@code compressible} emits a low-entropy pattern instead, to see what a ~10x compression ratio does to
     * chunk counts and therefore to the split.
     */
    private static final class Filler
    {
        private final boolean random;
        private long state;

        Filler(String payload, long seed)
        {
            this.random = !"compressible".equals(payload);
            this.state = seed == 0 ? 1 : seed;
        }

        /**
         * A <em>fresh</em> buffer every time, deliberately. The sorted writer accumulates a whole
         * {@code PartitionUpdate} before serialising it, so a shared array would leave every row of a
         * partition holding the last row's bytes -- which compresses beautifully and quietly turns an
         * "incompressible" corpus into a 3x-compressible one.
         */
        ByteBuffer next(int size)
        {
            byte[] buffer = new byte[size];
            if (!random)
            {
                // Repeating, but not constant, so the compressor has real work to do and the ratio is stable.
                for (int i = 0; i < size; i++)
                    buffer[i] = (byte) ('a' + (i % 16));
                return ByteBuffer.wrap(buffer);
            }
            int i = 0;
            while (i + 8 <= size)
            {
                long v = nextLong();
                for (int b = 0; b < 8; b++)
                    buffer[i + b] = (byte) (v >>> (8 * b));
                i += 8;
            }
            long v = nextLong();
            while (i < size)
            {
                buffer[i++] = (byte) v;
                v >>>= 8;
            }
            return ByteBuffer.wrap(buffer);
        }

        private long nextLong()
        {
            state ^= state >>> 12;
            state ^= state << 25;
            state ^= state >>> 27;
            return state * 0x2545F4914F6CDD1DL;
        }
    }

    /**
     * The property {@code Murmur3PartitionerTest.testLongTokenInverse} asserts for the whole token range, but a
     * corpus that takes hours to build should not discover a violation of it three hours in via a confusing
     * "keys must be written in ascending order" from the writer.
     */
    private static void assertInverse(ByteBuffer key, long token)
    {
        long actual = Murmur3Partitioner.instance.getToken(key).token;
        if (actual != token)
        {
            throw new IllegalStateException("murmur3 inverse is wrong: keyForToken(" + token + ") hashes to "
                                            + actual);
        }
    }

    // ================================================================================================
    // Instrumentation
    // ================================================================================================

    /**
     * A snapshot of {@code /proc/self/io}. {@code rchar}/{@code wchar} count bytes moved by read/write
     * syscalls, so they include page-cache hits; {@code read_bytes}/{@code write_bytes} count bytes that
     * actually crossed the block layer. The pair together is what distinguishes "the algorithm touched less
     * data" from "the second run just found the data in cache".
     */
    static final class ProcIo
    {
        static final String[] FIELDS = { "rchar", "wchar", "syscr", "syscw", "read_bytes", "write_bytes" };

        final Map<String, Long> values;

        private ProcIo(Map<String, Long> values)
        {
            this.values = values;
        }

        static ProcIo read()
        {
            if (!Files.isReadable(PROC_SELF_IO))
                return null;
            try
            {
                Map<String, Long> values = new LinkedHashMap<>();
                for (String line : Files.readAllLines(PROC_SELF_IO))
                {
                    int colon = line.indexOf(':');
                    if (colon < 0)
                        continue;
                    String name = line.substring(0, colon).trim();
                    if (Arrays.asList(FIELDS).contains(name))
                        values.put(name, Long.parseLong(line.substring(colon + 1).trim()));
                }
                return new ProcIo(values);
            }
            catch (Throwable t)
            {
                return null;
            }
        }

        static ProcIo delta(ProcIo before, ProcIo after)
        {
            if (before == null || after == null)
                return null;
            Map<String, Long> values = new LinkedHashMap<>();
            for (String field : FIELDS)
            {
                Long b = before.values.get(field);
                Long a = after.values.get(field);
                if (b != null && a != null)
                    values.put(field, a - b);
            }
            return new ProcIo(values);
        }

        long get(String field)
        {
            Long value = values.get(field);
            return value == null ? -1 : value;
        }
    }

    /** Reflective, so this still compiles and runs on a JVM that does not expose com.sun.management. */
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
                // already enabled, or not settable; the probe below decides
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

    private static long delta(long before, long after)
    {
        return (before < 0 || after < 0) ? -1 : after - before;
    }

    // ================================================================================================
    // Filesystem helpers
    // ================================================================================================

    /**
     * Drop every component of an sstable from page cache, so the next run reads from the device.
     * {@code posix_fadvise(POSIX_FADV_DONTNEED)} only evicts clean pages, which is why the corpus is fsynced
     * when it is written. A no-op off Linux -- {@link Report#preamble} says which.
     */
    private static void evict(Descriptor descriptor, Set<Component> components)
    {
        for (Component component : components)
        {
            File file = descriptor.fileFor(component);
            if (file.exists())
                NativeLibrary.trySkipCache(file.absolutePath(), 0, 0);
        }
    }

    private static void fsync(File file)
    {
        if (!file.exists())
            return;
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE))
        {
            channel.force(true);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("could not fsync " + file, e);
        }
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

    private static void requireFreeSpace(File directory, long needed, String what)
    {
        try
        {
            Path path = directory.toPath();
            while (path != null && !Files.exists(path))
                path = path.getParent();
            if (path == null)
                return;
            long usable = Files.getFileStore(path).getUsableSpace();
            if (usable < needed)
            {
                throw new IllegalStateException(String.format("%s needs about %s free under %s but only %s is available",
                                                              what, bytes(needed), directory, bytes(usable)));
            }
        }
        catch (IOException e)
        {
            // Not being able to ask is not a reason to refuse to run.
        }
    }

    /**
     * Finding out that a six-hour corpus does not fit the heap when the split starts would be an expensive way
     * to learn it. Both of the splitter's Index.db passes stream, so this is now bounded by the largest child's
     * bloom filter and index summary rather than by the parent's partition count -- which is why it scales
     * down with {@code --children}.
     */
    private void checkHeadroom(Parent parent)
    {
        if (parent.partitions <= 0)
            return;
        long perChild = parent.partitions / Math.max(1, options.children);
        long needed = perChild * HEAP_BYTES_PER_CHILD_PARTITION;
        long max = Runtime.getRuntime().maxMemory();
        report.format("# splitter needs about %s of heap: %,d partitions over %d children, and it holds one "
                      + "child's filter and summary at a time (-Xmx is %s)",
                      bytes(needed), parent.partitions, options.children, bytes(max));
        if (needed > max / 2)
        {
            throw new IllegalStateException(String.format(
                "building a child needs about %s of heap for its %,d partitions but -Xmx is only %s. Raise "
                + "-Xmx, raise --children so each child is smaller, or raise --partition-size so the same "
                + "bytes are spread over fewer partitions.",
                bytes(needed), perChild, bytes(max)));
        }
    }

    // ================================================================================================
    // Reporting
    // ================================================================================================

    /** One measured split. */
    static final class Measurement
    {
        final String path;
        final int iteration;
        final long nanos;
        /** null when /proc/self/io is unavailable. */
        final ProcIo io;
        /** Exact: summed on-disk component lengths of the produced children. */
        final long writtenBytes;
        final long allocatedBytes;
        final int children;

        long deadPrefixBytes = -1;
        long duplicatedChunkBytes = -1;
        /** Bytes shared with the parent as copy-on-write extents instead of copied; 0 without reflink support. */
        long clonedBytes = -1;
        /** Alignment padding at the head of the children's Data.db, the price of being able to share at all. */
        long headPadBytes = -1;
        /** Total Data.db bytes across the children, i.e. what {@link #clonedBytes} is a fraction of. */
        long childDataBytes = -1;
        long verifyNanos = -1;
        long verifiedPartitions = -1;
        long verifiedRows = -1;

        Measurement(String path, int iteration, long nanos, ProcIo io, long writtenBytes, long allocatedBytes,
                    int children)
        {
            this.path = path;
            this.iteration = iteration;
            this.nanos = nanos;
            this.io = io;
            this.writtenBytes = writtenBytes;
            this.allocatedBytes = allocatedBytes;
            this.children = children;
        }

        /**
         * Bytes that were really written, i.e. the children's components minus whatever was shared with the
         * parent as copy-on-write extents. This, not {@link #writtenBytes}, is what W_AMP is computed from:
         * a shared extent is a refcount update, and calling it a write would make the column disagree with
         * WR_DISK_MiB right next to it.
         */
        long physicalWrittenBytes()
        {
            return writtenBytes - Math.max(0, clonedBytes);
        }

        double seconds()
        {
            return nanos / 1_000_000_000.0;
        }

        long io(String field)
        {
            return io == null ? -1 : io.get(field);
        }
    }

    /**
     * Cassandra's test harness replaces {@link System#out} with a bridge into SLF4J that splits each message on
     * whitespace, which turns every padded column of a printf table into its own log line. So the report goes
     * straight to fd 1 and simultaneously to a file, which is the durable artifact you want from a benchmark
     * anyway.
     */
    static final class Report implements AutoCloseable
    {
        private final PrintStream console;
        private final PrintStream file;

        private Report(PrintStream console, PrintStream file)
        {
            this.console = console;
            this.file = file;
        }

        static Report open(String path)
        {
            PrintStream console = new PrintStream(new FileOutputStream(FileDescriptor.out), true);
            PrintStream file = null;
            if (path != null)
            {
                try
                {
                    java.io.File target = new java.io.File(path).getAbsoluteFile();
                    if (target.getParentFile() != null)
                        target.getParentFile().mkdirs();
                    file = new PrintStream(new FileOutputStream(target, true), true);
                    console.println("report: " + target);
                }
                catch (IOException e)
                {
                    console.println("could not open report file " + path + ": " + e);
                }
            }
            return new Report(console, file);
        }

        void line(String s)
        {
            console.println(s);
            if (file != null)
                file.println(s);
        }

        void format(String format, Object... args)
        {
            line(String.format(format, args));
        }

        /** Whether the scratch filesystem can actually clone; a disabled-by-hardware reflink is not a result. */
        boolean reflinkPossible = true;

        void preamble(Options options)
        {
            line("");
            line(rule());
            line(" ZeroCopySSTableSplitter vs SSTableSplitter (full rewrite) -- large scale");
            format("   scratch          : %s", options.scratch.absolutePath());
            format("   corpus           : %s", options.corpus.absolutePath());
            format("   children         : %d, iterations: %d, paths: %s",
                   options.children, options.iterations, String.join(",", options.paths));
            format("   page cache evict : %s",
                   !options.evict ? "disabled (--evict none) -- numbers are warm cache"
                                  : NativeLibrary.isAvailable() && isLinux()
                                    ? "posix_fadvise(DONTNEED) before every timed run"
                                    : "REQUESTED BUT UNAVAILABLE on this platform -- numbers are warm cache");
            format("   io accounting    : %s",
                   ProcIo.read() == null ? "/proc/self/io unavailable -- READ/WRITE columns are blank"
                                         : "/proc/self/io (rchar/wchar are syscall bytes, rd_disk/wr_disk crossed the block layer)");
            format("   allocation       : %s",
                   THREAD_ALLOCATED_BYTES == null ? "unavailable on this JVM"
                                                  : "com.sun.management.ThreadMXBean, calling thread only");
            format("   compaction rate  : %s",
                   DatabaseDescriptor.getCompactionThroughputMebibytesPerSec() == 0
                   ? "unthrottled (the baseline is a real CompactionTask and would otherwise be rate limited)"
                   : DatabaseDescriptor.getCompactionThroughputMebibytesPerSec() + " MiB/s -- THROTTLED, the baseline number is meaningless");
            // mmap_index_only is normalised away by DatabaseDescriptor into data=standard + index=mmap, so
            // print what is actually in effect rather than what was asked for.
            format("   disk access      : data %s, index %s, chunk cache %s%s",
                   DatabaseDescriptor.getDiskAccessMode(),
                   DatabaseDescriptor.getIndexAccessMode(),
                   DatabaseDescriptor.getFileCacheEnabled() ? "on" : "off",
                   options.evict && DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap
                   ? "  -- WARNING: Data.db is mmapped, so fadvise cannot evict it and these are warm numbers"
                   : "");
            format("   reflink          : %s%s", options.reflink ? "enabled" : "disabled",
                   options.reflink && !reflinkPossible
                   ? "  -- but this filesystem cannot clone ranges, so children will be copied"
                   : "");
            format("   digest           : %s", options.digest
                                               ? "Digest.crc32 written (a full pass over each child)"
                                               : "SKIPPED (zero_copy_split_digest_enabled=false)");
            format("   verification     : %s", options.verify);
            line(rule());
        }

        void parent(Parent parent)
        {
            line("");
            format(" parent          : %s", parent.descriptor);
            format("   on disk       : %s (%s uncompressed, ratio %.2fx)",
                   bytes(parent.onDiskBytes), bytes(parent.uncompressedBytes),
                   parent.onDiskBytes == 0 ? 0 : parent.uncompressedBytes / (double) parent.onDiskBytes);
            format("   Index.db      : %s (%.2f%% of Data.db)",
                   bytes(parent.indexBytes),
                   parent.onDiskBytes == 0 ? 0 : 100.0 * parent.indexBytes / parent.onDiskBytes);
            format("   partitions    : %,d%s", parent.partitions, parent.rows >= 0
                                                                  ? String.format(", rows: %,d", parent.rows) : "");
            format("   chunk length  : %s", bytes(parent.chunkLength));
            format("   origin        : %s", parent.origin);
            if (parent.signature != null)
                format("   key signature : %s", parent.signature);
            line("");
        }

        void tableHeader()
        {
            format("%-10s %4s %10s %10s %12s %12s %12s %12s %8s %6s %11s",
                   "PATH", "IT", "WALL_S", "MiB/s", "RCHAR_MiB", "RD_DISK_MiB", "WCHAR_MiB", "WR_DISK_MiB",
                   "W_AMP", "KIDS", "ALLOC_MiB");
        }

        void row(Measurement m, Parent parent)
        {
            format("%-10s %4d %10.2f %10.1f %12s %12s %12s %12s %8.3f %6d %11s",
                   m.path,
                   m.iteration,
                   m.seconds(),
                   m.seconds() == 0 ? 0 : (parent.onDiskBytes / MIB) / m.seconds(),
                   mib(m.io("rchar")),
                   mib(m.io("read_bytes")),
                   mib(m.io("wchar")),
                   mib(m.io("write_bytes")),
                   parent.onDiskBytes == 0 ? 0 : m.physicalWrittenBytes() / (double) parent.onDiskBytes,
                   m.children,
                   mib(m.allocatedBytes));

            if ("zerocopy".equals(m.path) && m.deadPrefixBytes >= 0)
            {
                format("%-10s %4s   dead prefix %s across %d children, %s duplicated by boundary chunks",
                       "", "", bytes(m.deadPrefixBytes), m.children, bytes(m.duplicatedChunkBytes));
                // The headline number when the scratch directory is on a reflink filesystem: cloned bytes were
                // neither read nor written nor allocated, so write_bytes above should collapse to the index and
                // metadata alone. `pad` is what the alignment cost, and is the only extra disk this buys.
                format("%-10s %4s   %s of %s Data.db shared as extents (%.1f%%), %s alignment pad",
                       "", "", bytes(m.clonedBytes), bytes(m.childDataBytes),
                       m.childDataBytes <= 0 ? 0.0 : 100.0 * m.clonedBytes / m.childDataBytes,
                       bytes(m.headPadBytes));
            }
            if (m.verifyNanos >= 0)
            {
                format("%-10s %4s   verified %,d partitions%s in %.1fs", "", "", m.verifiedPartitions,
                       m.verifiedRows >= 0 ? String.format(" / %,d rows", m.verifiedRows) : "",
                       m.verifyNanos / 1_000_000_000.0);
            }
        }

        void summary(List<Measurement> measurements, Parent parent, Options options)
        {
            line("");
            line(rule());
            line(" SUMMARY (median over iterations)");
            line(rule());
            format("%-10s %10s %10s %14s %14s %10s",
                   "PATH", "WALL_S", "MiB/s", "RD_DISK_MiB", "WR_DISK_MiB", "W_AMP");

            Map<String, Double> medianSeconds = new LinkedHashMap<>();
            for (String path : options.paths)
            {
                List<Measurement> runs = new ArrayList<>();
                for (Measurement m : measurements)
                {
                    if (m.path.equals(path))
                        runs.add(m);
                }
                if (runs.isEmpty())
                    continue;

                double seconds = median(runs, m -> m.seconds());
                medianSeconds.put(path, seconds);
                format("%-10s %10.2f %10.1f %14s %14s %10.3f",
                       path,
                       seconds,
                       seconds == 0 ? 0 : (parent.onDiskBytes / MIB) / seconds,
                       mib((long) median(runs, m -> (double) m.io("read_bytes"))),
                       mib((long) median(runs, m -> (double) m.io("write_bytes"))),
                       parent.onDiskBytes == 0 ? 0 : median(runs, m -> m.physicalWrittenBytes() / (double) parent.onDiskBytes));
            }

            Double baseline = medianSeconds.get("baseline");
            Double zeroCopy = medianSeconds.get("zerocopy");
            if (baseline != null && zeroCopy != null && zeroCopy > 0)
            {
                line("");
                format(" speedup: %.1fx  (baseline %.1fs -> zero-copy %.1fs on a %s parent split into %d)",
                       baseline / zeroCopy, baseline, zeroCopy, bytes(parent.onDiskBytes), options.children);
            }
            line(rule());
            line("");
        }

        private static double median(List<Measurement> runs, java.util.function.ToDoubleFunction<Measurement> f)
        {
            double[] values = new double[runs.size()];
            for (int i = 0; i < runs.size(); i++)
                values[i] = f.applyAsDouble(runs.get(i));
            Arrays.sort(values);
            int mid = values.length / 2;
            return values.length % 2 == 1 ? values[mid] : (values[mid - 1] + values[mid]) / 2;
        }

        private static String mib(long value)
        {
            return value < 0 ? "-" : String.format("%.1f", value / (double) MIB);
        }

        private static String rule()
        {
            return "=========================================================================================================";
        }

        @Override
        public void close()
        {
            console.flush();
            if (file != null)
                file.close();
        }
    }

    /**
     * Optional machine-readable sink, appended to across runs. The padded report is for reading; this is what
     * you plot from, so nothing downstream has to parse a printf table.
     */
    static final class Csv implements AutoCloseable
    {
        private static final String HEADER =
            "shape,evict,disk_access_mode,parent_on_disk,parent_uncompressed,partitions,rows,chunk_length,"
            + "children_requested,path,iteration,wall_ms,rchar,read_bytes,wchar,write_bytes,written_bytes,"
            + "alloc_bytes,children_produced,dead_prefix,duplicated_chunk,cloned_bytes,head_pad,w_amp";

        private final PrintStream out;

        private Csv(PrintStream out)
        {
            this.out = out;
        }

        static Csv open(String path)
        {
            if (path == null)
                return null;
            try
            {
                java.io.File target = new java.io.File(path).getAbsoluteFile();
                if (target.getParentFile() != null)
                    target.getParentFile().mkdirs();
                boolean fresh = !target.exists() || target.length() == 0;
                PrintStream out = new PrintStream(new FileOutputStream(target, true), true);
                if (fresh)
                    out.println(HEADER);
                return new Csv(out);
            }
            catch (IOException e)
            {
                return null;
            }
        }

        void row(Options options, Parent parent, Measurement m)
        {
            // 24 conversions for 24 header columns, in the same order. Getting this wrong is silent until
            // the first row is written, which is a long way into a benchmark run.
            out.printf("%s,%s,%s,%d,%d,%d,%d,%d,%d,%s,%d,%.3f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.6f%n",
                       options.shapeLabel(),
                       options.evict ? "fadvise" : "none",
                       DatabaseDescriptor.getDiskAccessMode(),
                       parent.onDiskBytes,
                       parent.uncompressedBytes,
                       parent.partitions,
                       parent.rows,
                       parent.chunkLength,
                       options.children,
                       m.path,
                       m.iteration,
                       m.nanos / 1_000_000.0,
                       m.io("rchar"),
                       m.io("read_bytes"),
                       m.io("wchar"),
                       m.io("write_bytes"),
                       m.writtenBytes,
                       m.allocatedBytes,
                       m.children,
                       m.deadPrefixBytes,
                       m.duplicatedChunkBytes,
                       m.clonedBytes,
                       m.headPadBytes,
                       parent.onDiskBytes == 0 ? 0 : m.physicalWrittenBytes() / (double) parent.onDiskBytes);
        }

        @Override
        public void close()
        {
            out.close();
        }
    }

    /** Periodic progress for a generation run that can legitimately take hours. */
    private static final class Progress
    {
        private final Report report;
        private final long total;
        private final long start = System.nanoTime();
        private long written;
        private long nextReport;

        Progress(Report report, long total)
        {
            this.report = report;
            this.total = total;
            this.nextReport = Math.max(total / 20, 256 * MIB);
        }

        void advance(long bytes)
        {
            written += bytes;
            if (written < nextReport)
                return;
            nextReport = written + Math.max(total / 20, 256 * MIB);

            double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
            double rate = seconds == 0 ? 0 : (written / (double) MIB) / seconds;
            long remaining = rate == 0 ? -1 : (long) ((total - written) / (rate * MIB));
            report.format("#   %5.1f%%  %s written  %.0f MiB/s  eta %s",
                          100.0 * written / total, bytes(written), rate,
                          remaining < 0 ? "?" : duration(remaining));
        }

        void done(long nanos)
        {
            double seconds = nanos / 1_000_000_000.0;
            report.format("# generated %s of logical data in %s including the final flush (%.0f MiB/s)",
                          bytes(written), duration((long) seconds),
                          seconds == 0 ? 0 : (written / (double) MIB) / seconds);
        }
    }

    // ================================================================================================
    // Formatting
    // ================================================================================================

    static String bytes(long value)
    {
        if (value < 0)
            return "-";
        if (value >= GIB)
            return String.format("%.2f GiB", value / (double) GIB);
        if (value >= MIB)
            return String.format("%.1f MiB", value / (double) MIB);
        if (value >= KIB)
            return String.format("%.0f KiB", value / (double) KIB);
        return value + " B";
    }

    static String duration(long seconds)
    {
        if (seconds < 60)
            return seconds + "s";
        if (seconds < 3600)
            return String.format("%dm%02ds", seconds / 60, seconds % 60);
        return String.format("%dh%02dm", seconds / 3600, (seconds % 3600) / 60);
    }

    private static boolean isLinux()
    {
        return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux");
    }

    // ================================================================================================
    // Options
    // ================================================================================================

    /** Parsed command line. Also constructible directly, which is how the JUnit wrapper drives the harness. */
    public static final class Options
    {
        public File scratch = new File("zerocopy-split-bench");
        public File corpus;
        public String reportFile;
        /** Optional append-only CSV of every measurement, for plotting. */
        public String csvFile;

        /** Logical (uncompressed) size of the generated parent. */
        public long targetBytes = 4 * GIB;
        public long partitionSize = 64 * KIB;
        public long valueSize = 4 * KIB;
        public long columnIndexSize = 64 * KIB;
        public int chunkKb = 16;
        public String compressor = "LZ4Compressor";
        public String payload = "random";
        /**
         * {@code blob}: a single 16-byte blob partition key and an int clustering, generated in token order by
         * inverting murmur3, so corpus size is bounded only by disk.
         * <p>
         * {@code uuid}: {@code PRIMARY KEY ((k1 uuid, k2 uuid), c uuid)} -- a composite partition key and a
         * UUID clustering. This is the shape that costs the rewrite path the most, because it has to
         * deserialise and materialise a CompositeType partition key and a UUID clustering for every row, none
         * of which the chunk-run copy ever looks at. Murmur3 cannot be inverted for a composite key (the
         * inverse only produces 16-byte preimages, and a serialised two-UUID composite is 38 bytes), so these
         * keys are generated then sorted by token, which costs ~16 bytes of heap per partition at generation
         * time only.
         */
        public String keyType = "blob";
        /**
         * Row values vary by up to {@code 1/jitterDenominator}, so partition offsets decorrelate from the
         * compression chunk grid. 0 disables it and makes every child exactly chunk aligned, which is the
         * algorithm's best case and not a real table. See {@code jitteredValueSize()}.
         */
        public int jitterDenominator = 16;
        public boolean regenerate;

        /** Set to split an sstable that already exists instead of generating one. */
        public String parentDataFile;
        /** The CREATE TABLE for {@link #parentDataFile}; required in that mode. */
        public String externalSchema;

        public int children = 4;
        public int iterations = 1;
        public List<String> paths = Arrays.asList("zerocopy", "baseline");
        public boolean evict = true;
        /**
         * mmap_index_only, not Cassandra's "auto" (which is mmap on a 64-bit JVM), because
         * POSIX_FADV_DONTNEED cannot evict mapped pages and a mapped Data.db would make every run after the
         * first a warm-cache run without saying so.
         */
        public String diskAccessMode = "mmap_index_only";
        public String verify = "keys";
        public boolean verifyRows;
        /**
         * Whether the splitter may share a child's Data.db extents with the parent instead of copying them.
         * On by default, which is a no-op on any filesystem that cannot do it. Turn it off to A/B the same
         * corpus against itself on xfs-reflink: that is the measurement the whole feature is for, and it is
         * only meaningful with {@code --evict fadvise} and against a corpus far larger than RAM.
         */
        public boolean reflink = true;
        /**
         * Whether the splitter writes Digest.crc32 for each child. On by default, matching the server default.
         * Turning it off removes the one pass whose cost is proportional to the data rather than to the index,
         * which is the whole remaining cost of a split once {@code --reflink on} has removed the copy -- so
         * {@code --reflink on --digest off} is the floor this tool can measure.
         */
        public boolean digest = true;

        public boolean verifyKeys()
        {
            return !"none".equals(verify);
        }

        /** Identifies a corpus by its shape, so different shapes coexist and each is generated at most once. */
        public String shapeLabel()
        {
            return String.format("%s-p%d-v%d-c%dk-%s-%s-j%d-%s",
                                 compact(targetBytes), partitionSize, valueSize, chunkKb,
                                 compressor.replace("Compressor", ""), payload, jitterDenominator, keyType);
        }

        boolean isUuidKey()
        {
            return "uuid".equals(keyType);
        }

        String schemaCql()
        {
            String columns = isUuidKey()
                             ? "  k1 uuid, k2 uuid, c uuid, v blob, PRIMARY KEY ((k1, k2), c)"
                             : "  k blob, c int, v blob, PRIMARY KEY (k, c)";
            return "CREATE TABLE " + DEFAULT_KEYSPACE + '.' + DEFAULT_TABLE + " (" + columns + ')'
                   + " WITH compression = {'class': '" + compressor + "', 'chunk_length_in_kb': " + chunkKb + '}'
                   + "   AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}";
        }

        String insertCql()
        {
            return isUuidKey()
                   ? "INSERT INTO " + DEFAULT_KEYSPACE + '.' + DEFAULT_TABLE + " (k1, k2, c, v) VALUES (?, ?, ?, ?)"
                   : "INSERT INTO " + DEFAULT_KEYSPACE + '.' + DEFAULT_TABLE + " (k, c, v) VALUES (?, ?, ?)";
        }

        String externalSchemaCql(String keyspace, String table)
        {
            if (externalSchema == null)
            {
                throw new IllegalArgumentException("--parent needs --schema/--schema-file: the CREATE TABLE for "
                                                   + keyspace + '.' + table
                                                   + " (copy it out of cqlsh DESCRIBE TABLE)");
            }
            return externalSchema;
        }

        void resolveDefaults()
        {
            if (corpus == null)
                corpus = new File(scratch, "corpus");
            if (reportFile == null)
                reportFile = new File(scratch, "report.txt").path();
            if (parentDataFile == null && valueSize > partitionSize)
                throw new IllegalArgumentException("--value-size must not exceed --partition-size");
            if (children < 1)
                throw new IllegalArgumentException("--children must be >= 1");
            if (iterations < 1)
                throw new IllegalArgumentException("--iterations must be >= 1");
            if (chunkKb < 1 || Integer.bitCount(chunkKb) != 1)
                throw new IllegalArgumentException("--chunk-kb must be a power of two, got " + chunkKb);
            for (String path : paths)
            {
                if (!"zerocopy".equals(path) && !"baseline".equals(path))
                    throw new IllegalArgumentException("unknown path '" + path + "', expected zerocopy or baseline");
            }
            if (!new TreeSet<>(Arrays.asList("blob", "uuid")).contains(keyType))
                throw new IllegalArgumentException("--key-type must be blob or uuid, got " + keyType);
            if (!new TreeSet<>(Arrays.asList("auto", "mmap", "mmap_index_only", "standard")).contains(diskAccessMode))
                throw new IllegalArgumentException("--disk-access-mode must be auto, mmap, mmap_index_only or standard");
            if (!new TreeSet<>(Arrays.asList("none", "keys", "rows")).contains(verify))
                throw new IllegalArgumentException("--verify must be none, keys or rows, got " + verify);
            verifyRows = "rows".equals(verify);
        }

        /** @return the parsed options, or null when {@code --help} was asked for and printed. */
        static Options parse(String[] args)
        {
            Options options = new Options();
            for (int i = 0; i < args.length; i++)
            {
                String arg = args[i];
                switch (arg)
                {
                    case "--help":
                    case "-h":
                        usage();
                        return null;
                    case "--scratch":
                        options.scratch = new File(next(args, ++i, arg));
                        break;
                    case "--corpus":
                        options.corpus = new File(next(args, ++i, arg));
                        break;
                    case "--report":
                        options.reportFile = next(args, ++i, arg);
                        break;
                    case "--csv":
                        options.csvFile = next(args, ++i, arg);
                        break;
                    case "--size":
                        options.targetBytes = parseBytes(next(args, ++i, arg));
                        break;
                    case "--partition-size":
                        options.partitionSize = parseBytes(next(args, ++i, arg));
                        break;
                    case "--value-size":
                        options.valueSize = parseBytes(next(args, ++i, arg));
                        break;
                    case "--column-index-size":
                        options.columnIndexSize = parseBytes(next(args, ++i, arg));
                        break;
                    case "--chunk-kb":
                        options.chunkKb = Integer.parseInt(next(args, ++i, arg));
                        break;
                    case "--compressor":
                        options.compressor = next(args, ++i, arg);
                        break;
                    case "--payload":
                        options.payload = next(args, ++i, arg);
                        break;
                    case "--key-type":
                        options.keyType = next(args, ++i, arg);
                        break;
                    case "--jitter":
                        options.jitterDenominator = Integer.parseInt(next(args, ++i, arg));
                        break;
                    case "--regenerate":
                        options.regenerate = true;
                        break;
                    case "--parent":
                        options.parentDataFile = next(args, ++i, arg);
                        break;
                    case "--schema":
                        options.externalSchema = next(args, ++i, arg);
                        break;
                    case "--schema-file":
                        options.externalSchema = readFile(next(args, ++i, arg));
                        break;
                    case "--children":
                        options.children = Integer.parseInt(next(args, ++i, arg));
                        break;
                    case "--iterations":
                        options.iterations = Integer.parseInt(next(args, ++i, arg));
                        break;
                    case "--paths":
                        options.paths = Arrays.asList(next(args, ++i, arg).split(","));
                        break;
                    case "--evict":
                        options.evict = !"none".equals(next(args, ++i, arg));
                        break;
                    case "--disk-access-mode":
                        options.diskAccessMode = next(args, ++i, arg);
                        break;
                    case "--verify":
                        options.verify = next(args, ++i, arg);
                        break;
                    case "--reflink":
                        options.reflink = !"off".equals(next(args, ++i, arg));
                        break;
                    case "--digest":
                        options.digest = !"off".equals(next(args, ++i, arg));
                        break;
                    default:
                        throw new IllegalArgumentException("unknown option " + arg);
                }
            }
            options.resolveDefaults();
            return options;
        }

        private static String next(String[] args, int i, String option)
        {
            if (i >= args.length)
                throw new IllegalArgumentException(option + " needs a value");
            return args[i];
        }

        private static String readFile(String path)
        {
            try
            {
                return new String(Files.readAllBytes(Paths.get(path)), java.nio.charset.StandardCharsets.UTF_8);
            }
            catch (IOException e)
            {
                throw new IllegalArgumentException("could not read " + path + ": " + e.getMessage());
            }
        }

        /** Accepts a plain byte count or a suffixed one: {@code 512MiB}, {@code 4G}, {@code 1TB}. */
        static long parseBytes(String value)
        {
            String s = value.trim().toUpperCase(Locale.ROOT).replace("IB", "").replace("B", "");
            long multiplier = 1;
            if (s.endsWith("K"))
                multiplier = KIB;
            else if (s.endsWith("M"))
                multiplier = MIB;
            else if (s.endsWith("G"))
                multiplier = GIB;
            else if (s.endsWith("T"))
                multiplier = 1024 * GIB;
            if (multiplier != 1)
                s = s.substring(0, s.length() - 1);
            try
            {
                return (long) (Double.parseDouble(s.trim()) * multiplier);
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("cannot parse '" + value + "' as a byte size");
            }
        }

        private static String compact(long value)
        {
            if (value % (1024 * GIB) == 0)
                return (value / (1024 * GIB)) + "TiB";
            if (value % GIB == 0)
                return (value / GIB) + "GiB";
            if (value % MIB == 0)
                return (value / MIB) + "MiB";
            return value + "B";
        }
    }

    private static void usage()
    {
        PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true);
        out.println("ZeroCopySSTableSplitter large-scale benchmark");
        out.println();
        out.println("  java -Xmx8G -jar zerocopy-split-bench.jar [options]");
        out.println();
        out.println("Where the work happens");
        out.println("  --scratch DIR          root for all Cassandra directories and for produced children.");
        out.println("                         Nothing is written outside it. Default: ./zerocopy-split-bench");
        out.println("  --corpus DIR           where the reusable generated parent lives. Default: <scratch>/corpus");
        out.println("  --report FILE          append the report here too. Default: <scratch>/report.txt");
        out.println("  --csv FILE             append one machine-readable row per measurement, for plotting");
        out.println();
        out.println("Generating a parent (the default mode; generated once, reused forever after)");
        out.println("  --size N               logical size of the parent, e.g. 512MiB, 64GiB, 2TiB. Default 4GiB");
        out.println("  --partition-size N     target bytes per partition. Default 64KiB");
        out.println("  --value-size N         bytes per row value. Default 4KiB. Raise it for very large");
        out.println("                         corpora: it is the row count, not the byte count, that makes");
        out.println("                         generation slow");
        out.println("  --column-index-size N  column_index_size, i.e. when partitions get a promoted index.");
        out.println("                         Default 64KiB");
        out.println("  --chunk-kb N           compression chunk_length_in_kb, power of two. Default 16");
        out.println("  --compressor NAME      LZ4Compressor (default), SnappyCompressor, ZstdCompressor, ...");
        out.println("  --key-type T           blob (default): one blob partition key + int clustering, made in");
        out.println("                         token order by inverting murmur3, so size is bounded by disk only.");
        out.println("                         uuid: PRIMARY KEY ((k1 uuid, k2 uuid), c uuid) -- the shape that");
        out.println("                         costs the rewrite path most, since it materialises a composite");
        out.println("                         key and a UUID clustering per row. Needs ~16 bytes of heap per");
        out.println("                         partition while generating (a token sort; murmur3 cannot be");
        out.println("                         inverted for a 38-byte composite key)");
        out.println("  --payload MODE         random (default, incompressible) or compressible");
        out.println("  --jitter N             vary row values by up to 1/N so partition offsets decorrelate");
        out.println("                         from the chunk grid. Default 16. Do not set 0 unless you want");
        out.println("                         every child exactly chunk aligned, which no real table is");
        out.println("  --regenerate           throw away a matching corpus and build it again");
        out.println();
        out.println("Splitting an sstable that already exists (point this at a snapshot of a real table)");
        out.println("  --parent PATH          .../nb-1234-big-Data.db to split. Only read and hard linked");
        out.println("  --schema-file FILE     the CREATE TABLE for it, from cqlsh DESCRIBE TABLE. Required");
        out.println("  --schema CQL           the same thing inline");
        out.println();
        out.println("What to measure");
        out.println("  --children N           number of children to split into. Default 4");
        out.println("  --iterations N         repeat each path N times and report the median. Default 1");
        out.println("  --paths LIST           zerocopy,baseline (default) -- drop baseline for a huge parent,");
        out.println("                         a full rewrite of a TiB takes hours");
        out.println("  --evict MODE           fadvise (default) drops page cache before each timed run, none");
        out.println("                         leaves it warm");
        out.println("  --disk-access-mode M   mmap_index_only (default), mmap, standard or auto. The default is");
        out.println("                         deliberate: posix_fadvise cannot evict mapped pages, so mmap");
        out.println("                         would quietly make every run after the first a warm one");
        out.println("  --verify MODE          keys (default, one Index.db pass), rows (full scan, slow) or none");
        out.println("  --reflink MODE         on (default) lets the splitter share each child's Data.db extents");
        out.println("                         with the parent via FICLONERANGE, which needs xfs -m reflink=1 or");
        out.println("                         btrfs and is a silent no-op anywhere else; off forces the copy.");
        out.println("                         Run both against one corpus to measure what the sharing is worth");
        out.println("  --digest MODE          on (default) writes Digest.crc32 per child, which costs one full");
        out.println("                         sequential read of every child and is the entire remaining cost of");
        out.println("                         a split once --reflink on has removed the copy; off skips it, which");
        out.println("                         nothing requires but which makes nodetool verify and");
        out.println("                         import --verify-sstables fall back to a full extended verification");
        out.println();
        out.println("Notes");
        out.println("  Budget 2x the parent size of free space under --scratch, and about 12 bytes of heap per");
        out.println("  partition for the splitter's Index.db pass -- both are checked before any work starts.");
        out.println("  Cold-cache measurement needs Linux (posix_fadvise + /proc/self/io); elsewhere the tool");
        out.println("  runs but says its numbers are warm.");
        out.flush();
    }
}
