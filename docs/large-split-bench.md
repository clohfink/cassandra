# Large-scale zero-copy split benchmark

`ZeroCopySSTableSplitterBenchTest` (in `test/long/`) compares `ZeroCopySSTableSplitter` against the existing
full-rewrite split on 16–512 MiB parents. Those all fit in page cache, so what it measures is CPU and
allocation, not I/O. This tool answers the question that decides whether the zero-copy path is worth shipping:
what happens on a multi-GiB or multi-TiB sstable that is **not** resident in memory.

- Harness and CLI: `test/long/org/apache/cassandra/io/sstable/LargeSSTableSplitBench.java`
- JUnit face (opt-in, small): `test/long/org/apache/cassandra/io/sstable/LargeSSTableSplitBenchTest.java`
- Packaging: the `zerocopy-split-bench-jar` target in `build.xml`

## Building and shipping it

```sh
ant zerocopy-split-bench-jar -Duse.jdk11=true
scp build/tools/lib/zerocopy-split-bench.jar host:/mnt/data/
```

One ~120 MB self-contained jar: Cassandra, every runtime dependency, and a logback config that prints warnings
to stderr and nothing else. It needs a JVM and nothing else — no Cassandra install, no config file, no startup
script. The module `--add-opens`/`--add-exports` grants that `bin/cassandra` normally passes on the command
line are baked into the manifest, which the JVM honours for the main jar of a `java -jar` launch.

```sh
java -Xmx8G -jar zerocopy-split-bench.jar --help
```

## The two modes

### Generate a synthetic parent (default)

```sh
java -Xmx8G -jar zerocopy-split-bench.jar --scratch /mnt/data/zcsplit --size 64GiB --children 8
```

The parent is written **once** into `<scratch>/corpus/<shape>/` and reused by every later invocation. The
corpus directory is keyed by the shape of the data (`64GiB-p65536-v4096-c16k-LZ4-random`), so several sizes and
chunk lengths can coexist and each is generated at most once, ever. Change `--children`, `--iterations`,
`--evict` or `--verify` freely; none of those regenerate anything. `--regenerate` forces a rebuild.

Two things make a terabyte-scale corpus possible:

- **The sorted sstable writer.** `CQLSSTableWriter.builder()...sorted()` (i.e. `SSTableSimpleWriter`) streams
  straight to disk and buffers nothing, unlike the unsorted writer, which accumulates in heap and spills.
- **Murmur3 run backwards.** The sorted writer requires strictly increasing decorated keys, which normally
  means generating the key set and sorting it — impossible at this scale. Instead the tool walks the token ring
  in even steps and asks `Murmur3Partitioner.LongToken.keyForToken(t)` (which runs
  `MurmurHash.inv_hash3_x64_128`) for a 16-byte key that hashes to `t`. The map from unsigned offsets to signed
  longs is order preserving, so evenly spaced tokens give pre-sorted keys with nothing held in memory.
  `LargeSSTableSplitBenchTest.murmur3InverseGivesSortedKeys` guards that property; it always runs.

Shape knobs: `--partition-size` (default 64 KiB), `--value-size` (4 KiB), `--chunk-kb` (16), `--compressor`
(LZ4Compressor), `--column-index-size` (64 KiB), `--payload` (`random`, i.e. incompressible, or
`compressible`).

**Raise `--value-size` for very large corpora.** Generation cost scales with the row count, not the byte count.
At 4 KiB rows a terabyte is 268 M rows; at 64 KiB rows it is 16 M and the CQL write path stops mattering next
to the disk.

### Split an sstable that already exists

This is how to point it at production-sized data:

```sh
java -Xmx16G -jar zerocopy-split-bench.jar --scratch /mnt/data/zcsplit \
     --parent /mnt/data/ks/tbl-abcd1234/nb-98765-big-Data.db \
     --schema-file tbl.cql --children 8
```

`--schema-file` is the `CREATE TABLE` out of `cqlsh DESCRIBE TABLE`; it has to define the same
`<keyspace>.<table>` the sstable's own directory names, since that is where `Descriptor` takes them from.

The supplied sstable is opened read-only and **hard linked** into the scratch tree. Each measured run gets its
own links, so the destructive baseline path — which obsoletes and deletes its parent — only ever unlinks a
link. Hard links do not cross filesystems, so `--scratch` has to be on the same filesystem as the parent.
Copying instead is deliberately not offered: at these sizes it would dominate the measurement.

Prefer a snapshot over a live sstable. This is a benchmark, not an operational tool.

## Reading the output

```
PATH         IT     WALL_S      MiB/s    RCHAR_MiB  RD_DISK_MiB    WCHAR_MiB  WR_DISK_MiB    W_AMP   KIDS   ALLOC_MiB
zerocopy      1       0.30      432.1        188.4          0.4        130.1        129.9    1.001      4         2.4
                  dead prefix 42 KiB across 4 children, 48 KiB duplicated by boundary chunks
                  0 B shared as extents (0.0% of the data), 0 B alignment pad
                  verified 2,048 partitions / 32,768 rows in 0.1s
```

- `RCHAR`/`WCHAR` are bytes moved by read/write **syscalls**, so they include page-cache hits.
  `RD_DISK`/`WR_DISK` (`read_bytes`/`write_bytes` from `/proc/self/io`) are bytes that actually crossed the
  block layer. The pair is what distinguishes "this algorithm touched less data" from "the second run found it
  in cache". Both columns are blank off Linux.
- `W_AMP` is exact: the summed on-disk length of every component of every produced child, **minus anything
  shared with the parent instead of written**, over the parent's on-disk length. A shared extent is a refcount
  update, so counting it as a write would put this column in direct contradiction with `WR_DISK_MiB`.
- `ALLOC_MiB` is `com.sun.management.ThreadMXBean` for the calling thread. Both paths do their work there, but
  allocation on Cassandra's shared executors is counted for neither.
- Wall clock covers the split call only. Corpus construction, linking, eviction and verification are all
  outside it.
- `shared as extents` is how many of the children's Data.db bytes they point at in the parent instead of holding
  a copy of (`Result.totalBytesCloned`). It is `0 B` unless `--scratch` is on a filesystem that can share
  extents, which is the interesting case and the one below.

## Extent sharing (reflink), and how to measure it

On xfs formatted with `-m reflink=1` (the mkfs default since xfsprogs 5.1) or on btrfs, the splitter does not
copy a child's Data.db at all: it hands the range to `FICLONERANGE`, the filesystem bumps the reference count
on the parent's extents, and no data block is read, written or allocated. `WR_DISK_MiB` and `W_AMP` collapse to
the index and metadata, and the split stops needing room for a second copy of the sstable.

There are two independent costs to remove, and `--reflink` only removes one of them, so the A/B is really a 2x2.
`--digest off` skips `Digest.crc32`, which is the one component whose cost is proportional to the data rather
than to the index; with the extents shared it is the *entire* remaining cost of a split. Run all four against one
corpus:

```sh
S="--scratch /mnt/xfs/zcsplit --size 512GiB --children 8 --paths zerocopy"
java -Xmx8G -jar zerocopy-split-bench.jar $S --reflink off --digest on    # today
java -Xmx8G -jar zerocopy-split-bench.jar $S --reflink on  --digest on    # extents shared
java -Xmx8G -jar zerocopy-split-bench.jar $S --reflink on  --digest off   # the floor
java -Xmx8G -jar zerocopy-split-bench.jar $S --reflink off --digest off   # isolates the digest
```

Measured on an xfs `-m reflink=1` loop mount (`bsize=4096`) inside a privileged container, 1 GiB parent of 16 KiB
values, split 4 ways, `--evict fadvise`, `disk_access_mode: mmap_index_only`. The byte counters are exact; treat
the wall clock as indicative only, since a loop-mounted image inside a VM is not a disk:

| | copy + digest | shared + digest | **shared, no digest** | copy, no digest |
|---|---|---|---|---|
| WALL_S | 1.05 | 0.35 | **0.10** | 0.54 |
| RCHAR_MiB (read syscalls) | 1998.4 | 1000.2 | **1.9** | 1000.1 |
| RD_DISK_MiB | 998.7 | 998.8 | **0.4** | 749.1 |
| WCHAR_MiB (write syscalls) | 999.2 | 1.1 | **1.0** | 999.2 |
| WR_DISK_MiB | 749.7 | 1.0 | **1.0** | 749.8 |
| W_AMP | 1.001 | 0.001 | **0.001** | 1.001 |
| ALLOC_MiB | 11.0 | 11.2 | 10.9 | 10.8 |
| shared as extents | 0 B | 998.3 of 998.4 MiB (100%) | same, 57 KiB pad | 0 B |
| `df` growth while parent + children coexist | 819 MiB | **0.6 MiB** | **0.6 MiB** | 819 MiB |

The two columns in the middle are the point. Sharing the extents takes the writes to zero but leaves `RD_DISK`
untouched, because the digest still reads every byte of every child — so column 2 is a one-pass read where
column 1 was a read plus a write. Dropping the digest as well leaves nothing that touches the data at all:
**1.9 MiB read and 1.0 MiB written to split a 1 GiB sstable**, which is the Index.db pass and the metadata. The
last column isolates the digest against the copy, and shows it is worth about as much as the copy is.

What to expect, and what to check:

- `shared as extents` should be ~100% of the data. If it is `0 B`, the filesystem refused: the splitter logs
  the errno once per data directory at INFO (`EOPNOTSUPP` / `ENOTTY` means no reflink support, `EXDEV` means
  the scratch directory and the data directory are different mounts). Check with
  `xfs_info /mnt/xfs | grep reflink`.
- `alignment pad` is the price: up to 64 KiB per child, because `FICLONERANGE` needs block-aligned offsets and
  a compression chunk boundary is aligned to nothing. It is on disk and it is in the digest.
- Wall clock will **not** fall by the same factor as the writes on `--reflink on` alone, because the copy is no
  longer what dominates. Add `--digest off` for that.
- `--digest off` is not free elsewhere: nothing requires the component, but `nodetool verify` and `nodetool
  import --verify-sstables` answer a missing digest by running a full extended verification instead of a
  whole-file CRC. The `verified N partitions` line this tool prints is its own check and is unaffected.
- `--evict fadvise` matters more here, not less. With a warm cache the copy is cheap too and the comparison
  says nothing.
- `du` on the scratch directory will over-report while both parent and children exist; `df` is the honest one.

## Cold cache

Every component of the parent is evicted with `posix_fadvise(POSIX_FADV_DONTNEED)` before each timed run
(`--evict none` to leave it warm). Three details make that actually work, and they are the reason the numbers
mean something:

- The corpus is fsynced when written. `DONTNEED` only drops *clean* pages.
- Eviction happens after the hard links are made but **before** the `SSTableReader` is opened. `DONTNEED` will
  not drop pages that are currently mapped, and `SSTableReader` mmaps Index.db.
- `disk_access_mode` is pinned to `mmap_index_only`, not Cassandra's `auto` (which resolves to `mmap` on a
  64-bit JVM). A mapped Data.db could not be evicted at all, and every run after the first would quietly be a
  warm one. `--disk-access-mode mmap` if you want to measure that case anyway; the preamble says loudly when
  the combination is self-defeating.

This all needs Linux. Elsewhere the tool runs and reports that its numbers are warm.

## Correctness

Never timings — a benchmark that fails on a slow box is a liability. But a benchmark that reports a throughput
number for an algorithm that lost data is worse, so after each split, outside the timed region:

- `--verify keys` (default): the children's partition keys must be exactly the parent's — count plus the XOR
  and the sum of a 64-bit hash of every key, read straight out of Index.db. One sequential pass over ~0.05% of
  the data; a dropped, duplicated or corrupted key changes it. The parent's signature is cached in the corpus
  manifest, so reusing a corpus does not rescan it.
- `--verify rows`: additionally scans every child in full and compares the row count. Correct at any size, but
  it reads everything twice; the JUnit test uses it, the jar defaults to keys.
- `--verify none`: skip it.

## Before it starts

Both are checked up front rather than three hours in:

- **Disk.** The persistent corpus, plus room for one full set of children per run — budget 2× the parent size
  under `--scratch`.
- **Heap.** Both of the splitter's Index.db passes stream, so heap scales with the largest *child*, not with
  the parent: a few bytes per partition of `P / --children`, for that child's bloom filter and index summary.
  A 1 TiB parent of 1 KiB partitions split 8 ways needs ~500 MiB. Raise `-Xmx`, raise `--children`, or raise
  `--partition-size`.

  This used to be the binding constraint on how large an sstable could be split at all: the splitter
  materialised one `long` per partition of the *parent* for its first Index.db pass, 16–24 bytes per partition
  once the doubling and the final trim are counted, so a terabyte of 1 KiB partitions wanted ~26 GiB of heap.
  Every access to that array turned out to be sequential, so it is gone — see the note on `RunSelector` in
  `ZeroCopySSTableSplitter`. The byte-share entry point now takes three sequential passes over Index.db instead
  of two (count, select, build); the boundary entry point that anticompaction uses still takes two.

## Safety

Every Cassandra directory — data, commitlog, hints, saved caches — is pinned under `--scratch` by a
`cassandra.yaml` the tool generates there itself. It deliberately does not look for an existing config:
a node's own yaml names its **live** data directories explicitly, which would override `cassandra.storagedir`
and have the benchmark write children into production data. After initialisation it re-checks every configured
directory and refuses to run if any of them escaped `--scratch`.

## Running it as a test

```sh
ant long-testsome -Duse.jdk11=true \
    -Dtest.name=org.apache.cassandra.io.sstable.LargeSSTableSplitBenchTest \
    -Dtest.jvm.args="-Dcassandra.test.zerocopysplit.largescale=true"
```

Opt-in, and 512 MiB by default (`-Dcassandra.test.zerocopysplit.largescale.size=...`). It drives the same
harness the jar does, with `--verify rows`. Anything past a couple of GiB will outrun the 10-minute
`test.long.timeout`; at that point use the jar, which is what it is for. Its corpus lives in
`build/test/zerocopy-split-largescale/` and survives between runs — `ant clean` removes it.

## Known fidelity notes

- `AbstractSSTableSimpleWriter` cannot be told how many partitions are coming, so it passes a key count of 0
  and the parent would land with a 16-byte `AlwaysPresent` Filter.db. The tool deletes that stub and reopens
  the sstable online, which makes `SSTableReaderBuilder` rebuild the filter from Index.db at the table's
  configured fp chance and persist it. The corpus is a faithful sstable; `sstabledump` and `sstablemetadata`
  work on it directly.
- The baseline is a real `SSTableSplitter.SplittingCompactionTask` against a real `ColumnFamilyStore` and a
  real `LifecycleTransaction`, not a reimplementation. `compaction_throughput` is pinned to 0, otherwise the
  measurement would be of the throttle. `MaxSSTableSizeWriter` rolls over on estimated on-disk bytes, so the
  baseline often produces one more child than asked for; that is the real behaviour, not an artifact.
