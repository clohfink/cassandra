# Efficient SSTable splits (BIG format, 4.1 / import-groups)

Scope: `src/java/org/apache/cassandra/io/sstable/format/big/**`, the compression layer, and every
consumer that touches a Data.db byte offset. Everything below is read out of *this* tree. Where a
claim could not be verified in code it is marked **UNVERIFIED**. Benchmark figures are labelled as
estimates and their harness is named.

---

## 1. Bottom line up front

- **Q1 (copy only the bytes we need): yes, and the right unit is the compression chunk run.** The
  uncompressed chunk grid is implicit — `chunkFor()` is `position / chunkLength` integer division
  (`src/java/org/apache/cassandra/io/compress/CompressionMetadata.java:237`) with **no per-chunk
  uncompressed length anywhere on disk**. So you cannot re-encode a short head chunk; you copy the
  chunk run `[i..j]` verbatim and accept `lo mod L` dead bytes at the head. Confidence: **high** —
  the write layout (`CompressionMetadata.java:357-379`), read layout (`:98-124`), and
  `Writer.open`'s `tCount = ceil(dataLength/chunkLength)` (`:424-439`) all agree that only the last
  chunk may be short.
- **Q2 (rebuild components without reading data): yes for PRIMARY_INDEX, FILTER, SUMMARY,
  COMPRESSION_INFO, TOC, and a usable STATS.** One sequential pass over the parent's Index.db is
  sufficient, and production already does exactly this pass —
  `SSTableReaderBuilder.buildSummaryAndBloomFilter` rebuilds the filter, the summary, and
  `first`/`last` from Index.db alone with no Data.db handle in the loop
  (`SSTableReaderBuilder.java:197-218`). Confidence: **high**.
- **Q3 (if we must scan, is it better than today): yes, but the wrong stage is being feared.** A
  forced *decompress* scan is the cheap direction; a rewrite additionally pays per-cell deserialize,
  per-cell re-serialize, and LZ4 *compress*. Even a full decompress-only scan is materially cheaper
  than today's rewrite. Confidence: **medium** — the stage ordering is robust, the specific
  percentages are single-laptop benchmark estimates and the S0 baseline was assembled from
  independently-timed stages rather than measured end to end.
- **The recommended first step is not the design that was scoped.** Build the **prefix-only case**
  (`i == 0`): copy chunks `[0..j]`, patch `dataLength` and `chunkCount`, copy the Index.db byte
  prefix verbatim. It needs **zero rebasing**, **zero core-class patches**, keeps the first index
  position at 0, and — critically — **preserves entire-SSTable zero-copy streaming eligibility**.
  It is a strict subset of the general design and is the whole feature for a suffix-discard trim.
- **New finding: the general (suffix) form silently disables entire-SSTable zero-copy streaming for
  every non-chunk-aligned child.** `contained()` tests
  `transferLength == sstable.uncompressedLength()` (`CassandraOutgoingFile.java:200-201`); for a
  suffix child `left = lo mod L > 0` so the sum is short and ZCS is refused permanently until the
  child is compacted. This was missed in the original analysis and is a real regression in
  bootstrap/replace/rebuild. Confidence: **high** (arithmetic over verified code, not measured).
- **Hard-link / shared-inode variants should be dropped as designed.** Four independent blockers,
  not one: they over-stream the *entire parent* under ZCS (`ComponentManifest.create` sizes
  components by `file.length()`, `ComponentManifest.java:59-63`); `onDiskLength()` is unfixably the
  whole parent file; incremental backup hard-links the shared inode again at birth
  (`Tracker.java:409-419`), pinning it; and `Files.createLink` cannot cross filesystems
  (`FileUtils.java:182`), so they cannot split a disk-boundary-straddling SSTable — which is the only
  population anticompaction needs.
- **"Exactly two lines in Scrubber and Verifier" is wrong.** `Scrubber` needs a third correlated
  edit: `nextPartitionPositionFromIndex` is initialised to 0 (`Scrubber.java:149-150`) and feeds
  `dataStartFromIndex`/`dataSizeFromIndex` (`:226-227`); without seeding it, the error-recovery
  branch at `:261-271` seeks into a *sibling child's* bytes and appends whatever deserialises there
  under this child's first key. That is silent mis-attribution, the one failure mode this design
  cannot have.
- **This is a CPU/GC win, not a write-amplification win.** S1/S2/S3 write the same bytes as S0 plus
  one duplicated boundary chunk per split point (~0.01–0.03% at a 16 KiB chunk and a 50 MB target).
  Only zero-byte-movement changes the bytes-written picture, and that option is blocked above. Do
  not sell this as SSD endurance.
- **The addressable population for anticompaction is unmeasured and probably small.**
  `mutateFullyContainedSSTables` already metadata-mutates every fully-contained SSTable with zero
  byte movement and zero throttle (`CompactionManager.java:842-863`, `:945-971`), and cleanup drops
  non-intersecting SSTables without reading them. Only *boundary-straddling* SSTables are in scope.
  Size that population before building anything for anticompaction.
- **Inherited STATS is defensible because it already ships.** Entire-SSTable streaming transplants
  the sender's VALIDATION/COMPACTION/STATS/HEADER wholesale and overrides only level and repair
  state (`CassandraEntireSSTableStreamReader.java:137-141`). Caveat, from adversarial review: that
  path is gated on a whole-file copy, so it is prior art for *transplant*, not for attaching
  row-derived stats to a key-range subset. Each field still needs its own argument (§4.3).

---

## 2. How splitting works today, and where the cost is

### 2.1 Splitting is literally a compaction

`SSTableSplitter` is a thin wrapper over a `CompactionTask` subclass:

```java
super(cfs, transaction, CompactionManager.NO_GC, false);
```
(`src/java/org/apache/cassandra/db/compaction/SSTableSplitter.java:49`; `NO_GC = Integer.MIN_VALUE`
at `src/java/org/apache/cassandra/db/compaction/CompactionManager.java:144`.) The output writer is
`MaxSSTableSizeWriter` (`SSTableSplitter.java:68`) and the boundary decision is one comparison per
partition against `getEstimatedOnDiskBytesWritten()`
(`src/java/org/apache/cassandra/db/compaction/writers/MaxSSTableSizeWriter.java:101`).

So the split *decision* is O(1) per partition and could be made from Index.db alone. All of the cost
is in the copy that follows it.

### 2.2 The per-byte pipeline a rewrite pays

Read side, per byte: `channel.read` → CRC32 over the compressed bytes (on by default,
`crc_check_chance = 1.0`, `src/java/org/apache/cassandra/schema/CompressionParams.java:99`) → LZ4
`safeDecompressor` (`src/java/org/apache/cassandra/io/util/CompressedChunkReader.java:257-266`).

Read side, per cell: `UnfilteredSerializer.deserialize` allocates one `Cell` object and one `byte[]`
per cell (`src/java/org/apache/cassandra/db/rows/UnfilteredSerializer.java:645`) and builds a
`BTreeRow` per row.

Write side, per row: the row body is serialised into a thread-local scratch buffer and then
`memcpy`'d into the data-file buffer so the size vint can be written first
(`UnfilteredSerializer.java:190-201`).

Write side, per byte: LZ4 `fastCompressor`, then two CRC32 passes — the inline per-chunk CRC and the
running whole-file digest (`src/java/org/apache/cassandra/io/compress/CompressedSequentialWriter.java:178`
`addOffset`, `:183` `channel.write`, `:187` `crcMetadata.appendDirect(toWrite, true)`, `:198`
`chunkOffset += compressedLength + 4`).

Write side, per cell again: a `StatsCollector` transformation is wrapped around every partition
(`src/java/org/apache/cassandra/io/sstable/format/big/BigTableWriter.java:220`), so every cell also
pays `MetadataCollector.update(Cell)`.

And every output SSTable rebuilds all nine components from scratch, including the promoted index
(one `IndexInfo` per `column_index_size` = 64 KiB, `Config.java:396`), a fresh `OffHeapBitSet`, a
fresh summary, and a fresh HLL.

### 2.3 What today's split does *not* do — and this matters

`sstablessplit` performs **no tombstone GC and no shadowed-data filtering**. Two independent reasons:

1. `SplitController.getPurgeEvaluator` returns `time -> false` (`SSTableSplitter.java:88`), which
   short-circuits `PurgeFunction`'s predicate regardless of `gcBefore` or the
   `ignoreGcGraceSeconds` bypass.
2. `StandaloneSplitter` opens one `LifecycleTransaction` per SSTable
   (`src/java/org/apache/cassandra/tools/StandaloneSplitter.java:155-159`), so there is a single
   scanner and `MergeIterator` selects `TrivialOneToOne` — rows pass through unreconciled. Note this
   is a property of *how the tool is invoked*, not of `SSTableSplitter`: with two or more inputs you
   get `ManyToOne` and real reconciliation even with a null row-merge listener.

The one thing the rewrite still reclaims: `AbstractCell.purge` converts expired-but-unpurgeable TTL
cells into value-less tombstones (`src/java/org/apache/cassandra/db/rows/AbstractCell.java:78-96`).
On a TTL-heavy table a byte copy forgoes that.

Cleanup and anticompaction are different: both build controllers with a real `gcBefore`
(`CompactionManager.java:1441` cleanup, `:1796` anticompaction), so replacing them with a copy
genuinely defers reclamation. Also note `nodetool compact -s` uses an unrelated class also named
`SplittingCompactionTask` (in `SizeTieredCompactionStrategy`) built with a real `gcBefore` — "split"
in this document means the offline tool and the cleanup/anticompaction range-trim paths only.

### 2.4 Where the wall clock actually goes

The compaction rate limiter meters **bytes scanned scaled by the compression ratio**, i.e.
approximately on-disk bytes:

```java
long lengthRead = (long) ((bytesScanned - lastBytesScanned) * compressionRatio) + 1;
```
(`CompactionManager.java:1508`, invoked from cleanup at `:1462` and anticompaction at `:1834`.) The
default is 64 MiB/s (`Config.java:407`, `conf/cassandra.yaml`), and the limiter is a single
node-wide instance shared with background compaction, scrub, and verify
(`CompactionManager.java:168`), disabled only when throughput is 0 or during bootstrap (`:207-209`).

Consequence: at defaults, cleanup and anticompaction of a compressed table are throttle-bound at
roughly `compressionRatio × 16` seconds per uncompressed GiB — ~10 s/GiB at the ~0.6 ratios we
measured, 16 s/GiB uncompressed, ~4 s/GiB at ratio 0.25. Removing CPU does not shorten wall clock
unless the copy path is given a separate, higher bandwidth budget, which means deliberately
exempting it from an I/O-protection mechanism. **The honest pitch for a copy-based split is CPU and
GC headroom returned to the read path, not latency.** (Confidence: **medium-high**; the code is
verified, the "is production actually at 64 MiB/s" premise is not — see §7.)

---

## 3. Q1 — copying only the parts of Data.db we need

### 3.1 The chunk-alignment constraint, stated precisely

Three facts decide everything.

**(a) The uncompressed chunk grid is implicit and rigid.**

```java
long idx = 8 * (position / parameters.chunkLength());
if (idx >= chunkOffsetsSize) throw new CorruptSSTableException(new EOFException(), indexFilePath);
long chunkOffset = chunkOffsets.getLong(idx);
long nextChunkOffset = (idx + 8 == chunkOffsetsSize) ? compressedFileLength : chunkOffsets.getLong(idx + 8);
return new Chunk(chunkOffset, (int) (nextChunkOffset - chunkOffset - 4)); // "4" bytes reserved for checksum
```
(`CompressionMetadata.java:237-251`.)

CompressionInfo.db contains only: compressor simple name, option count + k/v pairs, `chunkLength`
(i32), `maxCompressedLength` (i32, gated on `desc.version.hasMaxCompressedLength()` at `:90`),
`dataLength` (i64), `chunkCount` (i32), then `chunkCount` absolute i64 offsets — write side
`:357-379`, read side `:98-124`. There is **no per-chunk uncompressed-length table**. The uncompressed
extent of chunk *k* is therefore *defined by arithmetic*: `[k·L, min((k+1)·L, dataLength))` where
`L = chunkLength`. `Writer.open` independently confirms it, recomputing
`tCount = ceil(dataLength / chunkLength)` (`:429-431`) — only the **last** chunk may be short.

**(b) The reader always aligns down.** `BufferManagingRebufferer.Aligned.alignedPosition` is
`position & -buffer.capacity()` (`src/java/org/apache/cassandra/io/util/BufferManagingRebufferer.java:139`),
capacity is `metadata.chunkLength()` (`CompressedChunkReader.java:76-79`), compressed files *always*
get the Aligned rebufferer (`CompressedChunkReader.java:88-91`), and both `readChunk`
implementations re-assert `(position & -uncompressed.capacity()) == position`
(`CompressedChunkReader.java:250` standard, `:324` mmap; assertions are live in production —
`conf/jvm-server.options:119` sets `-ea`).

**(c) The physical offsets are fully explicit and rebaseable.** The offsets array is read verbatim
(`CompressionMetadata.java:206-209`) and used verbatim as file offsets. Nothing else in the file is
offset-dependent.

**So: the uncompressed origin is pinned to a multiple of `L`; the physical origin is free.**

### 3.2 The two candidate approaches, and why (b) is the only one

**Option (a) — re-encode the boundary chunk as a short chunk 0. Fatal.** Suppose the split point is
`Poff` with `r = Poff mod L ≠ 0`, and you build the child's chunk 0 from only the `K = L − r` tail
bytes of source chunk `i = Poff / L`, then copy chunks `i+1..j` verbatim.

The child's chunk 0 now holds uncompressed `[0, K)`. But `chunkFor()` maps *all* of `[0, L)` to
chunk 0 unconditionally, so uncompressed positions `[K, L)` are **structurally unaddressable** and
every following byte is displaced by `r`:

- A read at position strictly inside `(K, L)` reaches
  `buffer.position(Ints.checkedCast(position - bufferHolder.offset()))`
  (`src/java/org/apache/cassandra/io/util/RandomAccessReader.java:69`) with `position > limit` →
  `IllegalArgumentException`, not even a `CorruptSSTableException`.
- A sequential read arriving at exactly `K` makes no forward progress: `reBuffer()` returns early
  only on EOF, `isEOF()` is false because `length()` is `dataLength > K`
  (`CompressedChunkReader.java:42`, `RandomAccessReader.java:139-142`, `:274-277`), so it re-aligns
  to 0 and hands back the same short buffer. `RebufferingInputStream` then yields `-1`, an
  `EOFException`, or a silently short `read(byte[],int,int)`.
- `dataLength` also becomes self-inconsistent: the reader's addressing requires
  `dataLength == (chunkCount−1)·L + lastChunkLen`, so writing the true (smaller) sum makes the tail
  unreachable and can drop the final offset from `Writer.open`'s `tCount`.

The codebase says this in prose: *"Otherwise it will leave a non-uniform size compressed block in
the middle of the file / and the compressed format can't handle that"*
(`src/java/org/apache/cassandra/io/util/SequentialWriter.java:197-201`). Consistently,
`CompressedSequentialWriter.flush()` throws `UnsupportedOperationException` (`:129-132`), and
`resetAndTruncate` deliberately re-reads, re-decompresses and *undoes* a partial chunk, truncating
the data file at that chunk's **start** and rolling the offsets array back (`:229-262`, plus
`CompressionMetadata.Writer.resetAndTruncate` = `count = chunkIndex` at `:456-462`). There is no
mechanism anywhere to split a chunk in place.

Precision note: the invariant is "at least", not "exactly" — the writer may zero-pad an
incompressible **final** chunk up to `maxCompressedLength`
(`CompressedSequentialWriter.java:154-171`, with its own comment *"this path is only reached at the
end of the file, where we use the file size to limit the buffer on reading"*). Too *few* bytes in any
chunk is what is fatal.

There is also a variant **(a′)** — decompress and recompress the *entire* tail so the whole
uncompressed stream shifts by `r`. It is correct, it eliminates the dead prefix, and it still skips
per-cell deserialize/serialize. But it produces entirely new compressed bytes, so no byte can be
shared or moved verbatim, every offset changes, and it pays LZ4 *compress* over the whole file — the
single most expensive stage. Strictly dominated by (b) unless the Scrubber/Verifier patch is
unacceptable.

Finally, note option (a) **degenerates into (b)**: make chunk 0 hold the full `L` bytes and its
content is byte-identical in meaning to source chunk `i`, so recompressing it is pure waste. **(b) is
the optimum of the family, not a compromise.**

**Option (b) — chunk-aligned verbatim copy of the chunk run.** For an output covering uncompressed
`[lo, hi)` (both partition boundaries taken from Index.db; `hi = cm.dataLength` for the last output):

```
i = lo / L                    // first chunk needed  (floor)
j = (hi - 1) / L              // last chunk needed   -- NOT hi/L
                              // equivalently: j = hi/L; if (hi % L == 0) j--;
```

That `−1` is the same adjustment the tree already makes:

```java
int endIndex = (int) (section.upperPosition / parameters.chunkLength());
if (section.upperPosition % parameters.chunkLength() == 0)
    endIndex--;
```
(`CompressionMetadata.java:266-268` in `getTotalSizeForSections`, `:300-302` in
`getChunksForSections`.)

Then:

| quantity | value |
|---|---|
| `C` (new `chunkCount`) | `j − i + 1` (≥ 1, since `lo < hi ⇒ i ≤ j`) |
| byte range to copy | `[O[i], O[j+1])`, contiguous |
| `F'` (new **physical** length) | `O[j+1] − O[i]`, must be exact |
| new offsets | `O'[k] = O[i+k] − O[i]`, so `O'[0] == 0` |
| `D'` (new `dataLength`) | `hi − i·L` |
| Index.db rebase | every `RowIndexEntry.position` shifts by `−(i·L)` |
| dead head bytes | `lo mod L`, in `[0, L)` |

Invariants worth asserting at build time: `(C−1)·L < D' ≤ C·L`. The left inequality holds *because*
of the `(hi−1)/L` form and is what guarantees `MmappedRegions` maps every copied chunk. The last
chunk's derived length is `F' − O'[C−1] − 4 = O[j+1] − O[j] − 4` — **identical to the source's**, so
the copied CRC lands exactly where the reader looks for it. No accessor is needed:
`O[k] = cm.chunkFor((long) k * L).offset`.

Waste, quantified: the left output's last chunk is `(hi−1)/L` and the right output's first chunk is
`hi/L`; these are equal iff `hi mod L ≠ 0`, so **exactly one duplicated chunk per non-aligned
boundary**. At the default `L = 16 KiB` (`CompressionParams.java:59`) and a 50 MB target output that
is ~0.01–0.03% extra physical bytes. Dead uncompressed bytes sum to exactly `L` per boundary (head of
the right output plus tail of the left), each uniform in `[0, L)`.

**The physical end must be exact.** `FileHandle.Builder.complete()` calls
`CompressionMetadata.create(channelCopy.filePath())` (`FileHandle.java:386-388`), and `create` is
`createWithLength(dataFilePath, new File(dataFilePath).length())` (`CompressionMetadata.java:77-80`).
So `compressedFileLength` is the *physical* file size and it is what the **last** chunk's length is
derived from. Get this wrong and the last chunk's length is inflated: with the default
`crc_check_chance` you get `CorruptBlockException` → `CorruptSSTableException`, and for an
incompressible last chunk you instead get an `IllegalArgumentException` from `ByteBuffer.limit` that
is *not* wrapped as a corruption exception. No trailing padding, no `fallocate`d tail; `ftruncate`
before the reader opens it.

**`dataLength` is the free lever for the tail.** The field is explicitly documented as allowed to be
short:

```java
// dataLength can represent either the true length of the file
// or some shorter value, in the case we want to impose a shorter limit on readers
// (when early opening, we want to ensure readers cannot read past fully written sections)
```
(`CompressionMetadata.java:56-58`.) It becomes `CompressedChunkReader`'s `fileLength` (`:42`) →
`FileHandle.dataLength()` (`FileHandle.java:98-101`) → `RandomAccessReader.length()`/`isEOF()` →
`SSTableReader.uncompressedLength()` (`SSTableReader.java:1468-1471`). Setting `D' = hi − i·L` — the
exact end of the last partition — hides all trailing dead bytes with **no code change**.

### 3.3 Which consumers see the dead head bytes

`grep -rn 'while (!dataFile.isEOF())' src/java` returns exactly two hits:
`Scrubber.java:196` and `Verifier.java:256`. Every partition-decoding consumer enters Data.db only
at a position obtained from Index.db: `AbstractSSTableIterator` does
`file.seek(indexEntry.position)` (`:96-98`), `BigTableScanner` does
`ifile.seek(sstable.getIndexScanPosition(position))` then `dfile.seek(dataPosition)` (`:187-210`) and
`dfile.seek(currentEntry.position)` (`:392`), bounded by `new Bounds<>(sstable.first, sstable.last)`
(`:139`); `SASIIndexBuilder` seeks `indexEntry.position` (`:96-98`); cleanup uses
`sstable.getScanner(rangesToScan)` (`CompactionManager.java:1580`). `SSTableReader.validate()` only
checks `first <= last` (`:1065-1071`) — nothing cross-checks Data.db length against the index.

So a leading dead prefix is never *interpreted as partition data* on the read, compaction, cleanup, or
repair-validation paths. **It is not, however, "unreachable" on the streaming path**, and the
original analysis overstated this. Three separate consumers read below the first index position:

- `CassandraStreamWriter` rounds each section start *down* to the CRC.db chunk boundary and
  CRC-validates the dead prefix: `long start = validator == null ? section.lowerPosition :
  validator.chunkStart(section.lowerPosition);` (`:98-102`), read at `:156`, validated at `:162`.
  So the prefix bytes must remain checksum-consistent — they cannot be zeroed or garbage.
- `CassandraCompressedStreamWriter` fuses whole chunks (`:70`, via
  `CompressionMetadata.getChunksForSections`) and transmits the entire first chunk including the dead
  prefix; the receiver skips it afterwards at `CassandraCompressedStreamReader.java:90`.
- `DataIntegrityMetadata.FileDigestValidator` digests the entire physical Data.db from byte 0
  (`:127`, `:143-154`), so `Digest.crc32` must cover exactly the copied range.

**And the finding nobody caught: the dead prefix costs you entire-SSTable zero-copy streaming.** The
ZCS decision is:

```java
long transferLength = sections.stream().mapToLong(p -> p.upperPosition - p.lowerPosition).sum();
return transferLength == sstable.uncompressedLength();
```
(`CassandraOutgoingFile.java:200-201`, gated at `:187-190`.) For a full-range request
`getPositionsForRanges` produces one section with
`left = getPosition(first.getToken().minKeyBound(), GT).position` and
`right = uncompressedLength()` (`SSTableReader.java:1312-1315`). For an ordinary SSTable `left == 0`,
so the identity holds and ZCS engages — that identity is *why* ZCS works today. For a suffix child
`left = lo mod L > 0`, so `transferLength = D' − (lo mod L) < D' = uncompressedLength()`,
`contained()` returns false, and **every non-chunk-aligned child permanently falls back to the
per-section compressed writer** until it is compacted. `dataLength` cannot fix this: the problem is at
the *left* edge, and `uncompressedLength()` is the right-edge clamp.

This is the strongest argument for the prefix-only recommendation: for `i == 0`, `left = 0`,
`right = D' = hi`, the identity holds, and ZCS is preserved.

### 3.4 The two things that actually break, and the real fix size

`Scrubber.java:187-188`:
```java
long firstRowPositionFromIndex = rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
assert firstRowPositionFromIndex == 0 : firstRowPositionFromIndex;
```
`Verifier.java:247-249` has the same check as an explicit
`markAndThrow(new RuntimeException("firstRowPositionFromIndex != 0: "+...))`, whose side effect is
resetting `repairedAt` to `UNREPAIRED_SSTABLE`.

The claimed fix — "replace the constant with a seek" — is **sufficient for Verifier and insufficient
for Scrubber**:

- `Verifier` derives its expected offset from the data pointer (`long dataStart =
  dataFile.getFilePointer(); long dataStartFromIndex = ... rowStart + 2 + currentIndexKey.remaining();`
  at `:303-306`), so it self-corrects. One-site change.
- `Scrubber` reads the first position into a deliberately throwaway local (`:186-188`, comment:
  *"throw away variable so we don't have a side effect in the assert"*) while the *field* it uses
  downstream, `nextPartitionPositionFromIndex`, is initialised to 0 (`:149-150`) and never seeded.
  `dataStartFromIndex = currentPartitionPositionFromIndex + 2 + currentIndexKey.remaining()` and
  `dataSizeFromIndex = nextPartitionPositionFromIndex - dataStartFromIndex` (`:226-227`) are
  therefore wrong by exactly the prefix length. That is not cosmetic: on any read error in the
  child's first partition, the recovery branch evaluates `dataStart != dataStartFromIndex` as true,
  takes the "Retrying from partition index" path, executes `dataFile.seek(dataStartFromIndex)`
  (`:271`) into the leading slack that belongs to a **sibling child's partition**, and calls
  `tryAppend(prevKey, key, writer)` — writing whatever deserialises there under this child's key.
  **Silent mis-attribution.** It also invalidates the "Impossible partition size" guard at `:246-247`.

Both sites are also reachable offline: `bin/sstablescrub:44` and `bin/sstableverify:44` pass `-ea`
explicitly. Two mitigating facts nobody stated: the `Verifier` site is only reached under
`nodetool verify -e` or when `Digest.crc32` is absent (`:208` quick-return, `:217-226`, `:238`); and
the default verify path instead whole-file-CRCs Data.db against DIGEST, which a regenerated digest
satisfies. But `SSTableImporter.verifySSTableForImport` runs `Verifier` with
`.extendedVerification(extendedVerify).quick(!verifySSTables)`
(`src/java/org/apache/cassandra/db/SSTableImporter.java:433-438`), so a split child imported through
`nodetool import` with `verifySSTables=true` **will** hit it. That is the fork's own workflow.

### 3.5 How the bytes actually move — three distinct mechanisms

These are routinely conflated. They are not the same thing.

| mechanism | what it is | reachable from the JVM | ext4 | reflink FS (xfs `-m reflink=1`, btrfs) |
|---|---|---|---|---|
| **True extent sharing** | `FICLONERANGE` ioctl; refcount-btree metadata only | no `ioctl` in `java.nio`; needs a JNA shim | `EOPNOTSUPP` | 0 bytes consumed, 0 pages of page cache |
| **Kernel-side copy** | `copy_file_range(2)`; bytes are written, no userspace round trip | **yes** — JDK 21 `FileChannel.transferTo(pos,count,FileChannel)` | real copy, populates page cache on both inodes | reflinks for free when both offsets are 4096-aligned |
| **Userspace copy** | read/write loop over compressed bytes | always | real copy | real copy |

Notes that matter for the design:

- **Hard links cannot express a byte range at all.** `FileUtils.createHardLink` is
  `Files.createLink` (`FileUtils.java:173-188`) — one inode, one size, one extent map. Truncating one
  link truncates both.
- **`FICLONERANGE` requires `src_offset`, `dest_offset` and `src_length` each to be a multiple of the
  filesystem block size** (4096 in every measurement). Compressed chunk offsets advance by
  `compressedLength + 4` (`CompressedSequentialWriter.java:198`), so a split offset is effectively
  uniform mod 4096. The recipe that *does* work — pwrite the leading partial block, clone the aligned
  interior, pwrite the tail — requires `dest_offset ≡ src_offset (mod 4096)`, i.e. **front-padding the
  destination**. That is legal at the format level (`chunkFor` reads `O'[0]` from the array) but
  illegal today at the mmap level (below).
- **`copy_file_range` does not rescue misalignment.** On xfs-reflink with matching-but-unaligned
  residues it performs a *full* real copy; it does not clone the aligned interior for you.
- **JDK version matters.** Production is JDK 21 (`build.gradle:134`), where `transferTo` between two
  `FileChannel`s compiles to one `copy_file_range`. JDK 11 gets `sendfile`/mmap loops. Treat it as a
  runtime capability, not a compile-time one. `ChannelProxy.transferTo` exists
  (`src/java/org/apache/cassandra/io/util/ChannelProxy.java:146-156`) but **does not loop**, and a
  single `copy_file_range` is capped at `0x7ffff000` — wrap it.
- **Reflink saves disk but doubles RAM.** Shared extents get one `address_space` per inode, so both
  inodes cache the same physical bytes independently; and `ChunkCache.Key` is
  `(path, reader class, position)` with `path.hashCode()` in the hash
  (`src/java/org/apache/cassandra/cache/ChunkCache.java:54-75`), so decompressed chunks are cached
  twice regardless. With `disk_access_mode: auto → mmap` on 64-bit
  (`DatabaseDescriptor.java:489-493`) the page cache *is* the read cache, so this can cost more read
  latency than the disk saving is worth.

**`MmappedRegions` is the thing that blocks front-padding — and only that.**

```java
long offset = 0; long lastSegmentOffset = 0; long segmentSize = 0;
while (offset < metadata.dataLength) { CompressionMetadata.Chunk chunk = metadata.chunkFor(offset);
    ... segmentSize += chunk.length + 4; //checksum
    offset += metadata.chunkLength(); }
```
(`src/java/org/apache/cassandra/io/util/MmappedRegions.java:155-184`.) Segments are placed at a
cumulative sum seeded at physical 0, and `CompressedChunkReader.Mmap` indexes as
`chunk.offset - region.offset()` (`:329-334`). Precise consequences:

- A contiguous chunk-run copy with `O'[0] == 0` satisfies it exactly. ✅ **Option (b) needs no
  change here.**
- Leading physical padding is *not* allowed today. Because `chunk.length` is itself derived as
  `next − cur − 4`, the loop's sum is `compressedFileLength − O'[0]`, so `state.length` is short by
  the padding and the tail chunks are simply unmapped — reads near a region end or EOF throw from
  `State.floor`'s `assert 0 <= position && position <= length` (`:293`) or from `ByteBuffer.limit`.
  Interior chunks read correctly, so a front-padded file would pass a smoke test and fail at the
  tail. Fixing it looks like seeding `lastSegmentOffset = metadata.chunkFor(0).offset`, plus updating
  `MmappedRegionsTest.java:335` which asserts `chunk.offset == region.offset()`. Sufficiency across
  other consumers is **UNVERIFIED**. *(Since done, and it was sufficient; the test did not need
  updating. See the superseding note at the end of this section.)*
- The requirement that copied chunks be *contiguous* does **not** come from this loop — it comes from
  `chunkFor` deriving length from the offset delta, so an interior gap inflates `chunk.length` and
  corrupts reads on the non-mmap path too.

**Recommendation on mechanics:** looped `FileChannel.transferTo` for the Data.db range, then compute
`Digest.crc32` in a second sequential read of the freshly written file (page-cache hot from the
`copy_file_range`); or a single userspace pass with an inline `CRC32` if you prefer one pass. Do
**not** build the design around reflink: it is filesystem-conditional (see §7), it doubles page-cache
footprint, and the front-padding it needs is blocked above.

> **SUPERSEDED (2026-07-29).** Reflink was implemented, as an option rather than a foundation, which is
> the part of the recommendation above that still stands. `ZeroCopySSTableSplitter.copyPlan` front-pads
> the child to a 64 KiB boundary and `org.apache.cassandra.io.util.Reflink` hands the aligned interior
> to `FICLONERANGE` through a JNA `ioctl` shim; the ≤64 KiB tail is transferred. Support is discovered
> by trying and remembered per directory, so on ext4 the cost is one failing syscall per data directory
> and the copy runs exactly as described above. Off with `zero_copy_split_reflink_enabled: false`.
>
> Three corrections to the analysis above:
>
> - **The `MmappedRegions` blocker was real, and the proposed fix is the fix.** Seeding
>   `lastSegmentOffset` with `metadata.chunkFor(0).offset` is the whole change
>   (`MmappedRegions.java:155-193`), and `MmappedRegionsTest:335` did **not** need updating: for an
>   unpadded file `chunkFor(0).offset` is 0, so every existing region offset is unchanged. Sufficiency
>   across other consumers is no longer UNVERIFIED — it was the only one. Two regression tests pin it:
>   `MmappedRegionsTest.testMapForCompressionMetadataWithFrontPad` (multi-region, offsets wrong) and
>   `ZeroCopySSTableSplitterTest.alignedChildrenAreReadableEverywhere` (single region, tail unmapped;
>   fails with `floor()`'s `position <= length` assert). The fuzz test runs half its matrix padded.
> - **"Reflink doubles page-cache footprint" overstates it for this use.** Two `address_space`s only
>   cost twice when both inodes stay hot; here the parent is unlinked as the children are published, so
>   the double-caching lasts as long as the split does. What survives is one duplicated boundary chunk
>   per interior split point.
> - **The digest, not the copy, is now the floor** — and it is optional. With the bytes shared,
>   `Digest.crc32` is the only full pass left, so a shared split costs the read half of the old cost.
>   `zero_copy_split_digest_enabled: false` removes it: measured on a 1 GiB parent split 4 ways, that is
>   1.9 MiB read and 1.0 MiB written, versus 1998 MiB read and 750 MiB written today (see
>   `docs/large-split-bench.md`). §3.4's audit holds and was re-verified — `Verifier` is the only reader,
>   a missing digest makes it upgrade to a full extended verification rather than fail, and the fork's own
>   backup manifest enumerates the component files that exist while `BackupMemtableContext`'s
>   `COMPONENTS_TO_DOWNLOAD` never asks for DIGEST. So the cost of skipping it is verification *speed* on
>   `nodetool verify` and `import --verify-sstables`, which is why it defaults to on.
>   §3.6's `crc32_combine` route — keep the component for 4 bytes per chunk, 1/4 of the data read at
>   `chunk_length_in_kb: 16` and 1/16 at 64 — is still the way to have both, and is still unimplemented.

### 3.6 Per-chunk CRCs survive for free; the whole-file digest does not

The per-chunk CRC32 is computed over the *stored* chunk bytes and written immediately after them:
`channel.write(toWrite); toWrite.rewind(); crcMetadata.appendDirect(toWrite, true);`
(`CompressedSequentialWriter.java:183-187`), with the offset recorded *before* the payload and
advanced by exactly `compressedLength + 4` (`:178`, `:198`). `ChecksumWriter.appendDirect` seeds a
fresh `CRC32` per chunk and resets it afterwards
(`src/java/org/apache/cassandra/io/util/ChecksumWriter.java:62-82`) — **no file offset, chunk index,
or prior-chunk state is mixed in**. The reader reads `chunk.length + 4` bytes at `chunk.offset`, CRCs
the first `chunk.length`, and compares the trailing int (`CompressedChunkReader.java:130-144` and
`:340-349`). A contiguous verbatim range with rebased offsets therefore preserves every chunk CRC
with **zero recomputation**.

`Digest.crc32` is a `CRC32` over *all* written bytes including the inline per-chunk CRCs
(`ChecksumWriter.java:74-81`, written as decimal ASCII at `:91-98`) and must be recomputed. It is
validated only by `Verifier` (`:217-221`) and, transitively, by `nodetool import`. Copying the
parent's digest is worse than omitting it: a mismatch trips `markAndThrow`, which mutates
`repairedAt` to `UNREPAIRED_SSTABLE` and then throws `CorruptSSTableException` into the disk failure
policy. **Regenerate it, do not inherit it.** Also note DIGEST is a streamed component
(`ComponentManifest.java:41-43`), so a stale one propagates.

Two residual hazards, both benign for option (b): the **padded raw final chunk** case requires
`uncompressedLength < maxCompressedLength`, i.e. a short chunk, i.e. only the file's last chunk — and
a contiguous run can only include the source's last chunk as `j`, where it stays last. And
**raw-vs-compressed classification** is `chunk.length < maxCompressedLength`
(`CompressedChunkReader.java:257`, `:351`), so copy `maxCompressedLength`, `chunkLength`, the
compressor name and its options unchanged. The robust way: copy the source CompressionInfo.db's
header **prefix verbatim**, whose length is derivable without parsing —
`prefixLen = srcCompInfoLength − 8 − 4 − 8·N`. This also sidesteps the fact that `writeHeader`
*re-derives* the compressor name via `getClass().getSimpleName()` (`:361`) and always writes
`maxCompressedLength` even for pre-`na` descriptors that will not read it.

### 3.7 Uncompressed SSTables

**Alignment: strictly easier, and this is where the prefix case shines.** The uncompressed reader is
`SimpleChunkReader` with `BufferManagingRebufferer.Unaligned`
(`src/java/org/apache/cassandra/io/util/SimpleChunkReader.java:57-61`) — arbitrary byte positions, no
grid. So you cut *exactly* at `Poff`: no dead prefix, no dead suffix, the child's first index
position is **0**, no CompressionInfo.db to rebase, and **Scrubber/Verifier need no patch at all**.
This is a compression-only problem. The `dataLength` lever does not exist for uncompressed tables
(`FileHandle.dataLength()` falls through to `rebuffererFactory::fileLength`, and the read path never
passes an `overrideLength` — `SSTableReaderBuilder.java:447` calls plain `complete()`), but with an
exact cut you never need it.

**Checksums: this is the real cost.** `ChecksummedSequentialWriter` writes
`[i32 chunkSize][i32 crc per chunkSize bytes]` to CRC.db, where `chunkSize` is the writer's buffer
capacity (`ChecksummedSequentialWriter.java:36-39`) — the `SequentialWriterOption` default of
**64 KiB**, since `BigTableWriter`'s `writerOption` sets only trickleFsync (`:72-75`). It is addressed
from origin 0: `reader.seek(((start / chunkSize) * 4L) + 4)` with
`chunkStart(offset) = (offset/chunkSize)*chunkSize` (`DataIntegrityMetadata.java:71-81`).

For a **prefix** cut at `hi`, all CRC entries for chunks `0 .. floor(hi/65536)−1` are byte-identical
and can be copied verbatim; only the final (now short) entry must be recomputed, and the array
truncated. This composes correctly with the stream writer, which sets
`bufferSize = validator.chunkSize` (`CassandraStreamWriter.java:89`) and
`minReadable = min(bufferSize, proxy.size() - start)` (`:149`) — so the short final chunk is CRC'd
over exactly the short tail, which is what a fresh writer would have produced. *(Derived from code,
not tested — see §7.)*

For a **suffix** cut the grid is misaligned and CRC.db must be regenerated wholesale (or omitted:
the only sstable consumer is `CassandraStreamWriter.java:85-87`, null-guarded on file existence, so
omitting it costs only stream-out validation). Since you must recompute `Digest.crc32` anyway,
regenerating CRC.db in the same pass is nearly free — one buffer, two `Checksum`s, exactly what
`ChecksumWriter.appendDirect(bb, false)` already does.

Do **not** align an uncompressed cut down to 64 KiB to make CRC.db byte-sliceable: that reintroduces
dead head bytes with no `dataLength` to hide the tail, so `getPositionsForRanges`' right-edge clamp
would ship garbage to a streaming peer.

---

## 4. Q2 — rebuilding the other components without reading the data

### 4.1 The central insight

**Only one field in an Index.db record is an absolute Data.db offset: the top-level
`RowIndexEntry.position`. Everything inside the promoted-index blob is partition-relative.** Therefore
rebasing a partition for a child means re-encoding exactly **one unsigned vint** and `memcpy`-ing the
rest, with no schema awareness whatsoever.

The on-disk record is `[u16 keylen][key][uvint position][uvint blobSize][blobSize opaque bytes]`
(`BigTableWriter.java:567-568`; serialize paths at
`src/java/org/apache/cassandra/db/RowIndexEntry.java:450`, `:601`, `:742`). Evidence for
position-independence of the blob:

- `headerLength` and `IndexInfo.offset`/`width` come from
  `currentPosition() = writer.position() - initialPosition`, with `initialPosition` reset per
  partition (`src/java/org/apache/cassandra/db/ColumnIndex.java:94`, `:116`, `:142-145`, `:171-175`).
  The reader composes them additively: `return indexEntry.position + index(i).offset;`
  (`src/java/org/apache/cassandra/db/columniterator/AbstractSSTableIterator.java:461-464`).
- The trailing `int[]` offsets table is relative to the *first* IndexInfo: `indexOffsets[0]` is 0 and
  later entries are `buffer.position()` within the blob (`ColumnIndex.java:193-204`), matching the
  format javadoc at `RowIndexEntry.java:60-61` (*"Each IndexInfo object's offset is relative to the
  first IndexInfo object"*).
- `ShallowIndexedEntry.indexFilePosition` is **never stored in Index.db** — it is supplied by the
  reader from its own file pointer at deserialize time (`RowIndexEntry.java:242-246`, `:317`,
  `:345-348`). It appears on disk only in the saved key cache.

And the walk is schema-blind: `RowIndexEntry.Serializer.skip` is `readPosition` + size vint +
`skipBytesFully(size)` (`:419-432`) and its `Version` argument is unused. No clustering comparator, no
`SerializationHeader`, no cell types.

**Two traps.**

1. **The position vint must be canonically minimal, never padded.**
   `ShallowIndexedEntry.openWithIndex` recomputes the blob's file position arithmetically:
   ```java
   return new ShallowInfoRetriever(indexFilePosition +
                                   VIntCoding.computeUnsignedVIntSize(position) +
                                   VIntCoding.computeUnsignedVIntSize(indexedPartSize + fieldsSerializedSize) +
                                   fieldsSerializedSize, ...
   ```
   (`RowIndexEntry.java:725-731`; `computeUnsignedVIntSize` is a pure function of the value,
   `src/java/org/apache/cassandra/utils/vint/VIntCoding.java:323-328`.) A padded vint decodes to the
   same value — `readUnsignedVInt` takes the length from the leading-ones count — so nothing errors,
   but the retriever's base is short and `fetchIndex` reads a garbage int from the offsets table
   (`:791-803`). Scope note: this path is only taken when the record exceeds
   `column_index_cache_size` (default 2 KiB, `Config.java:398`; branch at `:336`); smaller records
   become `IndexedEntry` and are read sequentially. Only the API available (`VIntCoding.writeUnsignedVInt`)
   is minimal anyway, so padding would require hand-rolled bytes.
2. **Child record lengths differ from the parent's**, because the position vint can shrink. So child
   Index.db offsets shift, and `Summary.db` — which stores **absolute Index.db offsets**
   (`src/java/org/apache/cassandra/io/sstable/IndexSummaryBuilder.java:206-208`) — must be rebuilt,
   not inherited. (For a *prefix* child positions do not change at all, so the Index.db byte prefix is
   valid verbatim and even the summary's stored offsets remain correct, though its `last` key and
   sampled tail must still be truncated — simplest to rebuild.)

### 4.2 Per-component table

| component | how it is produced for a child | cost class |
|---|---|---|
| **DATA** | verbatim chunk-run copy (§3.2); prefix child = copy `[0, O[j+1])` | `O(compressed bytes)`, kernel-side |
| **PRIMARY_INDEX** | scan the parent's Index.db byte range; re-encode one uvint per partition, `memcpy` the blob. Prefix child: verbatim byte prefix, zero re-encoding | `O(index bytes)` |
| **FILTER** | `bf.add(key)` per key during the same pass — 1 murmur3-128 + K bit-sets. Or omit and let the reader rebuild+persist | `O(partitions)` |
| **SUMMARY** | `IndexSummaryBuilder.maybeAddEntry(key, newIndexPos)` in the same pass; 1 entry per `min_index_interval` = 128 (`TableParams.java:394`). Also yields exact `first`/`last`. Or omit | `O(partitions / 128)` |
| **COMPRESSION_INFO** | copy the source header prefix verbatim; write `dataLength`, `chunkCount`, rebased offsets | `O(chunks)`, **zero file reads** |
| **CRC.db** (uncompressed only) | prefix: copy header + all-but-last entry verbatim, recompute the short final entry. Suffix: regenerate | `O(bytes)` streaming CRC32, or `O(entries)` for a prefix |
| **STATS** (VALIDATION/COMPACTION/STATS/HEADER) | inherit the parent's file, then `MetadataSerializer.mutate` to patch level + repair triple; optionally derive `estimatedPartitionSize` and the HLL exactly from the same index pass | `O(1)`, or `O(partitions)` if deriving |
| **DIGEST** | recompute a whole-file `CRC32` over the child's bytes; fold into the copy or a second page-cache-hot pass | `O(bytes)`, intrinsified |
| **TOC.txt** | `SSTable.appendTOC(descriptor, components)` | `O(1)` |

Notes on the "or omit" options: `SSTableReaderBuilder.load` takes the
`!components.contains(Component.FILTER)` branch, rebuilds from Index.db, and **persists** both filter
and summary (`:399-405`, `:433-455`). But that is `!isOffline`-only, it moves an `O(index)` scan into
the foreground `SSTableReader.open`, and — a subtlety worth writing down — `saveSummary`/
`saveBloomFilter` do **not** register the component in TOC.txt or the reader's component set. Since
`componentsFor` prefers TOC.txt, a child whose TOC omits FILTER would re-scan the whole index and
rewrite Filter.db on **every** open, forever, and never read what it wrote. **Write a TOC.txt that
lists FILTER and SUMMARY even if the files are absent** (`readTOC(desc, skipMissing=true)` tolerates
the gap on the first open and self-heals on the second). Given that the index pass is already running,
just build them.

Two things that cannot be inherited or subset, for the record: a bloom filter's bit positions are
`hash % bitset.capacity()` (`src/java/org/apache/cassandra/utils/BloomFilter.java:90`), so slicing a
parent bitset is meaningless — inherit whole (correct: superset ⇒ no false negatives) or rebuild. And
`bf.add` performs exactly **one** hash per key, deriving all K indices combinatorially
(`BloomFilter.java:83-92`, `:94-102`), so rebuilding is tens of nanoseconds per partition. Note the
inherited-filter cost is **only memory**, not false-positive rate: bits/key and K are functions of
`fpChance` alone (`FilterFactory.java:69` + `BloomCalculations.computeBloomSpec`), so an
oversized filter has the *same* FP rate for absent keys, and out-of-range keys never reach the filter
because `View.select(SSTableSet.LIVE, key)` prunes via the `first`/`last` interval tree first. Rebuild
because it is free inside the pass, not because FP rate demands it.

### 4.3 StatsMetadata field by field

`Statistics.db` is a container for four components ordered by `MetadataType.ordinal()`: VALIDATION,
COMPACTION, STATS, HEADER (`MetadataType.java:25-28`; framing at `MetadataSerializer.java:46-104`).
Classification below: **derive** = exactly reconstructible from an Index.db-only pass; **inherit** =
copy the parent's value; **scan** = needs cell-level data to be exact.

| field | class | consequence of inheriting |
|---|---|---|
| `estimatedPartitionSize` | **derive, byte-exactly** (§4.4) | `count()` inflated K×; over-sizes a rebuilt filter and index summary (`SSTableReaderBuilder.java:185-193`), wrong read-buffer percentile (`:438`), wrong `nodetool tablehistograms`. No correctness consumer. |
| COMPACTION `cardinalityEstimator` (HLL) | **derive, exactly** — `addKey` consumes only key bytes (`MetadataCollector.java:152-158`) | consumers *merge* sketches (`SSTableReader.java:267-296`), and merging K identical register arrays returns the same sketch, so the union stays exactly the parent's cardinality. Harmless. |
| `estimatedCellPerPartitionCount` | scan (or rescale by partition share) | skews `getEstimatedDroppableTombstoneRatio` denominator and mean-cells metrics. Heuristics only. |
| `estimatedTombstoneDropTime` | scan (or rescale) | **over-counts droppable tombstones K×** ⇒ spurious single-SSTable tombstone compactions on every child (`AbstractCompactionStrategy.java:387-435`). Wasted I/O; self-healing. This is the field most worth rescaling. |
| `totalRows`, `totalColumnsSet` | scan (or rescale) | metrics only (`SSTableReader.java:1825-1830`). |
| `minTimestamp` | inherit (≤ true child min) | lower min ⇒ `getPurgeEvaluator` returns a *lower* purge bound ⇒ **fewer** tombstones purged (`CompactionController.java:268`, `:289-291`); fewer fully-expired drops. Conservative. |
| `maxTimestamp` | inherit (≥ true child max) | higher max ⇒ read loop does **not** break early (`SinglePartitionReadCommand.java:711-715`, `:890-891`); `reduceFilter` drops fewer columns; expired-SSTable candidate retained. Only real cost: wrong TWCS bucket. Conservative. |
| `minLocalDeletionTime` | inherit — **never narrow** | `mayHaveTombstones()` is `min != Cell.NO_DELETION_TIME` (`SSTableReader.java:1798-1803`). Inheriting makes it return `true` more often (extra probes — conservative). **Narrowing toward `NO_DELETION_TIME` makes the read path skip an SSTable holding a partition delete → resurrected data.** |
| `maxLocalDeletionTime` | inherit — **never lower** | gates whole-SSTable expiry drop `maxLocalDeletionTime < gcBefore` (`CompactionController.java:180`, `:203`). Inherited (higher) ⇒ never wrongly dropped. Lowering it is data loss. |
| `minTTL` | **inherit verbatim (mandatory)** | must stay consistent with the inherited HEADER `EncodingStats` (§below). |
| `maxTTL` | inherit | **zero consumers** outside `SSTableMetadataViewer`/JMX. Cosmetic. |
| `minClusteringValues`, `maxClusteringValues` | inherit (the *wide*, safe direction) | consumed by `ClusteringIndexSliceFilter.shouldInclude` (`:129-137`), `ClusteringIndexNamesFilter` (`:145-157`), and `UnfilteredRowIteratorWithLowerBound.getMetadataLowerBound` (`:255-263`). `Slice.intersects` widens toward inclusion, and a looser synthetic lower bound still satisfies the runtime assert at `UnfilteredRowIteratorWithLowerBound.java:116-119`. Narrowing would abort reads. |
| `compressionRatio` | derive arithmetically from the copied chunk range, or inherit (near-exact) | affects only the compaction rate limiter's accounting and expected-compacted-size. |
| `hasLegacyCounterShards` | inherit verbatim, or widen to `true` | `true` disables entire-SSTable ZCS (`CassandraOutgoingFile.java:187`). Fabricating `false` would be a counter-correctness bug. |
| `commitLogIntervals` + `originatingHostId` | **inherit as an atomic pair, verbatim** | see §4.5 — this is the one field where a *superset* is unsafe. |
| `sstableLevel` | **inherit** | children are disjoint, so the LCS no-overlap-within-a-level invariant holds. See §6.4 for the trap. |
| `repairedAt`, `pendingRepair`, `isTransient` | **set per child from the split's intent** | see §4.6. |
| VALIDATION `partitioner` | inherit verbatim (mandatory) | checked on open; mismatch logs and `System.exit(1)` in `openForBatch` (`SSTableReader.java:429-436`). |
| VALIDATION `bloomFilterFPChance` | inherit verbatim | no functional consumer in this tree (the rebuild decision tests `validation == null`, `SSTableReaderBuilder.java:399`). Cosmetic. |
| HEADER `keyType`, `clusteringTypes`, static/regular column supersets, `stats` (`EncodingStats`) | **inherit verbatim, byte-for-byte — MANDATORY** | see below. |

**The HEADER is the hard blocker, and it is why a copy-based split is only possible at all.**
`SerializationHeader` encodes timestamps, localDeletionTime and TTL as **unsigned vint deltas** off
`stats.minTimestamp/minLocalDeletionTime/minTTL`:

```java
public void writeTimestamp(long timestamp, DataOutputPlus out) throws IOException
{
    out.writeUnsignedVInt(timestamp - stats.minTimestamp);
}
```
(`src/java/org/apache/cassandra/db/SerializationHeader.java:165-177`.) And rows encode their
present-columns as a bitmap subset of `header.columns()`
(`UnfilteredSerializer.java:231` → `Columns.serializeSubset`). So a child that inherits Data.db bytes
verbatim **must** inherit the parent's HEADER byte-for-byte, including the exact column superset and
the exact minima. Tightening either corrupts every relocated row — silently, with all CRCs still
passing, and since reconciliation is per-cell by write timestamp, an inflated timestamp can beat a
legitimate later tombstone. This is also the reason a rewrite's per-cell re-serialize is *not* pure
overhead: it is the step that rebases the encoding.

Corollary for the whole design: **one source per output, no purging, no stats rebase, no column-set
change.** A copy-based split satisfies all four; a multi-source merge does not.

Additionally: `MetadataCollector.updatePartitionDeletionPresence` **does not exist in this tree**
(`grep` returns nothing; `PartitionStatisticsCollector` has exactly five methods at
`PartitionStatisticsCollector.java:25-29`). Partition-level deletions reach the collector through the
untyped `update(DeletionTime)` (`MetadataCollector.java:203-211`), so there is no
"has partition deletions" bit to consult or preserve. Any design step assuming one must add it.

### 4.4 `estimatedPartitionSize` is byte-exactly derivable — proof

- `beforeAppend` returns the data-file position where the partition's bytes begin, and that same
  local is both written into Index.db and used for the size: `long startPosition = beforeAppend(key);`
  (`BigTableWriter.java:214`) → `RowIndexEntry.create(startPosition, ...)` (`:228`) and
  `long rowSize = endPosition - startPosition; metadataCollector.addPartitionSizeInBytes(rowSize);`
  (`:237-241`). `BigTableWriter.java:241` is the *only* caller of `addPartitionSizeInBytes` in the
  tree.
- `ColumnIndex` is the only writer of Data.db (constructed with that handle at
  `BigTableWriter.java:114`) and it writes the partition key first (`ColumnIndex.java:130`) with
  `initialPosition` captured at reset (`:94`), so nothing is written between partitions.
- Therefore `rowSize_i ≡ position_{i+1} − position_i` identically, and for the last partition
  `rowSize_last = logicalDataEnd − position_last`, where `logicalDataEnd` is
  `CompressionMetadata.dataLength` for compressed tables and the file length otherwise.
- The histogram is a fixed-layout `EstimatedHistogram(150)` (`MetadataCollector.java:59-63`) and
  `add(long n)` is an order-independent bucket increment (`EstimatedHistogram.java:134-137`), and the
  serializer writes the offsets array itself. So replaying the identical multiset of deltas into
  `defaultPartitionSizeHistogram()` produces a **byte-identical** component.

Caveats: `beforeAppend` literally returns `(lastWrittenKey == null) ? 0 : dataFile.position()`
(`:176`) — an invariant that holds only because BIG Data.db has no header; and for a chunk-aligned
child the first index position is `lo mod L`, not 0, so do not substitute 0 (delta arithmetic is
unaffected — the dead prefix belongs to no partition).

### 4.5 `commitLogIntervals` — the one field where a superset is unsafe

This is not a range *of data in the SSTable*; it is a claim that these commitlog positions are already
durable and **may be discarded**. The consumer builds a per-table union, gated on host identity:

```java
UUID originatingHostId = reader.getSSTableMetadata().originatingHostId;
if (originatingHostId != null && originatingHostId.equals(localhostId))
    builder.addAll(reader.getSSTableMetadata().commitLogIntervals);
```
(`src/java/org/apache/cassandra/db/commitlog/CommitLogReplayer.java:338-347`, per table at `:149`,
`:177`.) A **wider** set ⇒ more commitlog discarded ⇒ acked-but-unflushed mutations never replayed on
restart. Two failure surfaces: `shouldReplay` (`:460-463`) skips individual mutations, and
`firstNotCovered` (`:371-377`) inspects only each set's *first* interval end to advance
`globalPosition`.

Why inheritance is nevertheless **exactly** correct: `IntervalSet.Builder.add` is normalizing and
idempotent (`IntervalSet.java:174-202`), so copying the parent's full set into all K children leaves
the per-table union bit-identical. The bug appears only if you fabricate wider intervals, or write
`originatingHostId = getLocalHostUUID()` (which `MetadataCollector`'s default constructor does,
`:124-133`) while inheriting a *foreign* parent's intervals — then foreign segment/position numbers
are interpreted against the local commitlog. Normal compaction avoids this by filtering
(`MetadataCollector.java:139-148`).

**Rule: inherit the pair atomically from the same parent** — either copy Statistics.db verbatim, or
use `MetadataCollector(Iterable<SSTableReader>, comparator, level)` so the `:144` filter applies. The
dangerous middle ground is the natural-looking
`new MetadataCollector(comparator).commitLogIntervals(parent.getSSTableMetadata().commitLogIntervals)`.
In-tree precedent for exactly the fan-out shape: `SplittingSizeTieredCompactionWriter.java:112` and
`MaxSSTableSizeWriter.java:122` already give every split output the same host-filtered union.

### 4.6 `repairedAt` / `pendingRepair` / `isTransient`

These are constructor arguments to `SSTableWriter` (`:66-68`, `:89-101`) and reach
`finalizeMetadata(..., repairedAt, pendingRepair, isTransient, header)` (`:323-330`). A copy-based
split must supply them explicitly, and there is an existing `O(1)` in-place path if you prefer to
patch after the fact: `StatsMetadata.mutateRepairedMetadata` / `mutateLevel` (`:152-200`) →
`MetadataSerializer.mutate` (tmp file + `renameWithConfirm`, `:249-272`) →
`SSTableReader.mutateRepairedAndReload` under `synchronized (tidy.global)` →
`CompactionStrategyManager.mutateRepaired` under the strategy write lock with `verifyMetadata`
(`:1237-1276`).

Plain inheritance from a *single* parent is correct by construction — a split partitions one parent's
rows, so every child has the parent's repair status — and is exactly what today's `SSTableSplitter`
does via `CompactionAwareWriter.java:80-82` (min over inputs; unanimity trivially satisfied). The
danger is a *mis-set* value, which is why per-range assignment is only needed when the split is doing
anticompaction:

- `isTransient = true` on a child holding full-replica data is **deleted at repair finalize**:
  `boolean obsoleteSSTables = isTransient && repairedAt > 0;` → `transaction.obsoleteOriginals();`
  (`src/java/org/apache/cassandra/db/compaction/PendingRepairManager.java:518-526`). The
  `Preconditions.checkState(Iterables.all(..., SSTableReader::isTransient))` at `:524` catches nothing
  — it re-reads the same metadata flag that routed the SSTable into that holder.
- `repairedAt > 0` on an unrepaired range means incremental repair never revisits it, and under
  `onlyPurgeRepairedTombstones` its tombstones become purgeable
  (`CompactionController.java:172`, `:255`, `:303-306`). Note that option is off by default.
- `SSTable.validateRepairedMetadata` (`:387-393`) makes the most obvious blind-inheritance mistakes
  fail loudly at write time (`isTransient` without `pendingRepair`, `pendingRepair` on a repaired
  SSTable), and `CompactionStrategyHolder.java:75-79` throws if `isTransient` is set without
  `isPendingRepair`.

### 4.7 The prior art that makes inherited stats defensible — and its limit

`CassandraEntireSSTableStreamReader.read` writes every manifest component byte-for-byte into a fresh
Descriptor and then applies exactly one transform:

```java
UnaryOperator<StatsMetadata> transform = stats -> stats.mutateLevel(header.sstableLevel)
                                                      .mutateRepairedMetadata(messageHeader.repairedAt, messageHeader.pendingRepair, false);
writer.descriptor.getMetadataSerializer().mutate(writer.descriptor, description, transform);
```
(`:137-141`.) Because `mutate` deserializes all four `MetadataType`s and replaces only STATS
(`MetadataSerializer.java:249-256`), the receiver inherits the **sender's** VALIDATION, COMPACTION
(HLL), HEADER, and every remaining STATS field including both histograms, all min/max bounds, the
tombstone-drop histogram, `commitLogIntervals` and `originatingHostId` — plus the sender's Filter.db
and Summary.db verbatim. `isTransient` is not inherited or taken from the session; it is hardcoded
`false` (`:138`). So **wholesale metadata inheritance with a narrow STATS override is a shipped,
production-exercised mechanism**, and `mutate`'s tmp+rename primitive is directly reusable.

The honest limit, from adversarial review: that path engages *only* when the transfer covers the whole
file byte-for-byte (`contained()`, `CassandraOutgoingFile.java:194-202`), so the inherited metadata is
exactly correct for the bytes it describes. Every shipped path that emits a row *subset* recomputes
instead — partial-range streaming goes through `RangeAwareSSTableWriter` + `MetadataCollector`
(`CassandraStreamReader.java:190`), and compaction inherits only level and host-id-matched
`commitLogIntervals`. **So this is prior art for transplant, not for attaching row-derived stats to a
key-range subset.** Each field still needs the per-consumer argument in §4.3. What the streaming path
does establish additionally is the pattern of defending individual dangerous fields downstream rather
than at write time (`CommitLogReplayer.java:342-344` host-id gate; `SSTableReader.java:493-499`
partitioner hard-exit).

### 4.8 Secondary indexes and fork-local components

- **SASI is the only per-SSTable index component and it is a hard blocker.**
  `Index.getFlushObserver` defaults to `null` (`Index.java:343-346`) and `SASIIndex` is the only
  override in the tree (`SASIIndex.java:306-309`). The observer contract requires row/cell events
  (`SSTableFlushObserver.nextUnfilteredCluster`), which an index-only pass cannot supply.
  **Refuse SASI-indexed tables in v1**, or emit children without `SI_*.db` and let SASI's own
  `O(Data.db)` `SASIIndexBuilder` rebuild them. Whether a missing `SI_*.db` is automatically
  scheduled for rebuild is **UNVERIFIED**. Classic `CassandraIndex` (index-as-hidden-table) adds no
  per-SSTable component and is unaffected. Independently, `CompactionIterator` installs a row merge
  listener when indexes exist (`:182-185`, `:208-209`), which already defeats the trivial
  single-source pass-through.
- **No custom `Component` or `MetadataComponent` exists under `src/java/com/netflix/cassandra/`.**
  But there is a non-`Component` sidecar: `<sstable>-Data.db.len`, holding the S3 object's compressed
  length, read via `DataLengthFileSerializer.readOrDelete` and fed to
  `CompressionMetadata.createWithLength` (`BackupMemtableContext.java:494-514`, `:711-721`). It is not
  in `Component.java` and not in TOC.txt, so TOC-driven logic misses it. Any S3-resident child needs
  its own. `BackupManifestBuilder` is directory-listing driven (`:205-235`), so children are picked up
  automatically at backup time.
- **`max_hints*` has no interaction.** The only options are `max_hints_delivery_threads`,
  `max_hints_file_size`, `max_hints_size_per_host` (`Config.java:531-536`), all governing
  `HintsService`'s own files, which are not SSTables. Question closed.

---

## 5. Q3 — if we must scan everything, is it better than the copies we do today?

### 5.1 Strategy definitions

| | strategy |
|---|---|
| **S0** | today: full rewrite through `CompactionTask` (decompress → per-cell deserialize → merge stack → per-cell re-serialize → recompress → CRC → write), every component rebuilt |
| **S1** | chunk-aligned verbatim chunk-run copy + index-only component rebuild + regenerated digest |
| **S2** | S1 plus a forced full decompress-only scan (read + per-chunk CRC + LZ4 uncompress to EOF), no object materialisation |
| **S3** | S1 plus a full deserializing scan (materialise every partition/row/cell) purely to compute exact STATS |
| **S4** | share the Data.db (hard link or logical bounds) + index-only rebuild; zero byte movement |

### 5.1a MEASURED — end-to-end, implementation vs. the real baseline

Added after the research above was written, and it **supersedes the stage-assembled estimates in §5.2** for
the one comparison it covers. `ZeroCopySSTableSplitter` now exists and
`test/long/org/apache/cassandra/io/sstable/ZeroCopySSTableSplitterBenchTest.java` drives it against the
actual `SSTableSplitter` full-rewrite path in-process. Numbers are from that harness, written to
`build/test/zerocopy-split-bench.txt`.

| Config (parent/chunk/children/shape) | baseline ms | zero-copy ms | speedup | baseline alloc | zero-copy alloc |
|---|---|---|---|---|---|
| 16 MiB / 4 KiB / k=4 / wide   | 364.6 | 116.8 | 3.1x | 88.8 MiB  | 3.2 MiB  |
| 16 MiB / 16 KiB / k=4 / wide  | 269.0 | 114.9 | 2.3x | 51.0 MiB  | 3.0 MiB  |
| 16 MiB / 64 KiB / k=4 / wide  | 235.2 | 103.8 | 2.3x | 40.6 MiB  | 2.9 MiB  |
| 16 MiB / 16 KiB / k=4 / narrow| 345.9 | 137.4 | 2.5x | 79.4 MiB  | 10.2 MiB |
| 64 MiB / 16 KiB / k=2 / wide  | 407.7 | 119.5 | 3.4x | 157.9 MiB | 2.6 MiB  |
| 64 MiB / 16 KiB / k=4 / wide  | 435.7 | 200.6 | 2.2x | 157.3 MiB | 3.9 MiB  |
| 64 MiB / 16 KiB / k=8 / wide  | 573.8 | 258.4 | 2.2x | 167.8 MiB | 6.5 MiB  |
| 64 MiB / 16 KiB / k=4 / narrow| 446.3 | 208.7 | 2.1x | 258.2 MiB | 25.5 MiB |

**2.1x–3.4x wall clock; 10x–60x less allocation.** The allocation collapse is the more robust result — it is
the direct, mechanical consequence of never building a row/cell object graph, and it is far less
laptop-dependent than the wall-clock ratio.

Three things these numbers do **not** say:

- **Write amplification is unchanged**: 1.008–1.024 baseline vs. 1.009–1.026 candidate. §5.5's conclusion
  stands — this is a CPU/GC win, not an SSD-endurance win. Do not sell it as one.
- **The read column is modelled, not measured.** `/proc/self/io` does not exist on macOS, so the harness
  prints `read-byte source: /proc/self/io UNAVAILABLE - read column is modelled` rather than inventing a
  figure. Wall clock, write bytes and allocation are real; read bytes are not.
- **The candidate currently reads ~2x the bytes it needs** (`RD_RATIO` ≈ 0.50 in every row). That is not the
  copy — it is the separate second pass over the finished child Data.db to compute `Digest.crc32`. Folding
  the CRC into the copy loop would roughly halve reads and is the obvious next optimisation.

Same laptop caveat as §5.2: single macOS/arm64 machine, single-threaded, no JMH, warm page cache. Treat the
ratios as indicative and the absolute latencies as meaningless for production. On Linux with a real
`copy_file_range` the copy stage should get cheaper, so the ratio is more likely a floor than a ceiling —
that is reasoning, not a measurement.

### 5.2 Cost comparison — read the labels

The measurements below come from a purpose-built harness (`/tmp/splitbench/*.java`) run against real
SSTables produced by `CQLSSTableWriter` (LZ4, `chunk_length_in_kb: 16`) and opened with this repo's own
compiled classes via `SSTableReader.openNoValidation`. **Caveats that matter:**

- Single **macOS/arm64 laptop**, single-threaded, no JMH, warm page cache, 3–4 iterations, medians.
  Absolute latencies are not production numbers. On macOS `FileChannel.transferTo` is *not*
  `copy_file_range` — so the "kernel-side copy" argument of §3.5 was never exercised in these numbers.
- **S0 was never measured end to end.** The S0 totals are hand-assembled sums of independently timed
  stages (`H + F + A + index`); nobody ran `sstablessplit`. The real S0 additionally pays the
  `CompactionIterator` transformation stack, per-cell `MetadataCollector` stats collection
  (`BigTableWriter.java:220`), per-partition `ColumnIndex`/bloom/summary work, and the rate limiter.
  **S0 is therefore under-stated and the S1 speedup under-stated** — but the "% of S0" table is
  arithmetic over a synthetic denominator and should be read as such.
- The **compress:decompress asymmetry is not reproducible at the originally claimed 4.5–5.5×.**
  Independent re-runs of the same harness gave **3.5–4.5× for the pure codec** and **2.2–3.6× for the
  real pipelines** (writer path vs reader path), and showed the original decompress rates were
  back-derived by subtracting stage C from stage D rather than measured. Use 2–4.5×.
- The **index-pass cost model is a two-point fit with zero degrees of freedom**, across two *different*
  tables, on SSTables that contained **zero promoted-index entries** (all partitions below
  `column_index_size` = 64 KiB) — so the "copy the blob verbatim" branch never executed, and the
  benchmark's child Index.db was in fact invalid (a constant shift made ~1400 positions negative,
  encoded as 9-byte vints). The `~10 ms` "fixed" term is one `fsync` on APFS and recurs per child.

Two deliberately opposite workloads:

| | A "cell-dense" | B "byte-heavy" |
|---|---|---|
| schema | `(k text, c int, v1 text, v2 int, v3 bigint)` | `(k text, c int, v blob)` 4 KiB blobs |
| uncompressed / compressed | 136.6 MiB / 79.2 MiB (ratio 0.580) | 393 MiB / 249 MiB (ratio 0.633) |
| partitions / rows / cells | 200k / 2M / 6M | 20k / 100k / 100k |
| **Index.db / uncompressed data** | **2.79 %** | **0.094 %** |
| Index.db / *compressed* data | 4.81 % | 0.148 % |

Strategy totals (**estimates**, single laptop, S0 synthetic):

| | composition | A | B |
|---|---|---|---|
| **S0** | H + F + A + index | 943 ms (baseline) | 663 ms (baseline) |
| **S1** | copy + digest + index | 125 ms (**7.5× cheaper**) | 260 ms (**2.6×**) |
| **S2** | S1 + decompress-only scan | 174 ms (**5.4×**) | 364 ms (**1.8×**) |
| **S3** | S1 + deserializing scan | 597 ms (1.6×) | 391 ms (1.7×) |
| **S4** | index only | 32 ms (29×) | 12.5 ms (53×) |

Share of the synthetic S0, by stage:

| stage | A | B |
|---|---|---|
| read + decompress + per-chunk CRC | 5 % | 16 % |
| **per-cell deserialize** | **45 %** | 4 % |
| **per-cell re-serialize** | **17 %** | 3 % |
| **LZ4 compress + CRC** | **21 %** | **43 %** |
| write compressed output + fsync | 9 % | 32 % |
| index/filter/summary/stats rebuild | 3 % | 2 % |
| **sum of the three stages a copy skips** | **~83 %** | **~50 %** |

### 5.3 The sharp answer to "is a forced scan any better?"

The question conflates three separable things:

1. **Touching the bytes** (sequential read) is ~5% of S0 and unavoidable in every strategy but S4.
2. **Decompressing** them is 4–16% of S0. LZ4 decompression is the *cheap* direction and CRC32 is
   nearly free (hardware-intrinsified `java.util.zip.CRC32`, `ChecksumType.java:51`).
3. **Deserializing into an object graph, re-serializing, and re-compressing** is 50–83% of S0.

So if a full scan is forced on us — to compute an exact digest, or exact compression accounting — the
stage we are forced into is **(2)**, and that is fundamentally cheap. **S2 costs roughly 18% of S0 for
cell-dense data and 55% for byte-heavy data.** Its advantage shrinks for byte-heavy data not because
decompression got expensive but because the physical write (32% of S0 there) is common to both.

Is compression "the single most expensive stage"? Honestly: *sometimes*. It is the largest single
stage in workload B (43%) and the second largest in A (21%, behind deserialization's 45%). The
defensible general claims are narrower and stronger:

- Compression is the only stage **unavoidable in any rewrite** regardless of workload shape, at 21–43%
  of end-to-end cost.
- Compression is **2–4.5× more expensive than the decompression you may be forced into**, measured on
  the same bytes with this repo's compressor (`LZ4Factory.fastestInstance().fastCompressor()` /
  `safeDecompressor()`, `LZ4Compressor.java:105`, `:110`). This gap grows with `lz4 high` and
  disappears entirely for uncompressed tables or raw-stored chunks.
- A copy avoids compression **in every case, including the worst case where you must decompress
  everything.** That asymmetry is the load-bearing fact.

**Never pay S3.** It buys only 1.6–1.7× over S0 for most of the copy path's complexity, it reintroduces
the per-cell allocation storm that is the main GC argument for the design, and every field it makes
exact is metrics or a compaction heuristic — never correctness. If you cannot inherit those fields,
rescale them by the child's byte or partition share (`EstimatedHistogram.getBuckets` +
the `(offsets, bucketData)` constructor; `TombstoneHistogram.forEach` +
`StreamingTombstoneHistogramBuilder.update(point, value)`), which is ~250 scalar operations per child.
Rescaling assumes the parent's cells-per-partition and tombstone-LDT distributions are uniform across
the key range, which is false for skewed/time-bucketed data — but the error only moves heuristics.

**Treat S2 as the safety net, not the goal.** The only thing genuinely forced is `Digest.crc32`, and
that needs a CRC32 pass (7+ GiB/s, page-cache hot), not a decompress pass.

### 5.4 What a copy-based split LOSES relative to a rewrite

| loss | who is affected | severity |
|---|---|---|
| **No tombstone purge / expired-data drop** | cleanup (`CompactionManager.java:1441`) and anticompaction (`:1796`) use a real `gcBefore`; `sstablessplit` already purges nothing (§2.3) | real for cleanup/anticompaction: reclamation is deferred to the next ordinary compaction. Gate the copy path on `getEstimatedDroppableTombstoneRatio(gcBefore)` — an O(1) check from inherited STATS. |
| **No expired-TTL value drop** | all paths, including `sstablessplit` (`AbstractCell.java:78-96` converts expired cells to value-less tombstones even under `NO_GC`) | real on TTL-heavy tables; magnitude unmeasured |
| **No shadowed-data drop** | **nobody, for a single-source split.** The row-merge listener is `null` without secondary indexes (`CompactionIterator.java:182-185`, `:208-209`), `trivialReduceIsTrivial()` is then true, `MergeIterator` returns `TrivialOneToOne`, and `Row.Merger.merge(activeDeletion)` never runs. `GarbageSkipper` is also inert (`compacting == null` ⇒ empty overlaps). | **none** — this was a false premise in the original framing |
| **Inherited stats reduce read-time pruning** | over-wide `maxTimestamp` disables the early break; inherited `minLocalDeletionTime` makes `mayHaveTombstones()` true; over-wide clustering bounds widen `shouldInclude`; `canUseMetadataLowerBound()` is `!mayHaveTombstones() && !isCompactTable()`, so a single parent tombstone permanently costs every child the lower-bound optimisation | second-order, all in the conservative direction; no wrong answers |
| **Inherited stats persist into future compactions** | `SSTableReader.stats()` returns the STATS-derived `EncodingStats` (`:1953-1958`), which becomes the *next* compaction output's vint encoding base (`SerializationHeader.java:88-99`) | on-disk size regression, not transient |
| **Duplicated boundary chunks** | one 16 KiB chunk per non-aligned boundary; `sum(uncompressedLength())` over children exceeds the parent's by up to `(N−1)(L−1)` | ~0.01–0.03%; a rounding error in compaction sizing |
| **Lost ZCS eligibility** (suffix children only) | bootstrap / replace / rebuild / `nodetool refresh` fan-out falls back to per-section compressed streaming, permanently, until compaction (§3.3) | **this is the biggest real loss and it is avoidable by restricting to prefix children** |
| **Coarse reclamation granularity** | chunk-granular copy can only discard at 16 KiB-of-uncompressed granularity. At 716 B/partition one chunk holds ~23 partitions, so if the owned/unowned alternation period is finer than that, essentially *every* chunk is retained: the copy reclaims ~0% while paying full cost | decisive for fragmented cleanup ranges. Guard: compute `sum(retained chunk bytes) / onDiskLength()` from `getPositionsForRanges` + `getChunksForSections` (both index-only) and fall back to S0 when it exceeds ~0.9 while the owned *partition* fraction is much lower. Free to compute. |
| **Small SSTables** | ~10 ms fixed cost per child (off-heap bloom allocation, summary build, index fsync); chunk granularity means you cannot produce an output smaller than one chunk, and `CompressionMetadata` rejects a zero-chunk file (`:192-193`) | keep S0 below ~10 MB; `StandaloneSplitter.java:187` already skips SSTables under the target size |

And what compaction strategies *do* tolerate, stated affirmatively so it stops being an objection:
**no strategy inspects Data.db bytes.** Strategy input is `onDiskLength()`, `first`/`last`,
`sstableLevel`, `repairedAt`, and STATS. The duplicated boundary chunk is invisible; overlapping stats
are visible only as over-wide min/max and inflated histogram counts, which §4.3 shows are conservative
for every consumer. The one observable effect is that `sum(uncompressedLength())` over children exceeds
the parent's, which perturbs `getExpectedCompactedFileSize` and STCS bucketing by a rounding error.

### 5.5 Write amplification — the part that is oversold

Per split of an SSTable with `C` compressed bytes into `K` outputs:

| | data read | data written | data WA |
|---|---|---|---|
| S0 | `C` | ≤ `C` (garbage dropped) | **1.0** or slightly less |
| S1 / S2 / S3 | `C` | `C` + (K−1) boundary chunks | **1.0 + ~0.01%** |
| S4 | 0 | 0 | **0** |

**S1/S2/S3 are not write-amplification wins. They are CPU wins.** Only zero-byte-movement changes the
bytes-written picture, and §6.5 explains why that option is blocked. Anyone selling a copy-based split
as an SSD-endurance or disk-bandwidth improvement is wrong.

Combined with §2.4, the genuine fleet-scale wins from S1 are: (a) compaction CPU returned to serving
reads — 6M short-lived `Cell` objects and 2M scratch-buffer row copies per 143 MB simply do not happen;
(b) no GC pressure from that allocation (magnitude **UNVERIFIED** — allocation rate was never
measured, only wall clock); (c) no uncompressed-side page-cache pollution; (d) escaping the compaction
rate limiter *only if* the copy path is given its own budget.

**The number nobody computed, and it dominates the ROI.** Anticompaction already metadata-mutates every
fully-contained SSTable with zero byte movement and zero throttle (`CompactionManager.java:842-863`,
`findSSTablesToAnticompact` at `:945-971`, metered separately as `bytesMutatedAnticompaction` at
`:857`), and cleanup drops non-intersecting SSTables without reading them (`:1410-1416`) and skips
fully-contained ones before execute (`:638-653`). So the addressable population for a cheap split is
only **boundary-straddling** SSTables — typically a handful per range boundary per strategy instance.
The 7.5×/29×/53× figures are per-SSTable factors on a population nobody sized. `sstablessplit` and the
fork's `TrimStep` are different: there the whole file is in scope.

---

## 6. What would have to change in this repo

### 6.1 The recommended shape: start with the prefix-only case

There is a strictly smaller change hiding inside the general design — the `i == 0` output. For the
output that retains the *prefix* `[0, hi)` (which is output 0 of any split, and is the **entire
operation** for the common "trim a suffix of the token range" case):

| | general suffix child | prefix-only (`i == 0`) |
|---|---|---|
| Data.db | copy `[O[i], O[j+1])` | copy `[0, O[j+1])` |
| offsets array | rebase all by `−O[i]` | **verbatim; `O'[0] == 0` already** |
| Index.db | re-encode one vint per partition | **verbatim byte prefix, zero re-encoding** |
| first index position | `lo mod L` > 0 | **0** |
| Scrubber/Verifier patch | required (≥3 lines, plus the mis-attribution hazard of §3.4) | **none** |
| dead bytes | leading (unhideable) + trailing | **trailing only, hidden by `dataLength = hi`** |
| duplicated chunk | 1 per boundary | 1 per boundary (identical) |
| `MmappedRegions` | fine | fine |
| **ZCS eligibility** | **lost** (§3.3) | **preserved** (`left = 0`, identity holds) |
| CRC.db (uncompressed) | regenerate wholesale | copy all-but-last entry verbatim, recompute one |
| `dataLength` shape | novel | exactly the sanctioned early-open shape (`Writer.open`) |
| new code | chunk copy + offsets rebase + index rebase + core patches | chunk copy + patch two scalars |

Everything else — Summary/Filter/HLL, STATS policy, DIGEST regeneration, TOC, the `-Data.db.len`
sidecar, the lifecycle transaction, LCS level handling, disk placement — is **shared** between the two.
So the prefix-only step is a strict subset that needs **no core-class patches at all**, preserves
zero-copy streaming, and is verifiable end to end with `nodetool verify`, `scrub`, `sstabledump` and a
real bootstrap. It also composes: extending to suffix children later is purely additive, and by then
you have a working harness to catch the off-by-ones.

### 6.2 Recommended algorithm (pseudocode)

```java
// ---- inputs, from an O(index) walk of the parent's Index.db (no Data.db read) ----
// p[0]=0 < p[1] < ... < p[M-1] : partition-start uncompressed offsets chosen as split points
// p[M] = cm.dataLength         : exact end of the last partition
final CompressionMetadata cm = parent.getCompressionMetadata();
final int  L = cm.chunkLength();            // power of two
final long F = cm.compressedFileLength;     // == physical Data.db length
final int  N = (int) (cm.offHeapSize() / 8);
long O(int k) { return k == N ? F : cm.chunkFor((long) k * L).offset; }   // no new accessor needed

for (each output covering [lo, hi) = [p[s], p[e]))
{
    // ---------- 1. chunk range, INCLUSIVE ----------
    int i = (int) ( lo       / L);
    int j = (int) ((hi - 1L) / L);           // NOT hi/L
    assert i <= j;
    int  C     = j - i + 1;                  // new chunkCount
    long from  = O(i), to = O(j + 1);        // contiguous; includes each chunk's 4-byte CRC
    long Fp    = to - from;                  // new PHYSICAL length -- must be exact
    long Dp    = hi - (long) i * L;          // new dataLength (may end mid-chunk)
    long shift = (long) i * L;               // uncompressed rebase for Index.db  (== 0 for prefix)
    assert Dp >  (long)(C - 1) * L;          // last chunk holds >=1 live byte => mmap maps it
    assert Dp <= (long) C * L;
    // PHASE 1 GATE: only i == 0 is in scope. shift == 0, no index re-encoding, no core patches.

    // ---------- 2. Data.db : verbatim compressed bytes ----------
    for (long done = 0; done < Fp; )         // MUST loop: copy_file_range caps at 0x7ffff000
        done += srcChannel.transferTo(from + done, Fp - done, dstChannel);
    truncateExactly(dstData, Fp);            // no trailing byte, ever (CompressionMetadata:77-80)
    long digest = crc32OfWholeFile(dstData); // 2nd pass, page-cache hot; or fold into a userspace copy

    // ---------- 3. CompressionInfo.db : verbatim prefix + 3 scalars ----------
    // layout: [prefix][i64 dataLength][i32 chunkCount][i64 offsets * N]
    int prefixLen = (int) (srcCompInfoLen - 8 - 4 - 8L * N);   // derivable without parsing
    out.write(srcCompInfoBytes, 0, prefixLen);  // compressor, options, chunkLength, maxCompressedLength
    out.writeLong(Dp);
    out.writeInt(C);
    for (int k = i; k <= j; k++)
        out.writeLong(O(k) - from);             // O'[0] == 0  (MmappedRegions requires it)

    // ---------- 4. Index.db + FILTER + SUMMARY + HLL : one scan of the parent's index range ----
    //   prefix child: this is a verbatim byte-prefix copy; still walk it to build bf/summary/HLL.
    for (each parent index record in [indexPos(p[s]), indexPos(p[e])))
    {
        key      = ByteBufferUtil.readWithShortLength(in);
        position = in.readUnsignedVInt();       // absolute into parent's Data.db
        blobSize = in.readUnsignedVInt();
        blob     = in.read(blobSize);           // promoted index: all offsets partition-relative

        long newIndexPos = idxOut.position();
        ByteBufferUtil.writeWithShortLength(key, idxOut);
        idxOut.writeUnsignedVInt(position - shift);   // ONLY rewritten field; canonical vint, NEVER padded
        idxOut.writeUnsignedVInt(blobSize);
        idxOut.write(blob);

        DecoratedKey dk = partitioner.decorateKey(key);
        bf.add(dk);                                   // 1 murmur3_x64_128
        summary.maybeAddEntry(dk, newIndexPos);       // NEW offset, not the parent's
        hll.offerHashed(MurmurHash.hash2_64(key, ...));
        sizeHist.add(nextPosition - position);        // exact estimatedPartitionSize (delta)
    }

    // ---------- 5. everything else ----------
    // FILTER   : saveBloomFilter(d, bf)
    // SUMMARY  : saveSummary(d, minimal(first), minimal(last), summary.build(partitioner))
    // STATS    : inherit parent's Statistics.db, then MetadataSerializer.mutate to patch
    //            sstableLevel + repairedAt/pendingRepair/isTransient; optionally overwrite
    //            estimatedPartitionSize + COMPACTION HLL with the exact values above, and
    //            rescale estimatedTombstoneDropTime / estimatedCellPerPartitionCount by share.
    //            HEADER + min* + commitLogIntervals/originatingHostId: VERBATIM, mandatory.
    // DIGEST   : write `digest` as decimal ASCII
    // CRC.db   : uncompressed only -- prefix: copy header + entries[0..n-2], recompute the last
    // TOC.txt  : appendTOC(descriptor, components)  -- MUST list FILTER and SUMMARY
    // (fork)   : regenerate the `-Data.db.len` sidecar if the child will be S3-resident
}
```

Build-time assertions, all cheap, that catch every off-by-one:
`child.getPosition(child.first, EQ).position == lo - shift` and `< L`;
`child.uncompressedLength() == Dp`; `new File(childData).length() == Fp`; a fresh `SSTableId` per child
(§6.3); and in debug mode a read-back comparing every partition of the child against the parent.

### 6.3 Lifecycle transaction, SSTableId, and caches — the plumbing nobody designed

**Transaction.** `LifecycleTransaction.trackNew(SSTable)` → `LogTransaction.trackNew` → `LogFile.add`
(`LifecycleTransaction.java:570-573`, `LogTransaction.java:142-150`). The record is built from
`table.getAllFilePaths().size()` and the max `lastModified` of the files already on disk
(`LogRecord.java:153-156`, `:190-192`), and on restart `LogFile.verifyRecord` compares both against
disk. So **a child's component set must be complete and stable at track time**; a raw-`FileChannel`
splitter that writes files outside an `SSTable`-shaped handle has no way to register them. The natural
vehicle is `BigTableZeroCopyWriter` (already an `SSTable` + `SSTableMultiWriter`, and already
synthesizes TOC.txt at `:150-158`), extended to accept `(Component.Type, FileChannel, offset, length)`
— today `writeComponent` is whole-file only (`:219`) and `ComponentContext.channel` asserts
`size == channel.size()` (`:88`). Abort is `untrackNew` → `LogFile.remove` →
`deleteRecordFiles(LogRecord.getExistingFiles(absolutePath))` (`:373-383`) — **glob by descriptor base
path**, which is also why hard-link variants "work": deletion is by *name*, so obsoleting the parent
unlinks only the parent's names and the shared inode survives.

**Announce it as one notification.** The split must emit
`SSTableListChangedNotification(added = children, removed = {parent})` through the Tracker. Do **not**
model it on `SingleSSTableLCSTask` (`:85-99`) or on `mutateFullyContainedSSTables`' `txn.cancel(...)`
(`CompactionManager.java:861`) — those are 1→1 shapes and would skip the only 1→N-aware code path.
Which matters twice on this branch:

- `ImportGroupLeveledCompactionStrategy.replaceSSTables` already implements the exact 1→N fan-out
  (`:332-353`: remove the parent's `import_group_membership` rows, `persistMember` for every output),
  reached via `CompactionStrategyManager.handleListChangedNotification` (`:752-758`, `:818-822`).
  Membership is durable per-SSTable state keyed by `sstable.descriptor.id.toString()`
  (`SystemKeyspace.java:454-464`, `:2092-2110`; map at `IGLCS.java:69`, `:137-139`). Skip the
  notification and children silently land in the main group. Note the same-group branch is guarded on
  *all* removed SSTables being in the *same* group and on `importGroups.get(id) != null` — if that set
  is absent the branch no-ops and the children reach **no manifest at all**.
- `PendingImportRegistry.Propagator` (`:204-232`) reacts **only** to `SSTableListChangedNotification`
  and is what makes outputs inherit the pending-import flag. That flag excludes still-uncoordinated
  imported SSTables from repair validation (`CassandraValidationIterator.java:154`) and anticompaction
  (`PendingAntiCompaction.java:206`). Splitting a pending-group SSTable without the notification lets
  repair validate the children and anticompaction mark them repaired while the cross-node import
  decision is still open — divergent repaired sets between replicas. That is a correctness hazard, not
  just a grouping one.

**SSTableId is a correctness requirement, not a formality.** The saved key cache is serialised keyed by
`SSTableId`, not by `Descriptor`:

```java
if (key.desc.id instanceof SequenceBasedSSTableId) out.writeInt(((SequenceBasedSSTableId) key.desc.id).generation);
else { out.writeInt(Integer.MIN_VALUE); ByteBufferUtil.writeWithShortLength(key.desc.id.asBytes(), out); }
```
(`src/java/org/apache/cassandra/service/CacheService.java:432-440`), and on load it is re-resolved
against live readers by `ssTableReader.descriptor.id` (`:460-480`). So **every child must take a fresh
id from `cfs.newSSTableDescriptor(...)`**; reusing or hand-minting one can resurrect a stale
`RowIndexEntry` — which carries an **absolute** position — against a rebased file. Related:
`SSTableReader.GlobalTidy` restores each SSTable's read meter from
`SystemKeyspace.getSSTableReadMeter(ks, cf, desc.id)` and persists it every five minutes
(`:2166-2178`), and `SSTableTidier` clears the parent's row on delete (`LogTransaction.java:383`), so
children start with cold `RestorableMeter`s. `LeveledGenerations` also tie-breaks non-L0 ordering on
`SSTableIdFactory.COMPARATOR` (`:77-82`), so id assignment is observable in manifest ordering.

**Caches.** `KeyCacheKey.equals/hashCode` include `desc`
(`src/java/org/apache/cassandra/cache/KeyCacheKey.java:60-83`), so in-memory aliasing is impossible —
the on-disk id requirement above is the real one. **The row cache is a non-issue**: `RowCacheKey` is
`(tableId, indexName, key bytes)` (`RowCacheKey.java:39-58`) with no descriptor and no position, so
there is nothing to invalidate. State that explicitly so it stops being re-litigated. `ChunkCache.Key`
*is* `(path, reader class, position)` (`ChunkCache.java:54-75`), so any variant sharing bytes under two
paths caches them twice; `ChunkCache.invalidateFile(String)` (`:194`) is the tool an in-place-mutation
variant would need.

**Lock.** Take `SSTableReader.runWithLock` (`synchronized (tidy.global)`, `:777-783`) around any
snapshot of the parent's STATS/SUMMARY/CompressionInfo. Precedent and rationale are already in the
tree: `CassandraOutgoingFile.java:155-166` takes it with the comment *"Acquire lock to avoid concurrent
sstable component mutation because of stats update or index summary redistribution, otherwise file
sizes recorded in component manifest will be different from actual file sizes."* This is a one-line
requirement, not an open question.

### 6.4 Compaction-strategy concerns

**LCS levels.** Inheriting `sstableLevel` is correct: K disjoint, adjacent children do not overlap, and
`LeveledManifest.replace` removes the parent *before* adding the children (`:142-163`), so the parent
cannot be mistaken for an overlapping neighbour. **The trap:** if children are surfaced to the strategy
in a *different* notification from the parent's removal — exactly what happens if you `trackNew` +
notify-added before `obsoleteOriginals`, or if you use the `SingleSSTableLCSTask` shape — then every
child overlaps the still-present parent:

```java
SSTableReader after = level.ceiling(sstable);
SSTableReader before = level.floor(sstable);
if (before != null && before.last.compareTo(sstable.first) >= 0 ||
    after  != null && after.first.compareTo(sstable.last)  <= 0)
    sendToL0(sstable);
```
(`src/java/org/apache/cassandra/db/compaction/LeveledGenerations.java:154-161`; `sendToL0` calls
`sstable.mutateLevelAndReload(0)` — an **on-disk STATS rewrite** — at `:175-189`.) All K children get
demoted to L0 and their `Statistics.db` rewritten, silently destroying the inherited level and forcing
a re-levelling storm. Secondary notes: `addAll` asserts `sstable.getSSTableLevel() < levelCount()`
(`:121`) and throws under `strictLCSChecksTest` when manifest and metadata disagree (`:129`); and
`replace` slams the round-robin cursor (`LeveledManifest.java:162`).

**Disk boundaries — and why hard links cannot split the interesting case.**
`DiskBoundaries.getDiskIndex(sstable)` is a binary search on **`sstable.first`** (`:104-114`), consumed
by `CompactionStrategyManager.compactionStrategyIndexFor` (`:370-386`), with a directory-based fallback
(`:118-129`). So:

1. Splitting an SSTable that straddles a disk boundary produces children in *different* strategy
   indices, and each child must be **written into the data directory matching its own `first`** or the
   index and the directory fallback disagree. `AbstractStrategyHolder.GroupedSSTableContainer.add`
   buckets `added` and `removed` independently, and `CompactionStrategyHolder.java:175-181` then turns
   a cross-index case into `replaceSSTables(removed_i, {})` on the parent's instance plus a bare
   `addSSTables(added_j)` on another — which loses import-group membership even with a perfectly
   announced notification, because each IGLCS instance owns a private `sstableToGroup`.
2. `FileUtils.createHardLink` is `Files.createLink` (`:182`), which fails `EXDEV` across filesystems.
   **Therefore hard-link variants cannot place a child in a different data directory than the parent,
   i.e. they are structurally unable to split a boundary-straddling SSTable** — the only population
   anticompaction and cleanup need.

**Snapshots and incremental backups.** `Tracker.maybeIncrementallyBackup` hard-links **every** newly
added SSTable into `backups/` when `incremental_backups` is on (`:409-419`, via `addSSTablesInternal`).
For S1 that backs up the duplicated boundary chunks (negligible). For a shared-inode variant it means
the parent inode acquires *another* name under `backups/`, **pinning the parent's full bytes
indefinitely** and defeating even the "reclaim on child compaction" story; restoring that backup
produces a child whose Data.db is the entire parent file. Snapshot accounting is name-based —
`Directories.SSTableSizeSummer.isAcceptable` skips files whose **name** is in the live set
(`:1324-1341`) and `createLinks` preserves `sourceFile.name()` — so a hard-link child under a different
name is not recognised as shared and snapshot "true size" over-reports by up to K×. Java exposes no
`st_blocks`, so this is unfixable without `stat(2)` via JNA. Also: any pre-existing snapshot pinning the
parent breaks option (b)'s "space is properly reclaimed" claim until the snapshot is cleared — equally
true today, but do not state reclamation unconditionally.

### 6.5 Why the hard-link / shared-inode family is dropped

Four independent blockers, not one:

1. **ZCS over-streams the entire parent.** For a hard-linked prefix child `left = 0` and
   `right = uncompressedLength() = hi`, so `contained()` returns **true** and ZCS engages — but
   `ComponentManifest.create` sizes each component by `file.length()` (`:59-63`), and the child's
   Data.db *is* the parent inode. The sender ships all K children's bytes; the receiver writes them,
   reads correctly (short `dataLength`, full offsets array, still-valid parent DIGEST), and
   `nodetool verify` passes. **Silent K× network and disk amplification, undetectable.** The very
   property listed as an advantage — the parent's whole-file digest staying valid — is what hides it.
2. **`onDiskLength()` is unfixably the whole parent.** `FileHandle.Builder.complete()` sets it from
   `compressionMetadata.compressedFileLength`, which is `new File(dataFilePath).length()`. That feeds
   `SSTableReader.onDiskLength()` (`:1478-1481`), STCS bucketing, `getExpectedCompactedFileSize`, the
   compression-ratio gauge (`TableMetrics.java:1355`), and `bytesOnDisk()`. Shortening `dataLength`
   does not help — an on-disk CompressionInfo cannot carry a truncated offsets array, because
   `Writer.open`'s compensating `compressedLength = tOffsets.getLong(tCount*8)` (`:435-436`) is
   in-memory only and the reopen path always takes the physical file length.
3. **Incremental backup pins the shared inode** (§6.4).
4. **Hard links cannot cross data directories**, so the boundary-straddling case is unreachable (§6.4).

Plus the accounting problem that was already known: `SSTable.bytesOnDisk()` sums `File.length()` per
component (`:301-309`) and feeds `StorageMetrics.load`, `liveDiskSpaceUsed` and `totalDiskSpaceUsed`
(`Tracker.java:183-187`), inflating them ~K×, while `FileStore.getUsableSpace()` guardrails
(`Directories.java:665`) see the truth — two contradictory sets of metrics. And space is reclaimed only
when the last link is unlinked, so for the cleanup/trim use case (`TrimStep.java:113` →
`forceCleanup`), whose entire purpose is discarding a token range, a shared-inode split frees **zero**
bytes.

For the record, the mechanism itself is elegant and the earlier analysis of it was right: no
`SSTableReader` start-bound is needed, because physically trimming Index.db plus a short `dataLength`
fully expresses the window, and both halves already exist as in-tree mechanisms (`cloneWithNewStart` /
`OpenReason.MOVED_START` at `:918-933`, `:1077-1083`, and `CompressionMetadata.Writer.open` at
`:424-439`). So the earlier claim that "no on-disk representation of a logical start bound exists" is
also wrong — 3a *is* that representation. It just does not survive the four blockers above.

**If a zero-byte-movement anticompaction path is wanted, the right lever is widening
`mutateFullyContainedSSTables`' eligibility** — e.g. choosing repair range boundaries that align to
SSTable `first`/`last` — not sharing inodes.

### 6.6 Ordered implementation plan, with the risk of each step

**Phase 1 — prefix-only truncation in offline `sstablessplit` and the fork's `TrimStep`.**
*Risk: low.* No core-class patches. `sstablessplit` is offline-only
(`StandaloneSplitter.java:157`), already takes a hard-linked pre-split snapshot (`:136`), and already
purges nothing (§2.3), so a copy is a semantic no-op — the ideal place to prove correctness. New code:
(1) a chunk-subrange component writer (extend `BigTableZeroCopyWriter` or add `BigTableSplitWriter`);
(2) CompressionInfo.db emission with a verbatim header prefix + `dataLength`/`chunkCount`; (3) the index
pass producing Index.db (verbatim prefix), Filter.db, Summary.db, exact `first`/`last`, exact
`estimatedPartitionSize`, HLL; (4) STATS via `MetadataSerializer.mutate`. Modified:
`SSTableSplitter.java` (copy path alongside the `CompactionTask` path), `StandaloneSplitter.java`
(flag), `TrimStep.java`. Wrap it in a real `LifecycleTransaction` with `trackNew` per child and a
single `removed={parent}, added={children}` notification (§6.3). For a suffix-discard trim this is the
whole feature.

**Phase 2 — measure before building more.** *Risk: none; highest information value.* Size the
boundary-straddling SSTable population for anticompaction and cleanup, and measure the real
Index.db : on-disk-Data.db ratio on production tables. If the population is small, **stop** — the
remaining work is not worth its risk.

**Phase 3 — suffix children (option (b) proper), gated.** *Risk: medium-high.* Requires: the ≥3-line
`Scrubber` fix **including** seeding `nextPartitionPositionFromIndex`; the `Verifier` fix; a build-time
assertion that `shift` equals the parent offset of the child's first key and that `position >= shift`;
CRC.db regeneration for uncompressed tables; and **an explicit, documented decision to accept losing
ZCS eligibility** (§3.3) or a design for a `first`-relative logical length. Gate on: compressed table,
`nb` version, no secondary indexes, size ≥ ~10 MB, fragmentation guard (§5.4), droppable-tombstone
guard (§5.4). Immediate consumers: `doCleanupOne` (`CompactionManager.java:1399-1472`) for
partially-overlapping SSTables, and anticompaction's slow path.

**Phase 4 — reflink.** *Risk: low value, medium cost. Defer.* On JDK 21 `transferTo` already
reflinks for free on xfs-reflink/btrfs when both offsets are 4096-aligned — which is true for the
*first* output only. Anything more needs a JNA `FICLONERANGE` shim (JNA 5.9.0 is already a
compile+runtime dependency at `build.xml:773`, and `NativeLibraryLinux` + `NativeLibrary.getfd`
already provide the pattern), a front-padded destination, and the `MmappedRegions.updateState` fix.
Whether production volumes are even reflink-capable is unknown (§7).

**Explicitly not planned: S3, and the hard-link family (§6.5).**

---

## 7. Open questions and things we could not verify

Everything in §§3–4 is from reading this tree. **No code was run, and no SSTable with a dead chunk
prefix was ever built, opened, scanned, scrubbed, verified, or streamed.** The benchmark numbers in §5
came from a separate harness whose limits are stated inline there.

### Unverified, ordered by how much they change the plan

1. **Is production ext4 or XFS-with-reflink=1?** Nothing in this repo (`build.gradle`, `conf/`,
   `.build/`) pins or probes the filesystem. This decides whether `transferTo` is a real copy or a free
   reflink, and hence whether Phase 4 and the `MmappedRegions` front-padding fix are worth anything.
   **UNVERIFIED.**
2. **The real Index.db : Data.db ratio on Netflix production tables.** There is no constant, comment or
   metric in this repo that bounds it. The formula is roughly
   `(2 + keylen + vint(position) + 1) / avgPartitionBytes` for partitions under `column_index_size`,
   plus ~one `IndexInfo` per 64 KiB above it. This repo's own test data contains a counterexample:
   `test/data/legacy-sstables/nb/legacy_tables/legacy_nb_clust/` has Data.db = 8,749 bytes and
   **Index.db = 157,553 bytes — 18×** — because each `IndexInfo` stores two full 1200-character
   clustering prefixes under a 4 KiB `column_index_size`. So "Index.db is a small percentage" is a
   common case, not an invariant. **UNVERIFIED for production.** A one-off `du` over a few tables
   replaces the entire estimate with a fact.
3. **The boundary-straddling SSTable population** for anticompaction and cleanup (§5.5). This dominates
   the ROI and nobody has counted it. **UNVERIFIED.**
4. **Is anticompaction/cleanup actually throttle-bound in production?**
   `CompactionTask.totalThrottleTimeSeconds` (`:212`, `:281-285`) is recorded but was never inspected.
   If nodes routinely raise or disable `compaction_throughput`, the CPU savings convert directly to
   wall clock and the priority ordering changes. **UNVERIFIED.**
5. **How much garbage do cleanup and anticompaction actually collect today** via their real `gcBefore`
   (`CompactionManager.java:1441`, `:1796`)? If a few percent, the copy path is nearly free
   semantically; if substantial on repaired TWCS-shaped tables, the droppable-tombstone gate matters a
   lot. **UNVERIFIED.**
6. **Whether the `MmappedRegions.updateState` fix is sufficient.** Seeding
   `lastSegmentOffset = metadata.chunkFor(0).offset` looks like a one-liner and keeps
   `state.length == compressedFileLength`, but sufficiency across streaming, digest, and `onDiskLength`
   consumers — and the existing assertion at `MmappedRegionsTest.java:335` — was not audited.
   **UNVERIFIED.** (Only needed for Phase 4.)
7. **Whether `ThreadLocalReadAheadBuffer` over-reads safely at the new file's EOF.** Used by
   `CompressedChunkReader.ScanCompressedReader` (`:150-209`) when
   `compressed_read_ahead_buffer_size > chunkLength`. Structurally identical to a normal SSTable (which
   also ends exactly at its last chunk's CRC), but `ThreadLocalReadAheadBuffer.read` was not read.
   **UNVERIFIED.**
8. **Whether the CRC.db prefix trick composes exactly.** §3.7 derives that a prefix child can copy all
   but the last CRC entry verbatim and recompute the short final one, from
   `bufferSize = validator.chunkSize` (`CassandraStreamWriter.java:89`) and
   `minReadable = min(bufferSize, proxy.size() - start)` (`:149`). Derived from code, **not tested.**
9. **Whether SASI schedules a rebuild for a newly-appearing SSTable without `SI_*.db`**, or silently
   serves queries with a missing index. `SASIIndexBuilder` can rebuild at `O(Data.db)`, but the
   `ColumnIndex.init` / `DataTracker.update` trigger path was not traced. Also unknown: whether any
   deployed Netflix schema uses SASI at all. **UNVERIFIED.**
10. **Whether `test/unit` encodes the offset-0 assumption** in ways the Scrubber/Verifier patch would
    break (`ScrubTest`, `VerifyTest`, `SSTableReaderTest`). The test tree was not audited.
    **UNVERIFIED.**
11. **Whether a rescaled `EstimatedHistogram` / `TombstoneHistogram` round-trips faithfully.** The
    `(long[] offsets, long[] bucketData)` constructor exists (`EstimatedHistogram.java:84`) and
    `StreamingTombstoneHistogramBuilder.update(point, value)` exists (`:99`), but neither
    `isOverflowed()`/`clearOverflow()` interaction with a scaled overflow bucket nor re-feeding
    already-rounded bin points was checked. **UNVERIFIED.**
12. **Whether `IndexSummaryRedistribution` degrades with many small summaries.** It operates on a fixed
    heap-fraction pool (`index_summary_capacity`, 5% of heap); a K-way split multiplies the number of
    summaries. **UNVERIFIED.**
13. **GC/allocation-rate impact.** Only wall clock was measured. The claim that eliminating ~6M `Cell`
    allocations per 143 MB meaningfully improves read latency on a loaded node is structurally sound
    (`UnfilteredSerializer.java:645`) but **UNVERIFIED**.
14. **Whether the Netflix `BackupManifest` needs per-child registration beyond the directory listing**,
    and whether any fork-local code under `src/java/com/netflix/cassandra/` reads `StatsMetadata` with
    assumptions beyond the stock consumers. Only `TableHLL` and `PartitionHistogramTable` were spot
    checked. **UNVERIFIED.**

### Experiments that would settle it, cheapest first

| # | experiment | settles |
|---|---|---|
| 1 | `du` Index.db vs Data.db across a handful of production tables | (2); decides whether the whole idea is worth building |
| 2 | Count boundary-straddling SSTables during one real cleanup and one real anticompaction (log it from `findSSTablesToAnticompact` / `doCleanupOne`) | (3); decides Phase 3 |
| 3 | **Run `sstablessplit` under a profiler on a real SSTable.** Nobody has done this. It replaces the synthetic S0 baseline of §5.2 with a measurement and gives the allocation/GC numbers | (13), the whole §5.2 denominator |
| 4 | `stat -f` / `mount` on a production data volume; a 1-block trial `FICLONERANGE` via a throwaway JNA shim | (1); decides Phase 4 |
| 5 | Read `CompactionTask.totalThrottleTimeSeconds` off a node doing anticompaction | (4); decides whether the pitch is CPU or latency |
| 6 | **Unit test: build a compressed SSTable, produce a prefix child (`i == 0`), and assert** a full `BigTableScanner` walk returns the expected partitions; a point read of a wide partition works (exercises `ShallowIndexedEntry.openWithIndex` and the canonical-vint requirement); `nodetool verify` (default and `-e`) passes; `nodetool scrub` passes; `sstabledump` matches; and `computeShouldStreamEntireSSTables()` returns **true** | Phase 1 correctness end to end, plus (8) |
| 7 | Same test at a deliberately **chunk-aligned** boundary (`lo mod L == 0`) and at a boundary where `i == j` (all partitions inside one chunk) | the degenerate paths |
| 8 | Same test for a **suffix** child, additionally asserting the Scrubber/Verifier patch, that `computeShouldStreamEntireSSTables()` is false, and that a bootstrap of the table still succeeds | Phase 3 correctness and the ZCS regression's real cost |
| 9 | Split a table with an active repair session in `pendingGroups` and assert children retain the pending-import flag and the import group | §6.3's IGLCS / `PendingImportRegistry` hazards |
| 10 | Split an SSTable that straddles a disk boundary and assert both children land in the correct data directory and the correct strategy index | §6.4 |

