# Remote Import Operations Runbook

This runbook provides operational guidance for Netflix's Remote SSTable Import system, including common failure scenarios, troubleshooting procedures, and performance tuning.

## Table of Contents
1. [Nodetool `remoteimport` Command](#nodetool-remoteimport-command)
2. [Common Failure Scenarios](#common-failure-scenarios)
3. [Monitoring and Alerting](#monitoring-and-alerting)
4. [Performance Tuning](#performance-tuning)
5. [Capacity Planning](#capacity-planning)
6. [Troubleshooting Guide](#troubleshooting-guide)
7. [JMX Operations](#jmx-operations)
8. [Emergency Procedures](#emergency-procedures)

---

## Nodetool `remoteimport` Command

The `nodetool remoteimport` command is the primary operator entry point. It
talks to the local `ImportJobManager` MBean over JMX and exposes every
hot-tunable property plus live job state.

```bash
# List active import jobs on this node
nodetool remoteimport status

# Detailed status for one job (same map exposed via netflix_views.local_import)
nodetool remoteimport status <jobId>

# Dump every hot-tunable config value
nodetool remoteimport getconfig

# Tune any single hot property at runtime — names match `getconfig` output
nodetool remoteimport setconfig import_concurrency 8
nodetool remoteimport setconfig import_http_retry_max_attempts 5
nodetool remoteimport setconfig import_disk_throughput_bytes_per_sec 209715200  # 200 MiB/s

# Cancel a single in-flight job
nodetool remoteimport cancel <jobId>

# Reap orphaned in-memory jobs and orphaned `imports/<jobId>/` dirs
nodetool remoteimport cleanup
```

Valid keys for `setconfig` (these are the same names returned by `getconfig`):

| Key | Type | Notes |
|-----|------|-------|
| `import_concurrency` | int | Resizes the unzip thread pool as a side effect |
| `import_max_disk_percentage` | int (0-100) | Disk-usage ceiling for staging |
| `import_disk_throughput_bytes_per_sec` | double | Disk write rate limit |
| `import_http_retry_max_attempts` | int | |
| `import_http_retry_initial_delay_ms` | int | |
| `import_http_retry_backoff_multiplier` | double | |
| `import_http_retry_max_delay_ms` | int | |
| `import_http_retry_jitter_percentage` | int (0-100) | |
| `import_cleanup_initial_delay_seconds` | int | Reschedules the cleanup task |
| `import_cleanup_period_seconds` | int | Reschedules the cleanup task |
| `import_cleanup_min_age_seconds` | int | |

Run on every node you want to tune — `setconfig` updates state on the local
node only, just like the cassandra.yaml values it shadows.

---

## Common Failure Scenarios

### 1. Import Stuck in STAGED State

**Symptoms:**
- Import job remains in STAGED state for extended period
- Some nodes show STAGED while others haven't reached it
- No progress for 15+ minutes

**Cause:**
- One or more nodes failed to download/extract SSTables
- Network partition preventing coordination
- Node crashed during download phase

**Recovery Procedure:**
```sql
-- 1. Check status on all nodes
SELECT * FROM netflix_views.cluster_view
WHERE keyspace_name = 'netflix_views' AND table_name = 'local_import';

-- 2. Identify stuck/failed nodes
-- Look for nodes with ERROR status or different states

-- 3. Check per-job state on a stuck node:
--    `nodetool remoteimport status` to list jobs on that node
--    `nodetool remoteimport status <jobId>` for the full status map
--    Logs: grep "ImportJob" /var/log/cassandra/system.log

-- 4. If recoverable, reset the job (resets failed nodes to VALIDATING)
UPDATE system_distributed.remote_import
SET state = 'staging'
WHERE id = <UUID> AND target_keyspace = '<ks>' AND target_table = '<tbl>';

-- 5. If not recoverable, cancel the import
DELETE FROM system_distributed.remote_import WHERE id = <UUID>;
```

**Prevention:**
- Per-step timeouts are defined in code via `ImportStep.timeoutMillis()`; default is `ImportJob.DEFAULT_TIMEOUT_MS` (1 hour)
- Ensure all nodes can reach HTTP source
- Monitor network connectivity between nodes

---

### 2. HTTP Download Failures

**Symptoms:**
- Import enters ERROR state during DOWNLOADING
- Metrics show high `downloadRetries` or `httpStatusErrors`
- Error message contains "HTTP", "connection", or "timeout"

**Common Causes:**
- S3 signed URL expired
- Network connectivity issues
- HTTP server overloaded or rate limiting
- Firewall blocking outbound connections

**Recovery Procedure:**
```bash
# 1. Verify source URL is accessible
curl -I "http://source-url/sstable.zip"

# 2. Check HTTP retry configuration
nodetool remoteimport getconfig | grep import_http_retry
# Defaults:
#   import_http_retry_max_attempts        3
#   import_http_retry_initial_delay_ms    1000
#   import_http_retry_backoff_multiplier  2.0
#   import_http_retry_max_delay_ms        30000
#   import_http_retry_jitter_percentage   10

# 3. Generate new signed URL if expired (for S3)
# Update the sources in remote_import table with new URL

# 4. Restart import with reset (resets failed nodes to VALIDATING)
UPDATE system_distributed.remote_import
SET state = 'staging'
WHERE id = <UUID> AND target_keyspace = '<ks>' AND target_table = '<tbl>';
```

**Prevention:**
- Generate S3 signed URLs with sufficient expiry time (recommend 24+ hours)
- Configure appropriate retry settings for your network environment
- Tune at runtime with `nodetool remoteimport setconfig` — e.g.
  `setconfig import_http_retry_max_attempts 5`,
  `setconfig import_http_retry_max_delay_ms 60000`

---

### 3. Disk Space Exhaustion

**Symptoms:**
- Import fails with "disk space" error
- Metrics show `diskSpaceErrors` incrementing
- Import stuck in DOWNLOADING or never starts

**Cause:**
- Insufficient disk space for staging directory
- Multiple concurrent imports exhausting disk
- Orphaned staging directories not cleaned up

**Recovery Procedure:**
```bash
# 1. Check disk space on affected node
df -h /path/to/cassandra/data

# 2. Check import staging directories (located in table data dirs)
# Staging path: <table_data_dir>/imports/<job_id>/
# Example: /var/lib/cassandra/data/mykeyspace/mytable-abc123/imports/uuid/
du -sh /var/lib/cassandra/data/*/*/imports/

# 3. Clean up orphaned staging directories
nodetool remoteimport cleanup
# (runs both orphaned-job cleanup and orphaned-directory cleanup)
# Or manually: find /var/lib/cassandra/data/*/*/imports/ -type d -mtime +1 -exec rm -rf {} \;

# 4. Adjust disk usage threshold if needed (default 75)
nodetool remoteimport setconfig import_max_disk_percentage 70
# Lower value = more conservative, higher value = more aggressive

# 5. Reduce concurrent imports or check what's running
nodetool remoteimport status
nodetool remoteimport setconfig import_concurrency 2
```

**Prevention:**
- Monitor disk usage on Cassandra data drives
- Set appropriate `import_max_disk_percentage` (recommend 70-80%)
- Schedule periodic cleanup: automatic cleanup runs on the `import_cleanup_period` interval (default 1h)
- Note: Staging space comes from table data directories, not a separate staging area

---

### 4. Import Timeout

**Symptoms:**
- Import enters ERROR state after long period
- Error message contains "timed out"
- Import was progressing but stopped

**Cause:**
- Large SSTables taking longer than timeout
- Slow network download
- S3 throttling
- Slow disk I/O during import phase

**Recovery Procedure:**
```bash
# 1. Check which step timed out from error message
# Each step defines its own timeout via ImportStep.timeoutMillis()

# 2. Adjust per-step timeouts in code (no yaml knob today)
#    Default is ImportJob.DEFAULT_TIMEOUT_MS (1 hour); override
#    timeoutMillis() on the specific step that is timing out.

# 3. Restart import after the code change ships and node restarts.

# 4. Consider splitting large imports into smaller chunks
```

**Prevention:**
- Set per-step timeouts based on expected import size and network speed
- For large imports (>10GB), raise the download/unzip step timeouts to 2-4 hours
- Monitor import duration metrics to establish baselines

---

### 5. Partitioner Mismatch

**Symptoms:**
- Import immediately fails in VALIDATING state
- Error message: "Partitioner mismatch"

**Cause:**
- Source SSTables created with different partitioner than target cluster

**Recovery Procedure:**
```bash
# 1. Verify source SSTable partitioner
# Check SSTable metadata or creation environment

# 2. Verify target cluster partitioner
SELECT partitioner FROM system.local;

# 3. Options:
#    a) Recreate source SSTables with correct partitioner
#    b) Import into cluster with matching partitioner

# No recovery possible - must address partitioner mismatch at source
```

**Prevention:**
- Document and validate partitioner before generating SSTables
- Add partitioner validation to SSTable generation pipeline
- Include partitioner in SSTable metadata/naming

---

## Monitoring and Alerting

### Critical Metrics

Monitor these metrics via Spectator/Grafana:

**Job Lifecycle:**
- `ImportJob.JobsStarted` - Counter of initiated imports
- `ImportJob.JobsCompleted` - Counter of successful imports
- `ImportJob.JobsFailed` - **ALERT** if rate increases
- `ImportJob.JobsCancelled` - Track manual cancellations

**Error Metrics:**
- `ImportJob.NetworkErrors` - **ALERT** if > 0
- `ImportJob.DiskSpaceErrors` - **ALERT** if > 0
- `ImportJob.TimeoutErrors` - **WARNING** if > 0
- `ImportJob.HttpErrors` - **ALERT** if rate > 5/hour
- `ImportJob.FileCorruptionErrors` - **CRITICAL** if > 0

**Resource Metrics:**
- Active job count via JMX
- Disk usage on staging directory
- Network throughput during downloads
- Memory usage during imports

### Recommended Alerts

```yaml
# Example alert thresholds

# Critical Alerts (Page on-call)
- ImportJob.JobsFailed rate > 1 per 10 minutes
- ImportJob.DiskSpaceErrors > 0
- ImportJob.FileCorruptionErrors > 0
- Active imports stuck for > 2 hours

# Warning Alerts (Ticket)
- ImportJob.TimeoutErrors rate > 1 per hour
- ImportJob.DownloadRetries rate > 10 per hour
- Import duration > 95th percentile baseline
- Disk usage on staging dir > 85%
```

---

## Performance Tuning

### Configuration Parameters

Located in `cassandra.yaml`:

```yaml
# Concurrency Settings
import_concurrency: 4
# Number of concurrent download/unzip operations
# Recommendations:
#   - Small instances (< 8 CPU): 2
#   - Medium instances (8-16 CPU): 4
#   - Large instances (> 16 CPU): 8
# Impact: Higher = faster imports but more CPU/memory usage

# Rate Limiting
import_disk_throughput: 64MiB/s
# Disk write rate limit (DataRateSpec — accepts e.g. 50MiB/s, 100MiB/s, 1GiB/s)
# Recommendations:
#   - SSD: 100MiB/s - 200MiB/s
#   - Cloud (EBS): 50MiB/s - 100MiB/s
# Impact: Prevents import from saturating disk I/O

# Disk Space Management
import_max_disk_percentage: 75
# Maximum disk usage percentage before rejecting imports
# Recommendations:
#   - Production: 70-80%
#   - Development: 85-90%
# Impact: Lower = more safety margin, higher = more utilization

# HTTP Retry Settings
import_http_retry_max_attempts: 3
import_http_retry_initial_delay: 1000ms
import_http_retry_backoff_multiplier: 2.0
import_http_retry_max_delay: 30000ms
import_http_retry_jitter_percentage: 10
# Adjust for network reliability

# Cleanup Task Settings
import_cleanup_initial_delay: 1h
import_cleanup_period: 1h
import_cleanup_min_age: 1d
import_cleanup_max_retries: 3
import_cleanup_retry_initial_delay: 300s
# Controls how often orphaned jobs/directories are reaped

# Timeouts
# Per-step timeouts are defined in code (ImportStep.timeoutMillis()).
# Default is ImportJob.DEFAULT_TIMEOUT_MS (1 hour). No yaml knob today.
```

### Performance Guidelines

**For Small Imports (< 1 GB):**
```yaml
import_concurrency: 2
import_disk_throughput: 50MiB/s
```

**For Medium Imports (1-10 GB):**
```yaml
import_concurrency: 4
import_disk_throughput: 100MiB/s
```

**For Large Imports (> 10 GB):**
```yaml
import_concurrency: 8
import_disk_throughput: 200MiB/s
```

---

## Capacity Planning

### Disk Space Requirements

**Staging Directory Sizing:**

**Important:** Staging directories are created within the table's data directory at `<table_data_dir>/imports/<job_id>/`, not in a separate staging area.

```
Additional Space Per Import = Largest SSTable Size × 2
```

Example:
- Concurrent imports: 3
- Largest SSTable: 5 GB
- Additional space needed: 3 × 5 GB × 2 = 30 GB per table data directory

**Why 2x?** Space needed for both compressed (zip) and extracted SSTables during processing.

**Planning Note:** Ensure each table's data directory has sufficient free space for concurrent imports. The space is temporary and cleaned up after import completion.

### Network Bandwidth

**Estimated Download Time:**
```
Time = (SSTable Size × Replication Factor) / (Bandwidth × import_concurrency)
```

Example:
- SSTable: 10 GB
- RF: 3
- Bandwidth: 100 MB/s
- Concurrency: 4

Time = (10 GB × 3) / (100 MB/s × 4) = 30 GB / 400 MB/s ≈ 75 seconds per node

### Memory Overhead

**Per Import Job:**
- Base overhead: ~50 MB
- Unzip buffer: ~10 MB per concurrent operation
- SSTable metadata: ~5-10 MB per SSTable

**Total Memory Estimate:**
```
Memory = 50 MB + (import_concurrency × 10 MB) + (SSTable Count × 10 MB)
```

---

## Troubleshooting Guide

### Import Not Starting

**Check:**
1. Import job exists in `system_distributed.remote_import`
2. Status is set to 'VALIDATING' (not 'PENDING')
3. Target keyspace and table exist
4. ImportJobManager is running (check logs)

**Verify:**
```sql
SELECT * FROM system_distributed.remote_import WHERE id = <UUID>;
SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '<target_keyspace>';
```

### Import Slow Progress

**Check:**
1. Network bandwidth to source
2. Disk I/O utilization
3. CPU usage during unzip
4. Concurrent compactions

**Investigate:**
```bash
# List active jobs and their current step/progress
nodetool remoteimport status

# Drill into a single job
nodetool remoteimport status <jobId>

# Check download rate metric
# Monitor: ImportJob.BytesDownloaded

# Check disk I/O
iostat -x 5

# Check current rate limit and raise if needed (bytes/sec)
nodetool remoteimport getconfig | grep import_disk_throughput
nodetool remoteimport setconfig import_disk_throughput_bytes_per_sec 209715200
```

### Cannot Cancel Import

**Symptoms:**
- DELETE from remote_import doesn't stop import
- Job continues running

**Solution:**
```bash
# 1. Cancel the in-memory job directly
nodetool remoteimport cancel <jobId>

# 2. Or, if the row was already deleted from system_distributed.remote_import
#    but the local job is still tracked, reap orphans:
nodetool remoteimport cleanup

# 3. If still running, restart Cassandra node
nodetool drain
# restart cassandra service

# 4. Clean up staging directory manually (in table data dirs)
find /var/lib/cassandra/data/*/*/imports/<import-id>* -type d -exec rm -rf {} +
```

---

## JMX Operations

Prefer `nodetool remoteimport` (see the [Nodetool `remoteimport` Command](#nodetool-remoteimport-command) section above). The raw MBean is documented here for the cases where you need to drive it programmatically or from another JMX client.

### ImportJobManagerMBean

**MBean Name:** `com.netflix.cassandra.importing:type=ImportJobManager`

**State / cleanup:**

```java
int getActiveJobCount()
Map<String, String> getActiveJobs()                  // jobId -> summary line
Map<String, String> getJobStatus(String jobId)       // detailed status map
boolean cancelJob(String jobId)                      // cancel one in-flight job
void cleanupOrphanedJobs()                           // reaps both jobs and imports/ dirs
```

**Hot-tunable configs (read + write):**

```java
Map<String, String> getConfiguration()
void setConfiguration(String name, String value)

// Equivalent typed getters/setters for individual properties:
int    getImportConcurrency()                    / setImportConcurrency(int)
int    getImportMaxDiskPercentage()              / setImportMaxDiskPercentage(int)
double getImportDiskThroughputBytesPerSec()      / setImportDiskThroughputBytesPerSec(double)
int    getImportHttpRetryMaxAttempts()           / setImportHttpRetryMaxAttempts(int)
double getImportHttpRetryBackoffMultiplier()     / setImportHttpRetryBackoffMultiplier(double)
int    getImportHttpRetryInitialDelayMs()        / setImportHttpRetryInitialDelayMs(int)
int    getImportHttpRetryMaxDelayMs()            / setImportHttpRetryMaxDelayMs(int)
int    getImportHttpRetryJitterPercentage()      / setImportHttpRetryJitterPercentage(int)
int    getImportCleanupInitialDelaySeconds()     / setImportCleanupInitialDelaySeconds(int)
int    getImportCleanupPeriodSeconds()           / setImportCleanupPeriodSeconds(int)
int    getImportCleanupMinAgeSeconds()           / setImportCleanupMinAgeSeconds(int)
```

`setImportConcurrency` also resizes the unzip thread pool;
`setImportCleanupInitialDelaySeconds` / `setImportCleanupPeriodSeconds` reschedule the cleanup task.

**Raw `sjk` examples (only when nodetool isn't available):**
```bash
# Active jobs map (jobId -> summary)
nodetool sjk mx -b com.netflix.cassandra.importing:type=ImportJobManager -f ActiveJobs

# Cancel one job
nodetool sjk mx -b com.netflix.cassandra.importing:type=ImportJobManager \
  -op cancelJob -a <jobId>

# Bump HTTP retry attempts at runtime
nodetool sjk mx -b com.netflix.cassandra.importing:type=ImportJobManager \
  -op setImportHttpRetryMaxAttempts -a 5
```

---

## Emergency Procedures

### Emergency Stop All Imports

```bash
# 1. Via CQL (preferred)
# Stop new imports from starting
# Existing imports will complete or timeout

# 2. Cancel each in-flight job individually:
for jobId in $(nodetool remoteimport status | awk 'NR>1 {print $1}'); do
  nodetool remoteimport cancel "$jobId"
done

# 3. If CQL is unavailable, DELETE the rows via the active coordinator and
# then run the cleanup on each node:
nodetool remoteimport cleanup

# 4. Last resort: stop accepting new imports at runtime
nodetool remoteimport setconfig import_concurrency 0
```

### Recover from Cluster-Wide Import Failure

```bash
# 1. Check cluster health
nodetool status
nodetool describecluster

# 2. Clear all import jobs from system table
cqlsh -e "TRUNCATE system_distributed.remote_import;"

# 3. Clean staging directories on all nodes
# Staging is in table data dirs: <table_data_dir>/imports/<job_id>/
parallel-ssh -h nodes.txt 'find /mnt/data/cassandra/data/*/*/imports/ -type d -exec rm -rf {} \;'
yolo2 --all-parallel --max-workers 100 env cluster 'find /mnt/data/cassandra/data/*/*/imports/ -type d -exec rm -rf {} \;'

# 4. Restart import process with fresh jobs
# Create new import_id, do not reuse failed IDs
```

### Handle Corrupted SSTable After Import

```bash
restart upstream job from scratch to generate new sstables and data set to import
```

---

## Additional Resources

- **Main Documentation:** [README.md](README.md)
- **Test Suite:** `test/distributed/org/apache/cassandra/distributed/test/netflix/`
- **Metrics:** lumen dashboard or `netflix_views.local_import` table
- **Code:** `src/java/com/netflix/cassandra/importing/`

---

## Support Contacts

For issues not covered in this runbook:
1. Check Cassandra logs: `/var/log/cassandra/system.log`
2. Review import-specific logs: grep for "ImportJob" or "RemoteImport"
3. Raise to CDE team
