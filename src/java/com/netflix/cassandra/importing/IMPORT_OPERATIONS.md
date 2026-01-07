# Remote Import Operations Runbook

This runbook provides operational guidance for Netflix's Remote SSTable Import system, including common failure scenarios, troubleshooting procedures, and performance tuning.

## Table of Contents
1. [Common Failure Scenarios](#common-failure-scenarios)
2. [Monitoring and Alerting](#monitoring-and-alerting)
3. [Performance Tuning](#performance-tuning)
4. [Capacity Planning](#capacity-planning)
5. [Troubleshooting Guide](#troubleshooting-guide)
6. [JMX Operations](#jmx-operations)
7. [Emergency Procedures](#emergency-procedures)

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

-- 3. Check error messages via JMX or logs on stuck nodes
-- See "JMX Operations" section below

-- 4. If recoverable, reset the job
UPDATE system_distributed.remote_import
SET status = 'VALIDATING'
WHERE id = <UUID>;

-- 5. If not recoverable, cancel the import
DELETE FROM system_distributed.remote_import WHERE id = <UUID>;
```

**Prevention:**
- Increase STAGED timeout: `import_staged_timeout_ms` (default: 3600000 = 1 hour)
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
# Via JMX: ImportJobManagerMBean attributes
# - import_http_max_retries (default: 5)
# - import_http_retry_delay_ms (default: 1000)

# 3. Generate new signed URL if expired (for S3)
# Update the sources in remote_import table with new URL

# 4. Restart import with reset
UPDATE system_distributed.remote_import
SET status = 'VALIDATING'
WHERE id = <UUID>;
```

**Prevention:**
- Generate S3 signed URLs with sufficient expiry time (recommend 24+ hours)
- Configure appropriate retry settings for your network environment
- Set `import_http_connect_timeout_ms` and `import_http_read_timeout_ms` appropriately

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
# Via JMX: ImportJobManagerMBean.cleanupOrphanedDirectories()
# Or manually: find /var/lib/cassandra/data/*/*/imports/ -type d -mtime +1 -exec rm -rf {} \;

# 4. Adjust disk usage threshold if needed
# import_max_disk_percentage (default: 80)
# Lower value = more conservative, higher value = more aggressive

# 5. Cancel or reduce concurrent imports
# Check: ImportJobManagerMBean.getActiveJobCount()
```

**Prevention:**
- Monitor disk usage on Cassandra data drives
- Set appropriate `import_max_disk_percentage` (recommend 70-80%)
- Schedule periodic cleanup: automatic cleanup runs every hour
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
# Different steps have different timeouts

# 2. Adjust timeout values via configuration
# - import_download_timeout_ms (default: 3600000 = 1 hour)
# - import_staged_timeout_ms (default: 3600000 = 1 hour)
# - import_default_timeout_ms (default: 3600000 = 1 hour)

# 3. Restart import after increasing timeout
# Edit cassandra.yaml and restart node, or
# Use JMX to update if supported

# 4. Consider splitting large imports into smaller chunks
```

**Prevention:**
- Set timeouts based on expected import size and network speed
- For large imports (>10GB), increase download timeout to 2-4 hours
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
import_disk_mbps: 100
# Disk write rate limit in MB/s
# Recommendations:
#   - SSD: 100-200 MB/s
#   - Cloud (EBS): 50-100 MB/s
# Impact: Prevents import from saturating disk I/O

# Disk Space Management
import_max_disk_percentage: 80
# Maximum disk usage percentage before rejecting imports
# Recommendations:
#   - Production: 70-80%
#   - Development: 85-90%
# Impact: Lower = more safety margin, higher = more utilization

# Timeouts
import_download_timeout_ms: 3600000  # 1 hour
import_staged_timeout_ms: 3600000     # 1 hour
import_default_timeout_ms: 3600000    # 1 hour
# Adjust based on:
#   - Typical SSTable size
#   - Network bandwidth
#   - Cluster size (STAGED timeout)

# HTTP Settings
import_http_max_retries: 5
import_http_retry_delay_ms: 1000
import_http_connect_timeout_ms: 10000
import_http_read_timeout_ms: 30000
# Adjust for network reliability
```

### Performance Guidelines

**For Small Imports (< 1 GB):**
```yaml
import_concurrency: 2
import_disk_mbps: 50
import_download_timeout_ms: 600000  # 10 minutes
```

**For Medium Imports (1-10 GB):**
```yaml
import_concurrency: 4
import_disk_mbps: 100
import_download_timeout_ms: 3600000  # 1 hour
```

**For Large Imports (> 10 GB):**
```yaml
import_concurrency: 8
import_disk_mbps: 200
import_download_timeout_ms: 7200000  # 2 hours
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
# Check active jobs
# Via JMX: ImportJobManagerMBean.getActiveJobCount()

# Check download rate
# Monitor metrics: ImportJob.BytesDownloaded

# Check disk I/O
iostat -x 5

# Check if rate limiting is too aggressive
# Review import_disk_mbps setting
```

### Cannot Cancel Import

**Symptoms:**
- DELETE from remote_import doesn't stop import
- Job continues running

**Solution:**
```bash
# 1. Force cancel via JMX
# ImportJobManagerMBean.cancelJob(importId)

# 2. If still running, restart Cassandra node
nodetool drain
# restart cassandra service

# 3. Clean up staging directory manually
rm -rf /path/to/staging/<import-id>*
```

---

## JMX Operations

### ImportJobManagerMBean

**MBean Name:** `com.netflix.cassandra.importing:type=ImportJobManager`

**Useful Operations:**

```java
// Get active imports
int getActiveJobCount()
List<String> getActiveJobIds()

// Cancel specific import
void cancelJob(String importId)

// Cleanup operations
void cleanupOrphanedJobs()
void cleanupOrphanedDirectories()

// Get job details
Map<String, String> getJobStatus(String importId)

// Configuration (read-only)
int getImportConcurrency()
int getImportMaxDiskPercentage()
String getImportStagingDirectory()
```

**Example using nodetool:**
```bash
# List active imports
nodetool sjk mx -b com.netflix.cassandra.importing:type=ImportJobManager -f getActiveJobIds

# Cancel import
nodetool sjk mx -b com.netflix.cassandra.importing:type=ImportJobManager \
  -op cancelJob -a <import-uuid>

# Cleanup orphaned directories
nodetool sjk mx -b com.netflix.cassandra.importing:type=ImportJobManager \
  -op cleanupOrphanedDirectories
```

---

## Emergency Procedures

### Emergency Stop All Imports

```bash
# 1. Via CQL (preferred)
# Stop new imports from starting
# Existing imports will complete or timeout

# 2. Via JMX (if CQL unavailable)
# Cancel each active import
for import_id in $(get_active_imports); do
  nodetool sjk mx -op cancelJob -a $import_id
done

# 3. Last resort: Disable import manager
# Edit cassandra.yaml, add:
# import_enabled: false
# Restart Cassandra
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
