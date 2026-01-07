# Remote Imports System

## End User Flow

### Step-by-Step Import Process

The typical workflow for performing a remote import involves coordinating through the distributed system tables:

#### 1. Insert Import Job Definition
First, insert all the SSTable sources you want to import. You can add multiple sources (typically S3 signed URLs to zipped SSTables) for the same import job by using the same ID but different source URLs:

```sql
-- Add first SSTable source
INSERT INTO system_distributed.remote_import 
(id, target_keyspace, target_table, source, source_type, start_token, end_token, size) 
VALUES 
(474cbd00-50cb-11f0-9035-c19c4fd95b36, 'my_keyspace', 'my_table', 
 'https://s3.amazonaws.com/bucket/sstable1.zip?AWSAccessKeyId=...', 'url', '0', '100000000', 298844160);

-- Add second SSTable source (same ID, different source)
INSERT INTO system_distributed.remote_import 
(id, target_keyspace, target_table, source, source_type, start_token, end_token, size) 
VALUES 
(474cbd00-50cb-11f0-9035-c19c4fd95b36, 'my_keyspace', 'my_table', 
 'https://s3.amazonaws.com/bucket/sstable2.zip?AWSAccessKeyId=...', 'url', '100000001', '200000000', 156422080);

-- Add more sources as needed...
```

**Parameters**:
- `id`: Unique UUID identifying this import job (same for all related sources)
- `target_keyspace`: Destination keyspace name
- `target_table`: Destination table name  
- `source`: URL or path to the data source (typically S3 signed URL to zipped SSTable)
- `source_type`: Currently supports `'url'` for HTTP/HTTPS sources
- `start_token`/`end_token`: Optional token range filtering (can be `null` for full import - defaults to partitioner min/max tokens)
- `dc_filter`: Optional datacenter filtering (can be `null` to include all datacenters)
- `size`: Expected size in bytes for disk space planning

**Important**: Add all SSTable sources before proceeding to step 2. Each source becomes a separate clustering row in the partition, allowing multiple SSTables to be imported as part of a single coordinated job.

#### 2. Start the Import Process
Update the job state to begin processing:

```sql
UPDATE system_distributed.remote_import 
SET state = 'staging' 
WHERE id = 474cbd00-50cb-11f0-9035-c19c4fd95b36 
  AND target_keyspace = 'my_keyspace' 
  AND target_table = 'my_table';
```

This triggers the `RemoteImportTrigger` which notifies all cluster nodes to begin their import jobs.

#### 3. Monitor Progress
Monitor the import progress using the local import view:

```sql
SELECT * FROM netflix_views.local_import 
WHERE id = 474cbd00-50cb-11f0-9035-c19c4fd95b36 
  AND target_keyspace = 'my_keyspace' 
  AND target_table = 'my_table';
```

**Note**: The `local_import` view only shows jobs running on the current node. To see the status across the entire cluster, use:

```sql
SELECT * FROM netflix_views.cluster_view 
WHERE keyspace_name = 'netflix_views' AND table_name = 'local_import';
```

The `status` map contains detailed progress information including:
- Current processing step (DOWNLOADING, TRIMMING, etc.)
- Progress percentages
- Bytes downloaded/processed
- Error messages if any

#### 4. Wait for Staging Completion
Continue monitoring until all nodes reach the `STAGED` state. The job will automatically progress through:
- `VALIDATING` → `FILTERING` → `DOWNLOADING` → `STAGED`

#### 5. Trigger Import Phase
Once all nodes are staged, trigger the final import phase:

```sql
UPDATE system_distributed.remote_import 
SET state = 'importing' 
WHERE id = 474cbd00-50cb-11f0-9035-c19c4fd95b36 
  AND target_keyspace = 'my_keyspace' 
  AND target_table = 'my_table';
```

#### 6. Monitor to Completion
Continue monitoring until the job reaches `DONE` state. The final phases are:
- `STAGED` → `IMPORTING` → `TRIMMING` → `DONE`

### Example Code Pattern
Based on `RemoteSnapshotTest.java`, a complete import looks like:

```java
UUID importId = UUID.randomUUID();

// Insert job definition - add all SSTable sources first
session.execute(
    "INSERT INTO system_distributed.remote_import " +
    "(id, target_keyspace, target_table, source, source_type, start_token, end_token, size) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    importId, "my_keyspace", "my_table", 
    "https://s3.amazonaws.com/bucket/sstable1.zip?AWSAccessKeyId=...", "url", 
    "0", "100000000", 298844160L
);

session.execute(
    "INSERT INTO system_distributed.remote_import " +
    "(id, target_keyspace, target_table, source, source_type, start_token, end_token, size) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    importId, "my_keyspace", "my_table", 
    "https://s3.amazonaws.com/bucket/sstable2.zip?AWSAccessKeyId=...", "url", 
    "100000001", "200000000", 156422080L
);

// Start the process
session.execute(
    "UPDATE system_distributed.remote_import SET state = 'staging' " +
    "WHERE id = ? AND target_keyspace = ? AND target_table = ?",
    importId, "my_keyspace", "my_table"
);

// Monitor until staged
boolean staged = false;
while (!staged) {
    Thread.sleep(5000);
    ResultSet result = session.execute(
        "SELECT status FROM netflix_views.local_import " +
        "WHERE id = ? AND target_keyspace = ? AND target_table = ?",
        importId, "my_keyspace", "my_table"
    );
    // Check if status indicates STAGED state
    staged = checkIfStaged(result);
}

// Trigger final import
session.execute(
    "UPDATE system_distributed.remote_import SET state = 'importing' " +
    "WHERE id = ? AND target_keyspace = ? AND target_table = ?",
    importId, "my_keyspace", "my_table"
);

// 5. Monitor to completion
// Continue monitoring until DONE state
```

### Important Notes
- **Token Range Awareness**: Jobs automatically filter sources based on each node's owned token ranges
- **Disk Space Management**: The system monitors disk space and will fail jobs if space is insufficient
- **Multi-Node Coordination**: All nodes must complete staging before any begin importing
- **Error Handling**: Failed jobs can be restarted by updating the state back to `'staging'`
- **Cleanup**: Staging directories are automatically cleaned up on completion or failure

### Restarting Failed Jobs

If one or more nodes fail during the import process, you can restart the failed jobs without affecting successful nodes:

#### Identifying Failed Jobs
Monitor individual nodes to identify failures:

```sql
-- Check for failed jobs where current_step == "ERROR"
SELECT * FROM netflix_views.cluster_view 
WHERE keyspace_name = 'netflix_views' AND table_name = 'local_import';
```

#### Restarting Failed Jobs
To restart a failed job, update the distributed state back to `'staging'`:

```sql
UPDATE system_distributed.remote_import 
SET state = 'staging' 
WHERE id = 474cbd00-50cb-11f0-9035-c19c4fd95b36
  AND target_keyspace = 'my_keyspace' 
  AND target_table = 'my_table';
```

**What happens when you restart**:
- **All nodes receive the state change**: The trigger notifies all cluster members
- **Successful nodes continue normally**: Nodes that haven't failed will handle the state change gracefully
- **Failed nodes restart completely**: Nodes in ERROR or CANCELLED state will reset and restart from VALIDATING
- **Fresh state machine**: The restarted job goes through the full cycle: VALIDATING → FILTERING → DOWNLOADING → STAGED

#### Restart Behavior
- **Node-specific recovery**: Only failed nodes actually restart; successful nodes remain unaffected
- **Clean restart**: Failed jobs start fresh from the beginning of the state machine
- **Automatic cleanup**: Previous failed attempts are cleaned up before restart
- **Safe operation**: Multiple restarts are safe and won't corrupt successful nodes

## System Tables

### system_distributed.remote_import Table

The `remote_import` table is a distributed system table that coordinates import operations across the entire Cassandra cluster.

**Schema Location**: `SystemDistributedKeyspace.java`

#### Table Structure
```sql
CREATE TABLE system_distributed.remote_import (
    id uuid,                    -- Unique import identifier (snapshot ID)
    target_keyspace text,       -- Target keyspace name
    target_table text,          -- Target table name  
    state text static,          -- Current import state (static column)
    source text,                -- Source URL or identifier
    source_type text,           -- Type of source ("url", etc.)
    start_token text,           -- Optional: Start token for range filtering (null uses partitioner minimum)
    end_token text,             -- Optional: End token for range filtering (null uses partitioner maximum)  
    dc_filter text,             -- Optional: Datacenter filter (null includes all datacenters)
    size bigint,                -- Size of data to import
    PRIMARY KEY ((id, target_keyspace, target_table), source)
);
```

#### Key Features
- **Composite Partition Key**: Groups related import sources by (id, target_keyspace, target_table)
- **Static State Column**: Shared state across all sources within a partition
- **Token Range Support**: Optional start/end tokens for distributed range-based imports
- **Datacenter Filtering**: Optional dc_filter for multi-datacenter deployments
- **Multi-source Imports**: Single import job can have multiple sources (clustering by source)

#### Datacenter Filtering
The `dc_filter` column provides flexible datacenter-aware source filtering:

**Filtering Behavior**:
- **null dc_filter**: Source is processed by all datacenters (default behavior)
- **Set dc_filter**: Source is only processed by nodes matching the specified datacenter

**Matching Logic** (case-insensitive):
1. **Local Datacenter Match**: Compares against node's configured datacenter (`DatabaseDescriptor.getLocalDataCenter()`)
2. **Netflix Region Match**: Compares against `NETFLIX_REGION` environment variable (Netflix-specific)

**Usage Examples**:
```sql
-- Source processed only by nodes in 'us-east-1' datacenter or Netflix region
INSERT INTO system_distributed.remote_import 
(id, target_keyspace, target_table, source, source_type, dc_filter, size) 
VALUES 
(bf415d36-3e31-4652-b23d-589e2e85f99f, 'my_keyspace', 'my_table', 'https://s3.amazonaws.com/bucket/sstable-useast.zip', 'url', 'us-east-1', 298844160);

-- Source processed only by nodes in 'us-west-2' datacenter or Netflix region  
INSERT INTO system_distributed.remote_import 
(id, target_keyspace, target_table, source, source_type, dc_filter, size) 
VALUES 
(bf415d36-3e31-4652-b23d-589e2e85f99f, 'my_keyspace', 'my_table', 'https://s3.amazonaws.com/bucket/sstable-uswest.zip', 'url', 'us-west-2', 156422080);

-- Source processed by all datacenters (backwards compatible)
INSERT INTO system_distributed.remote_import 
(id, target_keyspace, target_table, source, source_type, size) 
VALUES 
(bf415d36-3e31-4652-b23d-589e2e85f99f, 'my_keyspace', 'my_table', 'https://s3.amazonaws.com/bucket/sstable-global.zip', 'url', 298844160);
```

**Multi-Datacenter Import Strategy**:
- Use different sources per datacenter for geo-distributed data
- Each datacenter processes only its relevant sources
- Maintains token range filtering within each datacenter
- Allows region-specific S3 buckets or CDN endpoints

## LocalImport Virtual Table

### Overview
The `LocalImport` virtual table provides a read-only view of all import jobs running on the local Cassandra node, enabling monitoring and debugging of import operations.

**Location**: `LocalImport.java`  
**Table Name**: `netflix_views.local_import`

### Schema
```sql
CREATE TABLE netflix_views.local_import (
    id uuid,           -- Import job identifier
    target_keyspace text,       -- Target keyspace name
    target_table text,          -- Target table name
    status map<text, text>,     -- Detailed status information
    PRIMARY KEY (id, target_keyspace, target_table)
);
```

### Usage
```sql
-- View all local import jobs on this node
SELECT * FROM netflix_views.local_import;

-- View specific import job status on this node
SELECT status FROM netflix_views.local_import 
WHERE id = ? AND target_keyspace = ? AND target_table = ?;

-- View import jobs across the entire cluster
SELECT * FROM netflix_views.cluster_view 
WHERE keyspace_name = 'netflix_views' AND table_name = 'local_import';
```

### Status Information
The `status` map contains detailed information about the current import job state:
- **Current Step**: Which import stage is currently executing
- **Progress**: Download/extraction/import progress percentages
- **Metrics**: Bytes downloaded, files processed, timing information
- **Error Details**: Error messages and stack traces if applicable
- **Timestamps**: When each phase started and completed

### Integration
- **Data Source**: Pulls live data from `ImportJobManager.getInstance().getAllJobs()`
- **Real-time Updates**: Status reflects current job state without caching
- **Local Scope**: Only shows jobs running on the queried node
- **Cluster-wide Monitoring**: Use `netflix_views.cluster_view` to see all nodes' import status
- **Monitoring Integration**: Can be used by monitoring systems to track import progress

## System Integration

### Distributed Coordination Flow
1. **Job Definition**: Import jobs defined in `system_distributed.remote_import`
2. **Trigger Activation**: `RemoteImportTrigger` detects state changes
3. **Cluster Notification**: All nodes receive state change messages
4. **Local Processing**: Each node's `ImportJobManager` processes changes
5. **Job Execution**: Import jobs advance through state machine stages
6. **Monitoring**: Progress visible via `netflix_views.local_import`

### Multi-Node Coordination
- **Distributed State**: Shared via `system_distributed.remote_import` table
- **Local Execution**: Each node runs its own `ImportJobManager` and jobs
- **Synchronized Phases**: STAGED phase ensures all nodes complete downloads before importing
- **Token Awareness**: Jobs filter sources based on owned token ranges

# UrlImportJob

## Overview
The UrlImportJob follows a state machine pattern to import SSTable files from remote URLs into Cassandra. The process downloads, extracts, and imports data while managing disk space and handling failures gracefully.

## State Flow Diagram
```
VALIDATING → FILTERING → DOWNLOADING → STAGED → IMPORTING → TRIMMING → DONE
                                      ↓
                          (waits for external trigger)
```

## Stage Details

### 1. VALIDATING (ValidationStep)
**Purpose**: Initial state when job is created
**Location**: `steps/ValidationStep.java`
**Actions**:
- Job initialized and validating configuration
- Validates target table metadata exists
- Logs initialization message

**Transition**: Immediately transitions to → FILTERING

### 2. FILTERING (SourceSelectionStep)
**Purpose**: Filter import sources based on owned token ranges and datacenter filter  
**Location**: `steps/SourceSelectionStep.java`  
**Actions**:
- Queries remote import sources from system table
- Gets normalized local token ranges for the keyspace
- For each source URL:
  - **Datacenter filtering**: If dc_filter specified, checks if it matches local datacenter or Netflix region
  - **Token range filtering**: If token range specified, checks if it overlaps with owned ranges
  - If no token range: includes the source
- Builds map of URLs to download with their expected sizes

**Datacenter Filter Logic**:
- Compares dc_filter (case-insensitive) against:
  1. Local datacenter (`DatabaseDescriptor.getLocalDataCenter()`)  
  2. Netflix region (`NETFLIX_REGION` environment variable)
- Sources with non-matching dc_filter are excluded from processing
- null dc_filter includes source for all datacenters

**Transition**: After filtering complete → DOWNLOADING

### 3. DOWNLOADING (DownloadStep)
**Purpose**: Download files from remote URLs in parallel  
**Location**: `steps/DownloadStep.java`  
**Actions**:
- Creates staging directory in Cassandra data directory
- Checks available disk space before starting downloads
- Downloads files in parallel using HTTP/2 client
- Tracks download progress (bytes and file count)
- Monitors disk space during downloads
- Handles download failures and retries
- Collects metrics (download timer, bytes downloaded, compression ratios)

**Features**:
- Skip downloads if file exists with correct size
- Atomic error handling (first error stops all downloads)
- Progress tracking with byte-level granularity

**Transition**: After all files extracted → STAGED  
**Error Handling**:
- Disk space errors trigger cleanup and job failure
- Extraction errors are propagated
- HTTP errors are logged and cause job failure

### 4. STAGED (StagedStep)
**Purpose**: Wait for external coordination signal before importing  
**Location**: `steps/StagedStep.java`  
**Actions**:
- Files are downloaded and ready
- Polls remote import state from system table
- Waits for state to change to "importing"

**Key Behavior**:
- This is a coordination point for multi-node imports
- Allows all nodes to download before any start importing
- External coordinator updates state in system table

**Transition**: When remote state = "importing" → IMPORTING  
**Check Delay**: Dynamic based on cluster size (100ms per node, 1s-60s range, ±10% jitter)

### 5. IMPORTING (ImportingStep)
**Purpose**: Load SSTable files into Cassandra  
**Location**: `steps/ImportingStep.java`  
**Actions**:
- Gets ColumnFamilyStore for target table
- Calls importNewSSTables() with staging directory
- Handles import failures
- Records metrics for import phase

**Transition**: After import complete → TRIMMING

### 6. TRIMMING (TrimStep)
**Purpose**: Clean up data outside owned token ranges  
**Location**: `steps/TrimStep.java`  
**Actions**:
- Runs forceCleanup(2) on the table
- Removes data outside owned token ranges
- Records completion metrics

**Transition**: After cleanup complete → DONE

### 7. DONE (DoneStep)
**Purpose**: Final state indicating successful completion  
**Location**: `steps/DoneStep.java`  
**Actions**:
- Logs completion
- Returns Long.MAX_VALUE for check delay (stops polling)
- Stays in this state indefinitely

## Error States

### ERROR
Can be reached from any state when an unrecoverable error occurs:
- Validation failures
- Download failures
- Disk space exhaustion
- Import failures
- Unhandled exceptions

### CANCELLED
Can be reached from states that support cancellation:
- DOWNLOADING: Cancels all pending downloads
- Other states log cancellation but may not stop immediately

## Key Features

### Disk Space Management
- Checks before downloads (total size)
- Monitors during downloads
- Checks before extraction (estimated uncompressed size)
- Monitors during extraction
- Configurable threshold via `import_max_disk_percentage`
- Automatic cleanup on disk space errors

### Metrics Collection
- Download timing and bytes
- Unzip timing and bytes
- Import timing
- Trim timing
- Compression ratios
- Error counters by type
- Job completion tracking

### Cleanup and Resource Management
- Automatic cleanup of staging directory on completion
- Cleanup on errors to free disk space
- Cancellable async operations (downloads)
- Resource cleanup in step cleanup() methods

### Coordination Features
- Token range filtering for distributed imports
- STAGED state for multi-node coordination
- Dynamic polling delays based on cluster size
- Jitter to avoid thundering herd

## Configuration
- `import_concurrency`: HTTP client thread pool size
- `import_buffer_size`: Buffer size for file extraction
- `import_max_disk_percentage`: Maximum disk usage percentage allowed

## Timeout Management

### Overview
ImportJob centralizes timeout tracking for all steps. Each step can define its own timeout duration via `timeoutMillis()`, and ImportJob tracks the elapsed time for the current step.

### How It Works
1. **Step Start Time**: ImportJob records the start time when a step becomes current
2. **Timeout Check**: On each `checkState()` call, ImportJob checks if `elapsed time > step.timeoutMillis()`
3. **Timer Reset**: The timer resets when transitioning to a new step
4. **Default Timeout**: Steps inherit a default 1-hour timeout (`ImportJob.DEFAULT_TIMEOUT_MS`)

### Custom Timeouts
Steps can override `timeoutMillis()` to specify custom timeout durations:
```java
@Override
public long timeoutMillis()
{
    return TimeUnit.MINUTES.toMillis(30); // 30-minute timeout for this step
}
```

### Testing Support
ImportJob uses a static `CLOCK` field (annotated with `@VisibleForTesting`) for testing timeout behavior:
```java
// Production: uses default Clock.Default()
ImportJob job = new UrlImportJob(id, keyspace, table);

// Testing: replace static clock with mock
Clock originalClock = ImportJob.CLOCK;
try {
    MockClock mockClock = new MockClock();
    ImportJob.CLOCK = mockClock;

    ImportJob job = new UrlImportJob(id, keyspace, table);
    mockClock.advance(timeoutValue + 1);
    job.checkState(); // Will timeout
} finally {
    ImportJob.CLOCK = originalClock; // Restore original clock
}
```

See `ImportJobTimeoutTest.java` for comprehensive timeout testing examples.

## Implementation Notes
1. Each step implements the `ImportStep` interface
2. Steps are not thread-safe (marked with @NotThreadSafe)
3. State transitions are managed by returning next step from `checkComplete()`
4. Step initialization via `init()` method, called by ImportJob when step becomes current
5. Timeout tracking centralized in ImportJob, steps define timeout via `timeoutMillis()`
6. Cleanup via `cleanup()` method for cancellation
7. Progress tracking via `toStatusMap()` for monitoring

## Import Job Manager

### Overview
The `ImportJobManager` is a singleton service that manages all active import jobs in the Cassandra cluster. It serves as the central coordination point for import operations, creating and tracking import jobs based on entries in the distributed system table.

**Location**: `ImportJobManager.java`

### Key Responsibilities
- **Job Lifecycle Management**: Creates, tracks, and manages import jobs throughout their lifecycle
- **State Change Processing**: Handles import state transitions triggered by the distributed system
- **Job Registry**: Maintains a concurrent map of all active import jobs indexed by snapshot ID
- **Type-based Job Creation**: Dynamically creates appropriate job types based on source configuration

### Core Methods

#### `getOrCreateJob(UUID snapshotId, String keyspace, String table)`
**Location**: `ImportJobManager.java`
- Retrieves existing job or creates new one if not found
- Queries `system_distributed.remote_import` table for job configuration
- Currently supports "url" source type jobs (creates `UrlImportJob`)
- Returns null if job cannot be created or configuration not found

#### `processImportStateChange(UUID snapshotId, String keyspace, String table, String currentState, String newState)`
**Location**: `ImportJobManager.java`
- Main entry point for handling state transitions from the distributed trigger system
- Handles state transitions: `validating`, `staged`, `staging`, `importing`
- Triggers job state checks to advance through import pipeline
- Supports job restart for jobs in ERROR or CANCELLED states

### State Handling Logic
The `remote_import` table supports only two primary states for coordination:
- **staging**: Download/preparation phase - nodes download and stage files locally
- **importing**: Import phase - nodes import the staged files into Cassandra

**State Transitions**:
- **staging**: Starts new import job execution, triggering download and staging on all nodes
- **importing**: Triggers transition from staging to actual import phase once all nodes are staged

**Monitoring States**: Individual nodes progress through internal states (VALIDATING, FILTERING, DOWNLOADING, STAGED, IMPORTING, TRIMMING, DONE) but only report progress via `netflix_views.local_import`

**Coordination**: To determine when all nodes are staged and ready for importing:
- Use `netflix_views.cluster_view` to check `local_import` status across all nodes
- Check each node's `local_import` table directly for detailed status
- All nodes must reach "Staged" state before updating `remote_import.state` to 'importing'

**Error Recovery**: Resets and restarts jobs in ERROR or CANCELLED states by setting state back to 'staging'


### RemoteImportTrigger

The `RemoteImportTrigger` is a Cassandra trigger that automatically responds to changes in the `remote_import` table to coordinate distributed import operations.

**Location**: `RemoteImportTrigger.java`

#### Trigger Mechanism
- **Automatic Activation**: Registered as a trigger on the `remote_import` table
- **State Change Detection**: Monitors updates to the static `state` column
- **Cluster-wide Notification**: Sends state change messages to all live nodes

#### Operation Flow
1. **State Update Detection**: Trigger fires when `state` column is modified
2. **Partition Key Parsing**: Extracts snapshot ID, keyspace, and table from composite key
3. **Cluster Broadcast**: Sends `ImportStateChangeRequest` to all live cluster members
4. **Local Processing**: Each node's `ImportJobManager` processes the state change

#### State Transitions Handled
- **staging**: Beginning of download/preparation phase
- **importing**: All nodes ready, begin actual SSTable import

#### Error Handling
- Logs failures in state change processing
- Continues operation even if some nodes fail to receive messages
- Does not block the original write operation

---

## Operations and Performance

### Operational Runbook

For production deployment, troubleshooting, and operational guidance, see:
**[IMPORT_OPERATIONS.md](IMPORT_OPERATIONS.md)**

The operational runbook covers:
- Common failure scenarios and recovery procedures
- Monitoring and alerting recommendations
- Performance tuning guidelines
- Capacity planning
- JMX operations and emergency procedures

### Performance Benchmarks

Performance characteristics based on test suite (see `RemoteImportBenchmarkTest.java`):

#### Throughput by Import Size

| Import Size | Expected Duration | Throughput | Configuration |
|-------------|-------------------|------------|---------------|
| Small (100 rows) | < 10 seconds | 10-50 rows/sec | concurrency=2 |
| Medium (1K rows) | < 30 seconds | 50-200 rows/sec | concurrency=2 |
| Large (10K rows) | < 120 seconds | 100-500 rows/sec | concurrency=4 |

**Note:** Actual performance varies based on:
- Network bandwidth to source
- Disk I/O capabilities
- CPU for decompression
- SSTable size and structure

#### Concurrency Impact

| Concurrency Level | Relative Performance | Resource Usage |
|-------------------|---------------------|----------------|
| 1 | 1.0x (baseline) | Low |
| 2 | 1.5-1.8x | Medium |
| 4 | 2.0-2.5x | High |
| 8 | 2.2-3.0x | Very High |

**Recommendation:** Start with concurrency=2 for small instances, concurrency=4 for medium/large instances. Diminishing returns beyond 4 for typical workloads.

#### Configuration Recommendations

**Production Settings (balanced):**
```yaml
import_concurrency: 4
import_disk_mbps: 100
import_max_disk_percentage: 75
import_download_timeout_ms: 3600000  # 1 hour
```

**High-throughput Settings (for large instances):**
```yaml
import_concurrency: 8
import_disk_mbps: 200
import_max_disk_percentage: 80
import_download_timeout_ms: 7200000  # 2 hours
```

**Conservative Settings (for shared clusters):**
```yaml
import_concurrency: 2
import_disk_mbps: 50
import_max_disk_percentage: 70
import_download_timeout_ms: 3600000  # 1 hour
```

### Testing

#### Test Coverage

The import system includes comprehensive test coverage:

**Distributed Tests** (`test/distributed/`):
- `RemoteImportBasicTest` - Happy path scenarios
- `RemoteImportReplicationTest` - Multi-node replication verification
- `RemoteImportValidationTest` - Failure scenarios
- `RemoteImportCleanupTest` - Orphaned job/directory cleanup
- `RemoteImportFailureTest` - Network partitions, split-brain, topology changes
- `RemoteImportResourceTest` - Disk exhaustion, retry exhaustion, large files
- `RemoteImportEdgeCasesTest` - Token boundaries, concurrent imports, DC filtering
- `RemoteImportMetricsTest` - Metrics accuracy and virtual table performance
- `RemoteImportBenchmarkTest` - Performance baselines and stress testing

**Unit Tests** (`test/unit/`):
- `ImportJobTimeoutTest` - Timeout behavior and state transitions
- `DownloadRetryTest` - HTTP edge cases and retry logic
- `StateTransitionTest` - State machine edge cases

#### Running Tests

```bash
# Run specific distributed test
ant testsome -Dtest.name=org.apache.cassandra.distributed.test.netflix.RemoteImportBasicTest

# Run specific unit test
ant testsome -Dtest.name=com.netflix.cassandra.importing.ImportJobTimeoutTest

# Run all remote import tests (not recommended - use testsome for individual classes)
ant testsome -Dtest.name=org.apache.cassandra.distributed.test.netflix.RemoteImport*
```

**Note:** Distributed tests create in-JVM clusters and have memory overhead. Run test classes individually using `ant testsome` rather than all tests at once.

---

## Additional Resources

- **Test Suite:** `test/distributed/org/apache/cassandra/distributed/test/netflix/`
- **Source Code:** `src/java/com/netflix/cassandra/importing/`
- **Metrics Dashboard:** Check Spectator/Grafana for `ImportJob.*` metrics
- **Virtual Tables:**
  - `netflix_views.local_import` - Local node import status
  - `netflix_views.cluster_view` - Cluster-wide aggregated status