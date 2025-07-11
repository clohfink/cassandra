# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is Netflix's internal fork of Apache Cassandra 4.1, used for testing and benchmarking at Netflix. The project combines the upstream Cassandra codebase with Netflix-specific extensions and packaging.

## Build System

### Primary Build (Ant-based)
- `ant` - Build using the main Ant build.xml (upstream Cassandra build). Use this to test compile
- `ant test` - Run all unit tests
- `ant realclean` - Clean build artifacts
- Always use `ant build -Duse.jdk11=true` not `ant compile` to check if it compiles
- Dont use javac, use `ant build -Duse.jdk11=true` to test compiling

### Gradle Wrapper (Netflix packaging) - IGNORE
- The `build.gradle` file is for Netflix internal packaging only
- Do not modify or use Gradle commands for development work
- All development should use the Ant build system

### Test Commands
- `ant test` - Run all unit tests
- `ant testsome -Dtest.name=ClassName` - Run specific test class
- `ant testsome -Dtest.name=org.apache.cassandra.dht.RangeStreamerTest -Dtest.methods=testSplitRangesForFetch` - Run specific test method

### Configuration
- Main config: `conf/cassandra.yaml`
- `Config.java` - POJO representation of cassandra.yaml settings with defaults
- `DatabaseDescriptor.java` - Singleton that controls access to configuration values
- Add configuration option to `Config.java`, then add accessor and setter in `DatabaseDescriptor.java`

### Dependencies
- Maintained in ant `build.xml`

## Testing Structure
The project follows Cassandra's testing philosophy with three test types:
1. **Unit Tests** (`test/unit/`) - Test individual components in isolation
2. **Integration Tests** (`test/distributed/`) - In JVM integration tests that require multiple nodes

## Development Workflow
This is a Netflix internal fork, not upstream Apache Cassandra:
- Changes are made directly to this repository
- No GitHub pull requests to upstream Apache Cassandra
- Testing focuses on Netflix-specific use cases and benchmarking

## Async Programming: Futures and Executors

Cassandra uses a sophisticated custom async programming framework instead of standard Java futures.

### Custom Future Framework
- **Main Interface**: `org.apache.cassandra.utils.concurrent.Future` - Unifies Netty, Guava, and Java Future APIs
- **Core Implementations**:
  - `AbstractFuture` - Lock-free base implementation with atomic field updaters
  - `AsyncFuture` - Non-blocking waits using wait queues for high concurrency
  - `SyncFuture` - Synchronized implementation for thread-safe operations
- **Promise Support**: `AsyncPromise`/`SyncPromise` extend Future with completion capabilities
- **Utilities**: `ImmediateFuture` (pre-completed), `FutureCombiner` (multiple future coordination)

### Custom Executor Architecture
- **Core Pool**: `SharedExecutorPool` + `SEPExecutor` - Shared worker threads that can hop between executors
- **Stage-Based**: `Stage` enum provides executors for READ, MUTATION, GOSSIP, etc. with per-stage concurrency
- **Specialized**: 
  - `InfiniteLoopExecutor` - Continuous background tasks
  - `ImmediateExecutor` - Synchronous execution on calling thread
- **Context-Aware**: `LocalAwareThreadPoolExecutorPlus` propagates thread-local context across async operations

**Key Design**: Lock-free concurrency, ordered listener notifications, unified API across different Future libraries, and efficient resource utilization through shared worker pools.

## Key Files
- `build.xml` - Main Ant build and dependency configuration
- `conf/cassandra.yaml` - Main Cassandra configuration
- `TESTING.md` - Comprehensive testing guidelines
- `src/java/org/apache/cassandra/` - Core Cassandra code
- `src/java/com/netflix/cassandra/` - Netflix extensions
