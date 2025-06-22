# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is Netflix's internal fork of Apache Cassandra 4.1, used for testing and benchmarking at Netflix. The project combines the upstream Cassandra codebase with Netflix-specific extensions and packaging.

## Build System

### Primary Build (Ant-based)
- `ant` - Build using the main Ant build.xml (upstream Cassandra build)
- `ant test` - Run all unit tests
- `ant realclean` - Clean build artifacts

### Gradle Wrapper (Netflix packaging) - IGNORE
- The `build.gradle` file is for Netflix internal packaging only
- Do not modify or use Gradle commands for development work
- All development should use the Ant build system

### Test Commands
- `ant test` - Run all unit tests
- `ant testsome -Dtest.name=ClassName` - Run specific test class
- `ant testsome -Dtest.name=org.apache.cassandra.dht.RangeStreamerTest -Dtest.methods=testSplitRangesForFetch` - Run specific test method

## Architecture

### Core Components
- **Database Engine**: Located in `src/java/org/apache/cassandra/`
  - `db/` - Core database functionality, storage engine, mutations
  - `dht/` - Distributed hash table, partitioning, token management
  - `gms/` - Gossip membership service for cluster communication
  - `locator/` - Replication strategies and replica placement
  - `net/` - Messaging system for inter-node communication
  - `service/` - High-level database services (storage, repair, etc.)

### Configuration
- Main config: `conf/cassandra.yaml`
- `Config.java` - POJO representation of cassandra.yaml settings with defaults
- `DatabaseDescriptor.java` - Singleton that controls access to configuration values

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

## Key Files
- `build.xml` - Main Ant build and dependency configuration
- `conf/cassandra.yaml` - Main Cassandra configuration
- `TESTING.md` - Comprehensive testing guidelines
- `src/java/org/apache/cassandra/` - Core Cassandra code
- `src/java/com/netflix/cassandra/` - Netflix extensions