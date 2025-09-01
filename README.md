# ClickHouse with SuRF and Grafite Integration

This repository contains a research implementation of advanced probabilistic data structures for approximate membership queries within ClickHouse, a leading open-source OLAP database system.

## Overview

This project extends ClickHouse's indexing capabilities by implementing:

- **SuRF (Succinct Range Filter)**: Probabilistic data structure supporting both membership queries and range operations
- **Grafite Filters**: Advanced range filters with predictable memory usage and guaranteed performance bounds

Both implementations serve as native data skipping indexes within ClickHouse's MergeTree storage engine, providing unified solutions for membership testing and range query operations.

## Key Features

- Native integration with ClickHouse's C++ codebase
- Support for key-based, token-based, and n-gram indexing strategies
- Comprehensive performance benchmarking framework
- Production-ready serialisation and memory management
- Compatibility with existing ClickHouse merge operations

## Repository Structure

This is a forked version of the original [ClickHouse repository](https://github.com/ClickHouse/ClickHouse) with additional implementations:

### New Implementation Files
- `src/Interpreters/SurfFilter.*` - Core SuRF implementation
- `src/Interpreters/GrafiteFilter.*` - Core Grafite implementation  
- `src/Storages/MergeTree/MergeTreeIndexSurfFilter.*` - SuRF integration layer
- `src/Storages/MergeTree/MergeTreeIndexGrafiteFilter.*` - Grafite integration layer
- `contrib/SuRF/` - Modernised SuRF library
- `contrib/grafite/` - Grafite library integration
- `experiments/` - Performance evaluation scripts

## Building

Follow standard ClickHouse build procedures:

```bash
git clone --recursive https://github.com/vsrivatsa-edinburgh/ClickHouse.git
cd ClickHouse
mkdir build && cd build
cmake ..
cmake --build .
```

## Usage

Create indexes using standard ClickHouse DDL:

```sql
-- SuRF index
CREATE TABLE test (id UInt64, name String) 
ENGINE = MergeTree() ORDER BY id
INDEX surf_idx name TYPE surf_filter(0) GRANULARITY 1;

-- Grafite index  
CREATE TABLE test (id UInt64, value UInt64)
ENGINE = MergeTree() ORDER BY id
INDEX grafite_idx value TYPE grafite_filter GRANULARITY 1;
```

## Research Context

This implementation is part of an MSc dissertation investigating efficient approximate membership queries in analytical database systems. The work demonstrates practical integration of academic data structures within production database environments.

## License

Apache License 2.0 (inherited from ClickHouse)