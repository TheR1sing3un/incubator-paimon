# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Paimon — a lake format for building Realtime Lakehouse Architecture with Flink and Spark. Uses LSM-tree based storage for real-time streaming updates. This is a Kuaishou internal fork.

## Syntax Requirements

- Based on JDK 8 and Scala 2.12. Higher version syntax features must not be used.

## Build and Test Commands

Prefer the smallest possible build/test scope for fast feedback loops.

```shell
# Compile a single module
mvn -pl <module> -DskipTests compile

# Compile with dependencies (-am rebuilds changed upstream modules)
mvn -pl <module> -am -DfailIfNoTests=false -DskipTests compile

# Run a single test method
mvn -pl <module> -Dtest=TestClassName#methodName test

# Run a single test class
mvn -pl <module> -Dtest=TestClassName test

# Fast local iteration (skip formatting/style checks)
mvn -pl <module> -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip -Dtest=TestClassName#methodName test

# Format code (Java via Google Java Format, Scala via scalafmt)
mvn spotless:apply

# Full build
mvn clean install -DskipTests
```

## Architecture

### Module Dependency Flow

```
paimon-api          (public interfaces: catalog, schema, table, types)
    ↓
paimon-common       (shared utilities, data types, filesystem, compression)
    ↓
paimon-core         (storage engine: LSM mergetree, manifest, compaction, snapshots, indexes)
    ↓
    ├── paimon-format          (ORC, Parquet, Avro, CSV, JSON file formats)
    ├── paimon-codegen         (code generation)
    ├── paimon-flink           (Flink 1.16–2.2 connectors, CDC)
    ├── paimon-spark           (Spark 3.2–4.0 connectors)
    ├── paimon-hive            (Hive 2.1–3.1 connectors)
    ├── paimon-rest-server     (Netty-based REST catalog server)
    ├── paimon-filesystems     (S3, OSS, Azure, GCS, COS, OBS cloud storage)
    ├── paimon-iceberg         (Iceberg compatibility, requires JDK 11)
    ├── paimon-lucene          (Lucene indexes, requires JDK 11)
    └── paimon-faiss           (FAISS vector index, JNI native)
    ↓
paimon-bundle       (shaded distribution JARs)
```

### Key Patterns

- **Catalog abstraction**: `paimon-api` defines catalog interfaces; `paimon-core` and `paimon-rest-server` provide implementations.
- **Plugin/SPI pattern**: Factories in `paimon-api` and `paimon-common` for pluggable components.
- **Multi-version engine support**: Version-specific submodules (e.g., `paimon-flink/paimon-flink-1.20`, `paimon-spark/paimon-spark-3.5`).
- **Shaded dependencies**: Jackson, Guava, Caffeine, Netty via `paimon-shade` artifacts to avoid classpath conflicts.
- **REST server**: Netty HTTP server with RouteDispatcher → RouteHandler → specific handlers (TableHandler, DatabaseHandler, etc.).

## Maven Profiles

- `flink1` — Flink 1.x modules (default 1.20.1)
- `flink2` — Flink 2.x modules (2.2.0)
- `spark3` — Spark 3.x modules (3.5.8)
- `spark4` — Spark 4.x modules
- `scala-2.13` — Scala 2.13 build
- `paimon-iceberg`, `paimon-lucene` — Auto-activated on JDK 11

## Code Quality

- **Spotless**: Google Java Format (1.7) + scalafmt. Fix with `mvn spotless:apply`.
- **Checkstyle**: Config at `tools/maven/checkstyle.xml`, suppressions at `tools/maven/suppressions.xml`.
- **License headers**: Required on all source files (template in `copyright.txt`).

## IDE Setup

Mark `paimon-common/target/generated-sources/antlr4` as Sources Root.
