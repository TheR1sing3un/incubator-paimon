# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
from datetime import timedelta
from enum import Enum
from typing import Dict, List, Optional

from pypaimon.common.memory_size import MemorySize
from pypaimon.common.options import Options
from pypaimon.common.options.config_option import ConfigOption
from pypaimon.common.options.config_options import ConfigOptions
from pypaimon.common.options.options_utils import OptionsUtils


class ExternalPathStrategy(str, Enum):
    """
    Strategy for selecting external paths.
    """
    NONE = "none"
    ROUND_ROBIN = "round-robin"
    SPECIFIC_FS = "specific-fs"
    ENTROPY_INJECT = "entropy-inject"
    WEIGHTED = "weight-robin"


class ChangelogProducer(str, Enum):
    """
    Available changelog producer modes.
    """
    NONE = "none"
    INPUT = "input"
    FULL_COMPACTION = "full-compaction"
    LOOKUP = "lookup"


class MergeEngine(str, Enum):
    """
    Specifies the merge engine for table with primary key.
    """
    DEDUPLICATE = "deduplicate"
    PARTIAL_UPDATE = "partial-update"
    AGGREGATE = "aggregation"
    FIRST_ROW = "first-row"
    VERSIONED_PARTIAL_UPDATE = "versioned-partial-update"


class DvReadMode(str, Enum):
    """Read mode for deletion vector tables."""
    PERFORMANCE = "performance"
    FRESHNESS = "freshness"


class SortOrder(str, Enum):
    """
    Specifies the order of ``sequence.field``. Mirrors Java
    ``CoreOptions.SortOrder``.
    """
    ASCENDING = "ascending"
    DESCENDING = "descending"


class GlobalIndexColumnUpdateAction(str, Enum):
    THROW_ERROR = "THROW_ERROR"
    DROP_PARTITION_INDEX = "DROP_PARTITION_INDEX"


class CoreOptions:
    """Core options for Paimon tables."""
    # File format constants
    FILE_FORMAT_ORC: str = "orc"
    FILE_FORMAT_AVRO: str = "avro"
    FILE_FORMAT_PARQUET: str = "parquet"
    FILE_FORMAT_BLOB: str = "blob"
    FILE_FORMAT_LANCE: str = "lance"
    FILE_FORMAT_VORTEX: str = "vortex"
    FILE_FORMAT_ROW: str = "row"

    # Basic options
    AUTO_CREATE: ConfigOption[bool] = (
        ConfigOptions.key("auto-create")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to auto create table.")
    )

    PATH: ConfigOption[str] = (
        ConfigOptions.key("path")
        .string_type()
        .no_default_value()
        .with_description("The file path of this table in the filesystem.")
    )

    TYPE: ConfigOption[str] = (
        ConfigOptions.key("type")
        .string_type()
        .default_value("primary-key")
        .with_description("Specify what type of table this is.")
    )

    BRANCH: ConfigOption[str] = (
        ConfigOptions.key("branch")
        .string_type()
        .default_value("main")
        .with_description("The branch name of this table.")
    )

    BUCKET: ConfigOption[int] = (
        ConfigOptions.key("bucket")
        .int_type()
        .default_value(-1)
        .with_description(
            "Bucket number for file store. If bucket is -1, the parallelism of "
            "sink determines the bucket number: write a record to the file corresponding to the "
            "hash value of one or more fields. When creating table, bucket number must be specified."
        )
    )

    BUCKET_KEY: ConfigOption[str] = (
        ConfigOptions.key("bucket-key")
        .string_type()
        .no_default_value()
        .with_description(
            "Specify the hash key by bucket-key. "
            "By default, if there is a primary key, the primary key will be used; "
            "if there is no primary key, the full row will be used. "
            "In this case, the sink parallelism must be set to the bucket number."
        )
    )

    DYNAMIC_BUCKET_TARGET_ROW_NUM: ConfigOption[int] = (
        ConfigOptions.key("dynamic-bucket.target-row-num")
        .int_type()
        .default_value(2000000)
        .with_description(
            "In dynamic bucket mode (bucket=-1), target row number per bucket; "
            "when exceeded, a new bucket is created (aligned with Java SimpleHashBucketAssigner)."
        )
    )

    DYNAMIC_BUCKET_MAX_BUCKETS: ConfigOption[int] = (
        ConfigOptions.key("dynamic-bucket.max-buckets")
        .int_type()
        .default_value(-1)
        .with_description(
            "In dynamic bucket mode, max buckets per partition. -1 means unlimited."
        )
    )

    SCAN_MANIFEST_PARALLELISM: ConfigOption[int] = (
        ConfigOptions.key("scan.manifest.parallelism")
        .int_type()
        .default_value(16)
        .with_description("The parallelism for scanning manifest files.")
    )

    # File format options
    FILE_FORMAT: ConfigOption[str] = (
        ConfigOptions.key("file.format")
        .string_type()
        .default_value(FILE_FORMAT_ORC)
        .with_description("Specify the message format of data files.")
    )

    FILE_COMPRESSION: ConfigOption[str] = (
        ConfigOptions.key("file.compression")
        .string_type()
        .default_value("zstd")
        .with_description("Default file compression format. For faster read and write, it is recommended to use zstd.")
    )

    FILE_COMPRESSION_ZSTD_LEVEL: ConfigOption[int] = (
        ConfigOptions.key("file.compression.zstd-level")
        .int_type()
        .default_value(1)
        .with_description(
            "Default file compression zstd level. For higher compression rates, it can be configured to 9, "
            "but the read and write speed will significantly decrease."
        )
    )

    FILE_COMPRESSION_PER_LEVEL: ConfigOption[Dict[str, str]] = (
        ConfigOptions.key("file.compression.per.level")
        .map_type()
        .default_value({})
        .with_description(
            "Define different compression policies for different level LSM data files, "
            "you can add the level and the corresponding compression type."
        )
    )

    FILE_FORMAT_PER_LEVEL: ConfigOption[Dict[str, str]] = (
        ConfigOptions.key("file.format.per.level")
        .map_type()
        .default_value({})
        .with_description(
            "Define different format types for different level LSM data files, "
            "you can add the level and the corresponding format type."
        )
    )

    FILE_BLOCK_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("file.block-size")
        .memory_type()
        .no_default_value()
        .with_description("Define the data block size.")
    )

    METADATA_STATS_MODE: ConfigOption[str] = (
        ConfigOptions.key("metadata.stats-mode")
        .string_type()
        .default_value("none")
        .with_description("Stats Mode, Python by default is none. Java is truncate(16).")
    )

    BLOB_AS_DESCRIPTOR: ConfigOption[bool] = (
        ConfigOptions.key("blob-as-descriptor")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to return blob values as serialized BlobDescriptor bytes when reading.")
    )

    BLOB_FIELD: ConfigOption[str] = (
        ConfigOptions.key("blob-field")
        .string_type()
        .no_default_value()
        .with_description("Comma-separated column names that should be stored as blob type.")
    )

    BLOB_DESCRIPTOR_FIELD: ConfigOption[str] = (
        ConfigOptions.key("blob-descriptor-field")
        .string_type()
        .no_default_value()
        .with_description(
            "Comma-separated BLOB field names that should be stored as serialized BlobDescriptor bytes "
            "inline in normal data files."
        )
    )

    BLOB_EXTERNAL_STORAGE_PATH: ConfigOption[str] = (
        ConfigOptions.key("blob-external-storage-path")
        .string_type()
        .no_default_value()
        .with_description(
            "The external storage path where raw BLOB data from fields configured "
            "by 'blob-external-storage-field' is written at write time. "
            "Orphan file cleanup is not applied to this path."
        )
    )

    BLOB_EXTERNAL_STORAGE_FIELD: ConfigOption[str] = (
        ConfigOptions.key("blob-external-storage-field")
        .string_type()
        .no_default_value()
        .with_description(
            "Comma-separated BLOB field names (must be a subset of 'blob-descriptor-field') "
            "whose raw data will be written to external storage at write time. "
            "The external storage path is configured via 'blob-external-storage-path'."
        )
    )

    BLOB_VIEW_FIELD: ConfigOption[str] = (
        ConfigOptions.key("blob-view-field")
        .string_type()
        .no_default_value()
        .with_description("Comma-separated field names to treat as BLOB view fields.")
    )

    VECTOR_FIELD: ConfigOption[str] = (
        ConfigOptions.key("vector-field")
        .string_type()
        .no_default_value()
        .with_description("Comma-separated column names that should be stored as vector type.")
    )

    TARGET_FILE_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("target-file-size")
        .memory_type()
        .no_default_value()
        .with_description("The target file size for data files.")
    )

    BLOB_TARGET_FILE_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("blob.target-file-size")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(256))
        .with_description("The target file size for blob files.")
    )

    VECTOR_COLUMN_FAMILY_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("vector-column-family.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("Enable vector column family separation for primary key tables. "
                          "Vector columns are written to append-only .vector.bin files "
                          "and replaced with VectorDescriptor BINARY columns in the main data files.")
    )

    VECTOR_COLUMN_FAMILY_COLUMNS: ConfigOption[str] = (
        ConfigOptions.key("vector-column-family.columns")
        .string_type()
        .no_default_value()
        .with_description("Comma-separated vector column names to participate in vector-column-family. "
                          "Empty = auto-detect all VectorType columns.")
    )

    VECTOR_COLUMN_FAMILY_TARGET_FILE_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("vector-column-family.target-file-size")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(128))
        .with_description("Target size of one .vector.bin file before rollover.")
    )

    VECTOR_FILE_FORMAT: ConfigOption[str] = (
        ConfigOptions.key("vector.file.format")
        .string_type()
        .no_default_value()
        .with_description("Store VECTOR type columns separately in the specified file format.")
    )

    VECTOR_TARGET_FILE_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("vector.target-file-size")
        .memory_type()
        .no_default_value()
        .with_description("Target file size for vector data. Default is the same as target-file-size.")
    )

    DATA_FILE_PREFIX: ConfigOption[str] = (
        ConfigOptions.key("data-file.prefix")
        .string_type()
        .default_value("data-")
        .with_description("Specify the file name prefix of data files.")
    )
    # Scan options
    SCAN_FALLBACK_BRANCH: ConfigOption[str] = (
        ConfigOptions.key("scan.fallback-branch")
        .string_type()
        .no_default_value()
        .with_description("The fallback branch for scanning.")
    )

    INCREMENTAL_BETWEEN_TIMESTAMP: ConfigOption[str] = (
        ConfigOptions.key("incremental-between-timestamp")
        .string_type()
        .no_default_value()
        .with_description("The timestamp range for incremental reading.")
    )

    SCAN_TAG_NAME: ConfigOption[str] = (
        ConfigOptions.key("scan.tag-name")
        .string_type()
        .no_default_value()
        .with_description("Optional tag name used in case of 'from-snapshot' scan mode.")
    )

    SCAN_SNAPSHOT_ID: ConfigOption[int] = (
        ConfigOptions.key("scan.snapshot-id")
        .long_type()
        .no_default_value()
        .with_description(
            "Optional snapshot id used in case of 'from-snapshot' or "
            "'from-snapshot-full' scan mode."
        )
    )

    SCAN_TIMESTAMP_MILLIS: ConfigOption[int] = (
        ConfigOptions.key("scan.timestamp-millis")
        .long_type()
        .no_default_value()
        .with_description(
            "Optional timestamp in milliseconds used for time travel to the "
            "latest snapshot equal to or earlier than the given timestamp."
        )
    )

    SCAN_TIMESTAMP: ConfigOption[str] = (
        ConfigOptions.key("scan.timestamp")
        .string_type()
        .no_default_value()
        .with_description(
            "Optional timestamp string (e.g. '2023-12-01 12:00:00') used for "
            "time travel. Will be converted to milliseconds internally."
        )
    )

    SCAN_WATERMARK: ConfigOption[int] = (
        ConfigOptions.key("scan.watermark")
        .long_type()
        .no_default_value()
        .with_description(
            "Optional watermark used for time travel to the first snapshot "
            "with watermark greater than or equal to the given value."
        )
    )

    SOURCE_SPLIT_TARGET_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("source.split.target-size")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(128))
        .with_description("The target size of a source split when scanning a table.")
    )

    SOURCE_SPLIT_OPEN_FILE_COST: ConfigOption[MemorySize] = (
        ConfigOptions.key("source.split.open-file-cost")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(4))
        .with_description(
            "The estimated cost to open a file, used when scanning a table. "
            "It is used to avoid opening too many small files."
        )
    )
    DELETION_VECTORS_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("deletion-vectors.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to enable deletion vectors.")
    )

    DELETION_VECTORS_READ_MODE: ConfigOption[DvReadMode] = (
        ConfigOptions.key("deletion-vectors.read-mode")
        .enum_type(DvReadMode)
        .default_value(DvReadMode.PERFORMANCE)
        .with_description(
            "Read mode for deletion vector tables. "
            "PERFORMANCE: only read compacted data (level >= 1), best read performance. "
            "FRESHNESS: read all levels including level-0 for better data freshness, "
            "level-0 files will be merged on read with deletion vector pre-filtering."
        )
    )

    CHANGELOG_PRODUCER: ConfigOption[ChangelogProducer] = (
        ConfigOptions.key("changelog-producer")
        .enum_type(ChangelogProducer)
        .default_value(ChangelogProducer.NONE)
        .with_description("The changelog producer for streaming reads. "
                          "Options: none, input, full-compaction, lookup.")
    )

    MERGE_ENGINE: ConfigOption[MergeEngine] = (
        ConfigOptions.key("merge-engine")
        .enum_type(MergeEngine)
        .default_value(MergeEngine.DEDUPLICATE)
        .with_description("Specify the merge engine for table with primary key. "
                          "Options: deduplicate, partial-update, aggregation, first-row, "
                          "versioned-partial-update.")
    )

    IGNORE_DELETE: ConfigOption[bool] = (
        ConfigOptions.key("ignore-delete")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to ignore delete records.")
    )

    VERSIONED_PARTIAL_UPDATE_MERGE_MODE: ConfigOption[str] = (
        ConfigOptions.key("versioned-partial-update.merge-mode")
        .string_type()
        .default_value("upsert")
        .with_description(
            "Per-job merge mode for versioned-partial-update merge engine. "
            "Options: upsert, ignore.")
    )

    VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("versioned-partial-update.ignore-mode.enabled")
        .boolean_type()
        .default_value(True)
        .with_description(
            "Whether to enable ignore mode for versioned-partial-update. "
            "When enabled, the table must have lookup capability.")
    )

    # Field-level aggregation options. Per-field keys ``fields.<field>.*`` are
    # read directly from the option map by helper methods below.
    FIELDS_DEFAULT_AGG_FUNC: ConfigOption[str] = (
        ConfigOptions.key("fields.default-aggregate-function")
        .string_type()
        .no_default_value()
        .with_description(
            "Default aggregate function for partial-update, "
            "versioned-partial-update and aggregate merge functions.")
    )
    FIELDS_PREFIX = "fields"
    AGG_FUNCTION_SUFFIX = "aggregate-function"
    DISTINCT_SUFFIX = "distinct"
    LIST_AGG_DELIMITER_SUFFIX = "list-agg-delimiter"

    SEQUENCE_FIELD: ConfigOption[str] = (
        ConfigOptions.key("sequence.field")
        .string_type()
        .no_default_value()
        .with_description("The field that generates the sequence number for "
                          "primary key table, the sequence number determines "
                          "which data is the most recent.")
    )

    SEQUENCE_FIELD_SORT_ORDER: ConfigOption[SortOrder] = (
        ConfigOptions.key("sequence.field.sort-order")
        .enum_type(SortOrder)
        .default_value(SortOrder.ASCENDING)
        .with_description("Specify the order of sequence.field.")
    )

    # Commit options
    COMMIT_USER_PREFIX: ConfigOption[str] = (
        ConfigOptions.key("commit.user-prefix")
        .string_type()
        .no_default_value()
        .with_description("The prefix for commit user.")
    )

    COMMIT_COMMITTER: ConfigOption[str] = (
        ConfigOptions.key("commit.committer")
        .string_type()
        .no_default_value()
        .with_description("The committer name for snapshot properties.")
    )

    COMMIT_MESSAGE: ConfigOption[str] = (
        ConfigOptions.key("commit.message")
        .string_type()
        .no_default_value()
        .with_description("The commit message for snapshot properties.")
    )

    COMMIT_METADATA_PREFIX = "commit.metadata."
    SNAPSHOT_COMMIT_PREFIX = "paimon."

    COMMIT_MAX_RETRIES: ConfigOption[int] = (
        ConfigOptions.key("commit.max-retries")
        .int_type()
        .default_value(10)
        .with_description("Maximum number of retries for commit operations.")
    )

    COMMIT_TIMEOUT: ConfigOption[timedelta] = (
        ConfigOptions.key("commit.timeout")
        .duration_type()
        .no_default_value()
        .with_description("Timeout for commit operations (e.g., '10s', '5m'). If not set, effectively unlimited.")
    )

    COMMIT_MIN_RETRY_WAIT: ConfigOption[timedelta] = (
        ConfigOptions.key("commit.min-retry-wait")
        .duration_type()
        .default_value(timedelta(milliseconds=10))
        .with_description("Minimum wait time between commit retries (e.g., '10ms', '100ms').")
    )

    COMMIT_MAX_RETRY_WAIT: ConfigOption[timedelta] = (
        ConfigOptions.key("commit.max-retry-wait")
        .duration_type()
        .default_value(timedelta(seconds=10))
        .with_description("Maximum wait time between commit retries (e.g., '1s', '10s').")
    )

    ROW_TRACKING_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("row-tracking.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to enable row tracking.")
    )

    SNAPSHOT_SEQUENCE_ORDERING: ConfigOption[bool] = (
        ConfigOptions.key("sequence.snapshot-ordering")
        .boolean_type()
        .default_value(False)
        .with_description(
            "When enabled, merge uses snapshot commit order to determine "
            "record ordering instead of sequence numbers."
        )
    )

    DATA_EVOLUTION_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("data-evolution.enabled")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to enable data evolution.")
    )
    # External paths options
    DATA_FILE_EXTERNAL_PATHS: ConfigOption[str] = (
        ConfigOptions.key("data-file.external-paths")
        .string_type()
        .no_default_value()
        .with_description("External paths for data files, separated by comma.")
    )

    DATA_FILE_EXTERNAL_PATHS_STRATEGY: ConfigOption[str] = (
        ConfigOptions.key("data-file.external-paths.strategy")
        .string_type()
        .default_value(ExternalPathStrategy.NONE)
        .with_description(
            "Strategy for selecting external paths. "
            "Options: none, round-robin, specific-fs, entropy-inject, weight-robin."
        )
    )

    DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS: ConfigOption[str] = (
        ConfigOptions.key("data-file.external-paths.specific-fs")
        .string_type()
        .no_default_value()
        .with_description("Specific filesystem for external paths when using specific-fs strategy.")
    )

    DATA_FILE_EXTERNAL_PATHS_WEIGHTS: ConfigOption[str] = (
        ConfigOptions.key("data-file.external-paths.weights")
        .string_type()
        .no_default_value()
        .with_description(
            "Weights for external paths when strategy is weight-robin. "
            "Format: comma-separated positive integers corresponding to paths in order."
        )
    )

    # Global Index options
    GLOBAL_INDEX_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("global-index.enabled")
        .boolean_type()
        .default_value(True)
        .with_description("Whether to enable global index for scan.")
    )

    GLOBAL_INDEX_THREAD_NUM: ConfigOption[int] = (
        ConfigOptions.key("global-index.thread-num")
        .int_type()
        .no_default_value()
        .with_description(
            "The maximum number of concurrent scanner for global index. "
            "By default is the number of processors available."
        )
    )

    LOCAL_CACHE_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("local-cache.enabled")
        .boolean_type()
        .default_value(False)
        .with_description(
            "Whether to enable local block cache for file reads. "
            "If local-cache.dir is configured, disk cache is used; otherwise memory cache is used."
        )
    )

    LOCAL_CACHE_DIR: ConfigOption[str] = (
        ConfigOptions.key("local-cache.dir")
        .string_type()
        .no_default_value()
        .with_description(
            "Directory for local block cache on disk. "
            "If not configured, memory cache is used instead."
        )
    )

    LOCAL_CACHE_MAX_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("local-cache.max-size")
        .memory_type()
        .no_default_value()
        .with_description("Maximum total size of the local block cache. Unlimited by default.")
    )

    LOCAL_CACHE_BLOCK_SIZE: ConfigOption[MemorySize] = (
        ConfigOptions.key("local-cache.block-size")
        .memory_type()
        .default_value(MemorySize.of_mebi_bytes(1))
        .with_description("Block size for local cache.")
    )

    LOCAL_CACHE_WHITELIST: ConfigOption[str] = (
        ConfigOptions.key("local-cache.whitelist")
        .string_type()
        .default_value("meta,global-index")
        .with_description(
            "Comma-separated list of file types to cache. "
            "Supported values: meta, global-index, bucket-index, data, file-index."
        )
    )

    READ_BATCH_SIZE: ConfigOption[int] = (
        ConfigOptions.key("read.batch-size")
        .int_type()
        .default_value(1024)
        .with_description("Read batch size for any file format if it supports.")
    )

    READ_PARALLELISM: ConfigOption[int] = (
        ConfigOptions.key("read.parallelism")
        .int_type()
        .default_value(1)
        .with_description(
            "Parallelism for reading splits within a single TableRead call. "
            "The value 1 (default) keeps reads serial. Values >= 2 enable a "
            "thread pool that reads splits concurrently and assembles the "
            "result in input order. Has no effect when fewer than 2 splits "
            "are passed.")
    )

    ADD_COLUMN_BEFORE_PARTITION: ConfigOption[bool] = (
        ConfigOptions.key("add-column-before-partition")
        .boolean_type()
        .default_value(False)
        .with_description(
            "When adding a new column, if the table has partition keys, "
            "insert the new column before the first partition column by default."
        )
    )

    VARIANT_SHREDDING_ENABLED: ConfigOption[bool] = (
        ConfigOptions.key("variant.shredding.enabled")
        .boolean_type()
        .default_value(True)
        .with_description(
            "Whether to enable VARIANT shredding. When True (default), writes apply the "
            "shredding schema configured via 'variant.shreddingSchema', and reads "
            "automatically reassemble shredded columns back to the standard "
            "struct<value, metadata> form. Set to False to bypass both behaviours."
        )
    )

    VARIANT_SHREDDING_SCHEMA: ConfigOption[str] = (
        ConfigOptions.key("variant.shreddingSchema")
        .string_type()
        .no_default_value()
        .with_description(
            "JSON-encoded ROW type specifying which VARIANT sub-fields to shred when "
            "writing Parquet (static shredding mode). The top-level fields map VARIANT "
            "column names to their sub-field schemas. "
            "Alias: 'parquet.variant.shreddingSchema'. "
            "Example: '{\"type\":\"ROW\",\"fields\":[{\"id\":0,\"name\":\"payload\","
            "\"type\":{\"type\":\"ROW\",\"fields\":[{\"id\":0,\"name\":\"age\","
            "\"type\":\"BIGINT\"}]}}]}'"
        )
    )

    PARTITION_DEFAULT_NAME: ConfigOption[str] = (
        ConfigOptions.key("partition.default-name")
        .string_type()
        .default_value("__DEFAULT_PARTITION__")
        .with_description(
            "The default partition name in case the dynamic partition"
            " column value is null/empty string."
        )
    )

    DYNAMIC_PARTITION_OVERWRITE: ConfigOption[bool] = (
        ConfigOptions.key("dynamic-partition-overwrite")
        .boolean_type()
        .default_value(True)
        .with_description(
            "Whether only overwrite dynamic partition when overwriting a partitioned table "
            "with dynamic partition columns. Works only when the table has partition keys."
        )
    )

    # FAISS Vector Index options
    VECTOR_DIM: ConfigOption[int] = (
        ConfigOptions.key("vector.dim")
        .int_type()
        .default_value(128)
        .with_description("The dimension of the vector.")
    )

    VECTOR_METRIC: ConfigOption[str] = (
        ConfigOptions.key("vector.metric")
        .string_type()
        .default_value("L2")
        .with_description("The similarity metric for vector search (L2, INNER_PRODUCT).")
    )

    VECTOR_INDEX_TYPE: ConfigOption[str] = (
        ConfigOptions.key("vector.index-type")
        .string_type()
        .default_value("IVF_SQ8")
        .with_description("The type of FAISS index (FLAT, HNSW, IVF, IVF_PQ, IVF_SQ8).")
    )

    VECTOR_M: ConfigOption[int] = (
        ConfigOptions.key("vector.m")
        .int_type()
        .default_value(32)
        .with_description("Maximum connections per element in HNSW index.")
    )

    VECTOR_EF_CONSTRUCTION: ConfigOption[int] = (
        ConfigOptions.key("vector.ef-construction")
        .int_type()
        .default_value(40)
        .with_description("Size of dynamic candidate list during HNSW construction.")
    )

    VECTOR_EF_SEARCH: ConfigOption[int] = (
        ConfigOptions.key("vector.ef-search")
        .int_type()
        .default_value(16)
        .with_description("Size of dynamic candidate list during HNSW search.")
    )

    VECTOR_NLIST: ConfigOption[int] = (
        ConfigOptions.key("vector.nlist")
        .int_type()
        .default_value(100)
        .with_description("Number of inverted lists (clusters) for IVF index.")
    )

    VECTOR_NPROBE: ConfigOption[int] = (
        ConfigOptions.key("vector.nprobe")
        .int_type()
        .default_value(64)
        .with_description("Number of clusters to visit during IVF search.")
    )

    VECTOR_SIZE_PER_INDEX: ConfigOption[int] = (
        ConfigOptions.key("vector.size-per-index")
        .int_type()
        .default_value(2000000)
        .with_description("Size of vectors stored in each vector index file.")
    )

    VECTOR_SEARCH_FACTOR: ConfigOption[int] = (
        ConfigOptions.key("vector.search-factor")
        .int_type()
        .default_value(10)
        .with_description("Multiplier for search limit when filtering is applied.")
    )

    VECTOR_NORMALIZE: ConfigOption[bool] = (
        ConfigOptions.key("vector.normalize")
        .boolean_type()
        .default_value(False)
        .with_description("Whether to L2 normalize vectors for cosine similarity.")
    )

    MAX_ROWS_PER_FILE: ConfigOption[int] = (
        ConfigOptions.key("max-rows-per-file")
        .int_type()
        .no_default_value()
        .with_description(
            "Maximum number of rows per data file. When the pending data exceeds this limit, "
            "the writer rolls to a new file. If not set, only target-file-size controls file rolling."
        )
    )

    def __init__(self, options: Options):
        self.options = options

    def set(self, key: ConfigOption, value):
        self.options.set(key, value)

    @staticmethod
    def copy(options: 'CoreOptions') -> 'CoreOptions':
        return CoreOptions.from_dict(dict(options.options.to_map()))

    @staticmethod
    def from_dict(options: dict) -> 'CoreOptions':
        return CoreOptions(Options(options))

    def path(self, default=None):
        return self.options.get(CoreOptions.PATH, default)

    def auto_create(self, default=None):
        return self.options.get(CoreOptions.AUTO_CREATE, default)

    def type(self, default=None):
        return self.options.get(CoreOptions.TYPE, default)

    def branch(self, default=None):
        return self.options.get(CoreOptions.BRANCH, default)

    def bucket(self, default=None):
        return self.options.get(CoreOptions.BUCKET, default)

    def bucket_key(self, default=None):
        return self.options.get(CoreOptions.BUCKET_KEY, default)

    def dynamic_bucket_target_row_num(self, default=None):
        return self.options.get(CoreOptions.DYNAMIC_BUCKET_TARGET_ROW_NUM, default)

    def dynamic_bucket_max_buckets(self, default=None):
        return self.options.get(CoreOptions.DYNAMIC_BUCKET_MAX_BUCKETS, default)

    def scan_manifest_parallelism(self, default=None):
        return self.options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM, default)

    def file_format(self, default=None):
        return self.options.get(CoreOptions.FILE_FORMAT, default)

    def file_compression(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION, default)

    def file_compression_zstd_level(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION_ZSTD_LEVEL, default)

    def file_compression_per_level(self, default=None):
        return self.options.get(CoreOptions.FILE_COMPRESSION_PER_LEVEL, default)

    def file_format_per_level(self, default=None):
        return self.options.get(CoreOptions.FILE_FORMAT_PER_LEVEL, default)

    def file_block_size(self, default=None):
        return self.options.get(CoreOptions.FILE_BLOCK_SIZE, default)

    def metadata_stats_enabled(self, default=None):
        return self.options.get(CoreOptions.METADATA_STATS_MODE, default) == "full"

    def blob_as_descriptor(self, default=None):
        return self.options.get(CoreOptions.BLOB_AS_DESCRIPTOR, default)

    def variant_shredding_enabled(self) -> bool:
        return self.options.get(CoreOptions.VARIANT_SHREDDING_ENABLED, True)

    def variant_shredding_schema(self) -> Optional[str]:
        val = self.options.get(CoreOptions.VARIANT_SHREDDING_SCHEMA)
        if val is None:
            # Support alias used by Java: parquet.variant.shreddingSchema
            val = self.options.data.get("parquet.variant.shreddingSchema")
        return val

    def blob_descriptor_fields(self, default=None):
        value = self.options.get(CoreOptions.BLOB_DESCRIPTOR_FIELD, default)
        if value is None:
            return set()
        if isinstance(value, str):
            return {field.strip() for field in value.split(",") if field.strip()}
        if isinstance(value, (list, set, tuple)):
            return {str(field).strip() for field in value if str(field).strip()}
        return set()

    def blob_external_storage_fields(self, default=None):
        value = self.options.get(CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD, default)
        if value is None:
            return set()
        if isinstance(value, str):
            return {field.strip() for field in value.split(",") if field.strip()}
        if isinstance(value, (list, set, tuple)):
            return {str(field).strip() for field in value if str(field).strip()}
        return set()

    def blob_external_storage_path(self, default=None):
        return self.options.get(CoreOptions.BLOB_EXTERNAL_STORAGE_PATH, default)

    def target_file_size(self, has_primary_key, default=None):
        return self.options.get(CoreOptions.TARGET_FILE_SIZE,
                                MemorySize.of_mebi_bytes(
                                    128 if has_primary_key else 256) if default is None else MemorySize.parse(
                                    default)).get_bytes()

    def blob_target_file_size(self, default=None):
        """
        Args:
            default: The standby value when the configuration item does not exist. If default is also None,
            then use the default value of the configuration item itself.
        """
        if self.options.contains(CoreOptions.BLOB_TARGET_FILE_SIZE):
            return self.options.get(CoreOptions.BLOB_TARGET_FILE_SIZE, None).get_bytes()
        elif default is not None:
            return MemorySize.parse(default).get_bytes()
        else:
            return self.target_file_size(has_primary_key=False)

    def vector_file_format(self, default=None):
        return self.options.get(CoreOptions.VECTOR_FILE_FORMAT, default)

    def with_vector_format(self) -> bool:
        return self.options.contains(CoreOptions.VECTOR_FILE_FORMAT)

    def vector_target_file_size(self, default=None):
        if self.options.contains(CoreOptions.VECTOR_TARGET_FILE_SIZE):
            return self.options.get(CoreOptions.VECTOR_TARGET_FILE_SIZE, None).get_bytes()
        elif default is not None:
            return MemorySize.parse(default).get_bytes()
        else:
            return self.target_file_size(has_primary_key=False)

    def data_file_prefix(self, default=None):
        return self.options.get(CoreOptions.DATA_FILE_PREFIX, default)

    def scan_fallback_branch(self, default=None):
        return self.options.get(CoreOptions.SCAN_FALLBACK_BRANCH, default)

    def incremental_between_timestamp(self, default=None):
        return self.options.get(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP, default)

    def scan_tag_name(self, default=None):
        return self.options.get(CoreOptions.SCAN_TAG_NAME, default)

    def scan_snapshot_id(self, default=None):
        return self.options.get(CoreOptions.SCAN_SNAPSHOT_ID, default)

    def scan_timestamp_millis(self, default=None):
        return self.options.get(CoreOptions.SCAN_TIMESTAMP_MILLIS, default)

    def scan_timestamp(self, default=None):
        return self.options.get(CoreOptions.SCAN_TIMESTAMP, default)

    def scan_watermark(self, default=None):
        return self.options.get(CoreOptions.SCAN_WATERMARK, default)

    def source_split_target_size(self, default=None):
        return self.options.get(CoreOptions.SOURCE_SPLIT_TARGET_SIZE, default).get_bytes()

    def source_split_open_file_cost(self, default=None):
        return self.options.get(CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST, default).get_bytes()

    def commit_user_prefix(self, default=None):
        return self.options.get(CoreOptions.COMMIT_USER_PREFIX, default)

    def row_tracking_enabled(self, default=None):
        return self.options.get(CoreOptions.ROW_TRACKING_ENABLED, default)

    def data_evolution_enabled(self, default=None):
        return self.options.get(CoreOptions.DATA_EVOLUTION_ENABLED, default)

    def deletion_vectors_enabled(self, default=None):
        return self.options.get(CoreOptions.DELETION_VECTORS_ENABLED, default)

    def changelog_producer(self, default=None):
        return self.options.get(CoreOptions.CHANGELOG_PRODUCER, default)

    def merge_engine(self, default=None):
        return self.options.get(CoreOptions.MERGE_ENGINE, default)

    def sequence_field(self) -> List[str]:
        """User-defined sequence fields, in declaration order. Empty list
        when ``sequence.field`` is unset. Mirrors Java
        ``CoreOptions.sequenceField()``.
        """
        raw = self.options.get(CoreOptions.SEQUENCE_FIELD)
        if not raw:
            return []
        # Mirror Java ``CoreOptions.sequenceField()``
        # (``Arrays.stream(s.split(',')).map(String::trim)``): Java's
        # ``String.split(",")`` drops *trailing* empty segments (so ``'ts,'``
        # yields ``['ts']``) but keeps interior ones, and each segment is
        # then trimmed. So an interior empty segment (``'ts,,ts2'``) survives
        # as an empty field name that ``check_sequence_field_valid`` rejects,
        # while a trailing comma is tolerated.
        segments = raw.split(",")
        while segments and segments[-1] == "":
            segments.pop()
        return [name.strip() for name in segments]

    def sequence_field_sort_order_is_ascending(self) -> bool:
        """Whether ``sequence.field.sort-order`` is ascending (the default).
        Mirrors Java ``CoreOptions.sequenceFieldSortOrderIsAscending()``.
        """
        return (self.options.get(CoreOptions.SEQUENCE_FIELD_SORT_ORDER)
                == SortOrder.ASCENDING)

    def ignore_delete(self) -> bool:
        raw = self.options.to_map()
        fallback_keys = (
            "ignore-delete", "first-row.ignore-delete",
            "deduplicate.ignore-delete",
            "partial-update.ignore-delete",
        )
        for key in fallback_keys:
            val = raw.get(key)
            if val is not None:
                return OptionsUtils.convert_to_boolean(val)
        return False

    def data_file_external_paths(self, default=None):
        external_paths_str = self.options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS, default)
        if not external_paths_str:
            return None
        return [path.strip() for path in external_paths_str.split(",") if path.strip()]

    def data_file_external_paths_strategy(self, default=None):
        return self.options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_STRATEGY, default)

    def data_file_external_paths_specific_fs(self, default=None):
        return self.options.get(CoreOptions.DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS, default)

    def data_file_external_paths_weights(self, default=None):
        value = self.options.get(
            CoreOptions.DATA_FILE_EXTERNAL_PATHS_WEIGHTS, default
        )
        if value is None:
            return None
        parts = value.split(",")
        weights = []
        for part in parts:
            parsed = int(part.strip())
            if parsed <= 0:
                raise ValueError(
                    f"Weight must be positive, got: {parsed}"
                )
            weights.append(parsed)
        return weights

    def commit_max_retries(self) -> int:
        return self.options.get(CoreOptions.COMMIT_MAX_RETRIES)

    def commit_timeout(self) -> int:
        timeout = self.options.get(CoreOptions.COMMIT_TIMEOUT)
        if timeout is None:
            return sys.maxsize
        return int(timeout.total_seconds() * 1000)

    def commit_min_retry_wait(self) -> int:
        wait = self.options.get(CoreOptions.COMMIT_MIN_RETRY_WAIT)
        return int(wait.total_seconds() * 1000)

    def commit_max_retry_wait(self) -> int:
        wait = self.options.get(CoreOptions.COMMIT_MAX_RETRY_WAIT)
        return int(wait.total_seconds() * 1000)

    def global_index_enabled(self, default=None):
        return self.options.get(CoreOptions.GLOBAL_INDEX_ENABLED, default)

    def global_index_thread_num(self) -> Optional[int]:
        return self.options.get(CoreOptions.GLOBAL_INDEX_THREAD_NUM)

    def local_cache_enabled(self) -> bool:
        return self.options.get(CoreOptions.LOCAL_CACHE_ENABLED)

    def local_cache_dir(self) -> Optional[str]:
        return self.options.get(CoreOptions.LOCAL_CACHE_DIR)

    def local_cache_max_size(self) -> Optional[MemorySize]:
        return self.options.get(CoreOptions.LOCAL_CACHE_MAX_SIZE)

    def local_cache_block_size(self) -> MemorySize:
        return self.options.get(CoreOptions.LOCAL_CACHE_BLOCK_SIZE)

    def local_cache_whitelist(self) -> str:
        return self.options.get(CoreOptions.LOCAL_CACHE_WHITELIST)

    def read_batch_size(self, default=None) -> int:
        return self.options.get(CoreOptions.READ_BATCH_SIZE, default or 1024)

    def read_parallelism(self, default=None) -> int:
        return self.options.get(CoreOptions.READ_PARALLELISM, default)

    def add_column_before_partition(self) -> bool:
        return self.options.get(CoreOptions.ADD_COLUMN_BEFORE_PARTITION, False)

    def dynamic_partition_overwrite(self) -> bool:
        return self.options.get(CoreOptions.DYNAMIC_PARTITION_OVERWRITE)

    # Vector column family accessors
    def vector_column_family_enabled(self, default=None):
        return self.options.get(CoreOptions.VECTOR_COLUMN_FAMILY_ENABLED, default)

    def vector_column_family_columns(self, default=None):
        raw = self.options.get(CoreOptions.VECTOR_COLUMN_FAMILY_COLUMNS, default)
        if raw is None:
            return []
        if isinstance(raw, str):
            return [c.strip() for c in raw.split(",") if c.strip()]
        if isinstance(raw, (list, set, tuple)):
            return [str(c).strip() for c in raw if str(c).strip()]
        return []

    def vector_column_family_target_file_size(self, default=None):
        value = self.options.get(CoreOptions.VECTOR_COLUMN_FAMILY_TARGET_FILE_SIZE, default)
        if value is None:
            return MemorySize.of_mebi_bytes(128).get_bytes()
        return value.get_bytes()

    # Commit message / committer / metadata accessors
    def commit_committer(self, default=None):
        return self.options.get(CoreOptions.COMMIT_COMMITTER, default)

    def commit_message(self, default=None):
        return self.options.get(CoreOptions.COMMIT_MESSAGE, default)

    def snapshot_sequence_ordering(self, default=None):
        return self.options.get(CoreOptions.SNAPSHOT_SEQUENCE_ORDERING, default)

    # Deletion vector read mode accessors
    def dv_read_mode(self, default=None):
        return self.options.get(CoreOptions.DELETION_VECTORS_READ_MODE, default)

    def dv_freshness_read_enabled(self) -> bool:
        return self.deletion_vectors_enabled() and self.dv_read_mode() == DvReadMode.FRESHNESS

    # Versioned partial-update / ignore-delete accessors
    def ignore_delete(self, default=None):
        return self.options.get(CoreOptions.IGNORE_DELETE, default)

    def versioned_partial_update_merge_mode(self, default=None):
        return self.options.get(CoreOptions.VERSIONED_PARTIAL_UPDATE_MERGE_MODE, default)

    def versioned_partial_update_ignore_mode_enabled(self, default=None):
        return self.options.get(CoreOptions.VERSIONED_PARTIAL_UPDATE_IGNORE_MODE_ENABLED, default)

    # Field-level aggregation helpers (per-field keys read directly from option map)
    def fields_default_agg_func(self, default=None) -> Optional[str]:
        return self.options.get(CoreOptions.FIELDS_DEFAULT_AGG_FUNC, default)

    def field_agg_func(self, field_name: str) -> Optional[str]:
        key = "fields.%s.aggregate-function" % field_name
        return self.options.to_map().get(key)

    def field_collect_agg_distinct(self, field_name: str) -> bool:
        key = "fields.%s.distinct" % field_name
        raw = self.options.to_map().get(key)
        return raw is True or (isinstance(raw, str) and raw.lower() == "true")

    def field_list_agg_delimiter(self, field_name: str) -> str:
        key = "fields.%s.list-agg-delimiter" % field_name
        raw = self.options.to_map().get(key)
        return raw if raw is not None else ","

    def defined_agg_func(self) -> bool:
        if self.options.contains(CoreOptions.FIELDS_DEFAULT_AGG_FUNC):
            return True
        for k in self.options.to_map().keys():
            if k.startswith("fields.") and k.endswith(".aggregate-function"):
                return True
        return False

    # FAISS vector index accessors
    def vector_dim(self, default=None):
        return self.options.get(CoreOptions.VECTOR_DIM, default)

    def vector_metric(self, default=None):
        return self.options.get(CoreOptions.VECTOR_METRIC, default)

    def vector_index_type(self, default=None):
        return self.options.get(CoreOptions.VECTOR_INDEX_TYPE, default)

    def vector_m(self, default=None):
        return self.options.get(CoreOptions.VECTOR_M, default)

    def vector_ef_construction(self, default=None):
        return self.options.get(CoreOptions.VECTOR_EF_CONSTRUCTION, default)

    def vector_ef_search(self, default=None):
        return self.options.get(CoreOptions.VECTOR_EF_SEARCH, default)

    def vector_nlist(self, default=None):
        return self.options.get(CoreOptions.VECTOR_NLIST, default)

    def vector_nprobe(self, default=None):
        return self.options.get(CoreOptions.VECTOR_NPROBE, default)

    def vector_size_per_index(self, default=None):
        return self.options.get(CoreOptions.VECTOR_SIZE_PER_INDEX, default)

    def vector_search_factor(self, default=None):
        return self.options.get(CoreOptions.VECTOR_SEARCH_FACTOR, default)

    def vector_normalize(self, default=None):
        return self.options.get(CoreOptions.VECTOR_NORMALIZE, default)

    def max_rows_per_file(self, default=None) -> Optional[int]:
        return self.options.get(CoreOptions.MAX_ROWS_PER_FILE, default)
