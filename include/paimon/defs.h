/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <cstdint>
#include <limits>

#include "paimon/visibility.h"

namespace paimon {

/// Enumeration of supported data types in Paimon tables.
enum class FieldType {
    BOOLEAN = 1,
    TINYINT = 2,
    SMALLINT = 3,
    INT = 4,
    BIGINT = 5,
    FLOAT = 6,
    DOUBLE = 7,
    STRING = 8,
    BINARY = 9,
    /// timestamp type only supports precision values of 0, 3, 6, 9:
    /// - 0: second precision
    /// - 3: millisecond precision
    /// - 6: microsecond precision
    /// - 9: nanosecond precision
    TIMESTAMP = 10,
    DECIMAL = 11,
    DATE = 12,
    ARRAY = 13,
    MAP = 14,
    STRUCT = 15,
    BLOB = 16,
    UNKNOWN = 128,
};

/// Configuration options and constants for Paimon table operations.
///
/// The Options struct contains static string constants that define configuration keys
/// used throughout the Paimon system.
struct PAIMON_EXPORT Options {
    /// @name merge-on-read configurations
    /// The 5 constants are the prefixes or suffixes for merge on read configuration.
    /// The complete configuration keys can be:
    /// - fields.$field_name.aggregate-function
    /// - fields.$field_name.ignore-retract
    /// - fields.$field_names.sequence-group ($field_names support one or more field_name, split
    /// with FIELDS_SEPARATOR)
    ///
    /// examples:
    /// - fields.f1.aggregate-function
    /// - fields.f2.sequence-group
    /// - fields.f3,f4.sequence-group
    ///
    /// @{

    /// FIELDS_SEPARATOR is ","
    static const char FIELDS_SEPARATOR[];
    /// FIELDS_PREFIX is "fields"
    static const char FIELDS_PREFIX[];
    /// AGG_FUNCTION is "aggregate-function"
    static const char AGG_FUNCTION[];
    /// DEFAULT_AGG_FUNCTION is "default-aggregate-function"
    static const char DEFAULT_AGG_FUNCTION[];
    /// IGNORE_RETRACT is "ignore-retract"
    static const char IGNORE_RETRACT[];
    /// SEQUENCE_GROUP is "sequence-group"
    static const char SEQUENCE_GROUP[];
    /// @}

    /// "bucket" - Bucket number for file store. It should either be equal to -1 (dynamic bucket
    /// mode), or it must be greater than 0 (fixed bucket mode).
    static const char BUCKET[];

    /// "bucket-key" - Specify the paimon distribution policy. Data is assigned to each bucket
    /// according to the hash value of bucket-key. If you specify multiple fields, delimiter is ','.
    /// If not specified, the primary key will be used, if there is no primary key, the full row
    /// will be used.
    static const char BUCKET_KEY[];

    // TODO(yonghao.fyh): This option has not been used yet
    /// "page-size" - Memory page size, default value 64 kb.
    static const char PAGE_SIZE[];

    /// "file.format" - Specify the message format of data files.
    /// Default value is parquet.
    static const char FILE_FORMAT[];

    /// "file-system" - Specify the file system.
    /// Default value is local.
    static const char FILE_SYSTEM[];

    /// "target-file-size" - Target size of a file. Default value is 256MB.
    // TODO(xinyu.lxy): change the default value to 128MB for primary key table.
    static const char TARGET_FILE_SIZE[];

    /// "blob.target-file-size" - Target size of a blob file. Default is TARGET_FILE_SIZE.
    static const char BLOB_TARGET_FILE_SIZE[];

    /// "partition.default-name" - The default partition name in case the dynamic partition column
    /// value is null/empty string.
    static const char PARTITION_DEFAULT_NAME[];

    /// "file.compression" - The default file compression is zstd. For faster read and write, it is
    /// recommended to use zstd.
    static const char FILE_COMPRESSION[];

    /// "file.compression.zstd-level"
    /// Default file compression zstd level. For higher compression rates, it can be configured to
    /// 9, but the read and write speed will significantly decrease. Default value is 1.
    static const char FILE_COMPRESSION_ZSTD_LEVEL[];

    /// "manifest.target-file-size" - Suggested file size of a manifest file.
    /// Default value is 8MB.
    static const char MANIFEST_TARGET_FILE_SIZE[];

    /// "manifest.format" - Specify the message format of manifest files.
    /// Default value is avro.
    static const char MANIFEST_FORMAT[];

    /// "manifest.compression" - File compression for manifest, default value is zstd.
    static const char MANIFEST_COMPRESSION[];

    /// "manifest.merge-min-count" - To avoid frequent manifest merges, this parameter specifies the
    /// minimum number of ManifestFileMeta to merge, default value is 30.
    static const char MANIFEST_MERGE_MIN_COUNT[];

    /// "manifest.full-compaction-threshold-size" - The size threshold for triggering full
    /// compaction of manifest, default value is 16MB.
    static const char MANIFEST_FULL_COMPACTION_FILE_SIZE[];

    /// "source.split.target-size" - Target size of a source split when scanning a bucket. Default
    /// value is 128MB.
    static const char SOURCE_SPLIT_TARGET_SIZE[];

    /// "source.split.open-file-cost" - Open file cost of a source file. It is used to avoid reading
    /// too many files with a source split, which can be very slow. Default value is 4MB.
    static const char SOURCE_SPLIT_OPEN_FILE_COST[];

    /// "scan.snapshot-id" - Optional snapshot id used in case of "from-snapshot" or
    /// "from-snapshot-full" scan mode
    static const char SCAN_SNAPSHOT_ID[];

    /// "scan.mode" - Specify the scanning behavior of the source. Values can be: "default",
    /// "latest-full", "latest", "from-snapshot", "from-snapshot-full". Default value is "default".
    static const char SCAN_MODE[];

    /// "read.batch-size" - Read batch size for any file format if it supports.
    /// The default value is 1024.
    static const char READ_BATCH_SIZE[];

    /// "write.batch-size" - Write batch size for any file format if it supports.
    /// The default value is 1024.
    static const char WRITE_BATCH_SIZE[];

    /// "write-buffer-size" - Amount of data to build up in memory before converting to a sorted
    /// on-disk file. The default value is 256 mb
    static const char WRITE_BUFFER_SIZE[];

    /// "snapshot.num-retained.min" - The minimum number of completed snapshots to retain. Should be
    /// greater than or equal to 1. Default value is 10
    static const char SNAPSHOT_NUM_RETAINED_MIN[];

    /// "snapshot.num-retained.max" - The maximum number of completed snapshots to retain. Should be
    /// greater than or equal to the minimum number. Default value is int32 max value.
    static const char SNAPSHOT_NUM_RETAINED_MAX[];

    /// "snapshot.time-retained" - The maximum time of completed snapshots to retain. Default value
    /// is 1 hour.
    static const char SNAPSHOT_TIME_RETAINED[];

    /// "snapshot.expire.limit" - The maximum number of snapshots allowed to expire at a time.
    /// Default value is 10.
    static const char SNAPSHOT_EXPIRE_LIMIT[];

    /// "snapshot.clean-empty-directories" - Whether to try to clean empty directories when expiring
    /// snapshots, if enabled, please note: hdfs: may print exceptions in NameNode. oss/s3: may
    /// cause performance issue. Default value is false.
    static const char SNAPSHOT_CLEAN_EMPTY_DIRECTORIES[];

    /// "commit.timeout" - Timeout duration of retry when commit failed. No default value.
    static const char COMMIT_TIMEOUT[];

    /// "commit.max-retries" - Maximum number of retries when commit failed. Default value is 10.
    static const char COMMIT_MAX_RETRIES[];

    /// "sequence.field" - The field that generates the sequence number for primary key table, the
    /// sequence number determines which data is the most recent. Value use "," as delimiter.
    static const char SEQUENCE_FIELD[];

    /// "sequence.field.sort-order" - Specify the order of sequence.field. Values can be:
    /// "ascending", "descending". Default value is "ascending".
    static const char SEQUENCE_FIELD_SORT_ORDER[];

    /// "merge-engine" - Specify the merge engine for table with primary key. Values can be:
    /// "deduplicate", "partial-update", "aggregation", "first-row". Default value is "deduplicate".
    static const char MERGE_ENGINE[];

    /// "sort-engine" - Specify the sort engine for table with primary key. Values can be:
    /// "min-heap", "loser-tree". Default value is "loser-tree".
    static const char SORT_ENGINE[];

    /// "ignore-delete" - Whether to ignore delete records. Default value is "false".
    static const char IGNORE_DELETE[];

    /// "fields.default-aggregate-function" - Default aggregate function of all fields for
    /// partial-update and aggregate merge function.
    static const char FIELDS_DEFAULT_AGG_FUNC[];

    /// "deletion-vectors.enabled" - Whether to enable deletion vectors mode. In this mode, index
    /// files containing deletion vectors are generated when data is written, which marks the data
    /// for deletion. During read operations, by applying these index files, merging can be avoided.
    /// Default value is false.
    static const char DELETION_VECTORS_ENABLED[];

    ///  @note `CHANGELOG_PRODUCER` currently only support `none`
    ///
    /// "changelog-producer" - Whether to double write to a changelog file. This changelog file
    /// keeps the details of data changes, it can be read directly during stream reads. This can be
    /// applied to tables with primary keys. Values can be "none", "input", "lookup",
    /// "full-compaction". Default value is "none".
    static const char CHANGELOG_PRODUCER[];

    /// "force-lookup" - Whether to force the use of lookup for compaction. Default value is
    /// "false".
    static const char FORCE_LOOKUP[];

    /// "partial-update.remove-record-on-delete" - Whether to remove the whole row in partial-update
    /// engine when records are received. Default value is "false".
    static const char PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE[];

    /// "partial-update.remove-record-on-sequence-group" - When records of the given sequence groups
    /// are received, remove the whole row.
    static const char PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP[];

    /// "scan.fallback-branch" - When a batch job queries from a table, if a partition does not
    /// exist in the current branch, the reader will try to get this partition from this fallback
    /// branch.
    static const char SCAN_FALLBACK_BRANCH[];

    /// "branch" - Specify branch name. Default value is "main".
    static const char BRANCH[];

    /// "file-index.read.enabled" - Whether enabled read file index. Default value is "true".
    static const char FILE_INDEX_READ_ENABLED[];

    /// "data-file.external-paths" - The external paths where the data of this table will be
    /// written, multiple elements separated by commas.
    static const char DATA_FILE_EXTERNAL_PATHS[];
    /// "data-file.external-paths.strategy" - The strategy of selecting an external path when
    /// writing data. Values can be: "none", "specific-fs", "round-robin". Default value is "none".
    static const char DATA_FILE_EXTERNAL_PATHS_STRATEGY[];
    /// "data-file.prefix" - Specify the file name prefix of data files. Default value is "data-".
    static const char DATA_FILE_PREFIX[];
    /// "index-file-in-data-file-dir" - Whether index file in data file directory. Default value is
    /// "false".
    static const char INDEX_FILE_IN_DATA_FILE_DIR[];
    /// "row-tracking.enabled" - Whether enable unique row id for append table. Default value is
    /// "false".
    static const char ROW_TRACKING_ENABLED[];
    /// "data-evolution.enabled" - Whether enable data evolution for row tracking table. Default
    /// value is "false".
    static const char DATA_EVOLUTION_ENABLED[];
    /// "partition.legacy-name" - The legacy partition name is using `ToString` for all types. If
    /// false, using casting to string for all types. Default value is "true".
    static const char PARTITION_GENERATE_LEGACY_NAME[];
    /// "blob-as-descriptor" - Read and write blob field using blob descriptor rather than blob
    /// bytes. Default value is "false".
    static const char BLOB_AS_DESCRIPTOR[];
    /// "global-index.enabled" - Whether to enable global index for scan. Default value is "true".
    static const char GLOBAL_INDEX_ENABLED[];
    /// "global-index.external-path" - Global index root directory, if not set, the global index
    /// files will be stored under the index directory.
    static const char GLOBAL_INDEX_EXTERNAL_PATH[];
};

static constexpr int64_t BATCH_WRITE_COMMIT_IDENTIFIER = std::numeric_limits<int64_t>::max();

}  // namespace paimon
