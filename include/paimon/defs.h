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

    /// "target-file-size" - Target size of a file. primary key table: the default value is 128 MB.
    /// append table: the default value is 256 MB.
    static const char TARGET_FILE_SIZE[];

    /// "blob.target-file-size" - Target size of a blob file. Default is TARGET_FILE_SIZE.
    static const char BLOB_TARGET_FILE_SIZE[];

    /// "partition.default-name" - The default partition name in case the dynamic partition column
    /// value is null/empty string. Default is "__DEFAULT_PARTITION__".
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

    /// "commit.force-compact" - Whether to force a compaction before commit. Default value is
    /// "false".
    static const char COMMIT_FORCE_COMPACT[];

    /// "commit.timeout" - Timeout duration of retry when commit failed. No default value.
    static const char COMMIT_TIMEOUT[];

    /// "commit.max-retries" - Maximum number of retries when commit failed. Default value is 10.
    static const char COMMIT_MAX_RETRIES[];

    /// "compaction.max-size-amplification-percent" - The size amplification is defined as the
    /// amount (in percentage) of additional storage needed to store a single byte of data in the
    /// merge tree for changelog mode table. Default value is 200.
    static const char COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT[];

    /// "compaction.size-ratio" - Percentage flexibility while comparing sorted run size for
    /// changelog mode table. If the candidate sorted run(s) size is 1% smaller than the next
    /// sorted run's size, then include next sorted run into this candidate set. Default value is 1.
    static const char COMPACTION_SIZE_RATIO[];

    /// "num-sorted-run.compaction-trigger" - The sorted run number to trigger compaction. Includes
    /// level0 files (one file one sorted run) and high-level runs (one level one sorted run).
    /// Default value is 5.
    static const char NUM_SORTED_RUNS_COMPACTION_TRIGGER[];

    /// "num-sorted-run.stop-trigger" - The number of sorted runs that trigger the stopping of
    /// writes, the default value is 'num-sorted-run.compaction-trigger' + 3.
    static const char NUM_SORTED_RUNS_STOP_TRIGGER[];

    /// "num-levels" - Total level number, for example, there are 3 levels, including 0,1,2 levels.
    /// No default value.
    static const char NUM_LEVELS[];

    /// "lookup-wait" - When need to lookup, commit will wait for compaction by lookup. Default
    /// value is "true".
    static const char LOOKUP_WAIT[];

    /// "lookup-compact" - Lookup compact mode used for lookup compaction. Default value is
    /// LookupCompactMode::RADICAL.
    static const char LOOKUP_COMPACT[];

    /// "compaction.force-up-level-0" - If set to true, compaction strategy will always include all
    /// level 0 files in candidates. Default value is false.
    static const char COMPACTION_FORCE_UP_LEVEL_0[];

    /// "lookup-compact.max-interval" - The max interval for a gentle mode lookup compaction to be
    /// triggered. For every interval, a forced lookup compaction will be performed to flush L0
    /// files to higher level. This option is only valid when lookup-compact mode is gentle. No
    /// default value.
    static const char LOOKUP_COMPACT_MAX_INTERVAL[];

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

    /// "deletion-vector.index-file.target-size" - The target size of deletion vector index file.
    /// Default value is 2MB.
    static const char DELETION_VECTOR_INDEX_FILE_TARGET_SIZE[];

    /// "deletion-vectors.bitmap64" - Enable 64 bit bitmap implementation. Note that only 64 bit
    /// bitmap implementation is compatible with Iceberg. Default value is "false".
    /// @note: bitmap64 dv is not supported.
    static const char DELETION_VECTOR_BITMAP64[];

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
    /// "scan.tag-name" - Optional tag name used in case of "from-snapshot" scan mode.
    static const char SCAN_TAG_NAME[];
    /// "write-only" - If set to "true", compactions and snapshot expiration will be skipped. This
    /// option is used along with dedicated compact jobs. Default value is "false".
    static const char WRITE_ONLY[];
    /// "compaction.min.file-num" - For file set [f_0,...,f_N], the minimum file number to trigger a
    /// compaction for append-only table. Default value is 5.
    static const char COMPACTION_MIN_FILE_NUM[];
    /// "compaction.force-rewrite-all-files" - Whether to force pick all files for a full
    /// compaction. Usually seen in a compaction task to external paths. Default value is "false".
    static const char COMPACTION_FORCE_REWRITE_ALL_FILES[];
    /// "compaction.optimization-interval" - Implying how often to perform an optimization
    /// compaction, this configuration is used to ensure the query timeliness of the read-optimized
    /// system table. No default value.
    static const char COMPACTION_OPTIMIZATION_INTERVAL[];
    /// "compaction.total-size-threshold" - When total size is smaller than this threshold, force a
    /// full compaction. No default value.
    static const char COMPACTION_TOTAL_SIZE_THRESHOLD[];
    /// "compaction.incremental-size-threshold" - When incremental size is bigger than this
    /// threshold, force a full compaction. No default value.
    static const char COMPACTION_INCREMENTAL_SIZE_THRESHOLD[];
    /// "compaction.offpeak.start.hour" - The start of off-peak hours, expressed as an integer
    /// between 0 and 23, inclusive. Set to -1 to disable off-peak. Default is -1.
    static const char COMPACT_OFFPEAK_START_HOUR[];
    /// "compaction.offpeak.end.hour" - The end of off-peak hours, expressed as an integer between 0
    /// and 23, exclusive. Set to -1 to disable off-peak. Default is -1.
    static const char COMPACT_OFFPEAK_END_HOUR[];
    /// "compaction.offpeak-ratio" - Allows you to set a different (by default, more aggressive)
    /// percentage ratio for determining whether larger sorted run's size are included in
    /// compactions during off-peak hours. Works in the same way as compaction.size-ratio. Only
    /// applies if offpeak.start.hour and offpeak.end.hour are also enabled.
    /// For instance, if your cluster experiences low pressure between 2 AM  and 6 PM , you can
    /// configure `compaction.offpeak.start.hour=2` and `compaction.offpeak.end.hour=18` to define
    /// this period as off-peak hours.  During these hours, you can increase the off-peak compaction
    /// ratio (e.g. `compaction.offpeak-ratio=20`) to enable more aggressive data compaction.
    /// Default is 0.
    static const char COMPACTION_OFFPEAK_RATIO[];
    /// "lookup.cache.bloom.filter.enabled" - Whether to enable the bloom filter for lookup cache.
    /// Default value is true.
    static const char LOOKUP_CACHE_BLOOM_FILTER_ENABLED[];
    /// "lookup.cache.bloom.filter.fpp" - Define the default false positive probability for lookup
    /// cache bloom filters. Default value is 0.05.
    static const char LOOKUP_CACHE_BLOOM_FILTER_FPP[];
    /// "lookup.remote-file.enabled" - Whether to enable the remote file for lookup.
    /// Default value is false.
    static const char LOOKUP_REMOTE_FILE_ENABLED[];
    /// "lookup.remote-file.level-threshold" - Level threshold of lookup to generate remote lookup
    /// files. Level files below this threshold will not generate remote lookup files.
    /// Default value is INT32_MIN.
    static const char LOOKUP_REMOTE_LEVEL_THRESHOLD[];
    /// "lookup.cache-spill-compression" - Spill compression for lookup cache, currently zstd, none,
    /// lz4 are supported. Default value is zstd.
    /// Noted that java paimon also supports lzo which paimon-cpp does not support for now.
    static const char LOOKUP_CACHE_SPILL_COMPRESSION[];
    /// "spill-compression.zstd-level" - Default spill compression zstd level. For higher
    /// compression rates, it can be configured to 9, but the read and write speed will
    /// significantly decrease. Default value is 1.
    static const char SPILL_COMPRESSION_ZSTD_LEVEL[];
    /// "cache-page-size" - Memory page size for caching. Default value is 64 kb.
    static const char CACHE_PAGE_SIZE[];
    /// "file.format.per.level" - Define different file format for different level, you can add the
    /// conf like this:  'file.format.per.level' = '0:avro,3:parquet', if the file format for level
    /// is not provided, the default format which set by FILE_FORMAT will be used.
    static const char FILE_FORMAT_PER_LEVEL[];
    /// "file.compression.per.level" - Define different compression policies for different level,
    /// you can add the conf like this: 'file.compression.per.level' = '0:lz4,1:zstd'.
    /// If a level is not configured, the default compression set by FILE_COMPRESSION will be used.
    static const char FILE_COMPRESSION_PER_LEVEL[];
    /// "lookup.cache-max-memory-size" - Max memory size for lookup cache. Default value is 256 mb.
    static const char LOOKUP_CACHE_MAX_MEMORY_SIZE[];
    /// "lookup.cache.high-priority-pool-ratio" - The fraction of cache memory that is reserved for
    /// high-priority data like index, filter. Default value is 0.25.
    static const char LOOKUP_CACHE_HIGH_PRIO_POOL_RATIO[];
    /// "lookup.cache-file-retention" - The cached files retention time for lookup.
    /// After the file expires, if there is a need for access, it will be re-read from the DFS
    /// to build an index on the local disk. Default value is 1 hour.
    static const char LOOKUP_CACHE_FILE_RETENTION[];
    /// "lookup.cache-max-disk-size" - Max disk size for lookup cache, you can use this option
    /// to limit the use of local disks. Default value is unlimited (INT64_MAX).
    static const char LOOKUP_CACHE_MAX_DISK_SIZE[];
};

static constexpr int64_t BATCH_WRITE_COMMIT_IDENTIFIER = std::numeric_limits<int64_t>::max();

}  // namespace paimon
