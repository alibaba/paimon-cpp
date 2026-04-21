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

#include "paimon/core/core_options.h"

#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/fs/resolving_file_system.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/options/time_duration.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/options/expire_config.h"
#include "paimon/core/options/lookup_strategy.h"
#include "paimon/core/options/sort_order.h"
#include "paimon/core/utils/branch_manager.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/status.h"

namespace paimon {

// ConfigParser is a helper class for parsing configurations from a map of strings.
class ConfigParser {
 public:
    explicit ConfigParser(const std::map<std::string, std::string>& map) : config_map_(map) {}

    // Parse basic type configurations
    template <typename T>
    Status Parse(const std::string& key, T* value) const {
        auto iter = config_map_.find(key);
        if (iter != config_map_.end()) {
            auto result = StringUtils::StringToValue<T>(iter->second);
            if (result) {
                *value = result.value();
                return Status::OK();
            }
            return Status::Invalid(fmt::format("Invalid Config [{}: {}]", key, iter->second));
        }
        return Status::OK();  // Return success even if the configuration does not exist
    }

    // Parse optional basic type configurations
    template <typename T>
    Status Parse(const std::string& key, std::optional<T>* value) const {
        auto iter = config_map_.find(key);
        if (iter != config_map_.end()) {
            auto result = StringUtils::StringToValue<T>(iter->second);
            if (result) {
                *value = result.value();
                return Status::OK();
            }
            return Status::Invalid(fmt::format("Invalid Config [{}: {}]", key, iter->second));
        }
        return Status::OK();  // Return success even if the configuration does not exist
    }

    // Parse list configurations
    template <typename T>
    Status ParseList(const std::string& key, const std::string& delimiter,
                     std::vector<T>* list) const {
        auto iter = config_map_.find(key);
        if (iter != config_map_.end()) {
            auto value_str_vec = StringUtils::Split(iter->second, delimiter, /*ignore_empty=*/true);
            for (const auto& value_str : value_str_vec) {
                if constexpr (std::is_same_v<T, std::string>) {
                    list->emplace_back(value_str);
                } else {
                    auto value = StringUtils::StringToValue<T>(value_str);
                    if (!value) {
                        return Status::Invalid(
                            fmt::format("Invalid Config [{}: {}]", key, iter->second));
                    }
                    list->emplace_back(value.value());
                }
            }
        }
        return Status::OK();  // Return success even if the configuration does not exist
    }

    // Parse string configurations
    Status ParseString(const std::string& key, std::string* value) const {
        auto iter = config_map_.find(key);
        if (iter != config_map_.end()) {
            *value = iter->second;
        }
        return Status::OK();
    }

    // Parse memory size configurations
    Status ParseMemorySize(const std::string& key, int64_t* value) const {
        auto iter = config_map_.find(key);
        if (iter != config_map_.end()) {
            PAIMON_ASSIGN_OR_RAISE(*value, MemorySize::ParseBytes(iter->second));
        }
        return Status::OK();
    }

    // Parse object configurations
    template <typename Factory, typename ObjectType>
    Status ParseObject(const std::string& key, const std::string& default_identifier,
                       std::shared_ptr<ObjectType>* value) const {
        auto iter = config_map_.find(key);
        if (iter != config_map_.end()) {
            std::string normalized_value = StringUtils::ToLowerCase(iter->second);
            PAIMON_ASSIGN_OR_RAISE(*value, Factory::Get(normalized_value, config_map_));
        } else {
            PAIMON_ASSIGN_OR_RAISE(
                *value, Factory::Get(StringUtils::ToLowerCase(default_identifier), config_map_));
        }
        return Status::OK();
    }

    // Parse file system
    Status ParseFileSystem(const std::map<std::string, std::string>& fs_scheme_to_identifier_map,
                           const std::shared_ptr<FileSystem>& specified_file_system,
                           std::shared_ptr<FileSystem>* value) const {
        if (specified_file_system) {
            // if exists user specified file system, first use
            *value = specified_file_system;
            return Status::OK();
        }
        std::string default_fs_identifier = "local";
        auto iter = config_map_.find(Options::FILE_SYSTEM);
        if (iter != config_map_.end()) {
            default_fs_identifier = StringUtils::ToLowerCase(iter->second);
        }
        *value = std::make_shared<ResolvingFileSystem>(fs_scheme_to_identifier_map,
                                                       default_fs_identifier, config_map_);
        return Status::OK();
    }

    // Parse SortOrder
    Status ParseSortOrder(SortOrder* sort_order) const {
        auto iter = config_map_.find(Options::SEQUENCE_FIELD_SORT_ORDER);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            if (str == "ascending") {
                *sort_order = SortOrder::ASCENDING;
            } else if (str == "descending") {
                *sort_order = SortOrder::DESCENDING;
            } else {
                return Status::Invalid(fmt::format("invalid sort order: {}", str));
            }
        }
        return Status::OK();
    }

    // Parse LookupCompactMode
    Status ParseLookupCompactMode(LookupCompactMode* mode) const {
        auto iter = config_map_.find(Options::LOOKUP_COMPACT);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            if (str == "radical") {
                *mode = LookupCompactMode::RADICAL;
            } else if (str == "gentle") {
                *mode = LookupCompactMode::GENTLE;
            } else {
                return Status::Invalid(fmt::format("invalid lookup mode: {}", str));
            }
        }
        return Status::OK();
    }

    // Parse SortEngine
    Status ParseSortEngine(SortEngine* sort_engine) const {
        auto iter = config_map_.find(Options::SORT_ENGINE);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            if (str == "min-heap") {
                *sort_engine = SortEngine::MIN_HEAP;
            } else if (str == "loser-tree") {
                *sort_engine = SortEngine::LOSER_TREE;
            } else {
                return Status::Invalid(fmt::format("invalid sort engine: {}", str));
            }
        }
        return Status::OK();
    }

    // Parse MergeEngine
    Status ParseMergeEngine(MergeEngine* merge_engine) const {
        auto iter = config_map_.find(Options::MERGE_ENGINE);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            if (str == "deduplicate") {
                *merge_engine = MergeEngine::DEDUPLICATE;
            } else if (str == "partial-update") {
                *merge_engine = MergeEngine::PARTIAL_UPDATE;
            } else if (str == "aggregation") {
                *merge_engine = MergeEngine::AGGREGATE;
            } else if (str == "first-row") {
                *merge_engine = MergeEngine::FIRST_ROW;
            } else {
                return Status::Invalid(fmt::format("invalid merge engine: {}", str));
            }
        }
        return Status::OK();
    }

    // Parse ChangelogProducer
    Status ParseChangelogProducer(ChangelogProducer* changelog_producer) const {
        auto iter = config_map_.find(Options::CHANGELOG_PRODUCER);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            if (str == "none") {
                *changelog_producer = ChangelogProducer::NONE;
            } else if (str == "input") {
                *changelog_producer = ChangelogProducer::INPUT;
            } else if (str == "full-compaction") {
                *changelog_producer = ChangelogProducer::FULL_COMPACTION;
            } else if (str == "lookup") {
                *changelog_producer = ChangelogProducer::LOOKUP;
            } else {
                return Status::Invalid(fmt::format("invalid changelog producer: {}", str));
            }
        }
        return Status::OK();
    }

    // Parse ExternalPathStrategy
    Status ParseExternalPathStrategy(ExternalPathStrategy* external_path_strategy) const {
        auto iter = config_map_.find(Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            if (str == "none") {
                *external_path_strategy = ExternalPathStrategy::NONE;
            } else if (str == "specific-fs") {
                *external_path_strategy = ExternalPathStrategy::SPECIFIC_FS;
            } else if (str == "round-robin") {
                *external_path_strategy = ExternalPathStrategy::ROUND_ROBIN;
            } else {
                return Status::Invalid(fmt::format("invalid external path strategy: {}", str));
            }
        }
        return Status::OK();
    }

    // Parse BucketFunctionType
    Status ParseBucketFunctionType(BucketFunctionType* bucket_function_type) const {
        auto iter = config_map_.find(Options::BUCKET_FUNCTION_TYPE);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            if (str == "default") {
                *bucket_function_type = BucketFunctionType::DEFAULT;
            } else if (str == "mod") {
                *bucket_function_type = BucketFunctionType::MOD;
            } else if (str == "hive") {
                *bucket_function_type = BucketFunctionType::HIVE;
            } else {
                return Status::Invalid(fmt::format("invalid bucket function type: {}", str));
            }
        }
        return Status::OK();
    }

    // Parse StartupMode
    Status ParseStartupMode(StartupMode* startup_mode) const {
        auto iter = config_map_.find(Options::SCAN_MODE);
        if (iter != config_map_.end()) {
            std::string str = StringUtils::ToLowerCase(iter->second);
            PAIMON_ASSIGN_OR_RAISE(*startup_mode, StartupMode::FromString(str));
        }
        return Status::OK();
    }

    // parse file.format.per.level
    Status ParseFileFormatPerLevel(
        std::map<int32_t, std::shared_ptr<FileFormat>>* file_format_per_level_ptr) const {
        auto& file_format_per_level = *file_format_per_level_ptr;
        std::string file_format_per_level_str;
        PAIMON_RETURN_NOT_OK(
            ParseString(Options::FILE_FORMAT_PER_LEVEL, &file_format_per_level_str));
        if (!file_format_per_level_str.empty()) {
            auto level2format =
                StringUtils::Split(file_format_per_level_str, std::string(","), std::string(":"));
            for (const auto& single_level : level2format) {
                if (single_level.size() != 2) {
                    return Status::Invalid(fmt::format(
                        "fail to parse key {}, value {} (usage example: 0:avro,3:parquet)",
                        Options::FILE_FORMAT_PER_LEVEL, file_format_per_level_str));
                }
                auto level = StringUtils::StringToValue<int32_t>(single_level[0]);
                if (!level || level.value() < 0) {
                    return Status::Invalid(
                        fmt::format("fail to parse level {} from string to int in {}",
                                    single_level[0], Options::FILE_FORMAT_PER_LEVEL));
                }
                std::shared_ptr<FileFormat> file_format;
                PAIMON_RETURN_NOT_OK(ParseObject<FileFormatFactory>(
                    "_no_use", /*default_identifier=*/single_level[1], &file_format));
                file_format_per_level[level.value()] = file_format;
            }
        }
        return Status::OK();
    }

    // parse file.compression.per.level
    Status ParseFileCompressionPerLevel(
        std::map<int32_t, std::string>* file_compression_per_level_ptr) const {
        auto& file_compression_per_level = *file_compression_per_level_ptr;
        std::string file_compression_per_level_str;
        PAIMON_RETURN_NOT_OK(
            ParseString(Options::FILE_COMPRESSION_PER_LEVEL, &file_compression_per_level_str));
        if (!file_compression_per_level_str.empty()) {
            auto level2compression = StringUtils::Split(file_compression_per_level_str,
                                                        std::string(","), std::string(":"));
            for (const auto& single_level : level2compression) {
                if (single_level.size() != 2) {
                    return Status::Invalid(fmt::format(
                        "fail to parse key {}, value {} (usage example: 0:lz4,1:zstd)",
                        Options::FILE_COMPRESSION_PER_LEVEL, file_compression_per_level_str));
                }
                auto level = StringUtils::StringToValue<int32_t>(single_level[0]);
                if (!level || level.value() < 0) {
                    return Status::Invalid(
                        fmt::format("fail to parse level {} from string to int in {}",
                                    single_level[0], Options::FILE_COMPRESSION_PER_LEVEL));
                }
                file_compression_per_level[level.value()] = single_level[1];
            }
        }
        return Status::OK();
    }

    bool ContainsKey(const std::string& key) const {
        return config_map_.find(key) != config_map_.end();
    }

 private:
    const std::map<std::string, std::string> config_map_;
};

// Impl is a private implementation of CoreOptions,
// storing various configurable fields and their default values.
struct CoreOptions::Impl {
    int64_t page_size = 64 * 1024;
    std::optional<int64_t> target_file_size;
    std::optional<int64_t> blob_target_file_size;
    int64_t source_split_target_size = 128 * 1024 * 1024;
    int64_t source_split_open_file_cost = 4 * 1024 * 1024;
    int64_t manifest_target_file_size = 8 * 1024 * 1024;
    int64_t deletion_vector_target_file_size = 2 * 1024 * 1024;
    int64_t manifest_full_compaction_file_size = 16 * 1024 * 1024;
    int64_t write_buffer_size = 256 * 1024 * 1024;
    int64_t commit_timeout = std::numeric_limits<int64_t>::max();

    std::shared_ptr<FileFormat> file_format;
    std::shared_ptr<FileSystem> file_system;
    std::shared_ptr<FileFormat> manifest_file_format;

    std::optional<int64_t> scan_snapshot_id;
    ExpireConfig expire_config;
    std::vector<std::string> sequence_field;
    std::vector<std::string> remove_record_on_sequence_group;

    std::string partition_default_name = "__DEFAULT_PARTITION__";
    StartupMode startup_mode = StartupMode::Default();
    std::string file_compression = "zstd";
    std::string manifest_compression = "zstd";
    std::string branch = BranchManager::DEFAULT_MAIN_BRANCH;
    std::string data_file_prefix = "data-";
    std::string file_system_scheme_to_identifier_map_str;

    std::optional<std::string> field_default_func;
    std::optional<std::string> scan_fallback_branch;
    std::optional<std::string> data_file_external_paths;

    std::map<std::string, std::string> raw_options;

    int32_t bucket = -1;

    int32_t manifest_merge_min_count = 30;
    int32_t read_batch_size = 1024;
    int32_t write_batch_size = 1024;
    int32_t commit_max_retries = 10;
    int32_t compaction_min_file_num = 5;
    int32_t compaction_max_size_amplification_percent = 200;
    int32_t compaction_size_ratio = 1;
    int32_t num_sorted_runs_compaction_trigger = 5;
    std::optional<int32_t> num_sorted_runs_stop_trigger;
    std::optional<int32_t> num_levels;

    SortOrder sequence_field_sort_order = SortOrder::ASCENDING;
    MergeEngine merge_engine = MergeEngine::DEDUPLICATE;
    SortEngine sort_engine = SortEngine::LOSER_TREE;
    ChangelogProducer changelog_producer = ChangelogProducer::NONE;
    ExternalPathStrategy external_path_strategy = ExternalPathStrategy::NONE;
    LookupCompactMode lookup_compact_mode = LookupCompactMode::RADICAL;
    std::optional<int32_t> lookup_compact_max_interval;
    BucketFunctionType bucket_function_type = BucketFunctionType::DEFAULT;

    int32_t file_compression_zstd_level = 1;

    bool ignore_delete = false;
    bool write_only = false;
    bool deletion_vectors_enabled = false;
    bool deletion_vectors_bitmap64 = false;
    bool force_lookup = false;
    bool lookup_wait = true;
    bool partial_update_remove_record_on_delete = false;
    bool aggregation_remove_record_on_delete = false;
    bool file_index_read_enabled = true;
    bool enable_adaptive_prefetch_strategy = true;
    bool index_file_in_data_file_dir = false;
    bool row_tracking_enabled = false;
    bool data_evolution_enabled = false;
    bool legacy_partition_name_enabled = true;
    bool global_index_enabled = true;
    bool commit_force_compact = false;
    bool compaction_force_rewrite_all_files = false;
    bool compaction_force_up_level_0 = false;
    std::optional<std::string> global_index_external_path;

    std::optional<std::string> scan_tag_name;
    std::optional<int64_t> optimized_compaction_interval;
    std::optional<int64_t> compaction_total_size_threshold;
    std::optional<int64_t> compaction_incremental_size_threshold;
    int32_t compact_off_peak_start_hour = -1;
    int32_t compact_off_peak_end_hour = -1;
    int32_t compact_off_peak_ratio = 0;
    bool lookup_cache_bloom_filter = true;
    double lookup_cache_bloom_filter_fpp = 0.05;
    bool lookup_remote_file_enabled = false;
    int32_t lookup_remote_level_threshold = INT32_MIN;
    CompressOptions lookup_compress_options{"zstd", 1};
    int64_t cache_page_size = 64 * 1024;  // 64KB
    std::map<int32_t, std::shared_ptr<FileFormat>> file_format_per_level;
    std::map<int32_t, std::string> file_compression_per_level;
    int64_t lookup_cache_max_memory = 256 * 1024 * 1024;
    double lookup_cache_high_prio_pool_ratio = 0.25;
    int64_t lookup_cache_file_retention_ms = 1 * 3600 * 1000;  // 1 hour
    int64_t lookup_cache_max_disk_size = INT64_MAX;

    // Parse basic table options: bucket, partition, file sizes, batch sizes, file system, etc.
    Status ParseBasicOptions(
        const ConfigParser& parser, const std::shared_ptr<FileSystem>& specified_file_system,
        const std::map<std::string, std::string>& fs_scheme_to_identifier_map) {
        // Parse bucket - bucket number, -1 for dynamic bucket mode, >0 for fixed bucket mode
        PAIMON_RETURN_NOT_OK(parser.Parse(Options::BUCKET, &bucket));
        // Parse partition.default-name - default partition name for null/empty partition values
        PAIMON_RETURN_NOT_OK(
            parser.ParseString(Options::PARTITION_DEFAULT_NAME, &partition_default_name));
        // Parse page-size - memory page size, default 64 kb
        PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::PAGE_SIZE, &page_size));
        // Parse target-file-size - target size of a data file
        if (parser.ContainsKey(Options::TARGET_FILE_SIZE)) {
            int64_t parsed_target_file_size;
            PAIMON_RETURN_NOT_OK(
                parser.ParseMemorySize(Options::TARGET_FILE_SIZE, &parsed_target_file_size));
            target_file_size = parsed_target_file_size;
        }
        // Parse blob.target-file-size - target size of a blob file
        if (parser.ContainsKey(Options::BLOB_TARGET_FILE_SIZE)) {
            int64_t parsed_blob_target_file_size;
            PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::BLOB_TARGET_FILE_SIZE,
                                                        &parsed_blob_target_file_size));
            blob_target_file_size = parsed_blob_target_file_size;
        }
        // Parse source.split.target-size - target size of a source split when scanning a bucket
        PAIMON_RETURN_NOT_OK(
            parser.ParseMemorySize(Options::SOURCE_SPLIT_TARGET_SIZE, &source_split_target_size));
        // Parse source.split.open-file-cost - open file cost to avoid reading too many files
        PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::SOURCE_SPLIT_OPEN_FILE_COST,
                                                    &source_split_open_file_cost));
        // Parse read.batch-size - read batch size for file formats
        PAIMON_RETURN_NOT_OK(parser.Parse(Options::READ_BATCH_SIZE, &read_batch_size));
        // Parse write.batch-size - write batch size for file formats
        PAIMON_RETURN_NOT_OK(parser.Parse(Options::WRITE_BATCH_SIZE, &write_batch_size));
        // Parse write-buffer-size - data to build up in memory before flushing to disk
        PAIMON_RETURN_NOT_OK(
            parser.ParseMemorySize(Options::WRITE_BUFFER_SIZE, &write_buffer_size));
        // Parse file-system - file system type, default "local"
        PAIMON_RETURN_NOT_OK(parser.ParseFileSystem(fs_scheme_to_identifier_map,
                                                    specified_file_system, &file_system));
        // Parse write-only - if true, compactions and snapshot expiration will be skipped
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::WRITE_ONLY, &write_only));
        // Parse partition.legacy-name - use legacy ToString for partition names, default true
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::PARTITION_GENERATE_LEGACY_NAME,
                                                &legacy_partition_name_enabled));
        // Only for test, parse enable-adaptive-prefetch-strategy
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>("test.enable-adaptive-prefetch-strategy",
                                                &enable_adaptive_prefetch_strategy));
        // Parse data-file.external-paths - external paths for data files, comma separated
        std::string parsed_external_paths;
        PAIMON_RETURN_NOT_OK(
            parser.ParseString(Options::DATA_FILE_EXTERNAL_PATHS, &parsed_external_paths));
        if (!parsed_external_paths.empty()) {
            data_file_external_paths = parsed_external_paths;
        }
        // Parse data-file.external-paths.strategy - strategy for selecting external path
        PAIMON_RETURN_NOT_OK(parser.ParseExternalPathStrategy(&external_path_strategy));
        // Parse data-file.prefix - file name prefix of data files, default "data-"
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::DATA_FILE_PREFIX, &data_file_prefix));
        // Parse row-tracking.enabled - whether to enable unique row id for append table
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::ROW_TRACKING_ENABLED, &row_tracking_enabled));
        // Parse data-evolution.enabled - whether to enable data evolution for row tracking
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::DATA_EVOLUTION_ENABLED, &data_evolution_enabled));
        // Parse bucket-function - bucket function type, default "DEFAULT"
        PAIMON_RETURN_NOT_OK(parser.ParseBucketFunctionType(&bucket_function_type));
        return Status::OK();
    }

    // Parse data file format, compression, and per-level format/compression configurations.
    Status ParseFileFormatOptions(const ConfigParser& parser) {
        // Parse file.format - data file format, default "parquet"
        PAIMON_RETURN_NOT_OK(parser.ParseObject<FileFormatFactory>(
            Options::FILE_FORMAT, /*default_identifier=*/"parquet", &file_format));
        // Parse file.compression - default file compression, default "zstd"
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::FILE_COMPRESSION, &file_compression));
        // Parse file.compression.zstd-level - zstd compression level, default 1
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::FILE_COMPRESSION_ZSTD_LEVEL, &file_compression_zstd_level));
        // Parse file.format.per.level - different file format for different levels
        PAIMON_RETURN_NOT_OK(parser.ParseFileFormatPerLevel(&file_format_per_level));
        // Parse file.compression.per.level - different compression for different levels
        PAIMON_RETURN_NOT_OK(parser.ParseFileCompressionPerLevel(&file_compression_per_level));
        return Status::OK();
    }

    // Parse manifest file configurations: format, compression, merge, and compaction thresholds.
    Status ParseManifestOptions(const ConfigParser& parser) {
        // Parse manifest.format - manifest file format, default "avro"
        PAIMON_RETURN_NOT_OK(parser.ParseObject<FileFormatFactory>(
            Options::MANIFEST_FORMAT, /*default_identifier=*/"avro", &manifest_file_format));
        // Parse manifest.compression - manifest file compression, default "zstd"
        PAIMON_RETURN_NOT_OK(
            parser.ParseString(Options::MANIFEST_COMPRESSION, &manifest_compression));
        // Parse manifest.target-file-size - suggested manifest file size, default 8MB
        PAIMON_RETURN_NOT_OK(
            parser.ParseMemorySize(Options::MANIFEST_TARGET_FILE_SIZE, &manifest_target_file_size));
        // Parse manifest.merge-min-count - minimum ManifestFileMeta count to trigger merge
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::MANIFEST_MERGE_MIN_COUNT, &manifest_merge_min_count));
        // Parse manifest.full-compaction-threshold-size - size threshold for full compaction
        PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::MANIFEST_FULL_COMPACTION_FILE_SIZE,
                                                    &manifest_full_compaction_file_size));
        return Status::OK();
    }

    // Parse snapshot expiration and retention configurations.
    Status ParseExpireOptions(const ConfigParser& parser) {
        // Parse snapshot.num-retained.min - minimum completed snapshots to retain, default 10
        int32_t snapshot_num_retain_min = 10;
        // Parse snapshot.num-retained.max - maximum completed snapshots to retain
        int32_t snapshot_num_retain_max = std::numeric_limits<int32_t>::max();
        // Parse snapshot.expire.limit - maximum snapshots allowed to expire at a time, default 10
        int32_t snapshot_expire_limit = 10;
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::SNAPSHOT_NUM_RETAINED_MIN, &snapshot_num_retain_min));
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::SNAPSHOT_NUM_RETAINED_MAX, &snapshot_num_retain_max));
        PAIMON_RETURN_NOT_OK(parser.Parse(Options::SNAPSHOT_EXPIRE_LIMIT, &snapshot_expire_limit));
        // Parse snapshot.time-retained - maximum time of completed snapshots to retain
        std::string snapshot_time_retained_str = "1 hour";
        PAIMON_RETURN_NOT_OK(
            parser.ParseString(Options::SNAPSHOT_TIME_RETAINED, &snapshot_time_retained_str));
        PAIMON_ASSIGN_OR_RAISE(int64_t snapshot_time_retained,
                               TimeDuration::Parse(snapshot_time_retained_str));
        // Parse snapshot.clean-empty-directories - whether to clean empty dirs on expiration
        bool snapshot_clean_empty_directories = false;
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES,
                                                &snapshot_clean_empty_directories));
        expire_config =
            ExpireConfig(snapshot_num_retain_max, snapshot_num_retain_min, snapshot_time_retained,
                         snapshot_expire_limit, snapshot_clean_empty_directories);
        return Status::OK();
    }

    // Parse commit configurations: timeout, retries, and force-compact.
    Status ParseCommitOptions(const ConfigParser& parser) {
        // Parse commit.force-compact - whether to force compaction before commit, default false
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::COMMIT_FORCE_COMPACT, &commit_force_compact));
        // Parse commit.timeout - timeout duration of retry when commit failed
        std::string commit_timeout_str;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::COMMIT_TIMEOUT, &commit_timeout_str));
        if (!commit_timeout_str.empty()) {
            PAIMON_ASSIGN_OR_RAISE(commit_timeout, TimeDuration::Parse(commit_timeout_str));
        }
        // Parse commit.max-retries - maximum retries when commit failed, default 10
        PAIMON_RETURN_NOT_OK(parser.Parse(Options::COMMIT_MAX_RETRIES, &commit_max_retries));
        return Status::OK();
    }

    // Parse merge engine, sort engine, sequence field, changelog, and partial-update options.
    Status ParseMergeAndSequenceOptions(const ConfigParser& parser) {
        // Parse sequence.field - field that generates sequence number for primary key table
        PAIMON_RETURN_NOT_OK(parser.ParseList<std::string>(
            Options::SEQUENCE_FIELD, Options::FIELDS_SEPARATOR, &sequence_field));
        // Parse sequence.field.sort-order - order of sequence field, default "ascending"
        PAIMON_RETURN_NOT_OK(parser.ParseSortOrder(&sequence_field_sort_order));
        // Parse sort-engine - sort engine for primary key table, default "loser-tree"
        PAIMON_RETURN_NOT_OK(parser.ParseSortEngine(&sort_engine));
        // Parse merge-engine - merge engine for primary key table, default "deduplicate"
        PAIMON_RETURN_NOT_OK(parser.ParseMergeEngine(&merge_engine));
        // Parse ignore-delete - whether to ignore delete records, default false
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::IGNORE_DELETE, &ignore_delete));
        // Parse fields.default-aggregate-function - default agg function for partial-update
        std::string parsed_default_func;
        PAIMON_RETURN_NOT_OK(
            parser.ParseString(Options::FIELDS_DEFAULT_AGG_FUNC, &parsed_default_func));
        if (!parsed_default_func.empty()) {
            field_default_func = parsed_default_func;
        }
        // Parse changelog-producer - whether to double write to a changelog file, default "none"
        PAIMON_RETURN_NOT_OK(parser.ParseChangelogProducer(&changelog_producer));
        // Parse partial-update.remove-record-on-delete - remove whole row on delete
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE,
                                                &partial_update_remove_record_on_delete));
        // Parse aggregation_remove_record_on_delete
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::AGGREGATION_REMOVE_RECORD_ON_DELETE,
                                                &aggregation_remove_record_on_delete));
        // Parse partial-update.remove-record-on-sequence-group
        PAIMON_RETURN_NOT_OK(parser.ParseList<std::string>(
            Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP, Options::FIELDS_SEPARATOR,
            &remove_record_on_sequence_group));
        return Status::OK();
    }

    // Parse deletion vector configurations.
    Status ParseDeletionVectorOptions(const ConfigParser& parser) {
        // Parse deletion-vectors.enabled - whether to enable deletion vectors mode, default false
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::DELETION_VECTORS_ENABLED, &deletion_vectors_enabled));
        // Parse deletion-vector.index-file.target-size - target size of dv index file, default 2MB
        PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::DELETION_VECTOR_INDEX_FILE_TARGET_SIZE,
                                                    &deletion_vector_target_file_size));
        // Parse deletion-vectors.bitmap64 - enable 64 bit bitmap implementation, default false
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::DELETION_VECTOR_BITMAP64, &deletion_vectors_bitmap64));
        return Status::OK();
    }

    // Parse scan, branch, and tag related configurations.
    Status ParseScanAndBranchOptions(const ConfigParser& parser) {
        // Parse scan.snapshot-id - optional snapshot id for "from-snapshot" scan mode
        PAIMON_RETURN_NOT_OK(parser.Parse(Options::SCAN_SNAPSHOT_ID, &scan_snapshot_id));
        // Parse scan.mode - scanning behavior of the source, default "default"
        PAIMON_RETURN_NOT_OK(parser.ParseStartupMode(&startup_mode));
        // Parse scan.fallback-branch - fallback branch when partition not found
        std::string parsed_fallback_branch;
        PAIMON_RETURN_NOT_OK(
            parser.ParseString(Options::SCAN_FALLBACK_BRANCH, &parsed_fallback_branch));
        if (!parsed_fallback_branch.empty()) {
            scan_fallback_branch = parsed_fallback_branch;
        }
        // Parse branch - branch name, default "main"
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::BRANCH, &branch));
        // Parse scan.tag-name - optional tag name for "from-snapshot" scan mode
        std::string parsed_tag_name;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::SCAN_TAG_NAME, &parsed_tag_name));
        if (!parsed_tag_name.empty()) {
            scan_tag_name = parsed_tag_name;
        }
        return Status::OK();
    }

    // Parse index-related configurations: file index, global index.
    Status ParseIndexOptions(const ConfigParser& parser) {
        // Parse file-index.read.enabled - whether to enable reading file index, default true
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::FILE_INDEX_READ_ENABLED, &file_index_read_enabled));
        // Parse index-file-in-data-file-dir - whether index file in data file directory
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::INDEX_FILE_IN_DATA_FILE_DIR, &index_file_in_data_file_dir));
        // Parse global-index.enabled - whether to enable global index for scan, default true
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::GLOBAL_INDEX_ENABLED, &global_index_enabled));
        // Parse global-index.external-path - global index root directory
        std::string parsed_global_index_external_path;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::GLOBAL_INDEX_EXTERNAL_PATH,
                                                &parsed_global_index_external_path));
        if (!parsed_global_index_external_path.empty()) {
            global_index_external_path = parsed_global_index_external_path;
        }
        return Status::OK();
    }

    // Parse compaction configurations: sorted run triggers, size ratios, thresholds, off-peak.
    Status ParseCompactionOptions(const ConfigParser& parser) {
        // Parse compaction.min.file-num - minimum file number to trigger compaction, default 5
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::COMPACTION_MIN_FILE_NUM, &compaction_min_file_num));
        // Parse compaction.max-size-amplification-percent - size amplification percent, default 200
        PAIMON_RETURN_NOT_OK(
            parser.Parse<int32_t>(Options::COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT,
                                  &compaction_max_size_amplification_percent));
        // Parse compaction.size-ratio - percentage flexibility for sorted run comparison, default 1
        PAIMON_RETURN_NOT_OK(
            parser.Parse<int32_t>(Options::COMPACTION_SIZE_RATIO, &compaction_size_ratio));
        // Parse num-sorted-run.compaction-trigger - sorted run number to trigger compaction
        PAIMON_RETURN_NOT_OK(parser.Parse<int32_t>(Options::NUM_SORTED_RUNS_COMPACTION_TRIGGER,
                                                   &num_sorted_runs_compaction_trigger));
        // Parse num-sorted-run.stop-trigger - sorted run number to stop writes
        PAIMON_RETURN_NOT_OK(parser.Parse<int32_t>(Options::NUM_SORTED_RUNS_STOP_TRIGGER,
                                                   &num_sorted_runs_stop_trigger));
        // Parse num-levels - total level number for LSM tree
        PAIMON_RETURN_NOT_OK(parser.Parse<int32_t>(Options::NUM_LEVELS, &num_levels));
        // Parse compaction.force-rewrite-all-files - force pick all files for full compaction
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::COMPACTION_FORCE_REWRITE_ALL_FILES,
                                                &compaction_force_rewrite_all_files));
        // Parse compaction.force-up-level-0 - always include all level 0 files in candidates
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::COMPACTION_FORCE_UP_LEVEL_0, &compaction_force_up_level_0));
        // Parse compaction.optimization-interval - how often to perform optimization compaction
        std::string optimized_compaction_interval_str;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::COMPACTION_OPTIMIZATION_INTERVAL,
                                                &optimized_compaction_interval_str));
        if (!optimized_compaction_interval_str.empty()) {
            PAIMON_ASSIGN_OR_RAISE(optimized_compaction_interval,
                                   TimeDuration::Parse(optimized_compaction_interval_str));
        }
        // Parse compaction.total-size-threshold - force full compaction when total size is smaller
        std::string compaction_total_size_threshold_str;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::COMPACTION_TOTAL_SIZE_THRESHOLD,
                                                &compaction_total_size_threshold_str));
        if (!compaction_total_size_threshold_str.empty()) {
            PAIMON_ASSIGN_OR_RAISE(compaction_total_size_threshold,
                                   MemorySize::ParseBytes(compaction_total_size_threshold_str));
        }
        // Parse compaction.incremental-size-threshold - force full compaction when incremental size
        // is bigger
        std::string compaction_incremental_size_threshold_str;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::COMPACTION_INCREMENTAL_SIZE_THRESHOLD,
                                                &compaction_incremental_size_threshold_str));
        if (!compaction_incremental_size_threshold_str.empty()) {
            PAIMON_ASSIGN_OR_RAISE(
                compaction_incremental_size_threshold,
                MemorySize::ParseBytes(compaction_incremental_size_threshold_str));
        }
        // Parse compaction.offpeak.start.hour - start of off-peak hours (0-23), -1 to disable
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::COMPACT_OFFPEAK_START_HOUR, &compact_off_peak_start_hour));
        // Parse compaction.offpeak.end.hour - end of off-peak hours (0-23), -1 to disable
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::COMPACT_OFFPEAK_END_HOUR, &compact_off_peak_end_hour));
        // Parse compaction.offpeak-ratio - more aggressive ratio during off-peak hours, default 0
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::COMPACTION_OFFPEAK_RATIO, &compact_off_peak_ratio));
        return Status::OK();
    }

    // Parse lookup configurations: compact mode, bloom filter, remote file, cache, compression.
    Status ParseLookupOptions(const ConfigParser& parser) {
        // Parse force-lookup - whether to force lookup for compaction, default false
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::FORCE_LOOKUP, &force_lookup));
        // Parse lookup-wait - commit will wait for compaction by lookup, default true
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::LOOKUP_WAIT, &lookup_wait));
        // Parse lookup-compact - lookup compact mode, default RADICAL
        PAIMON_RETURN_NOT_OK(parser.ParseLookupCompactMode(&lookup_compact_mode));
        // Parse lookup-compact.max-interval - max interval for gentle mode lookup compaction
        PAIMON_RETURN_NOT_OK(
            parser.Parse(Options::LOOKUP_COMPACT_MAX_INTERVAL, &lookup_compact_max_interval));
        // Parse lookup.cache.bloom.filter.enabled - enable bloom filter for lookup cache
        PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::LOOKUP_CACHE_BLOOM_FILTER_ENABLED,
                                                &lookup_cache_bloom_filter));
        // Parse lookup.cache.bloom.filter.fpp - false positive probability, default 0.05
        PAIMON_RETURN_NOT_OK(parser.Parse<double>(Options::LOOKUP_CACHE_BLOOM_FILTER_FPP,
                                                  &lookup_cache_bloom_filter_fpp));
        // Parse lookup.remote-file.enabled - whether to enable remote file for lookup
        PAIMON_RETURN_NOT_OK(
            parser.Parse<bool>(Options::LOOKUP_REMOTE_FILE_ENABLED, &lookup_remote_file_enabled));
        // Parse lookup.remote-file.level-threshold - level threshold for remote lookup files
        PAIMON_RETURN_NOT_OK(parser.Parse<int32_t>(Options::LOOKUP_REMOTE_LEVEL_THRESHOLD,
                                                   &lookup_remote_level_threshold));
        // Parse lookup.cache-spill-compression - spill compression for lookup cache, default "zstd"
        std::string lookup_compress_options_compression_str;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::LOOKUP_CACHE_SPILL_COMPRESSION,
                                                &lookup_compress_options_compression_str));
        if (!lookup_compress_options_compression_str.empty()) {
            lookup_compress_options.compress = lookup_compress_options_compression_str;
        }
        // Parse spill-compression.zstd-level - zstd level for spill compression, default 1
        PAIMON_RETURN_NOT_OK(parser.Parse<int32_t>(Options::SPILL_COMPRESSION_ZSTD_LEVEL,
                                                   &(lookup_compress_options.zstd_level)));
        // Parse cache-page-size - memory page size for caching, default 64 kb
        PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::CACHE_PAGE_SIZE, &cache_page_size));
        // Parse lookup.cache-max-memory-size - max memory size for lookup cache, default 256 mb
        PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::LOOKUP_CACHE_MAX_MEMORY_SIZE,
                                                    &lookup_cache_max_memory));
        // Parse lookup.cache.high-priority-pool-ratio - fraction for high-priority data, default
        // 0.25
        PAIMON_RETURN_NOT_OK(parser.Parse<double>(Options::LOOKUP_CACHE_HIGH_PRIO_POOL_RATIO,
                                                  &lookup_cache_high_prio_pool_ratio));
        if (lookup_cache_high_prio_pool_ratio < 0.0 || lookup_cache_high_prio_pool_ratio >= 1.0) {
            return Status::Invalid(fmt::format(
                "The high priority pool ratio should in the range [0, 1), while input is {}",
                lookup_cache_high_prio_pool_ratio));
        }
        // Parse lookup.cache-file-retention - cached files retention time, default "1 hour"
        std::string lookup_cache_file_retention_str;
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::LOOKUP_CACHE_FILE_RETENTION,
                                                &lookup_cache_file_retention_str));
        if (!lookup_cache_file_retention_str.empty()) {
            PAIMON_ASSIGN_OR_RAISE(lookup_cache_file_retention_ms,
                                   TimeDuration::Parse(lookup_cache_file_retention_str));
        }
        // Parse lookup.cache-max-disk-size - max disk size for lookup cache, default unlimited
        PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::LOOKUP_CACHE_MAX_DISK_SIZE,
                                                    &lookup_cache_max_disk_size));
        return Status::OK();
    }
};

// Parse configurations from a map and return a populated CoreOptions object.
Result<CoreOptions> CoreOptions::FromMap(
    const std::map<std::string, std::string>& options_map,
    const std::shared_ptr<FileSystem>& specified_file_system,
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map) {
    CoreOptions options;
    auto& impl = options.impl_;
    impl->raw_options = options_map;
    ConfigParser parser(options_map);

    PAIMON_RETURN_NOT_OK(
        impl->ParseBasicOptions(parser, specified_file_system, fs_scheme_to_identifier_map));
    PAIMON_RETURN_NOT_OK(impl->ParseFileFormatOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseManifestOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseExpireOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseCommitOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseMergeAndSequenceOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseDeletionVectorOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseScanAndBranchOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseIndexOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseCompactionOptions(parser));
    PAIMON_RETURN_NOT_OK(impl->ParseLookupOptions(parser));

    return options;
}

CoreOptions::CoreOptions() : impl_(std::make_unique<Impl>()) {}

CoreOptions::CoreOptions(const CoreOptions& rhs)
    : impl_(std::make_unique<Impl>(*(rhs.impl_.get()))) {}

CoreOptions& CoreOptions::operator=(const CoreOptions& rhs) {
    if (this != &rhs) {
        impl_ = std::make_unique<Impl>(*(rhs.impl_.get()));
    }
    return *this;
}

CoreOptions::~CoreOptions() = default;

int32_t CoreOptions::GetBucket() const {
    return impl_->bucket;
}

std::shared_ptr<FileFormat> CoreOptions::GetWriteFileFormat(int32_t level) const {
    auto iter = impl_->file_format_per_level.find(level);
    if (iter != impl_->file_format_per_level.end()) {
        return iter->second;
    }
    return impl_->file_format;
}

std::shared_ptr<FileFormat> CoreOptions::GetFileFormat() const {
    return impl_->file_format;
}

std::shared_ptr<FileSystem> CoreOptions::GetFileSystem() const {
    return impl_->file_system;
}

const std::string& CoreOptions::GetFileCompression() const {
    return impl_->file_compression;
}

const std::string& CoreOptions::GetWriteFileCompression(int32_t level) const {
    auto iter = impl_->file_compression_per_level.find(level);
    if (iter != impl_->file_compression_per_level.end()) {
        return iter->second;
    }
    return impl_->file_compression;
}

int32_t CoreOptions::GetFileCompressionZstdLevel() const {
    return impl_->file_compression_zstd_level;
}

int64_t CoreOptions::GetPageSize() const {
    return impl_->page_size;
}

int64_t CoreOptions::GetTargetFileSize(bool has_primary_key) const {
    if (impl_->target_file_size == std::nullopt) {
        return has_primary_key ? 128 * 1024 * 1024 : 256 * 1024 * 1024;
    }
    return impl_->target_file_size.value();
}

int64_t CoreOptions::GetBlobTargetFileSize() const {
    if (impl_->blob_target_file_size == std::nullopt) {
        return GetTargetFileSize(/*has_primary_key=*/false);
    }
    return impl_->blob_target_file_size.value();
}

int64_t CoreOptions::GetCompactionFileSize(bool has_primary_key) const {
    // file size to join the compaction, we don't process on middle file size to avoid
    // compact a same file twice (the compression is not calculate so accurately. the output
    // file maybe be less than target file generated by rolling file write).
    return GetTargetFileSize(has_primary_key) / 10 * 7;
}

std::string CoreOptions::GetPartitionDefaultName() const {
    return impl_->partition_default_name;
}

std::shared_ptr<FileFormat> CoreOptions::GetManifestFormat() const {
    return impl_->manifest_file_format;
}

int64_t CoreOptions::GetSourceSplitTargetSize() const {
    return impl_->source_split_target_size;
}
int64_t CoreOptions::GetSourceSplitOpenFileCost() const {
    return impl_->source_split_open_file_cost;
}
std::optional<int64_t> CoreOptions::GetScanSnapshotId() const {
    return impl_->scan_snapshot_id;
}
int64_t CoreOptions::GetManifestTargetFileSize() const {
    return impl_->manifest_target_file_size;
}

int32_t CoreOptions::GetManifestMergeMinCount() const {
    return impl_->manifest_merge_min_count;
}

int64_t CoreOptions::GetManifestFullCompactionThresholdSize() const {
    return impl_->manifest_full_compaction_file_size;
}

const std::string& CoreOptions::GetManifestCompression() const {
    return impl_->manifest_compression;
}

StartupMode CoreOptions::GetStartupMode() const {
    if (impl_->startup_mode == StartupMode::Default()) {
        if (GetScanSnapshotId() != std::nullopt || GetScanTagName() != std::nullopt) {
            return StartupMode::FromSnapshot();
        }
        return StartupMode::LatestFull();
    }
    return impl_->startup_mode;
}

int32_t CoreOptions::GetReadBatchSize() const {
    return impl_->read_batch_size;
}

int32_t CoreOptions::GetWriteBatchSize() const {
    return impl_->write_batch_size;
}

int64_t CoreOptions::GetWriteBufferSize() const {
    return impl_->write_buffer_size;
}

bool CoreOptions::CommitForceCompact() const {
    return impl_->commit_force_compact;
}

int64_t CoreOptions::GetCommitTimeout() const {
    return impl_->commit_timeout;
}

int32_t CoreOptions::GetCommitMaxRetries() const {
    return impl_->commit_max_retries;
}

int32_t CoreOptions::GetCompactionMinFileNum() const {
    return impl_->compaction_min_file_num;
}

int32_t CoreOptions::GetCompactionMaxSizeAmplificationPercent() const {
    return impl_->compaction_max_size_amplification_percent;
}

int32_t CoreOptions::GetCompactionSizeRatio() const {
    return impl_->compaction_size_ratio;
}

int32_t CoreOptions::GetNumSortedRunsCompactionTrigger() const {
    return impl_->num_sorted_runs_compaction_trigger;
}

int32_t CoreOptions::GetNumSortedRunsStopTrigger() const {
    int32_t compact_trigger = GetNumSortedRunsCompactionTrigger();
    int32_t stop_trigger = 0;
    if (impl_->num_sorted_runs_stop_trigger.has_value()) {
        stop_trigger = impl_->num_sorted_runs_stop_trigger.value();
    } else {
        int64_t computed = static_cast<int64_t>(compact_trigger) + 3;
        if (computed > std::numeric_limits<int32_t>::max()) {
            computed = std::numeric_limits<int32_t>::max();
        }
        stop_trigger = static_cast<int32_t>(computed);
    }
    return std::max(compact_trigger, stop_trigger);
}

int32_t CoreOptions::GetNumLevels() const {
    // By default, this ensures that the compaction does not fall to level 0, but at least to
    // level 1
    if (impl_->num_levels.has_value()) {
        return impl_->num_levels.value();
    }

    int64_t incremented = static_cast<int64_t>(GetNumSortedRunsCompactionTrigger()) + 1;
    if (incremented > std::numeric_limits<int32_t>::max()) {
        incremented = std::numeric_limits<int32_t>::max();
    }
    return static_cast<int32_t>(incremented);
}

LookupCompactMode CoreOptions::GetLookupCompactMode() const {
    return impl_->lookup_compact_mode;
}

int32_t CoreOptions::GetLookupCompactMaxInterval() const {
    int32_t compact_trigger = GetNumSortedRunsCompactionTrigger();
    int32_t max_interval;
    if (impl_->lookup_compact_max_interval.has_value()) {
        max_interval = impl_->lookup_compact_max_interval.value();
    } else {
        int64_t doubled = static_cast<int64_t>(compact_trigger) * 2;
        if (doubled > std::numeric_limits<int32_t>::max()) {
            doubled = std::numeric_limits<int32_t>::max();
        }
        max_interval = static_cast<int32_t>(doubled);
    }

    if (max_interval < compact_trigger) {
        max_interval = compact_trigger;
    }
    return max_interval;
}

const ExpireConfig& CoreOptions::GetExpireConfig() const {
    return impl_->expire_config;
}

const std::vector<std::string>& CoreOptions::GetSequenceField() const {
    return impl_->sequence_field;
}

bool CoreOptions::SequenceFieldSortOrderIsAscending() const {
    return impl_->sequence_field_sort_order == SortOrder::ASCENDING;
}

MergeEngine CoreOptions::GetMergeEngine() const {
    return impl_->merge_engine;
}

SortEngine CoreOptions::GetSortEngine() const {
    return impl_->sort_engine;
}

bool CoreOptions::IgnoreDelete() const {
    return impl_->ignore_delete;
}

bool CoreOptions::WriteOnly() const {
    return impl_->write_only;
}

std::optional<std::string> CoreOptions::GetFieldsDefaultFunc() const {
    return impl_->field_default_func;
}

bool CoreOptions::EnableAdaptivePrefetchStrategy() const {
    return impl_->enable_adaptive_prefetch_strategy;
}

Result<std::optional<std::string>> CoreOptions::GetFieldAggFunc(
    const std::string& field_name) const {
    ConfigParser parser(impl_->raw_options);
    std::string field_agg_func = "";
    std::string key = std::string(Options::FIELDS_PREFIX) + "." + field_name + "." +
                      std::string(Options::AGG_FUNCTION);
    PAIMON_RETURN_NOT_OK(parser.ParseString(key, &field_agg_func));
    if (!field_agg_func.empty()) {
        return std::optional<std::string>(field_agg_func);
    }
    return std::optional<std::string>();
}

Result<bool> CoreOptions::FieldAggIgnoreRetract(const std::string& field_name) const {
    ConfigParser parser(impl_->raw_options);
    bool field_agg_ignore_retract = false;
    std::string key = std::string(Options::FIELDS_PREFIX) + "." + field_name + "." +
                      std::string(Options::IGNORE_RETRACT);
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(key, &field_agg_ignore_retract));
    return field_agg_ignore_retract;
}

Result<std::string> CoreOptions::FieldListAggDelimiter(const std::string& field_name) const {
    ConfigParser parser(impl_->raw_options);
    std::string delimiter = ",";
    std::string key = std::string(Options::FIELDS_PREFIX) + "." + field_name + "." +
                      std::string(Options::LIST_AGG_DELIMITER);
    PAIMON_RETURN_NOT_OK(parser.ParseString(key, &delimiter));
    return delimiter;
}

Result<bool> CoreOptions::FieldCollectAggDistinct(const std::string& field_name) const {
    ConfigParser parser(impl_->raw_options);
    bool distinct = false;
    std::string key = std::string(Options::FIELDS_PREFIX) + "." + field_name + "." +
                      std::string(Options::DISTINCT);
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(key, &distinct));
    return distinct;
}

bool CoreOptions::DeletionVectorsEnabled() const {
    return impl_->deletion_vectors_enabled;
}

bool CoreOptions::DeletionVectorsBitmap64() const {
    return impl_->deletion_vectors_bitmap64;
}
int64_t CoreOptions::DeletionVectorTargetFileSize() const {
    return impl_->deletion_vector_target_file_size;
}

ChangelogProducer CoreOptions::GetChangelogProducer() const {
    return impl_->changelog_producer;
}

LookupStrategy CoreOptions::GetLookupStrategy() const {
    return LookupStrategy::From(
        /*is_first_row=*/GetMergeEngine() == MergeEngine::FIRST_ROW,
        /*produce_changelog=*/GetChangelogProducer() == ChangelogProducer::LOOKUP,
        /*deletion_vector=*/DeletionVectorsEnabled(),
        /*force_lookup=*/impl_->force_lookup);
}

const std::map<std::string, std::string>& CoreOptions::ToMap() const {
    return impl_->raw_options;
}

bool CoreOptions::NeedLookup() const {
    return GetLookupStrategy().need_lookup;
}

bool CoreOptions::PrepareCommitWaitCompaction() const {
    if (!NeedLookup()) {
        return false;
    }
    return impl_->lookup_wait;
}

bool CoreOptions::CompactionForceRewriteAllFiles() const {
    return impl_->compaction_force_rewrite_all_files;
}

bool CoreOptions::CompactionForceUpLevel0() const {
    return impl_->compaction_force_up_level_0;
}

std::map<std::string, std::string> CoreOptions::GetFieldsSequenceGroups() const {
    auto raw_options = impl_->raw_options;
    std::map<std::string, std::string> sequence_groups;
    for (const auto& [key, value] : raw_options) {
        if (StringUtils::StartsWith(key, Options::FIELDS_PREFIX, /*start_pos=*/0) &&
            StringUtils::EndsWith(key, Options::SEQUENCE_GROUP)) {
            std::string seq_fields_str =
                key.substr(std::strlen(Options::FIELDS_PREFIX) + 1,
                           key.size() - std::strlen(Options::FIELDS_PREFIX) -
                               std::strlen(Options::SEQUENCE_GROUP) - 2);
            sequence_groups[seq_fields_str] = value;
        }
    }
    return sequence_groups;
}

bool CoreOptions::PartialUpdateRemoveRecordOnDelete() const {
    return impl_->partial_update_remove_record_on_delete;
}

bool CoreOptions::AggregationRemoveRecordOnDelete() const {
    return impl_->aggregation_remove_record_on_delete;
}

std::vector<std::string> CoreOptions::GetPartialUpdateRemoveRecordOnSequenceGroup() const {
    return impl_->remove_record_on_sequence_group;
}

std::optional<std::string> CoreOptions::GetScanFallbackBranch() const {
    return impl_->scan_fallback_branch;
}

std::string CoreOptions::GetBranch() const {
    return impl_->branch;
}

bool CoreOptions::FileIndexReadEnabled() const {
    return impl_->file_index_read_enabled;
}

std::optional<std::string> CoreOptions::GetDataFileExternalPaths() const {
    return impl_->data_file_external_paths;
}

ExternalPathStrategy CoreOptions::GetExternalPathStrategy() const {
    return impl_->external_path_strategy;
}

Result<std::vector<std::string>> CoreOptions::CreateExternalPaths() const {
    std::vector<std::string> external_paths;
    std::optional<std::string> data_file_external_paths = GetDataFileExternalPaths();
    ExternalPathStrategy strategy = GetExternalPathStrategy();
    if (strategy == ExternalPathStrategy::SPECIFIC_FS) {
        return Status::NotImplemented("do not support specific-fs external path strategy for now");
    }
    if (data_file_external_paths == std::nullopt || strategy == ExternalPathStrategy::NONE) {
        return external_paths;
    }
    for (const auto& p : StringUtils::Split(data_file_external_paths.value(), ",")) {
        std::string tmp_path = p;
        StringUtils::Trim(&tmp_path);
        PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(tmp_path));
        if (path.scheme.empty()) {
            return Status::Invalid(fmt::format("scheme is null, path is {}", p));
        }
        external_paths.push_back(path.ToString());
    }
    if (external_paths.empty()) {
        return Status::Invalid("external paths is empty");
    }
    return external_paths;
}

std::string CoreOptions::DataFilePrefix() const {
    return impl_->data_file_prefix;
}

bool CoreOptions::IndexFileInDataFileDir() const {
    return impl_->index_file_in_data_file_dir;
}

bool CoreOptions::RowTrackingEnabled() const {
    return impl_->row_tracking_enabled;
}

bool CoreOptions::DataEvolutionEnabled() const {
    return impl_->data_evolution_enabled;
}

bool CoreOptions::LegacyPartitionNameEnabled() const {
    return impl_->legacy_partition_name_enabled;
}

bool CoreOptions::GlobalIndexEnabled() const {
    return impl_->global_index_enabled;
}

std::optional<std::string> CoreOptions::GetGlobalIndexExternalPath() const {
    return impl_->global_index_external_path;
}

Result<std::optional<std::string>> CoreOptions::CreateGlobalIndexExternalPath() const {
    std::optional<std::string> global_index_external_path = GetGlobalIndexExternalPath();
    if (global_index_external_path == std::nullopt) {
        return global_index_external_path;
    }
    std::string tmp_path = global_index_external_path.value();
    StringUtils::Trim(&tmp_path);
    PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(tmp_path));
    if (path.scheme.empty()) {
        return Status::Invalid(fmt::format("scheme is null, path is {}", tmp_path));
    }
    return std::optional<std::string>(path.ToString());
}

std::optional<std::string> CoreOptions::GetScanTagName() const {
    return impl_->scan_tag_name;
}

std::optional<int64_t> CoreOptions::GetOptimizedCompactionInterval() const {
    return impl_->optimized_compaction_interval;
}
std::optional<int64_t> CoreOptions::GetCompactionTotalSizeThreshold() const {
    return impl_->compaction_total_size_threshold;
}
std::optional<int64_t> CoreOptions::GetCompactionIncrementalSizeThreshold() const {
    return impl_->compaction_incremental_size_threshold;
}

int32_t CoreOptions::GetCompactOffPeakStartHour() const {
    return impl_->compact_off_peak_start_hour;
}
int32_t CoreOptions::GetCompactOffPeakEndHour() const {
    return impl_->compact_off_peak_end_hour;
}
int32_t CoreOptions::GetCompactOffPeakRatio() const {
    return impl_->compact_off_peak_ratio;
}

bool CoreOptions::LookupCacheBloomFilterEnabled() const {
    return impl_->lookup_cache_bloom_filter;
}

double CoreOptions::GetLookupCacheBloomFilterFpp() const {
    return impl_->lookup_cache_bloom_filter_fpp;
}

const CompressOptions& CoreOptions::GetLookupCompressOptions() const {
    return impl_->lookup_compress_options;
}

bool CoreOptions::LookupRemoteFileEnabled() const {
    return impl_->lookup_remote_file_enabled;
}

int32_t CoreOptions::GetLookupRemoteLevelThreshold() const {
    return impl_->lookup_remote_level_threshold;
}

int32_t CoreOptions::GetCachePageSize() const {
    return static_cast<int32_t>(impl_->cache_page_size);
}

int64_t CoreOptions::GetLookupCacheMaxMemory() const {
    return impl_->lookup_cache_max_memory;
}

double CoreOptions::GetLookupCacheHighPrioPoolRatio() const {
    return impl_->lookup_cache_high_prio_pool_ratio;
}

BucketFunctionType CoreOptions::GetBucketFunctionType() const {
    return impl_->bucket_function_type;
}

int64_t CoreOptions::GetLookupCacheFileRetentionMs() const {
    return impl_->lookup_cache_file_retention_ms;
}

int64_t CoreOptions::GetLookupCacheMaxDiskSize() const {
    return impl_->lookup_cache_max_disk_size;
}

}  // namespace paimon
