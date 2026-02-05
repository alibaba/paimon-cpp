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
#include <utility>

#include "fmt/format.h"
#include "paimon/common/fs/resolving_file_system.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/options/time_duration.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/core/options/expire_config.h"
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
            PAIMON_ASSIGN_OR_RAISE(*value, Factory::Get(default_identifier, config_map_));
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
            const auto& str = iter->second;
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

    // Parse SortEngine
    Status ParseSortEngine(SortEngine* sort_engine) const {
        auto iter = config_map_.find(Options::SORT_ENGINE);
        if (iter != config_map_.end()) {
            const auto& str = iter->second;
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
            const auto& str = iter->second;
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
            const auto& str = iter->second;
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
            const auto& str = iter->second;
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

 private:
    const std::map<std::string, std::string> config_map_;
};

// Impl is a private implementation of CoreOptions,
// storing various configurable fields and their default values.
struct CoreOptions::Impl {
    int64_t page_size = 64 * 1024;
    int64_t target_file_size = 256 * 1024 * 1024;  // TODO(xinyu.lxy): target_file_size of primary
                                                   // key table is 128 MB and append table is 256 MB
    int64_t blob_target_file_size = 256 * 1024 * 1024;
    int64_t source_split_target_size = 128 * 1024 * 1024;
    int64_t source_split_open_file_cost = 4 * 1024 * 1024;
    int64_t manifest_target_file_size = 8 * 1024 * 1024;
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

    SortOrder sequence_field_sort_order = SortOrder::ASCENDING;
    MergeEngine merge_engine = MergeEngine::DEDUPLICATE;
    SortEngine sort_engine = SortEngine::LOSER_TREE;
    ChangelogProducer changelog_producer = ChangelogProducer::NONE;
    ExternalPathStrategy external_path_strategy = ExternalPathStrategy::NONE;

    int32_t file_compression_zstd_level = 1;

    bool ignore_delete = false;
    bool deletion_vectors_enabled = false;
    bool force_lookup = false;
    bool partial_update_remove_record_on_delete = false;
    bool file_index_read_enabled = true;
    bool enable_adaptive_prefetch_strategy = true;
    bool index_file_in_data_file_dir = false;
    bool row_tracking_enabled = false;
    bool data_evolution_enabled = false;
    bool legacy_partition_name_enabled = true;
    bool global_index_enabled = true;
    std::optional<std::string> global_index_external_path;
};

// Parse configurations from a map and return a populated CoreOptions object
Result<CoreOptions> CoreOptions::FromMap(
    const std::map<std::string, std::string>& options_map,
    const std::shared_ptr<FileSystem>& specified_file_system,
    const std::map<std::string, std::string>& fs_scheme_to_identifier_map) {
    CoreOptions options;
    auto& impl = options.impl_;
    impl->raw_options = options_map;
    ConfigParser parser(options_map);

    // Parse basic configurations
    PAIMON_RETURN_NOT_OK(parser.Parse(Options::BUCKET, &impl->bucket));
    PAIMON_RETURN_NOT_OK(
        parser.Parse(Options::MANIFEST_MERGE_MIN_COUNT, &impl->manifest_merge_min_count));
    PAIMON_RETURN_NOT_OK(parser.Parse(Options::SCAN_SNAPSHOT_ID, &impl->scan_snapshot_id));
    PAIMON_RETURN_NOT_OK(parser.Parse(Options::READ_BATCH_SIZE, &impl->read_batch_size));
    PAIMON_RETURN_NOT_OK(parser.Parse(Options::WRITE_BATCH_SIZE, &impl->write_batch_size));
    PAIMON_RETURN_NOT_OK(
        parser.ParseMemorySize(Options::WRITE_BUFFER_SIZE, &impl->write_buffer_size));
    PAIMON_RETURN_NOT_OK(parser.Parse(Options::COMMIT_MAX_RETRIES, &impl->commit_max_retries));
    PAIMON_RETURN_NOT_OK(parser.ParseString(Options::FILE_COMPRESSION, &impl->file_compression));
    PAIMON_RETURN_NOT_OK(
        parser.Parse(Options::FILE_COMPRESSION_ZSTD_LEVEL, &impl->file_compression_zstd_level));
    PAIMON_RETURN_NOT_OK(
        parser.ParseString(Options::MANIFEST_COMPRESSION, &impl->manifest_compression));
    PAIMON_RETURN_NOT_OK(
        parser.ParseString(Options::PARTITION_DEFAULT_NAME, &impl->partition_default_name));

    // Parse memory size configurations
    PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::PAGE_SIZE, &impl->page_size));
    PAIMON_RETURN_NOT_OK(
        parser.ParseMemorySize(Options::TARGET_FILE_SIZE, &impl->target_file_size));
    impl->blob_target_file_size = impl->target_file_size;
    PAIMON_RETURN_NOT_OK(
        parser.ParseMemorySize(Options::BLOB_TARGET_FILE_SIZE, &impl->blob_target_file_size));
    PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::MANIFEST_TARGET_FILE_SIZE,
                                                &impl->manifest_target_file_size));
    PAIMON_RETURN_NOT_OK(
        parser.ParseMemorySize(Options::SOURCE_SPLIT_TARGET_SIZE, &impl->source_split_target_size));
    PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::SOURCE_SPLIT_OPEN_FILE_COST,
                                                &impl->source_split_open_file_cost));
    PAIMON_RETURN_NOT_OK(parser.ParseMemorySize(Options::MANIFEST_FULL_COMPACTION_FILE_SIZE,
                                                &impl->manifest_full_compaction_file_size));

    // Parse file format and file system configurations
    PAIMON_RETURN_NOT_OK(parser.ParseObject<FileFormatFactory>(
        Options::FILE_FORMAT, /*default_identifier=*/"parquet", &impl->file_format));
    PAIMON_RETURN_NOT_OK(parser.ParseObject<FileFormatFactory>(
        Options::MANIFEST_FORMAT, /*default_identifier=*/"avro", &impl->manifest_file_format));
    PAIMON_RETURN_NOT_OK(parser.ParseFileSystem(fs_scheme_to_identifier_map, specified_file_system,
                                                &impl->file_system));

    // Parse startup mode
    std::string startup_mode_str;
    if (options_map.find(Options::SCAN_MODE) != options_map.end()) {
        PAIMON_RETURN_NOT_OK(parser.ParseString(Options::SCAN_MODE, &startup_mode_str));
        PAIMON_ASSIGN_OR_RAISE(auto startup_mode, StartupMode::FromString(startup_mode_str));
        impl->startup_mode = startup_mode;
    }

    // Special handling for ExpireConfig
    int32_t snapshot_num_retain_min = 10;
    int32_t snapshot_num_retain_max = std::numeric_limits<int32_t>::max();
    int32_t snapshot_expire_limit = 10;
    PAIMON_RETURN_NOT_OK(
        parser.Parse(Options::SNAPSHOT_NUM_RETAINED_MIN, &snapshot_num_retain_min));
    PAIMON_RETURN_NOT_OK(
        parser.Parse(Options::SNAPSHOT_NUM_RETAINED_MAX, &snapshot_num_retain_max));
    PAIMON_RETURN_NOT_OK(parser.Parse(Options::SNAPSHOT_EXPIRE_LIMIT, &snapshot_expire_limit));

    std::string snapshot_time_retained_str = "1 hour";
    PAIMON_RETURN_NOT_OK(
        parser.ParseString(Options::SNAPSHOT_TIME_RETAINED, &snapshot_time_retained_str));
    PAIMON_ASSIGN_OR_RAISE(int64_t snapshot_time_retained,
                           TimeDuration::Parse(snapshot_time_retained_str));
    bool snapshot_clean_empty_directories = false;
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES,
                                            &snapshot_clean_empty_directories));
    impl->expire_config =
        ExpireConfig(snapshot_num_retain_max, snapshot_num_retain_min, snapshot_time_retained,
                     snapshot_expire_limit, snapshot_clean_empty_directories);

    std::string commit_timeout_str;
    PAIMON_RETURN_NOT_OK(parser.ParseString(Options::COMMIT_TIMEOUT, &commit_timeout_str));
    if (!commit_timeout_str.empty()) {
        PAIMON_ASSIGN_OR_RAISE(impl->commit_timeout, TimeDuration::Parse(commit_timeout_str));
    }

    // Parse sequence field
    PAIMON_RETURN_NOT_OK(parser.ParseList<std::string>(
        Options::SEQUENCE_FIELD, Options::FIELDS_SEPARATOR, &impl->sequence_field));
    PAIMON_RETURN_NOT_OK(parser.ParseSortOrder(&impl->sequence_field_sort_order));
    // Parse merge and sort engine
    PAIMON_RETURN_NOT_OK(parser.ParseSortEngine(&impl->sort_engine));
    PAIMON_RETURN_NOT_OK(parser.ParseMergeEngine(&impl->merge_engine));
    // Parse ignore delete
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::IGNORE_DELETE, &impl->ignore_delete));

    // Parse default agg function
    std::string field_default_func;
    PAIMON_RETURN_NOT_OK(parser.ParseString(Options::FIELDS_DEFAULT_AGG_FUNC, &field_default_func));
    if (!field_default_func.empty()) {
        impl->field_default_func = field_default_func;
    }
    // Parse deletion vectors enabled & force lookup
    PAIMON_RETURN_NOT_OK(
        parser.Parse<bool>(Options::DELETION_VECTORS_ENABLED, &impl->deletion_vectors_enabled));
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::FORCE_LOOKUP, &impl->force_lookup));
    // Parse changelog producer
    PAIMON_RETURN_NOT_OK(parser.ParseChangelogProducer(&impl->changelog_producer));

    // Parse partial_update_remove_record_on_delete
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE,
                                            &impl->partial_update_remove_record_on_delete));
    // Parse partial-update.remove-record-on-sequence-group
    PAIMON_RETURN_NOT_OK(parser.ParseList<std::string>(
        Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP, Options::FIELDS_SEPARATOR,
        &impl->remove_record_on_sequence_group));

    // Parse scan.fallback-branch
    std::string scan_fallback_branch;
    PAIMON_RETURN_NOT_OK(parser.ParseString(Options::SCAN_FALLBACK_BRANCH, &scan_fallback_branch));
    if (!scan_fallback_branch.empty()) {
        impl->scan_fallback_branch = scan_fallback_branch;
    }
    // Parse branch name
    PAIMON_RETURN_NOT_OK(parser.ParseString(Options::BRANCH, &impl->branch));

    // Parse file-index.read.enabled
    PAIMON_RETURN_NOT_OK(
        parser.Parse<bool>(Options::FILE_INDEX_READ_ENABLED, &impl->file_index_read_enabled));

    // Parse data-file.external-paths
    std::string data_file_external_paths;
    PAIMON_RETURN_NOT_OK(
        parser.ParseString(Options::DATA_FILE_EXTERNAL_PATHS, &data_file_external_paths));
    if (!data_file_external_paths.empty()) {
        impl->data_file_external_paths = data_file_external_paths;
    }
    // Parse external path strategy
    PAIMON_RETURN_NOT_OK(parser.ParseExternalPathStrategy(&impl->external_path_strategy));
    // Only for test, parse enable-adaptive-prefetch-strategy
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>("test.enable-adaptive-prefetch-strategy",
                                            &impl->enable_adaptive_prefetch_strategy));
    // Parse data file prefix
    PAIMON_RETURN_NOT_OK(parser.ParseString(Options::DATA_FILE_PREFIX, &impl->data_file_prefix));

    // Parse index-file-in-data-file-dir
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::INDEX_FILE_IN_DATA_FILE_DIR,
                                            &impl->index_file_in_data_file_dir));
    // Parse row-tracking.enabled
    PAIMON_RETURN_NOT_OK(
        parser.Parse<bool>(Options::ROW_TRACKING_ENABLED, &impl->row_tracking_enabled));
    // Parse data-evolution.enabled
    PAIMON_RETURN_NOT_OK(
        parser.Parse<bool>(Options::DATA_EVOLUTION_ENABLED, &impl->data_evolution_enabled));
    // Parse partition.legacy-name
    PAIMON_RETURN_NOT_OK(parser.Parse<bool>(Options::PARTITION_GENERATE_LEGACY_NAME,
                                            &impl->legacy_partition_name_enabled));
    // Parse global-index.enabled
    PAIMON_RETURN_NOT_OK(
        parser.Parse<bool>(Options::GLOBAL_INDEX_ENABLED, &impl->global_index_enabled));

    // Parse global_index.external-path
    std::string global_index_external_path;
    PAIMON_RETURN_NOT_OK(
        parser.ParseString(Options::GLOBAL_INDEX_EXTERNAL_PATH, &global_index_external_path));
    if (!global_index_external_path.empty()) {
        impl->global_index_external_path = global_index_external_path;
    }

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

std::shared_ptr<FileFormat> CoreOptions::GetWriteFileFormat() const {
    return impl_->file_format;
}

std::shared_ptr<FileSystem> CoreOptions::GetFileSystem() const {
    return impl_->file_system;
}

const std::string& CoreOptions::GetFileCompression() const {
    return impl_->file_compression;
}

int32_t CoreOptions::GetFileCompressionZstdLevel() const {
    return impl_->file_compression_zstd_level;
}

int64_t CoreOptions::GetPageSize() const {
    return impl_->page_size;
}

int64_t CoreOptions::GetTargetFileSize() const {
    return impl_->target_file_size;
}

int64_t CoreOptions::GetBlobTargetFileSize() const {
    return impl_->blob_target_file_size;
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
        if (GetScanSnapshotId() != std::nullopt) {
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

int64_t CoreOptions::GetCommitTimeout() const {
    return impl_->commit_timeout;
}

int32_t CoreOptions::GetCommitMaxRetries() const {
    return impl_->commit_max_retries;
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

bool CoreOptions::DeletionVectorsEnabled() const {
    return impl_->deletion_vectors_enabled;
}

ChangelogProducer CoreOptions::GetChangelogProducer() const {
    return impl_->changelog_producer;
}

const std::map<std::string, std::string>& CoreOptions::ToMap() const {
    return impl_->raw_options;
}

bool CoreOptions::NeedLookup() const {
    return GetMergeEngine() == MergeEngine::FIRST_ROW ||
           GetChangelogProducer() == ChangelogProducer::LOOKUP || DeletionVectorsEnabled() ||
           impl_->force_lookup;
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

}  // namespace paimon
