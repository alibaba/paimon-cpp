/*
 * Copyright 2026-present Alibaba Inc.
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
#include <algorithm>

#include "paimon/common/data/serializer/row_compacted_serializer.h"
#include "paimon/core/io/key_value_data_file_record_reader.h"
#include "paimon/core/mergetree/lookup/lookup_serializer_factory.h"
#include "paimon/core/mergetree/lookup/persist_processor.h"
#include "paimon/core/mergetree/lookup_file.h"
#include "paimon/core/mergetree/lookup_utils.h"
#include "paimon/core/operation/raw_file_split_read.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/disk/io_manager.h"
#include "paimon/result.h"

namespace paimon {

template <typename T>
class RemoteLookupFileManager;

/// Remote sst file with serVersion.
struct RemoteSstFile {
    std::string sst_file_name;
    std::string ser_version;
};

/// Provide lookup by key.
template <typename T>
class LookupLevels : public Levels::DropFileCallback {
 public:
    static Result<std::unique_ptr<LookupLevels<T>>> Create(
        const std::shared_ptr<FileSystem>& fs, const BinaryRow& partition, int32_t bucket,
        const CoreOptions& options, const std::shared_ptr<SchemaManager>& schema_manager,
        const std::shared_ptr<IOManager>& io_manager,
        const std::shared_ptr<FileStorePathFactory>& path_factory,
        const std::shared_ptr<TableSchema>& table_schema, const std::shared_ptr<Levels>& levels,
        DeletionVector::Factory dv_factory,
        const std::shared_ptr<typename PersistProcessor<T>::Factory>& processor_factory,
        const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
        const std::shared_ptr<LookupStoreFactory>& lookup_store_factory,
        const std::shared_ptr<MemoryPool>& pool);

    const std::shared_ptr<Levels>& GetLevels() const {
        return levels_;
    }

    Result<std::optional<T>> Lookup(const std::shared_ptr<InternalRow>& key, int32_t start_level);

    Result<std::optional<T>> LookupLevel0(
        const std::shared_ptr<InternalRow>& key,
        const std::set<std::shared_ptr<DataFileMeta>, Levels::Level0Comparator>& level0);

    Result<std::optional<T>> Lookup(const std::shared_ptr<InternalRow>& key,
                                    const SortedRun& level);

    void NotifyDropFile(const std::string& file) override;

    void SetRemoteLookupFileManager(RemoteLookupFileManager<T>* manager) {
        remote_lookup_file_manager_ = manager;
    }

    std::optional<RemoteSstFile> RemoteSst(const std::shared_ptr<DataFileMeta>& file) const;

    std::string NewRemoteSst(const std::shared_ptr<DataFileMeta>& file, int64_t length) const;

    Result<std::shared_ptr<LookupFile>> CreateLookupFile(const std::shared_ptr<DataFileMeta>& file);

    void AddLocalFile(const std::shared_ptr<DataFileMeta>& file,
                      const std::shared_ptr<LookupFile>& lookup_file);

    Status Close() {
        // TODO(xinyu.lxy): invalid cache
        lookup_file_cache_.clear();
        return Status::OK();
    }

 private:
    LookupLevels(const std::shared_ptr<FileSystem>& fs, const BinaryRow& partition, int32_t bucket,
                 const CoreOptions& options, const std::shared_ptr<SchemaManager>& schema_manager,
                 const std::shared_ptr<IOManager>& io_manager,
                 std::unique_ptr<FieldsComparator>&& key_comparator,
                 const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
                 std::unique_ptr<RawFileSplitRead>&& split_read,
                 const std::shared_ptr<TableSchema>& table_schema,
                 const std::shared_ptr<arrow::Schema>& partition_schema,
                 const std::shared_ptr<arrow::Schema>& key_schema,
                 const std::shared_ptr<Levels>& levels, DeletionVector::Factory dv_factory,
                 const std::shared_ptr<typename PersistProcessor<T>::Factory>& processor_factory,
                 std::unique_ptr<RowCompactedSerializer>&& key_serializer,
                 const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
                 const std::shared_ptr<LookupStoreFactory>& lookup_store_factory,
                 const std::shared_ptr<MemoryPool>& pool);

    Result<std::optional<T>> Lookup(const std::shared_ptr<InternalRow>& key,
                                    const std::shared_ptr<DataFileMeta>& file);

    Status CreateSstFileFromDataFile(const std::shared_ptr<DataFileMeta>& file,
                                     const std::string& kv_file_path);

    /// Try to download remote sst file. Returns the ser_version if successful.
    std::optional<std::string> TryToDownloadRemoteSst(const std::shared_ptr<DataFileMeta>& file,
                                                      const std::string& local_file);

    Result<std::shared_ptr<PersistProcessor<T>>> GetOrCreateProcessor(
        int64_t schema_id, const std::string& ser_version);

    static constexpr const char* REMOTE_LOOKUP_FILE_SUFFIX = ".lookup";

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> fs_;
    BinaryRow partition_;
    int32_t bucket_;
    CoreOptions options_;
    std::shared_ptr<SchemaManager> schema_manager_;
    std::shared_ptr<IOManager> io_manager_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<DataFilePathFactory> data_file_path_factory_;
    std::unique_ptr<RawFileSplitRead> split_read_;

    std::shared_ptr<TableSchema> table_schema_;
    std::shared_ptr<arrow::Schema> partition_schema_;
    std::shared_ptr<arrow::Schema> read_schema_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<Levels> levels_;
    DeletionVector::Factory dv_factory_;

    std::shared_ptr<typename PersistProcessor<T>::Factory> processor_factory_;
    std::unique_ptr<RowCompactedSerializer> key_serializer_;
    std::shared_ptr<LookupSerializerFactory> serializer_factory_;
    std::shared_ptr<LookupStoreFactory> lookup_store_factory_;

    std::map<std::string, std::shared_ptr<LookupFile>> lookup_file_cache_;
    std::map<std::pair<int64_t, std::string>, std::shared_ptr<PersistProcessor<T>>>
        schema_id_and_ser_version_to_processors_;

    RemoteLookupFileManager<T>* remote_lookup_file_manager_ = nullptr;
};
}  // namespace paimon
