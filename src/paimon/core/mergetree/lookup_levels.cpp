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

#include "paimon/core/mergetree/lookup_levels.h"

#include "fmt/format.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/key_value_data_file_record_reader.h"
#include "paimon/core/mergetree/lookup/file_position.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
#include "paimon/core/mergetree/lookup/remote_lookup_file_manager.h"
#include "paimon/core/mergetree/lookup_utils.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/result.h"
namespace paimon {
template <typename T>
Result<std::unique_ptr<LookupLevels<T>>> LookupLevels<T>::Create(
    const std::shared_ptr<FileSystem>& fs, const BinaryRow& partition, int32_t bucket,
    const CoreOptions& options, const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<IOManager>& io_manager,
    const std::shared_ptr<FileStorePathFactory>& path_factory,
    const std::shared_ptr<TableSchema>& table_schema, const std::shared_ptr<Levels>& levels,
    DeletionVector::Factory dv_factory,
    const std::shared_ptr<typename PersistProcessor<T>::Factory>& processor_factory,
    const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
    const std::shared_ptr<LookupStoreFactory>& lookup_store_factory,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> pk_fields,
                           table_schema->TrimmedPrimaryKeyFields());

    auto pk_schema = DataField::ConvertDataFieldsToArrowSchema(pk_fields);
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RowCompactedSerializer> key_serializer,
                           RowCompactedSerializer::Create(pk_schema, pool));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FieldsComparator> key_comparator,
                           FieldsComparator::Create(pk_fields, /*is_ascending_order=*/true));

    PAIMON_ASSIGN_OR_RAISE(std::vector<DataField> partition_fields,
                           table_schema->GetFields(table_schema->PartitionKeys()));
    auto partition_schema = DataField::ConvertDataFieldsToArrowSchema(partition_fields);

    // TODO(xinyu.lxy): set executor
    // TODO(xinyu.lxy): temporarily disabled pre-buffer for parquet, which may cause high memory
    // usage during compaction. Will fix via parquet format refactor.
    ReadContextBuilder read_context_builder(path_factory->RootPath());
    read_context_builder.SetOptions(options.ToMap())
        .EnablePrefetch(true)
        .SetPrefetchMaxParallelNum(1)
        .SetPrefetchBatchCount(3)
        .WithMemoryPool(pool)
        .AddOption("parquet.read.enable-pre-buffer", "false");
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ReadContext> read_context,
                           read_context_builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<InternalReadContext> internal_read_context,
        InternalReadContext::Create(read_context, table_schema, options.ToMap()));
    auto split_read = std::make_unique<RawFileSplitRead>(path_factory, internal_read_context, pool,
                                                         CreateDefaultExecutor());

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<DataFilePathFactory> data_file_path_factory,
                           path_factory->CreateDataFilePathFactory(partition, bucket));

    return std::unique_ptr<LookupLevels>(new LookupLevels(
        fs, partition, bucket, options, schema_manager, io_manager, std::move(key_comparator),
        data_file_path_factory, std::move(split_read), table_schema, partition_schema, pk_schema,
        levels, dv_factory, processor_factory, std::move(key_serializer), serializer_factory,
        lookup_store_factory, pool));
}
template <typename T>
Result<std::optional<T>> LookupLevels<T>::Lookup(const std::shared_ptr<InternalRow>& key,
                                                 int32_t start_level) {
    auto lookup = [this](const std::shared_ptr<InternalRow>& key,
                         const SortedRun& level) -> Result<std::optional<T>> {
        return this->Lookup(key, level);
    };
    auto lookup_level0 =
        [this](const std::shared_ptr<InternalRow>& key,
               const std::set<std::shared_ptr<DataFileMeta>, Levels::Level0Comparator>& level0)
        -> Result<std::optional<T>> { return this->LookupLevel0(key, level0); };
    return LookupUtils::Lookup(*levels_, key, start_level, std::function(lookup),
                               std::function(lookup_level0));
}

template <typename T>
Result<std::optional<T>> LookupLevels<T>::LookupLevel0(
    const std::shared_ptr<InternalRow>& key,
    const std::set<std::shared_ptr<DataFileMeta>, Levels::Level0Comparator>& level0) {
    auto lookup = [this](const std::shared_ptr<InternalRow>& key,
                         const std::shared_ptr<DataFileMeta>& file) -> Result<std::optional<T>> {
        return this->Lookup(key, file);
    };
    return LookupUtils::LookupLevel0(key_comparator_, key, level0, std::function(lookup));
}

template <typename T>
Result<std::optional<T>> LookupLevels<T>::Lookup(const std::shared_ptr<InternalRow>& key,
                                                 const SortedRun& level) {
    auto lookup = [this](const std::shared_ptr<InternalRow>& key,
                         const std::shared_ptr<DataFileMeta>& file) -> Result<std::optional<T>> {
        return this->Lookup(key, file);
    };
    return LookupUtils::Lookup(key_comparator_, key, level, std::function(lookup));
}

template <typename T>
LookupLevels<T>::LookupLevels(
    const std::shared_ptr<FileSystem>& fs, const BinaryRow& partition, int32_t bucket,
    const CoreOptions& options, const std::shared_ptr<SchemaManager>& schema_manager,
    const std::shared_ptr<IOManager>& io_manager,
    std::unique_ptr<FieldsComparator>&& key_comparator,
    const std::shared_ptr<DataFilePathFactory>& data_file_path_factory,
    std::unique_ptr<RawFileSplitRead>&& split_read,
    const std::shared_ptr<TableSchema>& table_schema,
    const std::shared_ptr<arrow::Schema>& partition_schema,
    const std::shared_ptr<arrow::Schema>& key_schema, const std::shared_ptr<Levels>& levels,
    DeletionVector::Factory dv_factory,
    const std::shared_ptr<typename PersistProcessor<T>::Factory>& processor_factory,
    std::unique_ptr<RowCompactedSerializer>&& key_serializer,
    const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
    const std::shared_ptr<LookupStoreFactory>& lookup_store_factory,
    const std::shared_ptr<MemoryPool>& pool)
    : pool_(pool),
      fs_(fs),
      partition_(partition),
      bucket_(bucket),
      options_(options),
      schema_manager_(schema_manager),
      io_manager_(io_manager),
      key_comparator_(std::move(key_comparator)),
      data_file_path_factory_(data_file_path_factory),
      split_read_(std::move(split_read)),
      table_schema_(table_schema),
      partition_schema_(partition_schema),
      key_schema_(key_schema),
      levels_(levels),
      dv_factory_(dv_factory),
      processor_factory_(processor_factory),
      key_serializer_(std::move(key_serializer)),
      serializer_factory_(serializer_factory),
      lookup_store_factory_(lookup_store_factory) {
    if constexpr (std::is_same_v<T, FilePosition>) {
        // if T is FilePosition, only read key fields to create sst file is enough
        value_schema_ = key_schema_;
    } else {
        value_schema_ = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    }
    read_schema_ = SpecialFields::CompleteSequenceAndValueKindField(value_schema_);
    levels_->AddDropFileCallback(this);
}

template <typename T>
void LookupLevels<T>::NotifyDropFile(const std::string& file) {
    lookup_file_cache_.erase(file);
}

template <typename T>
Result<std::optional<T>> LookupLevels<T>::Lookup(const std::shared_ptr<InternalRow>& key,
                                                 const std::shared_ptr<DataFileMeta>& file) {
    auto iter = lookup_file_cache_.find(file->file_name);
    std::shared_ptr<LookupFile> lookup_file;
    if (iter == lookup_file_cache_.end()) {
        PAIMON_ASSIGN_OR_RAISE(lookup_file, CreateLookupFile(file));
        AddLocalFile(file, lookup_file);
    } else {
        lookup_file = iter->second;
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> key_bytes,
                           key_serializer_->SerializeToBytes(*key));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> value_bytes, lookup_file->GetResult(key_bytes));
    if (!value_bytes) {
        return std::optional<T>();
    }

    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<PersistProcessor<T>> processor,
        GetOrCreateProcessor(lookup_file->SchemaId(), lookup_file->SerVersion()));
    PAIMON_ASSIGN_OR_RAISE(
        T result, processor->ReadFromDisk(key, lookup_file->Level(), value_bytes, file->file_name));
    return std::optional<T>(std::move(result));
}
template <typename T>
Result<std::shared_ptr<LookupFile>> LookupLevels<T>::CreateLookupFile(
    const std::shared_ptr<DataFileMeta>& file) {
    PAIMON_ASSIGN_OR_RAISE(
        std::string prefix,
        LookupFile::LocalFilePrefix(partition_schema_, partition_, bucket_, file->file_name));
    PAIMON_ASSIGN_OR_RAISE(std::string kv_file_path, io_manager_->GenerateTempFilePath(prefix));

    int64_t schema_id = table_schema_->Id();
    std::string file_ser_version = serializer_factory_->Version();
    auto download_ser_version = TryToDownloadRemoteSst(file, kv_file_path);
    if (download_ser_version.has_value()) {
        // use schema id from remote file
        schema_id = file->schema_id;
        file_ser_version = download_ser_version.value();
    } else {
        PAIMON_RETURN_NOT_OK(CreateSstFileFromDataFile(file, kv_file_path));
    }

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<LookupStoreReader> reader,
                           lookup_store_factory_->CreateReader(fs_, kv_file_path, pool_));
    return std::make_shared<LookupFile>(fs_, kv_file_path, file->level, schema_id, file_ser_version,
                                        std::move(reader));
}

template <typename T>
std::optional<RemoteSstFile> LookupLevels<T>::RemoteSst(
    const std::shared_ptr<DataFileMeta>& file) const {
    // Find the first extra file that ends with REMOTE_LOOKUP_FILE_SUFFIX
    std::optional<std::string> sst_file_name;
    for (const auto& extra_file : file->extra_files) {
        if (extra_file.has_value()) {
            const std::string& name = extra_file.value();
            if (StringUtils::EndsWith(name, REMOTE_LOOKUP_FILE_SUFFIX)) {
                sst_file_name = name;
                break;
            }
        }
    }
    if (!sst_file_name.has_value()) {
        return std::nullopt;
    }

    // Parse: {fileName}.{length}.{processorId}.{serVersion}.lookup
    // Split by '.'
    const std::string& name = sst_file_name.value();
    std::vector<std::string> parts = StringUtils::Split(name, ".");
    // Need at least 3 parts: ...processorId.serVersion.lookup
    if (parts.size() < 3) {
        return std::nullopt;
    }

    // parts[size-1] is "lookup", parts[size-2] is serVersion, parts[size-3] is processorId
    const std::string& processor_id = parts[parts.size() - 3];
    if (processor_id != processor_factory_->Identifier()) {
        return std::nullopt;
    }

    const std::string& ser_version = parts[parts.size() - 2];
    return RemoteSstFile{name, ser_version};
}

template <typename T>
std::string LookupLevels<T>::NewRemoteSst(const std::shared_ptr<DataFileMeta>& file,
                                          int64_t length) const {
    return fmt::format("{}.{}.{}.{}{}", file->file_name, std::to_string(length),
                       processor_factory_->Identifier(), serializer_factory_->Version(),
                       REMOTE_LOOKUP_FILE_SUFFIX);
}

template <typename T>
void LookupLevels<T>::AddLocalFile(const std::shared_ptr<DataFileMeta>& file,
                                   const std::shared_ptr<LookupFile>& lookup_file) {
    lookup_file_cache_[file->file_name] = lookup_file;
}

template <typename T>
std::optional<std::string> LookupLevels<T>::TryToDownloadRemoteSst(
    const std::shared_ptr<DataFileMeta>& file, const std::string& local_file) {
    if (remote_lookup_file_manager_ == nullptr) {
        return std::nullopt;
    }
    auto remote_sst_file = RemoteSst(file);
    if (!remote_sst_file.has_value()) {
        return std::nullopt;
    }

    const auto& remote_sst = remote_sst_file.value();

    // Validate schema matched, no error status here
    auto processor_result = GetOrCreateProcessor(file->schema_id, remote_sst.ser_version);
    if (!processor_result.ok()) {
        return std::nullopt;
    }

    bool success =
        remote_lookup_file_manager_->TryToDownload(file, remote_sst.sst_file_name, local_file);
    if (!success) {
        return std::nullopt;
    }

    return remote_sst.ser_version;
}
template <typename T>
Status LookupLevels<T>::CreateSstFileFromDataFile(const std::shared_ptr<DataFileMeta>& file,
                                                  const std::string& kv_file_path) {
    if constexpr (std::is_same_v<T, bool>) {
        // Short-circuit logic: if T is bool, just write empty lookup file.
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<BloomFilter> bloom_filter,
            LookupStoreFactory::BfGenerator(file->row_count, options_, pool_.get()));
        PAIMON_ASSIGN_OR_RAISE(
            std::unique_ptr<LookupStoreWriter> kv_writer,
            lookup_store_factory_->CreateWriter(fs_, kv_file_path, bloom_filter, pool_));
        return kv_writer->Close();
    }
    // Prepare reader to iterate KeyValue
    PAIMON_ASSIGN_OR_RAISE(
        std::vector<std::unique_ptr<FileBatchReader>> raw_readers,
        split_read_->CreateRawFileReaders(partition_, {file}, read_schema_,
                                          /*predicate=*/nullptr, dv_factory_,
                                          /*row_ranges=*/std::nullopt, data_file_path_factory_));
    if (raw_readers.size() != 1) {
        return Status::Invalid("Unexpected, CreateSstFileFromDataFile only create single reader");
    }
    auto& raw_reader = raw_readers[0];
    auto reader = std::make_unique<KeyValueDataFileRecordReader>(std::move(raw_reader), key_schema_,
                                                                 value_schema_, file->level, pool_);

    // Create processor to persist value
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<PersistProcessor<T>> processor,
        GetOrCreateProcessor(table_schema_->Id(), serializer_factory_->Version()));

    // Prepare writer to write lookup file
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<BloomFilter> bloom_filter,
                           LookupStoreFactory::BfGenerator(file->row_count, options_, pool_.get()));
    PAIMON_ASSIGN_OR_RAISE(
        std::unique_ptr<LookupStoreWriter> kv_writer,
        lookup_store_factory_->CreateWriter(fs_, kv_file_path, bloom_filter, pool_));

    ScopeGuard write_guard([&]() -> void {
        [[maybe_unused]] auto status = kv_writer->Close();
        reader->Close();
        [[maybe_unused]] auto delete_status = fs_->Delete(kv_file_path, /*recursive=*/false);
    });

    // Read each KeyValue and write to lookup file with or without position.
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<KeyValueRecordReader::Iterator> iter,
                               reader->NextBatch());
        if (iter == nullptr) {
            break;
        }
        auto typed_iter = dynamic_cast<KeyValueDataFileRecordReader::Iterator*>(iter.get());
        assert(typed_iter);
        while (typed_iter->HasNext()) {
            std::pair<int64_t, KeyValue> kv_and_pos;
            PAIMON_ASSIGN_OR_RAISE(kv_and_pos, typed_iter->NextWithFilePos());
            const auto& [file_pos, kv] = kv_and_pos;
            PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> key_bytes,
                                   key_serializer_->SerializeToBytes(*(kv.key)));
            std::shared_ptr<Bytes> value_bytes;
            if (processor->WithPosition()) {
                PAIMON_ASSIGN_OR_RAISE(value_bytes, processor->PersistToDisk(kv, file_pos));
            } else {
                PAIMON_ASSIGN_OR_RAISE(value_bytes, processor->PersistToDisk(kv));
            }
            PAIMON_RETURN_NOT_OK(kv_writer->Put(std::move(key_bytes), std::move(value_bytes)));
        }
    }
    PAIMON_RETURN_NOT_OK(kv_writer->Close());
    write_guard.Release();
    return Status::OK();
}
template <typename T>
Result<std::shared_ptr<PersistProcessor<T>>> LookupLevels<T>::GetOrCreateProcessor(
    int64_t schema_id, const std::string& ser_version) {
    auto key = std::make_pair(schema_id, ser_version);
    auto iter = schema_id_and_ser_version_to_processors_.find(key);
    if (iter != schema_id_and_ser_version_to_processors_.end()) {
        return iter->second;
    }
    auto file_schema = value_schema_;
    if (table_schema_->Id() != schema_id) {
        PAIMON_ASSIGN_OR_RAISE(auto file_table_schema, schema_manager_->ReadSchema(schema_id));
        file_schema = DataField::ConvertDataFieldsToArrowSchema(file_table_schema->Fields());
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<PersistProcessor<T>> processor,
        processor_factory_->Create(ser_version, serializer_factory_, file_schema, pool_));
    schema_id_and_ser_version_to_processors_[key] = processor;
    return processor;
}

template class LookupLevels<KeyValue>;
template class LookupLevels<FilePosition>;
template class LookupLevels<PositionedKeyValue>;
template class LookupLevels<bool>;
}  // namespace paimon
