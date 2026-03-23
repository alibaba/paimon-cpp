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
#include "arrow/api.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/async_key_value_producer_and_consumer.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/rolling_file_writer.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/compact_rewriter.h"
#include "paimon/core/mergetree/merge_tree_writer.h"
#include "paimon/core/operation/merge_file_split_read.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/core/utils/file_store_path_factory_cache.h"
namespace paimon {
/// Default `CompactRewriter` for merge trees.
class MergeTreeCompactRewriter : public CompactRewriter {
 public:
    using MergeFunctionWrapperFactory =
        std::function<Result<std::shared_ptr<MergeFunctionWrapper<KeyValue>>>(int32_t)>;

    static Result<std::unique_ptr<MergeTreeCompactRewriter>> Create(
        int32_t bucket, const BinaryRow& partition,
        const std::shared_ptr<TableSchema>& table_schema, DeletionVector::Factory dv_factory,
        const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
        const CoreOptions& options, const std::shared_ptr<MemoryPool>& memory_pool);

    Result<CompactResult> Rewrite(int32_t output_level, bool drop_delete,
                                  const std::vector<std::vector<SortedRun>>& sections) override;

    Result<CompactResult> Upgrade(int32_t output_level,
                                  const std::shared_ptr<DataFileMeta>& file) override;

    Status Close() override {
        return Status::OK();
    }

 protected:
    Result<CompactResult> RewriteCompaction(int32_t output_level, bool drop_delete,
                                            const std::vector<std::vector<SortedRun>>& sections);

    virtual void NotifyRewriteCompactBefore(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) {}

    virtual std::vector<std::shared_ptr<DataFileMeta>> NotifyRewriteCompactAfter(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) {
        return files;
    }

    static std::vector<std::shared_ptr<DataFileMeta>> ExtractFilesFromSections(
        const std::vector<std::vector<SortedRun>>& sections);

    MergeTreeCompactRewriter(const BinaryRow& partition, int32_t bucket, int64_t schema_id,
                             const std::vector<std::string>& trimmed_primary_keys,
                             const CoreOptions& options,
                             const std::shared_ptr<arrow::Schema>& data_schema,
                             const std::shared_ptr<arrow::Schema>& write_schema,
                             DeletionVector::Factory dv_factory,
                             const std::shared_ptr<FileStorePathFactoryCache>& path_factory_cache,
                             std::unique_ptr<MergeFileSplitRead>&& merge_file_split_read,
                             MergeFunctionWrapperFactory merge_function_wrapper_factory,
                             const std::shared_ptr<MemoryPool>& pool);

    using KeyValueRollingFileWriter =
        RollingFileWriter<KeyValueBatch, std::shared_ptr<DataFileMeta>>;
    using KeyValueMergeReader = AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>;
    using KeyValueConsumerCreator =
        AsyncKeyValueProducerAndConsumer<KeyValue, KeyValueBatch>::ConsumerCreator;

    std::unique_ptr<KeyValueRollingFileWriter> CreateRollingRowWriter(int32_t level);

    Result<KeyValueConsumerCreator> GenerateKeyValueConsumer() const;

    Status MergeReadAndWrite(int32_t output_level, bool drop_delete,
                             const std::vector<SortedRun>& section,
                             const KeyValueConsumerCreator& create_consumer,
                             KeyValueRollingFileWriter* rolling_writer,
                             std::vector<std::shared_ptr<KeyValueMergeReader>>* reader_holders_ptr);

 protected:
    CoreOptions options_;
    std::unique_ptr<MergeFileSplitRead> merge_file_split_read_;

 private:
    Result<std::shared_ptr<DataFilePathFactory>> CreateDataFilePathFactory(
        const std::string& format);

 private:
    std::shared_ptr<MemoryPool> pool_;
    BinaryRow partition_;
    int32_t bucket_;
    int64_t schema_id_;
    std::vector<std::string> trimmed_primary_keys_;
    // all data fields in table schema
    std::shared_ptr<arrow::Schema> data_schema_;
    // SequenceNumber + ValueKind + data_schema_
    std::shared_ptr<arrow::Schema> write_schema_;
    DeletionVector::Factory dv_factory_;
    std::shared_ptr<FileStorePathFactoryCache> path_factory_cache_;
    MergeFunctionWrapperFactory merge_function_wrapper_factory_;
};

}  // namespace paimon
