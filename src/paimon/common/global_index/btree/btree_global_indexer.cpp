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
#include "paimon/common/global_index/btree/btree_global_indexer.h"

#include <memory>
#include <string>

#include "arrow/c/bridge.h"
#include "paimon/common/compression/block_compression_factory.h"
#include "paimon/common/global_index/btree/btree_file_footer.h"
#include "paimon/common/global_index/btree/btree_global_index_writer.h"
#include "paimon/common/global_index/btree/btree_index_meta.h"
#include "paimon/common/global_index/btree/key_serializer.h"
#include "paimon/common/global_index/btree/lazy_filtered_btree_reader.h"
#include "paimon/common/memory/memory_slice.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/options/memory_size.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/crc32c.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/preconditions.h"
#include "paimon/core/options/compress_options.h"
#include "paimon/executor.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/memory/bytes.h"
#include "paimon/utils/roaring_bitmap64.h"
namespace paimon {
Result<std::unique_ptr<BTreeGlobalIndexer>> BTreeGlobalIndexer::Create(
    const std::map<std::string, std::string>& options) {
    // parse cache options
    PAIMON_ASSIGN_OR_RAISE(std::string cache_size_str, OptionsUtils::GetValueFromMap<std::string>(
                                                           options, BtreeDefs::kBtreeIndexCacheSize,
                                                           BtreeDefs::kDefaultBtreeIndexCacheSize));
    PAIMON_ASSIGN_OR_RAISE(int64_t cache_size, MemorySize::ParseBytes(cache_size_str));

    PAIMON_ASSIGN_OR_RAISE(
        double high_priority_pool_ratio,
        OptionsUtils::GetValueFromMap<double>(options, BtreeDefs::kBtreeIndexHighPriorityPoolRatio,
                                              BtreeDefs::kDefaultBtreeIndexHighPriorityPoolRatio));
    auto cache_manager = std::make_shared<CacheManager>(cache_size, high_priority_pool_ratio);
    return std::unique_ptr<BTreeGlobalIndexer>(new BTreeGlobalIndexer(cache_manager, options));
}

Result<std::shared_ptr<GlobalIndexWriter>> BTreeGlobalIndexer::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::DataType> arrow_type,
                                      arrow::ImportType(arrow_schema));
    // check data type
    auto struct_type = std::dynamic_pointer_cast<arrow::StructType>(arrow_type);
    PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
        struct_type, "arrow schema must be struct type when create BTreeGlobalIndexWriter"));

    // parse options
    PAIMON_ASSIGN_OR_RAISE(
        std::string block_size_str,
        OptionsUtils::GetValueFromMap<std::string>(options_, BtreeDefs::kBtreeIndexBlockSize,
                                                   BtreeDefs::kDefaultBtreeIndexBlockSize));
    PAIMON_ASSIGN_OR_RAISE(int64_t block_size, MemorySize::ParseBytes(block_size_str));
    if (block_size > INT32_MAX) {
        return Status::Invalid("invalid block size, exceed INT32_MAX");
    }
    PAIMON_ASSIGN_OR_RAISE(
        std::string compress_str,
        OptionsUtils::GetValueFromMap<std::string>(options_, BtreeDefs::kBtreeIndexCompression,
                                                   BtreeDefs::kDefaultBtreeIndexCompression));
    PAIMON_ASSIGN_OR_RAISE(
        int32_t compress_level,
        OptionsUtils::GetValueFromMap<int32_t>(options_, BtreeDefs::kBtreeIndexCompressionLevel,
                                               BtreeDefs::kDefaultBtreeIndexCompressionLevel));
    CompressOptions compress_options{compress_str, compress_level};
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::BlockCompressionFactory> compression_factory,
                           BlockCompressionFactory::Create(compress_options));
    return BTreeGlobalIndexWriter::Create(field_name, struct_type, file_writer,
                                          static_cast<int32_t>(block_size), compression_factory,
                                          pool);
}

Result<std::shared_ptr<GlobalIndexReader>> BTreeGlobalIndexer::CreateReader(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    // Get field type from arrow schema
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Schema> schema,
                                      arrow::ImportSchema(arrow_schema));
    if (schema->num_fields() != 1) {
        return Status::Invalid(
            "invalid schema for BTreeGlobalIndexReader, supposed to have single field.");
    }
    auto key_type = schema->field(0)->type();

    std::optional<int32_t> read_buffer_size;
    if (auto iter = options_.find(BtreeDefs::kBtreeIndexReadBufferSize); iter != options_.end()) {
        PAIMON_ASSIGN_OR_RAISE(int64_t tmp_buffer_size, MemorySize::ParseBytes(iter->second));
        if (tmp_buffer_size <= 0 || tmp_buffer_size > INT_MAX) {
            return Status::Invalid(
                fmt::format("In BTreeGlobalIndexer::CreateReader: option {} is {}, exceed INT_MAX "
                            "or less than 0",
                            BtreeDefs::kBtreeIndexReadBufferSize, iter->second));
        }
        read_buffer_size = static_cast<int32_t>(tmp_buffer_size);
    }
    // TODO(lisizhuo.lsz): Allow users to specify an executor
    std::shared_ptr<Executor> executor = CreateDefaultExecutor();
    return std::make_shared<LazyFilteredBTreeReader>(read_buffer_size, files, key_type, file_reader,
                                                     cache_manager_, pool, executor);
}

}  // namespace paimon
