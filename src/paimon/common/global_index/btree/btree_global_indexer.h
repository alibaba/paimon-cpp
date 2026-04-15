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

#include <memory>
#include <string>
#include <vector>

#include "paimon/common/global_index/btree/btree_global_index_reader.h"
#include "paimon/common/sst/block_cache.h"
#include "paimon/common/sst/block_handle.h"
#include "paimon/file_index/file_index_result.h"
#include "paimon/global_index/global_indexer.h"
#include "paimon/global_index/io/global_index_file_reader.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
class BTreeGlobalIndexer : public GlobalIndexer {
 public:
    explicit BTreeGlobalIndexer(const std::map<std::string, std::string>& options)
        : options_(options) {}

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    static Result<std::shared_ptr<GlobalIndexResult>> ToGlobalIndexResult(
        int64_t range_end, const std::shared_ptr<FileIndexResult>& result);

    static Result<std::shared_ptr<RoaringBitmap64>> ReadNullBitmap(
        const std::shared_ptr<BlockCache>& cache, const std::shared_ptr<BlockHandle>& block_handle);

 private:
    std::map<std::string, std::string> options_;
};

}  // namespace paimon
