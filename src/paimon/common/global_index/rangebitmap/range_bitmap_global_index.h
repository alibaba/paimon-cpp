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

#include "paimon/common/file_index/rangebitmap/range_bitmap_file_index.h"
#include "paimon/common/global_index/wrap/file_index_reader_wrapper.h"
#include "paimon/global_index/global_indexer.h"

namespace paimon {
class RangeBitmapGlobalIndex : public GlobalIndexer {
 public:
    explicit RangeBitmapGlobalIndex(const std::shared_ptr<RangeBitmapFileIndex>& index)
        : index_(index) {}

    Result<std::shared_ptr<GlobalIndexWriter>> CreateWriter(
        const std::string& field_name, ::ArrowSchema* arrow_schema,
        const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<GlobalIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
        const std::vector<GlobalIndexIOMeta>& files,
        const std::shared_ptr<MemoryPool>& pool) const override;

 private:
    std::shared_ptr<RangeBitmapFileIndex> index_;
};

class RangeBitmapGlobalIndexReader : public FileIndexReaderWrapper {
 public:
    RangeBitmapGlobalIndexReader(const std::shared_ptr<FileIndexReader>& reader,
                                 const std::function<Result<std::shared_ptr<GlobalIndexResult>>(
                                     const std::shared_ptr<FileIndexResult>&)>& transform)
        : FileIndexReaderWrapper(reader, transform) {}

    static inline const char kIdentifier[] = "range-bitmap";

    bool IsThreadSafe() const override {
        return false;
    }

    std::string GetIndexType() const override {
        return kIdentifier;
    }
};

}  // namespace paimon
