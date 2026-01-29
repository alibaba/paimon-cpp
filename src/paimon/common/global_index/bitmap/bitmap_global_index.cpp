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
#include "paimon/common/global_index/bitmap/bitmap_global_index.h"

#include "paimon/common/global_index/wrap/file_index_reader_wrapper.h"
#include "paimon/common/global_index/wrap/file_index_writer_wrapper.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {
Result<std::shared_ptr<GlobalIndexWriter>> BitmapGlobalIndex::CreateWriter(
    const std::string& field_name, ::ArrowSchema* arrow_schema,
    const std::shared_ptr<GlobalIndexFileWriter>& file_writer,
    const std::shared_ptr<MemoryPool>& pool) const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FileIndexWriter> writer,
                           index_->CreateWriter(arrow_schema, pool));
    return std::make_shared<FileIndexWriterWrapper>(
        /*index_type=*/"bitmap", file_writer, writer);
}

Result<std::shared_ptr<GlobalIndexReader>> BitmapGlobalIndex::CreateReader(
    ::ArrowSchema* arrow_schema, const std::shared_ptr<GlobalIndexFileReader>& file_reader,
    const std::vector<GlobalIndexIOMeta>& files, const std::shared_ptr<MemoryPool>& pool) const {
    if (files.size() != 1) {
        return Status::Invalid(
            "invalid GlobalIndexIOMeta for BitmapGlobalIndex, exist multiple metas");
    }
    const auto& meta = files[0];
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> in,
                           file_reader->GetInputStream(meta.file_path));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<FileIndexReader> reader,
        index_->CreateReader(arrow_schema, /*start=*/0, meta.file_size, in, pool));
    auto transform = [range_end = meta.range_end](const std::shared_ptr<FileIndexResult>& result)
        -> Result<std::shared_ptr<GlobalIndexResult>> {
        return ToGlobalIndexResult(range_end, result);
    };
    return std::make_shared<BitmapGlobalIndexReader>(reader, transform);
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapGlobalIndex::ToGlobalIndexResult(
    int64_t range_end, const std::shared_ptr<FileIndexResult>& result) {
    if (auto remain = std::dynamic_pointer_cast<Remain>(result)) {
        return std::make_shared<BitmapGlobalIndexResult>([range_end]() -> Result<RoaringBitmap64> {
            RoaringBitmap64 bitmap;
            bitmap.AddRange(0, range_end + 1);
            return bitmap;
        });
    } else if (auto skip = std::dynamic_pointer_cast<Skip>(result)) {
        return std::make_shared<BitmapGlobalIndexResult>(
            []() -> Result<RoaringBitmap64> { return RoaringBitmap64(); });
    } else if (auto bitmap_result = std::dynamic_pointer_cast<BitmapIndexResult>(result)) {
        return std::make_shared<BitmapGlobalIndexResult>(
            [bitmap_result]() -> Result<RoaringBitmap64> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, bitmap_result->GetBitmap());
                return RoaringBitmap64(*bitmap);
            });
    }
    return Status::Invalid(
        "invalid FileIndexResult, supposed to be Remain or Skip or BitmapIndexResult");
}

}  // namespace paimon
