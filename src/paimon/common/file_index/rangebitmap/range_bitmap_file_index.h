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

#include <map>
#include <memory>
#include <string>

#include "paimon/common/file_index/bitmap/bitmap_file_index.h"
#include "paimon/common/file_index/rangebitmap/range_bitmap.h"
#include "paimon/file_index/file_index_reader.h"
#include "paimon/file_index/file_index_writer.h"
#include "paimon/file_index/file_indexer.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class RangeBitmapFileIndexWriter;
class RangeBitmapFileIndexReader;

class RangeBitmapFileIndex final : public FileIndexer {
 public:
    explicit RangeBitmapFileIndex(const std::map<std::string, std::string>& options);

    ~RangeBitmapFileIndex() override = default;

    Result<std::shared_ptr<FileIndexReader>> CreateReader(
        ::ArrowSchema* arrow_schema, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input_stream,
        const std::shared_ptr<MemoryPool>& pool) const override;

    Result<std::shared_ptr<FileIndexWriter>> CreateWriter(
        ::ArrowSchema* arrow_schema, const std::shared_ptr<MemoryPool>& pool) const override;

    static constexpr char CHUNK_SIZE[] = "chunk-size";

 private:
    std::map<std::string, std::string> options_;
};

class RangeBitmapFileIndexWriter final : public FileIndexWriter {
 public:
    static constexpr char DEFAULT_CHUNK_SIZE[] = "16kb";

    static Result<std::shared_ptr<RangeBitmapFileIndexWriter>> Create(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const std::string& field_name,
        const std::map<std::string, std::string>& options, const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(::ArrowArray* batch) override;
    Result<PAIMON_UNIQUE_PTR<Bytes>> SerializedBytes() const override;

    RangeBitmapFileIndexWriter(const std::shared_ptr<arrow::DataType>& arrow_type,
                               const std::map<std::string, std::string>& options,
                               const std::shared_ptr<MemoryPool>& pool,
                               const std::shared_ptr<KeyFactory>& key_factory,
                               std::unique_ptr<RangeBitmap::Appender> appender);

 private:
    std::shared_ptr<arrow::DataType> arrow_type_;
    std::map<std::string, std::string> options_;
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<KeyFactory> key_factory_;
    std::unique_ptr<RangeBitmap::Appender> appender_;
};

class RangeBitmapFileIndexReader final
    : public FileIndexReader,
      public std::enable_shared_from_this<RangeBitmapFileIndexReader> {
 public:
    static Result<std::shared_ptr<RangeBitmapFileIndexReader>> Create(
        const std::shared_ptr<arrow::DataType>& arrow_type, int32_t start, int32_t length,
        const std::shared_ptr<InputStream>& input_stream, const std::shared_ptr<MemoryPool>& pool);

 private:
    explicit RangeBitmapFileIndexReader(std::unique_ptr<RangeBitmap> range_bitmap);

    Result<std::shared_ptr<FileIndexResult>> VisitEqual(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitNotEqual(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitIn(const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<FileIndexResult>> VisitNotIn(
        const std::vector<Literal>& literals) override;
    Result<std::shared_ptr<FileIndexResult>> VisitIsNull() override;
    Result<std::shared_ptr<FileIndexResult>> VisitIsNotNull() override;
    Result<std::shared_ptr<FileIndexResult>> VisitGreaterThan(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitLessThan(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitGreaterOrEqual(const Literal& literal) override;
    Result<std::shared_ptr<FileIndexResult>> VisitLessOrEqual(const Literal& literal) override;

    std::unique_ptr<RangeBitmap> range_bitmap_;
};

}  // namespace paimon
