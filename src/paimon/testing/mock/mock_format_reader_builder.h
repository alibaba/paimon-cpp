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

#pragma once

#include <memory>
#include <string>

#include "arrow/type.h"
#include "paimon/format/reader_builder.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/type_fwd.h"

namespace paimon::test {

class MockFormatReaderBuilder : public ReaderBuilder {
 public:
    MockFormatReaderBuilder(const std::shared_ptr<arrow::Array>& data,
                            const std::shared_ptr<arrow::DataType>& schema, int32_t read_batch_size)
        : MockFormatReaderBuilder(data, schema, std::nullopt, read_batch_size) {}

    MockFormatReaderBuilder(const std::shared_ptr<arrow::Array>& data,
                            const std::shared_ptr<arrow::DataType>& schema,
                            const std::optional<RoaringBitmap32>& bitmap, int32_t read_batch_size)
        : data_(data), schema_(schema), bitmap_(bitmap), read_batch_size_(read_batch_size) {}

    ~MockFormatReaderBuilder() override = default;

    ReaderBuilder* WithMemoryPool(const std::shared_ptr<MemoryPool>& pool) override {
        return this;
    }

    Result<std::unique_ptr<FileBatchReader>> Build(
        const std::shared_ptr<InputStream>& path) const override {
        if (bitmap_) {
            return std::make_unique<MockFileBatchReader>(data_, schema_, bitmap_.value(),
                                                         read_batch_size_);
        }
        return std::make_unique<MockFileBatchReader>(data_, schema_, read_batch_size_);
    }

    Result<std::unique_ptr<FileBatchReader>> Build(const std::string& path) const override {
        return Status::Invalid("do not support build reader with path in mock format");
    }

 private:
    std::shared_ptr<arrow::Array> data_;
    std::shared_ptr<arrow::DataType> schema_;
    std::optional<RoaringBitmap32> bitmap_;
    int32_t read_batch_size_;
};

}  // namespace paimon::test
