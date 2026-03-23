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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "fmt/core.h"
#include "fmt/format.h"

namespace paimon {
/// Indicates the deletion vector info of member data_file_name, e.g., the length of dv.
/// * DeletionVectorMeta is used when serialize to manifest file.
class DeletionVectorMeta {
 public:
    static const std::shared_ptr<arrow::DataType>& DataType() {
        static std::shared_ptr<arrow::DataType> schema = arrow::struct_(
            {arrow::field("f0", arrow::utf8(), false), arrow::field("f1", arrow::int32(), false),
             arrow::field("f2", arrow::int32(), false),
             arrow::field("_CARDINALITY", arrow::int64(), true)});
        return schema;
    }
    DeletionVectorMeta(const std::string& data_file_name, int32_t offset, int32_t length,
                       const std::optional<int64_t>& cardinality)
        : data_file_name_(data_file_name),
          offset_(offset),
          length_(length),
          cardinality_(cardinality) {}

    bool operator==(const DeletionVectorMeta& other) const {
        if (this == &other) {
            return true;
        }
        return data_file_name_ == other.data_file_name_ && offset_ == other.offset_ &&
               length_ == other.length_ && cardinality_ == other.cardinality_;
    }

    bool TEST_Equal(const DeletionVectorMeta& other) const {
        if (this == &other) {
            return true;
        }
        // ignore data_file_name
        return offset_ == other.offset_ && length_ == other.length_ &&
               cardinality_ == other.cardinality_;
    }

    std::string ToString() const {
        return fmt::format(
            "DeletionVectorMeta{{data_file_name = {}, offset = {}, length = {}, cardinality = {}}}",
            data_file_name_, offset_, length_,
            cardinality_ == std::nullopt ? "null" : std::to_string(cardinality_.value()));
    }

    const std::string& GetDataFileName() const {
        return data_file_name_;
    }

    int32_t GetOffset() const {
        return offset_;
    }

    int32_t GetLength() const {
        return length_;
    }

    std::optional<int64_t> GetCardinality() const {
        return cardinality_;
    }

 private:
    std::string data_file_name_;
    int32_t offset_;
    int32_t length_;
    std::optional<int64_t> cardinality_;
};
}  // namespace paimon
