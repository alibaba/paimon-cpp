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
#include <utility>

#include "paimon/core/io/key_value_data_file_record_reader.h"

namespace paimon::test {
// mock reader hold data array
class MockKeyValueDataFileRecordReader : public KeyValueDataFileRecordReader {
 public:
    MockKeyValueDataFileRecordReader(std::unique_ptr<BatchReader>&& reader, int32_t key_arity,
                                     const std::shared_ptr<arrow::Schema>& value_schema,
                                     int32_t level, const std::shared_ptr<MemoryPool>& pool)
        : KeyValueDataFileRecordReader(std::move(reader), key_arity, value_schema, level, pool) {}

    void Reset() override {
        for (const auto& field : key_fields_) {
            data_holder_.push_back(field);
        }
        for (const auto& field : value_fields_) {
            data_holder_.push_back(field);
        }
        KeyValueDataFileRecordReader::Reset();
    }

 private:
    arrow::ArrayVector data_holder_;
};

}  // namespace paimon::test
