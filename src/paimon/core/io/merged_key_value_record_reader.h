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
#include <optional>

#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"

namespace paimon {
class FieldsComparator;

class MergedKeyValueRecordReader : public KeyValueRecordReader {
 public:
    MergedKeyValueRecordReader(
        std::unique_ptr<KeyValueRecordReader>&& reader,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper);

    class Iterator : public KeyValueRecordReader::Iterator {
     public:
        static Result<std::unique_ptr<Iterator>> Create(MergedKeyValueRecordReader* reader);

        bool HasNext() const override {
            return next_key_value_.has_value();
        }

        Result<KeyValue> Next() override;

     private:
        explicit Iterator(MergedKeyValueRecordReader* reader) : reader_(reader) {}

        Status LoadNextKeyValue();

     private:
        MergedKeyValueRecordReader* reader_;
        std::unique_ptr<KeyValueRecordReader::Iterator> current_iterator_;
        std::optional<KeyValue> next_key_value_;
    };

    Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() override;

    std::shared_ptr<Metrics> GetReaderMetrics() const override;

    void Close() override;

 private:
    bool visited_ = false;
    std::unique_ptr<KeyValueRecordReader> reader_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
};
}  // namespace paimon
