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

/// Merges consecutive key-value records with the same primary key from the wrapped reader.
/// The wrapped reader must return records ordered by primary key, so all records for the same
/// primary key are contiguous.
class MergedKeyValueRecordReader : public KeyValueRecordReader {
 public:
    MergedKeyValueRecordReader(
        std::unique_ptr<KeyValueRecordReader>&& reader,
        const std::shared_ptr<FieldsComparator>& key_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper);

    class Iterator : public KeyValueRecordReader::Iterator {
     public:
        explicit Iterator(MergedKeyValueRecordReader* reader) : reader_(reader) {}

        Result<bool> HasNext() const override;

        Result<KeyValue> Next() override;

     private:
        Result<bool> PrepareNextMergedKeyValue() const;
        Result<bool> MergeNextKey() const;
        Status LoadNextRawKeyValue() const;

     private:
        MergedKeyValueRecordReader* reader_;
        mutable std::unique_ptr<KeyValueRecordReader::Iterator> current_iterator_;
        // Lookahead raw kv used to detect the boundary between two keys.
        mutable std::optional<KeyValue> next_raw_key_value_;
        // Merged kv prepared by HasNext() and consumed by Next().
        mutable std::optional<KeyValue> merged_key_value_;
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
