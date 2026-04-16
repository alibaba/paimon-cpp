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

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/concat_key_value_record_reader.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/result.h"

namespace paimon {
class Metrics;
template <typename T>
class MergeFunctionWrapper;

/// `SortMergeReader` implemented with min-heap. Merge the KeyValue parsed by
/// KeyValueDataFileRecordReader and return the iterator of KeyValue
class SortMergeReaderWithMinHeap : public SortMergeReader {
 public:
    SortMergeReaderWithMinHeap(
        std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
        const std::shared_ptr<FieldsComparator>& user_key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper);

    class Iterator : public SortMergeReader::Iterator {
     public:
        explicit Iterator(SortMergeReaderWithMinHeap* reader) : reader_(reader) {}
        Result<bool> HasNext() override;
        KeyValue&& Next() override {
            return std::move(result_).value();
        }

     private:
        Result<bool> NextImpl();

     private:
        SortMergeReaderWithMinHeap* reader_;
        std::optional<KeyValue> result_;
    };

    Result<std::unique_ptr<SortMergeReader::Iterator>> NextBatch() override;

    void Close() override {
        for (const auto& reader : readers_holder_) {
            reader->Close();
        }
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return MetricsImpl::CollectReadMetrics(readers_holder_);
    }

 private:
    struct Element {
        Element(KeyValue&& _kv, std::unique_ptr<KeyValueRecordReader::Iterator>&& _iterator,
                KeyValueRecordReader* _reader)
            : kv(std::move(_kv)), reader(_reader), iterator(std::move(_iterator)) {
            assert(iterator);
            assert(reader);
        }

        Element(Element&& other) noexcept : kv(std::move(other.kv)) {
            iterator = std::move(other.iterator);
            reader = other.reader;
        }

        Element& operator=(Element&& other) noexcept {
            if (&other == this) {
                return *this;
            }
            kv = std::move(other.kv);
            iterator = std::move(other.iterator);
            reader = other.reader;
            return *this;
        }

        Result<bool> Update() {
            if (!iterator->HasNext()) {
                return false;
            }
            PAIMON_ASSIGN_OR_RAISE(KeyValue tmp_kv, iterator->Next());
            kv = std::move(tmp_kv);
            return true;
        }

     public:
        KeyValue kv;
        KeyValueRecordReader* reader;
        std::unique_ptr<KeyValueRecordReader::Iterator> iterator;
    };

    class HeapSorter {
     public:
        HeapSorter(const std::shared_ptr<FieldsComparator>& key_comparator,
                   const std::shared_ptr<FieldsComparator>& seq_comparator)
            : key_comparator_(key_comparator), seq_comparator_(seq_comparator) {
            assert(key_comparator_);
        }
        bool operator()(const Element& lhs, const Element& rhs) const {
            int32_t result = key_comparator_->CompareTo(*(lhs.kv.key), *(rhs.kv.key));
            if (result != 0) {
                return result > 0;
            }
            if (seq_comparator_ != nullptr) {
                int32_t seq_result = seq_comparator_->CompareTo(*(lhs.kv.value), *(rhs.kv.value));
                if (seq_result != 0) {
                    return seq_result > 0;
                }
            }
            return lhs.kv.sequence_number > rhs.kv.sequence_number;
        }

     private:
        std::shared_ptr<FieldsComparator> key_comparator_;
        std::shared_ptr<FieldsComparator> seq_comparator_;
    };

 private:
    // must hold all readers, as data array is allocated by the pool of data file reader
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers_holder_;
    std::vector<KeyValueRecordReader*> next_batch_readers_;
    std::shared_ptr<FieldsComparator> user_key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
    std::priority_queue<Element, std::vector<Element>, HeapSorter> min_heap_;
    std::vector<Element> polled_;
};
}  // namespace paimon
