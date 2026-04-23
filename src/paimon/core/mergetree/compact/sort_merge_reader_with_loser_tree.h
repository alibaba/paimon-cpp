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
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/io/concat_key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/core/mergetree/compact/loser_tree.h"
#include "paimon/core/mergetree/compact/merge_function_wrapper.h"
#include "paimon/core/mergetree/compact/sort_merge_reader.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class FieldsComparator;
class KeyValueRecordReader;
class Metrics;

/// `SortMergeReader` implemented with min-heap. Merge the KeyValue parsed by
/// KeyValueDataFileRecordReader and return the iterator of KeyValue
class SortMergeReaderWithLoserTree : public SortMergeReader {
 public:
    SortMergeReaderWithLoserTree(
        std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
        const std::shared_ptr<FieldsComparator>& user_key_comparator,
        const std::shared_ptr<FieldsComparator>& user_defined_seq_comparator,
        const std::shared_ptr<MergeFunctionWrapper<KeyValue>>& merge_function_wrapper);

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return loser_tree_->GetReaderMetrics();
    }

    void Close() override {
        loser_tree_->Close();
    }

    class Iterator : public SortMergeReader::Iterator {
     public:
        explicit Iterator(SortMergeReaderWithLoserTree* reader) : reader_(reader) {}

        Result<bool> HasNext() override;

        KeyValue&& Next() override {
            return std::move(result_).value();
        }

     private:
        Status Merge() {
            while (reader_->loser_tree_->PeekWinner() != std::nullopt) {
                PAIMON_RETURN_NOT_OK(reader_->merge_function_wrapper_->Add(
                    std::move(reader_->loser_tree_->PopWinner().value())));
            }
            PAIMON_ASSIGN_OR_RAISE(result_, reader_->merge_function_wrapper_->GetResult());
            return Status::OK();
        }

     private:
        SortMergeReaderWithLoserTree* reader_;
        std::optional<KeyValue> result_;
    };

    Result<std::unique_ptr<SortMergeReader::Iterator>> NextBatch() override {
        PAIMON_RETURN_NOT_OK(loser_tree_->InitializeIfNeeded());
        return loser_tree_->PeekWinner() == std::nullopt
                   ? std::unique_ptr<SortMergeReader::Iterator>()
                   : std::make_unique<Iterator>(this);
    }

 private:
    std::unique_ptr<LoserTree> loser_tree_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
};
}  // namespace paimon
