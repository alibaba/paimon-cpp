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
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/key_value.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {
class Metrics;

class LoserTree {
 public:
    using CompareFunc =
        std::function<int32_t(const std::optional<KeyValue>&, const std::optional<KeyValue>&)>;
    LoserTree(std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
              const CompareFunc& first_comparator, const CompareFunc& second_comparator);

    /// Initialize the loser tree in the same way as the regular loser tree.
    Status InitializeIfNeeded();

    /// Adjust the Key that needs to be returned in the next round.
    Status AdjustForNextLoop();

    /// Pop the current winner and update its state to `State#WINNER_POPPED`.
    std::optional<KeyValue> PopWinner();

    /// Peek the current winner, mainly for key comparisons.
    const std::optional<KeyValue>& PeekWinner() const;

    std::shared_ptr<Metrics> GetReaderMetrics() const {
        return MetricsImpl::CollectReadMetrics(readers_holder_);
    }

    void Close() {
        for (const auto& reader : readers_holder_) {
            reader->Close();
        }
    }

 private:
    struct LeafIterator;

    /// Adjust the winner from bottom to top. Using different `State`, we can quickly compare
    /// whether all the current same keys have been processed.
    void Adjust(int32_t winner);

    /// The winner node has the same userKey as the global winner.
    void AdjustWithSameWinnerKey(int32_t index, LeafIterator* parent_node,
                                 LeafIterator* winner_node);

    /// The userKey of the new local winner node is different from that of the previous global
    /// winner.
    void AdjustWithNewWinnerKey(int32_t index, LeafIterator* parent_node,
                                LeafIterator* winner_node);

 private:
    enum class State {
        LOSER_WITH_NEW_KEY = 1,
        LOSER_WITH_SAME_KEY = 2,
        LOSER_POPPED = 3,
        WINNER_WITH_NEW_KEY = 4,
        WINNER_WITH_SAME_KEY = 5,
        WINNER_POPPED = 6
    };

    static bool IsWinner(State state) {
        if (state == State::LOSER_WITH_NEW_KEY || state == State::LOSER_WITH_SAME_KEY ||
            state == State::LOSER_POPPED) {
            return false;
        }
        return true;
    }

    struct LeafIterator {
        explicit LeafIterator(KeyValueRecordReader* reader) : reader(reader) {}

        const std::optional<KeyValue>& Peek() const {
            return kv;
        }

        std::optional<KeyValue>&& Pop() {
            state = State::WINNER_POPPED;
            return std::move(kv);
        }

        void SetFirstSameKeyIndex(int32_t index) {
            if (first_same_key_index == -1) {
                first_same_key_index = index;
            }
        }

        /// Reads the next kv if any, otherwise returns null.
        Status AdvanceIfAvailable() {
            first_same_key_index = -1;
            state = State::WINNER_WITH_NEW_KEY;
            bool has_next = false;
            if (iterator != nullptr) {
                PAIMON_ASSIGN_OR_RAISE(has_next, iterator->HasNext());
            }
            if (iterator == nullptr || !has_next) {
                while (!end_of_input) {
                    PAIMON_ASSIGN_OR_RAISE(iterator, reader->NextBatch());
                    if (!iterator) {
                        // read eof
                        reader->Close();
                        end_of_input = true;
                        kv = std::nullopt;
                    } else {
                        PAIMON_ASSIGN_OR_RAISE(has_next, iterator->HasNext());
                        if (!has_next) {
                            continue;
                        }
                        PAIMON_ASSIGN_OR_RAISE(kv, iterator->Next());
                        break;
                    }
                }
            } else {
                PAIMON_ASSIGN_OR_RAISE(kv, iterator->Next());
            }
            return Status::OK();
        }

        bool end_of_input = false;
        int32_t first_same_key_index = -1;
        State state = State::WINNER_WITH_NEW_KEY;
        KeyValueRecordReader* reader;
        std::unique_ptr<KeyValueRecordReader::Iterator> iterator;
        std::optional<KeyValue> kv;
    };

 private:
    int32_t size_;
    bool initialized_;
    // must hold all readers, as data array is allocated by the pool of data file
    // reader
    std::vector<std::unique_ptr<KeyValueRecordReader>> readers_holder_;

    std::vector<int32_t> tree_;
    std::vector<LeafIterator> leaves_;
    /// if comparator.compare('a', 'b') > 0, then 'a' is the winner. In the following
    /// implementation, we always let 'a' represent the parent node.
    CompareFunc first_comparator_;
    /// same as first_comparator, but mainly used to compare sequenceNumber.
    CompareFunc second_comparator_;
};
}  // namespace paimon
