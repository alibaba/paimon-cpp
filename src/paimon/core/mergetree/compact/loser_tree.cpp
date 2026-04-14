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

#include "paimon/core/mergetree/compact/loser_tree.h"

#include <algorithm>
#include <cassert>

namespace paimon {
LoserTree::LoserTree(std::vector<std::unique_ptr<KeyValueRecordReader>>&& readers,
                     const CompareFunc& first_comparator, const CompareFunc& second_comparator)
    : size_(readers.size()),
      initialized_(false),
      readers_holder_(std::move(readers)),
      tree_(size_),
      first_comparator_(first_comparator),
      second_comparator_(second_comparator) {
    leaves_.reserve(size_);
    for (const auto& reader : readers_holder_) {
        leaves_.emplace_back(reader.get());
    }
}

Status LoserTree::InitializeIfNeeded() {
    if (!initialized_) {
        std::fill(tree_.begin(), tree_.end(), -1);
        for (int32_t i = size_ - 1; i >= 0; i--) {
            PAIMON_RETURN_NOT_OK(leaves_[i].AdvanceIfAvailable());
            Adjust(i);
        }
        initialized_ = true;
    }
    return Status::OK();
}

Status LoserTree::AdjustForNextLoop() {
    LeafIterator* winner = &leaves_[tree_[0]];
    while (winner->state == State::WINNER_POPPED) {
        PAIMON_RETURN_NOT_OK(winner->AdvanceIfAvailable());
        Adjust(tree_[0]);
        winner = &leaves_[tree_[0]];
    }
    return Status::OK();
}

std::optional<KeyValue> LoserTree::PopWinner() {
    LeafIterator* winner = &leaves_[tree_[0]];
    if (winner->state == State::WINNER_POPPED) {
        // if the winner has already been popped, it means that all the same key has been
        // processed.
        return std::nullopt;
    }
    std::optional<KeyValue> result = std::move(winner->Pop());
    Adjust(tree_[0]);
    return result;
}

const std::optional<KeyValue>& LoserTree::PeekWinner() const {
    static const std::optional<KeyValue> empty_kv = std::nullopt;
    return leaves_[tree_[0]].state != State::WINNER_POPPED ? leaves_[tree_[0]].Peek() : empty_kv;
}

void LoserTree::Adjust(int32_t winner) {
    for (int32_t parent = (winner + size_) / 2; parent > 0 && winner >= 0; parent /= 2) {
        LeafIterator* winner_node = &leaves_[winner];
        LeafIterator* parent_node = nullptr;

        if (tree_[parent] == -1) {
            // initialize the tree.
            winner_node->state = State::LOSER_WITH_NEW_KEY;
        } else {
            parent_node = &leaves_[tree_[parent]];
            switch (winner_node->state) {
                case State::WINNER_WITH_NEW_KEY: {
                    AdjustWithNewWinnerKey(parent, parent_node, winner_node);
                    break;
                }
                case State::WINNER_WITH_SAME_KEY: {
                    AdjustWithSameWinnerKey(parent, parent_node, winner_node);
                    break;
                }
                case State::WINNER_POPPED: {
                    if (winner_node->first_same_key_index < 0) {
                        // fast path, which means that the same key is not yet processed in the
                        // current tree.
                        parent = -1;
                    } else {
                        // fast path. Directly exchange positions with the same key that has not
                        // yet been processed, no need to compare level by level.
                        parent = winner_node->first_same_key_index;
                        parent_node = &leaves_[tree_[parent]];
                        winner_node->state = State::LOSER_POPPED;
                        parent_node->state = State::WINNER_WITH_SAME_KEY;
                    }
                    break;
                }
                default:
                    assert(false);
            }
        }

        // if the winner loses, exchange nodes.
        if (!IsWinner(winner_node->state)) {
            std::swap(winner, tree_[parent]);
        }
    }
    tree_[0] = winner;
}

void LoserTree::AdjustWithSameWinnerKey(int32_t index, LeafIterator* parent_node,
                                        LeafIterator* winner_node) {
    switch (parent_node->state) {
        case State::LOSER_WITH_SAME_KEY: {
            // the key of the previous loser is the same as the key of the current winner,
            // only the sequence needs to be compared.
            const auto& parent_key = parent_node->Peek();
            const auto& child_key = winner_node->Peek();
            int32_t second_result = second_comparator_(parent_key, child_key);
            if (second_result > 0) {
                parent_node->state = State::WINNER_WITH_SAME_KEY;
                winner_node->state = State::LOSER_WITH_SAME_KEY;
                parent_node->SetFirstSameKeyIndex(index);
            } else {
                winner_node->SetFirstSameKeyIndex(index);
            }
            return;
        }
        case State::LOSER_WITH_NEW_KEY:
        case State::LOSER_POPPED:
            return;
        default:
            assert(false);
    }
}

void LoserTree::AdjustWithNewWinnerKey(int32_t index, LeafIterator* parent_node,
                                       LeafIterator* winner_node) {
    switch (parent_node->state) {
        case State::LOSER_WITH_NEW_KEY: {
            // when the new winner is also a new key, it needs to be compared.
            const auto& parent_key = parent_node->Peek();
            const auto& child_key = winner_node->Peek();
            int32_t first_result = first_comparator_(parent_key, child_key);
            if (first_result == 0) {
                // if the compared keys are the same, we need to update the state of the node
                // and record the index of the same key for the winner.
                int32_t second_result = second_comparator_(parent_key, child_key);
                if (second_result < 0) {
                    parent_node->state = State::LOSER_WITH_SAME_KEY;
                    winner_node->SetFirstSameKeyIndex(index);
                } else {
                    winner_node->state = State::LOSER_WITH_SAME_KEY;
                    parent_node->state = State::WINNER_WITH_NEW_KEY;
                    parent_node->SetFirstSameKeyIndex(index);
                }
            } else if (first_result > 0) {
                // the two keys are completely different and just need to update the state.
                parent_node->state = State::WINNER_WITH_NEW_KEY;
                winner_node->state = State::LOSER_WITH_NEW_KEY;
            }
            return;
        }
        case State::LOSER_WITH_SAME_KEY: {
            // A node in the WINNER_WITH_NEW_KEY state cannot encounter a node in the
            // LOSER_WITH_SAME_KEY state.
            assert(false);
            break;
        }
        case State::LOSER_POPPED: {
            // this case will only happen during adjustForNextLoop.
            parent_node->state = State::WINNER_POPPED;
            parent_node->first_same_key_index = -1;
            winner_node->state = State::LOSER_WITH_NEW_KEY;
            return;
        }
        default:
            assert(false);
    }
}

}  // namespace paimon
