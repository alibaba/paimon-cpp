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

#include <atomic>
#include <string>
#include <unordered_map>
#include <utility>

#include "paimon/memory/memory_pool.h"

namespace paimon {
class MemoryPoolAdaptorSlot {
 public:
    explicit MemoryPoolAdaptorSlot(MemoryPool::AdaptorCreator creator)
        : creator_(std::move(creator)) {}

    void* GetOrCreateAdaptor(MemoryPool& pool) {
        std::call_once(once_flag_, [this, &pool] { ptr_ = creator_(pool); });
        return ptr_.get();
    }

 private:
    MemoryPool::AdaptorCreator creator_;
    MemoryPool::AdaptorPtr ptr_{nullptr, [](void*) {}};
    std::once_flag once_flag_;
};

class MemoryPoolAdaptorHolder {
 public:
    MemoryPoolAdaptorSlot& GetOrCreateSlot(const std::string& identifier,
                                           const MemoryPool::AdaptorCreator& creator) {
        auto snapshot = std::atomic_load(&slot_map_);
        if (auto it = snapshot->find(identifier); it != snapshot->end()) {
            return *it->second;
        }
        auto new_slot = std::make_shared<MemoryPoolAdaptorSlot>(creator);
        while (true) {
            auto new_snapshot = std::make_shared<SlotMap>(*snapshot);
            auto [slot, inserted] = new_snapshot->emplace(identifier, new_slot);
            if (std::atomic_compare_exchange_strong(&slot_map_, &snapshot, new_snapshot)) {
                return *slot->second;
            }
        }
    }

 private:
    using SlotPtr = std::shared_ptr<MemoryPoolAdaptorSlot>;
    using SlotMap = std::unordered_map<std::string, SlotPtr>;
    std::shared_ptr<SlotMap> slot_map_{std::make_shared<SlotMap>()};
};

}  // namespace paimon
