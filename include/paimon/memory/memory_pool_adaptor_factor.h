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

#include "paimon/factories/factory.h"

namespace paimon {

class MemoryPool;

using MemoryPoolAdaptorPtr = std::unique_ptr<void, void (*)(void*)>;

class MemoryPoolAdaptorFactory : public Factory {
 public:
    static MemoryPoolAdaptorPtr Get(const std::string& identifier, MemoryPool& pool);

    template <typename AdaptorType>
    static MemoryPoolAdaptorPtr MakeAdaptor(MemoryPool& pool) {
        auto temp = std::make_unique<AdaptorType>(pool);
        return MemoryPoolAdaptorPtr(temp.release(),
                                    [](void* ptr) { delete static_cast<AdaptorType*>(ptr); });
    }

    virtual MemoryPoolAdaptorPtr Create(MemoryPool& pool) const = 0;
};

}  // namespace paimon
