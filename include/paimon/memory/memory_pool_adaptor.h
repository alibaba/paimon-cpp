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
#include <string>

#include "paimon/memory/memory_pool.h"

namespace paimon {

/// CRTP base class for memory pool adaptors.
///
/// This class provides the common interface required by MemoryPool::AsSpecifiedMemoryPool().
/// Subclasses should inherit from MemoryPoolAdaptor<Subclass> and implement:
/// - A static Identifier() method returning a unique string identifier
/// - A constructor accepting MemoryPool& as parameter
///
/// @tparam Adaptor The derived adaptor class (CRTP pattern).
///
/// @example
///   class MyPoolAdaptor : public SomePoolInterface,
///                         public MemoryPoolAdaptor<MyPoolAdaptor> {
///   public:
///       explicit MyPoolAdaptor(MemoryPool& pool) : pool_(pool) {}
///       static std::string Identifier() { return "MyPoolAdaptor"; }
///       // ... implement SomePoolInterface methods ...
///   private:
///       MemoryPool& pool_;
///   };
///
///   SomePoolInterface* AsSomePool(MemoryPool& pool) {
///       return pool.AsSpecifiedMemoryPool<MyPoolAdaptor>();
///   }
template <typename Adaptor>
class MemoryPoolAdaptor {
 public:
    static std::string Identifier() {
        return Adaptor::Identifier();
    }

    static MemoryPool::AdaptorPtr Create(MemoryPool& pool) {
        auto adaptor = std::make_unique<Adaptor>(pool);
        return MemoryPool::AdaptorPtr(adaptor.release(),
                                      [](void* ptr) { delete static_cast<Adaptor*>(ptr); });
    }
};

}  // namespace paimon
