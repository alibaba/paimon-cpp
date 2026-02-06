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

#include "paimon/memory/memory_pool_adaptor_factor.h"

namespace paimon {

MemoryPoolAdaptorPtr MemoryPoolAdaptorFactory::Get(const std::string& identifier,
                                                   MemoryPool& pool) {
    auto factory_creator = FactoryCreator::GetInstance();
    if (factory_creator == nullptr) {
        return {nullptr, [](void*) {}};
    }
    auto memory_pool_adaptor_factory =
        dynamic_cast<MemoryPoolAdaptorFactory*>(factory_creator->Create(identifier));
    if (memory_pool_adaptor_factory == nullptr) {
        return {nullptr, [](void*) {}};
    }
    return memory_pool_adaptor_factory->Create(pool);
}

}  // namespace paimon
