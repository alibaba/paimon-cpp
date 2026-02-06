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

#include "paimon/common/utils/arrow/arrow_memory_pool_adaptor_factor.h"

#include "paimon/common/utils/arrow/arrow_memory_pool_adaptor.h"
#include "paimon/memory/memory_pool_adaptor_traits.h"

namespace paimon {

const char* ArrowMemoryPoolAdaptorFactory::Identifier() const {
    return MemoryPoolAdaptorTraits<arrow::MemoryPool>::identifier;
}

MemoryPoolAdaptorPtr ArrowMemoryPoolAdaptorFactory::Create(MemoryPool& pool) const {
    return MakeAdaptor<ArrowMemoryPoolAdaptor>(pool);
}

REGISTER_PAIMON_FACTORY(ArrowMemoryPoolAdaptorFactory);

}  // namespace paimon
