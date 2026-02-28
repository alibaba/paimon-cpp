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

#include "paimon/memory/bytes.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {

class Dictionary {
 public:
    virtual ~Dictionary() = default;

    virtual Result<int32_t> Find(const Literal& key) = 0;

    virtual Result<Literal> Find(int32_t code) = 0;

    class Appender {
     public:
        virtual ~Appender() = default;

        virtual Status AppendSorted(const Literal& key, int32_t code) = 0;

        virtual Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize() = 0;
    };
};

}  // namespace paimon
