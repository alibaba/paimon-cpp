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

#include "paimon/core/key_value.h"
#include "paimon/metrics.h"
namespace paimon {
class KeyValueRecordReader {
 public:
    virtual ~KeyValueRecordReader() = default;

    class Iterator {
     public:
        virtual ~Iterator() = default;
        virtual bool HasNext() const = 0;
        virtual Result<KeyValue> Next() = 0;
    };

    virtual Result<std::unique_ptr<KeyValueRecordReader::Iterator>> NextBatch() = 0;

    virtual std::shared_ptr<Metrics> GetReaderMetrics() const = 0;

    virtual void Close() = 0;
};
}  // namespace paimon
