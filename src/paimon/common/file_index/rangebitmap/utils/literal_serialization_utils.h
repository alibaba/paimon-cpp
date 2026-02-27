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

#include <functional>
#include <memory>

#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class LiteralSerializationUtils {
 public:
    LiteralSerializationUtils() = delete;

    ~LiteralSerializationUtils() = delete;

    static Result<std::function<Result<Literal>()>> CreateValueReader(
        FieldType field_type, const std::shared_ptr<DataInputStream>& input_stream,
        MemoryPool* pool = nullptr);

    static Result<std::function<Status(const Literal&)>> CreateValueWriter(
        FieldType field_type, const std::shared_ptr<MemorySegmentOutputStream>& output_stream);

    static Result<int32_t> GetFixedFieldSize(const FieldType& field_type);

    static Result<int32_t> GetSerializedSizeInBytes(const Literal& literal);
};

}  // namespace paimon
