// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Adapted from Apache Arrow
// https://github.com/apache/arrow/blob/apache-arrow-17.0.0/cpp/src/arrow/adapters/orc/util.h

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/builder_base.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/key_value_metadata.h"
#include "orc/OrcFile.hh"
#include "orc/Type.hh"
#include "paimon/result.h"
#include "paimon/status.h"

namespace orc {
struct ColumnVectorBatch;
}  // namespace orc

namespace arrow {
class MemoryPool;
}  // namespace arrow

namespace paimon::orc {
class OrcAdapter {
 public:
    OrcAdapter() = delete;
    ~OrcAdapter() = delete;

    static Result<std::shared_ptr<arrow::DataType>> GetArrowType(const ::orc::Type* type);

    static Result<std::unique_ptr<::orc::Type>> GetOrcType(const arrow::Schema& schema);

    static Result<std::shared_ptr<const arrow::KeyValueMetadata>> GetFieldMetadata(
        const ::orc::Type* type);

    static Result<std::shared_ptr<arrow::Field>> GetArrowField(const std::string& name,
                                                               const ::orc::Type* type,
                                                               bool nullable = true);

    static Result<std::shared_ptr<arrow::Array>> AppendBatch(
        const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* batch,
        arrow::MemoryPool* pool);

    static Status WriteBatch(const std::shared_ptr<arrow::Array>& array,
                             ::orc::ColumnVectorBatch* column_vector_batch);
};
}  // namespace paimon::orc
