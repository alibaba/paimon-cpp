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
// https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/schema_internal.h

#pragma once

#include <cstdint>
#include <memory>

#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet::schema {
class PrimitiveNode;
}  // namespace parquet::schema

namespace arrow {
class DataType;
}

namespace paimon::parquet {

::arrow::Result<std::shared_ptr<::arrow::DataType>> FromByteArray(
    const ::parquet::LogicalType& logical_type);
::arrow::Result<std::shared_ptr<::arrow::DataType>> FromFLBA(
    const ::parquet::LogicalType& logical_type, int32_t physical_length);
::arrow::Result<std::shared_ptr<::arrow::DataType>> FromInt32(
    const ::parquet::LogicalType& logical_type);
::arrow::Result<std::shared_ptr<::arrow::DataType>> FromInt64(
    const ::parquet::LogicalType& logical_type);

::arrow::Result<std::shared_ptr<::arrow::DataType>> GetArrowType(
    ::parquet::Type::type physical_type, const ::parquet::LogicalType& logical_type,
    int32_t type_length, ::arrow::TimeUnit::type int96_arrow_time_unit = ::arrow::TimeUnit::NANO);

::arrow::Result<std::shared_ptr<::arrow::DataType>> GetArrowType(
    const ::parquet::schema::PrimitiveNode& primitive,
    ::arrow::TimeUnit::type int96_arrow_time_unit = ::arrow::TimeUnit::NANO);

}  // namespace paimon::parquet
