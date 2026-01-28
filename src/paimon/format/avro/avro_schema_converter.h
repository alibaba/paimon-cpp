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
#include <string>

#include "arrow/api.h"
#include "avro/Node.hh"
#include "avro/Schema.hh"
#include "avro/ValidSchema.hh"
#include "paimon/result.h"

namespace paimon::avro {

class AvroSchemaConverter {
 public:
    AvroSchemaConverter() = delete;
    ~AvroSchemaConverter() = delete;

    // TODO(menglingda.mld): add field id for avro
    static Result<::avro::ValidSchema> ArrowSchemaToAvroSchema(
        const std::shared_ptr<arrow::Schema>& arrow_schema);

    static Result<std::shared_ptr<arrow::DataType>> AvroSchemaToArrowDataType(
        const ::avro::ValidSchema& avro_schema);

 private:
    static Result<std::shared_ptr<arrow::DataType>> GetArrowType(const ::avro::NodePtr& avro_node,
                                                                 bool* nullable);

    static Result<::avro::Schema> ArrowTypeToAvroSchema(const std::shared_ptr<arrow::Field>& field,
                                                        const std::string& row_name);

    static ::avro::Schema NullableSchema(const ::avro::Schema& schema);

    static void AddRecordField(::avro::RecordSchema* record_schema, const std::string& field_name,
                               const ::avro::Schema& field_schema);

    static Result<bool> CheckUnionType(const ::avro::NodePtr& avro_node);

    static Result<std::shared_ptr<arrow::Field>> GetArrowField(const std::string& name,
                                                               const ::avro::NodePtr& avro_node);
};

}  // namespace paimon::avro
