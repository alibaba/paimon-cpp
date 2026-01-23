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

#include "paimon/format/avro/avro_adaptor.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "avro/GenericDatum.hh"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/core/utils/manifest_meta_reader.h"
#include "paimon/format/avro/avro_schema_converter.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::avro::test {

TEST(AvroAdaptorTest, Simple) {
    arrow::FieldVector fields = {arrow::field("f0", arrow::boolean(), /*nullable=*/true),
                                 arrow::field("f1", arrow::int8(), /*nullable=*/true),
                                 arrow::field("f2", arrow::int16(), /*nullable=*/true),
                                 arrow::field("f3", arrow::int32(), /*nullable=*/true),
                                 arrow::field("field_null", arrow::int32(), /*nullable=*/true),
                                 arrow::field("f4", arrow::int64(), /*nullable=*/true),
                                 arrow::field("f5", arrow::float32(), /*nullable=*/true),
                                 arrow::field("f6", arrow::float64(), /*nullable=*/true),
                                 arrow::field("f7", arrow::utf8(), /*nullable=*/true)};
    // TODO(jinli.zjw): add test for struct and binary
    std::shared_ptr<arrow::DataType> data_type = arrow::struct_(fields);
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);

    AvroAdaptor adaptor(data_type);

    std::string data_str =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327" ],
            [false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327" ],
            [null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326"]])";
    auto array = arrow::ipc::internal::json::ArrayFromJSON(data_type, {data_str}).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(auto avro_schema, AvroSchemaConverter::ArrowSchemaToAvroSchema(schema));
    ASSERT_OK_AND_ASSIGN(std::vector<::avro::GenericDatum> datums,
                         adaptor.ConvertArrayToGenericDatums(array, avro_schema));
    ASSERT_EQ(4, datums.size());
}

}  // namespace paimon::avro::test
