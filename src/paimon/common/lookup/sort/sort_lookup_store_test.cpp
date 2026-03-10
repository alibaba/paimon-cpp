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

#include "arrow/api.h"
#include "arrow/ipc/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/data/serializer/row_compacted_serializer.h"
#include "paimon/common/lookup/lookup_store_factory.h"
#include "paimon/common/lookup/sort/sort_lookup_store_factory.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
TEST(SortLookupStoreTest, TestSimple) {
    auto pool = GetDefaultPool();
    auto check_result = [&](const std::map<std::string, std::string>& options_map) {
        auto dir = UniqueTestDirectory::Create("local");
        std::string file_path = dir->Str() + "/test.lookup";
        auto fs = dir->GetFileSystem();

        arrow::FieldVector fields = {
            arrow::field("key", arrow::utf8()),
            arrow::field("value", arrow::int32()),
        };
        auto key_type = arrow::struct_({fields[0]});
        auto value_type = arrow::struct_({fields[1]});
        ASSERT_OK_AND_ASSIGN(auto comparator, RowCompactedSerializer::CreateSliceComparator(
                                                  arrow::schema(key_type->fields()), pool));
        ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(options_map));

        ASSERT_OK_AND_ASSIGN(auto factory, LookupStoreFactory::Create(comparator, options));
        int64_t row_count = 6;
        ASSERT_OK_AND_ASSIGN(auto bloom_filter,
                             LookupStoreFactory::BfGenerator(row_count, options, pool.get()));
        ASSERT_OK_AND_ASSIGN(auto writer, factory->CreateWriter(fs, file_path, bloom_filter, pool));

        std::shared_ptr<arrow::Array> key_array =
            arrow::ipc::internal::json::ArrayFromJSON(key_type,
                                                      R"([
        ["Alex"],
        ["Alice"],
        ["Bob"],
        ["David"],
        ["Lily"],
        ["Lucy"]
    ])")
                .ValueOrDie();
        std::shared_ptr<arrow::Array> value_array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type,
                                                      R"([
        [0],
        [10],
        [20],
        [30],
        [40],
        [100]
    ])")
                .ValueOrDie();

        ASSERT_OK_AND_ASSIGN(auto key_serializer, RowCompactedSerializer::Create(
                                                      arrow::schema(key_type->fields()), pool));
        ASSERT_OK_AND_ASSIGN(auto value_serializer, RowCompactedSerializer::Create(
                                                        arrow::schema(value_type->fields()), pool));
        // write data
        std::vector<std::pair<std::shared_ptr<Bytes>, std::shared_ptr<Bytes>>> key_value_bytes_vec;
        for (int64_t i = 0; i < row_count; i++) {
            auto key_struct_array = std::dynamic_pointer_cast<arrow::StructArray>(key_array);
            auto value_struct_array = std::dynamic_pointer_cast<arrow::StructArray>(value_array);
            ASSERT_TRUE(key_struct_array);
            ASSERT_TRUE(value_struct_array);
            auto key_row = std::make_shared<ColumnarRow>(
                /*struct_array=*/nullptr, key_struct_array->fields(), pool, /*row_id=*/i);
            auto value_row = std::make_shared<ColumnarRow>(
                /*struct_array=*/nullptr, value_struct_array->fields(), pool, /*row_id=*/i);
            ASSERT_OK_AND_ASSIGN(auto key_bytes, key_serializer->SerializeToBytes(*key_row));
            ASSERT_OK_AND_ASSIGN(auto value_bytes, value_serializer->SerializeToBytes(*value_row));
            key_value_bytes_vec.emplace_back(key_bytes, value_bytes);
            ASSERT_OK(writer->Put(std::move(key_bytes), std::move(value_bytes)));
        }
        ASSERT_OK(writer->Close());

        // read data
        ASSERT_TRUE(fs->Exists(file_path).value());
        ASSERT_OK_AND_ASSIGN(auto reader, factory->CreateReader(fs, file_path, pool));
        for (int64_t i = 0; i < row_count; i++) {
            const auto& [key, value] = key_value_bytes_vec[i];
            ASSERT_OK_AND_ASSIGN(auto value_bytes, reader->Lookup(key));
            ASSERT_TRUE(value_bytes);
            // test value bytes
            ASSERT_EQ(*value_bytes, *value);

            // test deserialize data
            ASSERT_OK_AND_ASSIGN(auto de_row, value_serializer->Deserialize(value_bytes));
            auto value_struct_array = std::dynamic_pointer_cast<arrow::StructArray>(value_array);
            ASSERT_TRUE(value_struct_array);
            auto typed_value_array =
                std::dynamic_pointer_cast<arrow::Int32Array>(value_struct_array->field(0));
            ASSERT_TRUE(typed_value_array);
            ASSERT_EQ(de_row->GetInt(0), typed_value_array->Value(i));
        }
        // test non-exist key
        auto non_exist_key = std::make_shared<Bytes>("non-exist", pool.get());
        ASSERT_OK_AND_ASSIGN(auto value_bytes, reader->Lookup(non_exist_key));
        ASSERT_FALSE(value_bytes);
        ASSERT_OK(reader->Close());
    };

    check_result({});
    check_result(std::map<std::string, std::string>(
        {{Options::LOOKUP_CACHE_BLOOM_FILTER_ENABLED, "false"}}));
    check_result(
        std::map<std::string, std::string>({{Options::LOOKUP_CACHE_SPILL_COMPRESSION, "lz4"}}));
    check_result(
        std::map<std::string, std::string>({{Options::SPILL_COMPRESSION_ZSTD_LEVEL, "3"}}));
    check_result(std::map<std::string, std::string>({{Options::CACHE_PAGE_SIZE, "4"}}));
}
}  // namespace paimon::test
