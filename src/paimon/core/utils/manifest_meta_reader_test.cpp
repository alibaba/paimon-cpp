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

#include "paimon/core/utils/manifest_meta_reader.h"

#include <string>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(ManifestMetaReaderTest, TestNoNeedCompleteNonExistField) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
    };
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]],
        [[7],          [["giraffe", 9]],                       [null, 40.1, true]],
        [null,         [["horse", 10], ["Panda", 11]],         [50, 50.1, null]],
        [[9],          null,                                   [60, 60.1, false]],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null]
    ])")
                         .ValueOrDie();
    arrow::FieldVector target_fields = fields;
    auto target_arrow_type = arrow::struct_(target_fields);
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto ret, ManifestMetaReader::AlignArrayWithSchema(
                                       src_array, target_arrow_type, arrow_pool.get()));
    ASSERT_TRUE(src_array->Equals(ret)) << ret->ToString();
}
TEST(ManifestMetaReaderTest, TestCompleteNonExistFieldSimple) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
    };
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true]],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true]],
        [[7],          [["giraffe", 9]],                       [null, 40.1, true]],
        [null,         [["horse", 10], ["Panda", 11]],         [50, 50.1, null]],
        [[9],          null,                                   [60, 60.1, false]],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null]
    ])")
                         .ValueOrDie();
    arrow::FieldVector target_fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int64())),
        arrow::field("f3", arrow::list(arrow::utf8())),
        arrow::field(
            "f2", arrow::struct_({field("sub1", arrow::int64()), field("sub2", arrow::float64()),
                                  field("sub3", arrow::boolean()), field("sub4", arrow::int8())})),
    };
    auto target_arrow_type = arrow::struct_(target_fields);
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto ret, ManifestMetaReader::AlignArrayWithSchema(
                                       src_array, target_arrow_type, arrow_pool.get()));

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(target_fields), R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          null, [10, 10.1, false, null]],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], null, [20, 20.1, true, null]],
        [[6],          [["elephant", 7], ["fox", 8]],          null, [null, 30.1, true, null]],
        [[7],          [["giraffe", 9]],                       null, [null, 40.1, true, null]],
        [null,         [["horse", 10], ["Panda", 11]],         null, [50, 50.1, null, null]],
        [[9],          null,                                   null, [60, 60.1, false, null]],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null, null]
    ])")
            .ValueOrDie();
    ASSERT_TRUE(expected_array->Equals(ret)) << ret->ToString();
}

TEST(ManifestMetaReaderTest, TestCompleteNonExistFieldNested) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::struct_(
                               {field("a", arrow::int64()), field("b", arrow::boolean())}))),
        arrow::field("f1", arrow::map(arrow::struct_({field("a", arrow::int64()),
                                                      field("b", arrow::boolean())}),
                                      arrow::boolean()))};
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[null, [1, true], null], [[[1, true], true]]],
        [[[2, false], null], null],
        [[[2, false], [3, true], [4, null]], [[[1, true], true], [[5, false], null]]],
        [null, null]
    ])")
                         .ValueOrDie();
    arrow::FieldVector target_fields = {
        arrow::field("f0", arrow::list(arrow::struct_({field("a", arrow::int64()),
                                                       field("b", arrow::boolean()),
                                                       field("c", arrow::int64())}))),
        arrow::field("f1", arrow::map(arrow::struct_({field("a", arrow::int64()),
                                                      field("b", arrow::boolean()),
                                                      field("c", arrow::int64())}),
                                      arrow::boolean()))};
    auto target_arrow_type = arrow::struct_(target_fields);
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto ret, ManifestMetaReader::AlignArrayWithSchema(
                                       src_array, target_arrow_type, arrow_pool.get()));

    auto expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(target_fields), R"([
        [[null, [1, true, null], null], [[[1, true, null], true]]],
        [[[2, false, null], null], null],
        [[[2, false, null], [3, true, null], [4, null, null]], [[[1, true, null], true], [[5, false, null], null]]],
        [null, null]
    ])")
            .ValueOrDie();
    ASSERT_TRUE(expected_array->Equals(ret)) << ret->ToString();
}

TEST(ManifestMetaReaderTest, TestCastInt32Type) {
    arrow::FieldVector src_fields = {
        arrow::field("f0", arrow::list(arrow::int32())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int32())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int32()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
        arrow::field("f3", arrow::int32()),
    };
    std::string array_json = R"([
        [[1, 2, 3],    [["apple", 3], ["banana", 4]],          [10, 10.1, false],  10],
        [[4, 5],       [["cat", 5], ["dog", 6], ["mouse", 7]], [20, 20.1, true],   20],
        [[6],          [["elephant", 7], ["fox", 8]],          [null, 30.1, true], 30],
        [[7],          [["giraffe", 9]],                       [null, 40.1, true], 40],
        [null,         [["horse", 10], ["Panda", 11]],         [50, 50.1, null],   50],
        [[9],          null,                                   [60, 60.1, false],  60],
        [[10, 11, 12], [["rabbit", null], ["tiger", 13]],      null,               70]
    ])";
    auto src_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(src_fields), array_json)
            .ValueOrDie();

    arrow::FieldVector target_fields = {
        arrow::field("f0", arrow::list(arrow::int8())),
        arrow::field("f1", arrow::map(arrow::utf8(), arrow::int16())),
        arrow::field("f2",
                     arrow::struct_({field("sub1", arrow::int8()), field("sub2", arrow::float64()),
                                     field("sub3", arrow::boolean())})),
        arrow::field("f3", arrow::int16()),
    };
    auto target_arrow_type = arrow::struct_(target_fields);
    auto target_array =
        arrow::ipc::internal::json::ArrayFromJSON(target_arrow_type, array_json).ValueOrDie();
    auto arrow_pool = GetArrowPool(GetDefaultPool());
    ASSERT_OK_AND_ASSIGN(auto result_array, ManifestMetaReader::AlignArrayWithSchema(
                                                src_array, target_arrow_type, arrow_pool.get()));
    ASSERT_TRUE(target_array->Equals(result_array)) << result_array->ToString();
}

}  // namespace paimon::test
