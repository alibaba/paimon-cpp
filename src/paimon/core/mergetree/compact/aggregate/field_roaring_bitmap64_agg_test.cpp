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

#include "paimon/core/mergetree/compact/aggregate/field_roaring_bitmap64_agg.h"

#include <memory>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon::test {
namespace {

VariantType Bitmap(std::initializer_list<int64_t> values) {
    RoaringBitmap64 bitmap;
    for (int64_t value : values) {
        bitmap.Add(value);
    }
    return VariantType(std::shared_ptr<Bytes>(bitmap.Serialize(/*pool=*/nullptr)));
}

}  // namespace

TEST(FieldRoaringBitmap64AggTest, UnionsSerializedBitmaps) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldRoaringBitmap64Agg> agg,
                         FieldRoaringBitmap64Agg::Create(arrow::binary(), "f"));
    ASSERT_OK_AND_ASSIGN(VariantType result,
                         agg->AggResult(Bitmap({1, 3}), Bitmap({2, 3, int64_t{1} << 40})));
    std::string_view bytes = DataDefine::GetStringView(result);
    RoaringBitmap64 bitmap;
    ASSERT_OK(bitmap.Deserialize(bytes.data(), bytes.size()));
    ASSERT_EQ(4, bitmap.Cardinality());
    ASSERT_TRUE(bitmap.Contains(1));
    ASSERT_TRUE(bitmap.Contains(2));
    ASSERT_TRUE(bitmap.Contains(3));
    ASSERT_TRUE(bitmap.Contains(int64_t{1} << 40));
}

TEST(FieldRoaringBitmap64AggTest, ReportsInvalidBytesAndType) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FieldRoaringBitmap64Agg> agg,
                         FieldRoaringBitmap64Agg::Create(arrow::binary(), "f"));
    ASSERT_NOK(agg->AggResult(VariantType(std::string_view("bad")), Bitmap({1})));
    ASSERT_NOK(FieldRoaringBitmap64Agg::Create(arrow::utf8(), "f"));
}

}  // namespace paimon::test
