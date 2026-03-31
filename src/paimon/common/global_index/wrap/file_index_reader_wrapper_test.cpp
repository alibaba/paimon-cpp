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
#include "paimon/common/global_index/wrap/file_index_reader_wrapper.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/global_index_result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FileIndexReaderWrapperTest, TestToGlobalIndexResult) {
    auto check_result = [](const std::shared_ptr<GlobalIndexResult>& result,
                           const std::vector<int64_t>& expected) {
        auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap64* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap64::From(expected))
            << "result=" << (typed_result->GetBitmap().value())->ToString()
            << ", expected=" << RoaringBitmap64::From(expected).ToString();
    };

    {
        ASSERT_OK_AND_ASSIGN(auto global_result, FileIndexReaderWrapper::ToGlobalIndexResult(
                                                     /*range_end=*/5l, FileIndexResult::Remain()));
        check_result(global_result, {0l, 1l, 2l, 3l, 4l, 5l});
    }
    {
        ASSERT_OK_AND_ASSIGN(auto global_result, FileIndexReaderWrapper::ToGlobalIndexResult(
                                                     /*range_end=*/5l, FileIndexResult::Skip()));
        check_result(global_result, {});
    }
    {
        auto bitmap_supplier = []() -> Result<RoaringBitmap32> {
            return RoaringBitmap32::From({1, 4, 2147483647});
        };
        auto file_result = std::make_shared<BitmapIndexResult>(bitmap_supplier);
        ASSERT_OK_AND_ASSIGN(auto global_result, FileIndexReaderWrapper::ToGlobalIndexResult(
                                                     /*range_end=*/2147483647l, file_result));
        check_result(global_result, {1l, 4l, 2147483647l});
    }
    {
        class FakeFileIndexResult : public FileIndexResult {
            Result<bool> IsRemain() const override {
                return true;
            }
            std::string ToString() const override {
                return "fake file index result";
            }
        };
        auto file_result = std::make_shared<FakeFileIndexResult>();
        ASSERT_NOK_WITH_MSG(
            FileIndexReaderWrapper::ToGlobalIndexResult(/*range_end=*/10l, file_result),
            "invalid FileIndexResult, supposed to be Remain or Skip or BitmapIndexResult");
    }
}

}  // namespace paimon::test
