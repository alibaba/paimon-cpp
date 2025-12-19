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
#include "paimon/global_index/bitmap_global_index_result.h"

namespace paimon {
Result<std::unique_ptr<GlobalIndexResult::Iterator>> BitmapGlobalIndexResult::CreateIterator()
    const {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, GetBitmap());
    auto iter = bitmap->Begin();
    return std::make_unique<BitmapGlobalIndexResult::Iterator>(bitmap, std::move(iter));
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapGlobalIndexResult::And(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(other);
    if (typed_result) {
        auto supplier = [typed_result, result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(
                                           shared_from_this())]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r1, result->GetBitmap());
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r2, typed_result->GetBitmap());
            return RoaringBitmap64::And(*r1, *r2);
        };
        return std::make_shared<BitmapGlobalIndexResult>(supplier);
    }
    return GlobalIndexResult::And(other);
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapGlobalIndexResult::Or(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto typed_result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(other);
    if (typed_result) {
        auto supplier = [typed_result, result = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(
                                           shared_from_this())]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r1, result->GetBitmap());
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r2, typed_result->GetBitmap());
            return RoaringBitmap64::Or(*r1, *r2);
        };
        return std::make_shared<BitmapGlobalIndexResult>(supplier);
    }
    return GlobalIndexResult::Or(other);
}

Result<const RoaringBitmap64*> BitmapGlobalIndexResult::GetBitmap() const {
    if (!initialized_) {
        PAIMON_ASSIGN_OR_RAISE(bitmap_, bitmap_supplier_());
        initialized_ = true;
    }
    return &bitmap_;
}

Result<bool> BitmapGlobalIndexResult::IsEmpty() const {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, GetBitmap());
    return bitmap->IsEmpty();
}

std::string BitmapGlobalIndexResult::ToString() const {
    auto bitmap = GetBitmap();
    if (!bitmap.ok()) {
        return bitmap.status().ToString();
    }
    return bitmap.value()->ToString();
}

std::shared_ptr<BitmapGlobalIndexResult> BitmapGlobalIndexResult::FromRanges(
    const std::vector<Range>& ranges) {
    BitmapGlobalIndexResult::BitmapSupplier supplier = [ranges]() -> Result<RoaringBitmap64> {
        RoaringBitmap64 bitmap;
        for (const auto& range : ranges) {
            bitmap.AddRange(range.from, range.to + 1);
        }
        return bitmap;
    };
    return std::make_shared<BitmapGlobalIndexResult>(supplier);
}
}  // namespace paimon
