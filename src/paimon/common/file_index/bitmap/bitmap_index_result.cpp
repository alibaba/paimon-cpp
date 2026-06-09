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

#include "paimon/file_index/bitmap_index_result.h"

#include <utility>

#include "paimon/status.h"

namespace paimon {
BitmapIndexResult::BitmapIndexResult(BitmapSupplier bitmap_supplier)
    : bitmap_supplier_(std::move(bitmap_supplier)) {}

BitmapIndexResult::~BitmapIndexResult() = default;

Result<const RoaringBitmap32*> BitmapIndexResult::GetBitmap() const {
    if (!initialized_) {
        PAIMON_ASSIGN_OR_RAISE(bitmap_, bitmap_supplier_());
        initialized_ = true;
    }
    return &bitmap_;
}

Result<bool> BitmapIndexResult::IsRemain() const {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, GetBitmap());
    return !bitmap->IsEmpty();
}

Result<std::shared_ptr<FileIndexResult>> BitmapIndexResult::And(
    const std::shared_ptr<FileIndexResult>& other) {
    auto typed_other = std::dynamic_pointer_cast<BitmapIndexResult>(other);
    if (typed_other) {
        return std::make_shared<BitmapIndexResult>(
            [result = std::dynamic_pointer_cast<BitmapIndexResult>(shared_from_this()),
             typed_other]() -> Result<RoaringBitmap32> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, result->GetBitmap());
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* other_bitmap,
                                       typed_other->GetBitmap());
                return RoaringBitmap32::And(*bitmap, *other_bitmap);
            });
    }
    return FileIndexResult::And(other);
}

Result<std::shared_ptr<FileIndexResult>> BitmapIndexResult::Or(
    const std::shared_ptr<FileIndexResult>& other) {
    auto typed_other = std::dynamic_pointer_cast<BitmapIndexResult>(other);
    if (typed_other) {
        return std::make_shared<BitmapIndexResult>(
            [result = std::dynamic_pointer_cast<BitmapIndexResult>(shared_from_this()),
             typed_other]() -> Result<RoaringBitmap32> {
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* bitmap, result->GetBitmap());
                PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap32* other_bitmap,
                                       typed_other->GetBitmap());
                return RoaringBitmap32::Or(*bitmap, *other_bitmap);
            });
    }
    return FileIndexResult::Or(other);
}

std::string BitmapIndexResult::ToString() const {
    auto bitmap = GetBitmap();
    if (!bitmap.ok()) {
        return bitmap.status().ToString();
    }
    return bitmap.value()->ToString();
}
}  // namespace paimon
