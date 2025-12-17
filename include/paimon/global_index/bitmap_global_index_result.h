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
#include <utility>

#include "paimon/global_index/global_index_result.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {
/// Represents a global index query result that **lazily materializes** its matching row IDs as a
/// Roaring bitmap. The underlying 64-bit Roaring bitmap is **not constructed during object
/// creation**; instead, it is built on-demand the first time GetBitmap() is called. This design
/// avoids unnecessary computation and memory allocation when the bitmap is not needed (e.g., during
/// early stopping).
class PAIMON_EXPORT BitmapGlobalIndexResult : public GlobalIndexResult {
 public:
    using BitmapSupplier = std::function<Result<RoaringBitmap64>()>;
    explicit BitmapGlobalIndexResult(BitmapSupplier bitmap_supplier)
        : bitmap_supplier_(bitmap_supplier) {}

    class Iterator : public GlobalIndexResult::Iterator {
     public:
        Iterator(const RoaringBitmap64* bitmap, RoaringBitmap64::Iterator&& iter)
            : bitmap_(bitmap), iter_(std::move(iter)) {}

        bool HasNext() const override {
            return iter_ != bitmap_->End();
        }

        int64_t Next() override {
            uint64_t value = *iter_;
            ++iter_;
            return value;
        }

     private:
        const RoaringBitmap64* bitmap_;
        RoaringBitmap64::Iterator iter_;
    };

    Result<std::unique_ptr<GlobalIndexResult::Iterator>> CreateIterator() const override;

    Result<std::shared_ptr<GlobalIndexResult>> And(
        const std::shared_ptr<GlobalIndexResult>& other) override;

    Result<std::shared_ptr<GlobalIndexResult>> Or(
        const std::shared_ptr<GlobalIndexResult>& other) override;

    Result<bool> IsEmpty() const override;

    std::string ToString() const override;

    /// @return A non-owning, const pointer to the bitmap. The returned pointer is valid as long as
    ///         this BitmapGlobalIndexResult object is alive. The caller must not modify the bitmap.
    /// @note **Lazy initialization**: The bitmap is constructed only on the first call to this
    /// method.
    ///       Subsequent calls return the cached instance. Construction may involve non-trivial
    ///       CPU/IO cost (e.g., read indexes or merging bitmap), so avoid calling this if the
    ///       bitmap is not actually required. **Not thread-safe**.
    Result<const RoaringBitmap64*> GetBitmap() const;

    /// @return A shared pointer to a new BitmapGlobalIndexResult instance representing the given
    ///         inclusive range [from, to].
    static std::shared_ptr<BitmapGlobalIndexResult> FromRange(const Range& range);

 private:
    mutable bool initialized_ = false;
    BitmapSupplier bitmap_supplier_;
    mutable RoaringBitmap64 bitmap_;
};
}  // namespace paimon
