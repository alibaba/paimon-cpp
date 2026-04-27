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

#pragma once
#include <optional>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/status.h"
namespace paimon {
class GlobalIndexUtils {
 public:
    GlobalIndexUtils() = delete;
    ~GlobalIndexUtils() = delete;

    static Status CheckRelativeRowIds(::ArrowArray* c_arrow_array,
                                      const std::vector<int64_t>& relative_row_ids,
                                      std::optional<int64_t> expected_next_row_id) {
        if (!c_arrow_array) {
            return Status::Invalid("CheckRelativeRowIds failed: null c_arrow_array");
        }
        int64_t length = c_arrow_array->length;
        ScopeGuard guard([c_arrow_array]() -> void { ArrowArrayRelease(c_arrow_array); });
        if (static_cast<int64_t>(relative_row_ids.size()) != length) {
            return Status::Invalid(fmt::format(
                "relative_row_ids length {} mismatch arrow_array length {} in CheckRelativeRowIds",
                relative_row_ids.size(), length));
        }
        if (!relative_row_ids.empty() && expected_next_row_id &&
            relative_row_ids[0] != expected_next_row_id.value()) {
            return Status::Invalid(
                fmt::format("first relative_row_ids {} mismatch inner expected_next_row_id {} in "
                            "CheckRelativeRowIds",
                            relative_row_ids[0], expected_next_row_id.value()));
        }
        guard.Release();
        return Status::OK();
    }
};
}  // namespace paimon
