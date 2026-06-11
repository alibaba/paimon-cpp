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

#include "paimon/testing/mock/mock_format_writer.h"

#include <string>
#include <utility>

#include "arrow/c/helpers.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

struct ArrowArray;

namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::test {
MockFormatWriter::MockFormatWriter(const std::shared_ptr<OutputStream>& out,
                                   const std::shared_ptr<MemoryPool>& pool)
    : FormatWriter(), out_(std::move(out)), pool_(pool) {}

Status MockFormatWriter::AddBatch(ArrowArray* batch) {
    ArrowArrayRelease(batch);
    std::string str = std::to_string(DateTimeUtils::GetCurrentUTCTimeUs()) + "\n";
    PAIMON_ASSIGN_OR_RAISE(int64_t res, out_->Write(str.data(), str.size()));
    if (res != static_cast<int64_t>(str.size())) {
        return Status::IOError("write size does not match");
    }
    counter_++;
    return Status::OK();
}
Status MockFormatWriter::Flush() {
    return out_->Flush();
}
Status MockFormatWriter::Finish() {
    return Flush();
}
Result<bool> MockFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (counter_ >= target_size) {
        return true;
    }
    return false;
}

}  // namespace paimon::test
