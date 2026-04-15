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

#include "paimon/common/utils/arrow/arrow_output_stream_adapter.h"

#include "arrow/result.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace paimon {

ArrowOutputStreamAdapter::ArrowOutputStreamAdapter(const std::shared_ptr<paimon::OutputStream>& out)
    : out_(out) {}

arrow::Status ArrowOutputStreamAdapter::Close() {
    // output stream close is called by paimon framework(such as single file writer), no need to
    // close here
    closed_ = true;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowOutputStreamAdapter::Tell() const {
    paimon::Result<int64_t> pos = out_->GetPos();
    if (!pos.ok()) {
        return ToArrowStatus(pos.status());
    }
    return pos.value();
}

bool ArrowOutputStreamAdapter::closed() const {
    return closed_;
}

arrow::Status ArrowOutputStreamAdapter::Write(const void* data, int64_t nbytes) {
    if (!InRange<uint32_t>(nbytes)) {
        return arrow::Status::Invalid(
            fmt::format("nbytes value {} is out of bound of uint32_t", nbytes));
    }
    Result<int32_t> len =
        out_->Write(static_cast<const char*>(data), static_cast<uint32_t>(nbytes));
    if (!len.ok()) {
        return ToArrowStatus(len.status());
    }
    return arrow::Status::OK();
}

arrow::Status ArrowOutputStreamAdapter::Flush() {
    return ToArrowStatus(out_->Flush());
}

}  // namespace paimon
