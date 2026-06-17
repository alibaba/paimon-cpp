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

#include "paimon/format/lance/lance_format_writer.h"

#include <cassert>
#include <map>
#include <string>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/format/lance/lance_utils.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::lance {

Result<std::unique_ptr<LanceFormatWriter>> LanceFormatWriter::Create(
    const std::string& file_path, const std::shared_ptr<arrow::Schema>& schema) {
    PAIMON_ASSIGN_OR_RAISE(std::string normalized_path,
                           LanceUtils::NormalizeLanceFilePath(file_path));
    std::string error_message;
    error_message.resize(1024, '\0');
    LanceFileWriter* file_writer = nullptr;
    ArrowSchema c_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
    ScopeGuard guard([&]() { ArrowSchemaRelease(&c_schema); });
    int32_t error_code = create_writer(normalized_path.data(), &c_schema, &file_writer,
                                       error_message.data(), error_message.size());
    PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(error_code, error_message));
    assert(file_writer);
    guard.Release();
    return std::unique_ptr<LanceFormatWriter>(
        new LanceFormatWriter(file_writer, schema, std::move(error_message)));
}

LanceFormatWriter::~LanceFormatWriter() {
    [[maybe_unused]] int32_t error_code =
        release_writer(file_writer_, error_message_.data(), error_message_.size());
    file_writer_ = nullptr;
}

Status LanceFormatWriter::AddBatch(ArrowArray* batch) {
    assert(batch);
    ArrowSchema c_schema;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema_, &c_schema));
    ScopeGuard guard([&]() {
        ArrowSchemaRelease(&c_schema);
        ArrowArrayRelease(batch);
    });
    int32_t error_code = write_c_arrow_array(file_writer_, batch, &c_schema, error_message_.data(),
                                             error_message_.size());
    PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(error_code, error_message_));
    guard.Release();
    return Status::OK();
}

Result<bool> LanceFormatWriter::ReachTargetSize(bool suggested_check, int64_t target_size) const {
    if (suggested_check) {
        uint64_t tell_pos = 0;
        int32_t error_code =
            writer_tell(file_writer_, &tell_pos, error_message_.data(), error_message_.size());
        PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(error_code, error_message_));
        return tell_pos >= static_cast<uint64_t>(target_size);
    }
    return false;
}

Status LanceFormatWriter::AddMetadata(const std::map<std::string, std::string>& /*metadata*/) {
    return Status::NotImplemented("AddMetadata is not supported by lance format writer.");
}

Status LanceFormatWriter::Finish() {
    int32_t error_code = finish_writer(file_writer_, error_message_.data(), error_message_.size());
    PAIMON_RETURN_NOT_OK(LanceToPaimonStatus(error_code, error_message_));
    return Status::OK();
}
}  // namespace paimon::lance
