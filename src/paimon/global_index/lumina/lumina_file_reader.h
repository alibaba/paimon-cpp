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

#include <functional>
#include <memory>

#include "fmt/format.h"
#include "lumina/io/FileReader.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/lumina/lumina_utils.h"
namespace paimon::lumina {
class LuminaFileReader : public ::lumina::io::FileReader {
 public:
    explicit LuminaFileReader(const std::shared_ptr<InputStream>& in) : in_(in) {}
    ~LuminaFileReader() override = default;

    ::lumina::core::Result<uint64_t> GetLength() const noexcept override {
        Result<int64_t> length_result = in_->Length();
        if (!length_result.ok()) {
            return ::lumina::core::Result<uint64_t>::Err(
                PaimonToLuminaStatus(length_result.status()));
        }
        Status status = ValidateValueInRange<uint64_t>(length_result.value(), "file length");
        if (!status.ok()) {
            return ::lumina::core::Result<uint64_t>::Err(PaimonToLuminaStatus(status));
        }
        return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(length_result.value()));
    }

    ::lumina::core::Result<uint64_t> GetPosition() const noexcept override {
        Result<int64_t> pos_result = in_->GetPos();
        if (!pos_result.ok()) {
            return ::lumina::core::Result<uint64_t>::Err(PaimonToLuminaStatus(pos_result.status()));
        }
        Status status = ValidateValueInRange<uint64_t>(pos_result.value(), "file position");
        if (!status.ok()) {
            return ::lumina::core::Result<uint64_t>::Err(PaimonToLuminaStatus(status));
        }
        return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(pos_result.value()));
    }

    ::lumina::core::Status Seek(uint64_t position) noexcept override {
        Status status = ValidateValueInRange<int64_t>(position, "seek position");
        if (!status.ok()) {
            return PaimonToLuminaStatus(status);
        }
        return PaimonToLuminaStatus(
            in_->Seek(static_cast<int64_t>(position), SeekOrigin::FS_SEEK_SET));
    }

    ::lumina::core::Status Read(char* data, uint64_t size) noexcept override {
        Status status = ValidateValueInRange<int64_t>(size, "read size");
        if (!status.ok()) {
            return PaimonToLuminaStatus(status);
        }
        Result<int64_t> read_result = in_->Read(data, static_cast<int64_t>(size));
        if (!read_result.ok()) {
            return PaimonToLuminaStatus(read_result.status());
        }
        if (read_result.value() != static_cast<int64_t>(size)) {
            return ::lumina::core::Status(
                ::lumina::core::ErrorCode::IoError,
                fmt::format("expect read len {} mismatch actual read len {}", size,
                            read_result.value()));
        }
        return ::lumina::core::Status::Ok();
    }

    void ReadAsync(char* data, uint64_t size, uint64_t offset,
                   std::function<void(::lumina::core::Status)> call_back) noexcept override {
        if (size == 0) {
            call_back(::lumina::core::Status::Ok());
            return;
        }

        Status status = ValidateValueInRange<int64_t>(size, "read size");
        if (!status.ok()) {
            call_back(PaimonToLuminaStatus(status));
            return;
        }
        status = ValidateValueInRange<int64_t>(offset, "read offset");
        if (!status.ok()) {
            call_back(PaimonToLuminaStatus(status));
            return;
        }
        in_->ReadAsync(data, static_cast<int64_t>(size), static_cast<int64_t>(offset),
                       [call_back = std::move(call_back)](const Status& status) {
                           call_back(PaimonToLuminaStatus(status));
                       });
    }

    ::lumina::core::Status Close() noexcept override {
        return PaimonToLuminaStatus(in_->Close());
    }

 private:
    std::shared_ptr<InputStream> in_;
};
}  // namespace paimon::lumina
