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

#include "paimon/fs/file_system.h"

#include <set>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/macros.h"

namespace paimon {
FileSystem::~FileSystem() = default;

Result<bool> FileSystem::IsObjectStore(const std::string& path_str) {
    static const std::set<std::string> non_object_store_schemes{"", "file", "hdfs", "dfs"};
    PAIMON_ASSIGN_OR_RAISE(Path path, PathUtil::ToPath(path_str));
    auto path_scheme = StringUtils::ToLowerCase(path.scheme);
    if (non_object_store_schemes.count(path_scheme)) {
        return false;
    }
    return true;
}

Status FileSystem::ReadFile(const std::string& path, std::string* content) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InputStream> in, Open(path));
    if (PAIMON_UNLIKELY(in == nullptr)) {
        return Status::Invalid("input stream is not suppose to be a null pointer, path: ", path);
    }
    {
        ScopeGuard guard([&in]() -> void {
            Status s = in->Close();
            (void)s;
        });
        PAIMON_ASSIGN_OR_RAISE(int64_t length, in->Length());
        PAIMON_RETURN_NOT_OK(ValidateValueNonNegative(length, "read length"));
        content->resize(static_cast<size_t>(length));
        PAIMON_ASSIGN_OR_RAISE(int64_t read_length, in->Read(content->data(), length));
        if (read_length != length) {
            return Status::IOError(fmt::format("path {}, expect read len {}, actual read len {}",
                                               path, length, read_length));
        }
    }
    return Status::OK();
}

Status FileSystem::WriteFile(const std::string& path, const std::string& content, bool overwrite) {
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<OutputStream> out, Create(path, overwrite));
    if (PAIMON_UNLIKELY(out == nullptr)) {
        return Status::Invalid("output stream is not suppose to be a null pointer, path: ", path);
    }
    {
        ScopeGuard guard([&out]() -> void {
            Status s = out->Close();
            (void)s;
        });
        auto length = static_cast<int64_t>(content.size());
        PAIMON_ASSIGN_OR_RAISE(int64_t write_length, out->Write(content.data(), length));
        if (write_length != length) {
            return Status::IOError(fmt::format("path {}, expect write len {}, actual write len {}",
                                               path, length, write_length));
        }
        PAIMON_RETURN_NOT_OK(out->Flush());
    }
    return Status::OK();
}

Status FileSystem::AtomicStore(const std::string& path, const std::string& content) {
    // do not support overwrite for now
    PAIMON_ASSIGN_OR_RAISE(std::string tmp_file_path, PathUtil::CreateTempPath(path));
    ScopeGuard guard([&]() {
        Status s = Delete(tmp_file_path);
        (void)s;
    });
    PAIMON_RETURN_NOT_OK(WriteFile(tmp_file_path, content, /*overwrite=*/false));
    PAIMON_RETURN_NOT_OK(Rename(tmp_file_path, path));
    guard.Release();
    return Status::OK();
}

}  // namespace paimon
