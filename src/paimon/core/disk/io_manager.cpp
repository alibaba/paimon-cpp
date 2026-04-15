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
#include "paimon/disk/io_manager.h"

#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/uuid.h"

namespace paimon {
class IOManagerImpl : public IOManager {
 public:
    explicit IOManagerImpl(const std::string& tmp_dir) : tmp_dir_(tmp_dir) {}

    const std::string& GetTempDir() const override {
        return tmp_dir_;
    }

    Result<std::string> GenerateTempFilePath(const std::string& prefix) const override {
        std::string uuid;
        if (!UUID::Generate(&uuid)) {
            return Status::Invalid("generate uuid for io manager tmp path failed.");
        }
        return PathUtil::JoinPath(tmp_dir_, prefix + "-" + uuid + std::string(kSuffix));
    }

 private:
    static constexpr char kSuffix[] = ".channel";
    std::string tmp_dir_;
};

std::unique_ptr<IOManager> IOManager::Create(const std::string& tmp_dir) {
    return std::make_unique<IOManagerImpl>(tmp_dir);
}

}  // namespace paimon
