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

#include "paimon/fs/jindo/jindo_file_system_factory.h"

#include <utility>

#include "JdoConfig.hpp"      // NOLINT(build/include_subdir)
#include "JdoFileSystem.hpp"  // NOLINT(build/include_subdir)
#include "fmt/format.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/factories/factory.h"
#include "paimon/fs/jindo/jindo_file_system.h"
#include "paimon/fs/jindo/jindo_utils.h"
#include "paimon/status.h"

namespace paimon::jindo {

const char JindoFileSystemFactory::IDENTIFIER[] = "jindo";
const char JindoFileSystemFactory::URI_KEY[] = "fs.oss.uri";
const char JindoFileSystemFactory::USER_KEY[] = "fs.oss.user";

Result<std::unique_ptr<FileSystem>> JindoFileSystemFactory::Create(
    const std::string& path, const std::map<std::string, std::string>& options) const {
    auto user_iter = options.find(USER_KEY);
    if (user_iter == options.end()) {
        return Status::Invalid(
            fmt::format("options must have '{}' key in JindoFileSystem", USER_KEY));
    }
    std::shared_ptr<JdoConfig> config = std::make_shared<JdoConfig>();
    for (const auto& [k, v] : options) {
        config->setString(k, v);
    }
    PAIMON_ASSIGN_OR_RAISE(Path new_path, PathUtil::ToPath(path));
    new_path.path = "";  // only use scheme and authority
    config->setString(URI_KEY, new_path.ToString());
    std::unique_ptr<JdoFileSystem> fs = std::make_unique<JdoFileSystem>();
    PAIMON_RETURN_NOT_OK_FROM_JINDO(fs->init(path, user_iter->second, config));
    return std::make_unique<JindoFileSystem>(std::move(fs));
}

REGISTER_PAIMON_FACTORY(JindoFileSystemFactory);

}  // namespace paimon::jindo
