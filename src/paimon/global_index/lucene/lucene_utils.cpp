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
#include "paimon/global_index/lucene/lucene_utils.h"
namespace paimon::lucene {

Result<std::string> LuceneUtils::GetJiebaDictionaryDir() {
    const char* env_dir = std::getenv(kJiebaDictDirEnv);
    if (env_dir && *env_dir != '\0') {
        return std::string(env_dir);
    }
#ifdef JIEBA_TEST_DICT_DIR
    return std::string(JIEBA_TEST_DICT_DIR);
#endif
    return Status::Invalid(
        fmt::format("cannot get dictionary dir for jieba, must set env {}", kJiebaDictDirEnv));
}

}  // namespace paimon::lucene
