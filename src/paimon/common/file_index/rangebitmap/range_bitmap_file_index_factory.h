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

#include <map>
#include <memory>
#include <string>

#include "paimon/file_index/file_indexer.h"
#include "paimon/file_index/file_indexer_factory.h"
#include "paimon/result.h"

namespace paimon {

class PAIMON_EXPORT RangeBitmapFileIndexFactory final : public FileIndexerFactory {
 public:
    const char* Identifier() const override {
        return "range-bitmap";
    }

    ~RangeBitmapFileIndexFactory() override;

    Result<std::unique_ptr<FileIndexer>> Create(
        const std::map<std::string, std::string>& options) const override;
};

}  // namespace paimon
