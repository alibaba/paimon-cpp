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

#include <memory>

#include "paimon/core/compact/compact_deletion_file.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"

namespace paimon {

// Changes to commit.
class CommitIncrement {
 public:
    CommitIncrement(const DataIncrement& data_increment, const CompactIncrement& compact_increment,
                    const std::shared_ptr<CompactDeletionFile>& compact_deletion_file)
        : data_increment_(data_increment),
          compact_increment_(compact_increment),
          compact_deletion_file_(compact_deletion_file) {}

    const DataIncrement& GetNewFilesIncrement() const {
        return data_increment_;
    }

    const CompactIncrement& GetCompactIncrement() const {
        return compact_increment_;
    }

    std::shared_ptr<CompactDeletionFile> GetCompactDeletionFile() const {
        return compact_deletion_file_;
    }

 private:
    DataIncrement data_increment_;
    CompactIncrement compact_increment_;
    std::shared_ptr<CompactDeletionFile> compact_deletion_file_;
};

}  // namespace paimon
