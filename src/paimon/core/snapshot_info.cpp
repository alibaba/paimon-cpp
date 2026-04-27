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

#include "paimon/snapshot/snapshot_info.h"

namespace paimon {

std::string SnapshotInfo::CommitKindToString(CommitKind kind) {
    switch (kind) {
        case CommitKind::APPEND:
            return "APPEND";
        case CommitKind::COMPACT:
            return "COMPACT";
        case CommitKind::OVERWRITE:
            return "OVERWRITE";
        case CommitKind::ANALYZE:
            return "ANALYZE";
        case CommitKind::UNKNOWN:
            return "UNKNOWN";
    }
    return "UNKNOWN";
}

}  // namespace paimon
