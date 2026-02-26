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

namespace paimon {

/// Metrics to measure clean operation.
class CleanMetrics {
 public:
    static constexpr char CLEAN_DURATION[] = "cleanDuration";
    static constexpr char CLEAN_LIST_DIRECTORIES_DURATION[] = "listDirectoriesDuration";
    static constexpr char CLEAN_LIST_DIRECTORIES[] = "listDirectories";
    static constexpr char CLEAN_LIST_FILE_STATUS_DURATION[] = "listFileStatusDuration";
    static constexpr char CLEAN_LIST_FILE_STATUS_TASKS[] = "listFileStatusTasks";
    static constexpr char CLEAN_LIST_USED_FILES_DURATION[] = "listUsedFilesDuration";
    static constexpr char CLEAN_SNAPSHOT_FILES[] = "snapshotFiles";
    static constexpr char CLEAN_USED_FILES[] = "usedFiles";
    static constexpr char CLEAN_SCAN_ORPHAN_FILES_DURATION[] = "scanOrphanFilesDuration";
    static constexpr char CLEAN_ORPHAN_FILES[] = "orphanFiles";
};

}  // namespace paimon
