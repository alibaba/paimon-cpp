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

/// Metrics to measure scan operation.
class ScanMetrics {
 public:
    static constexpr char LAST_SCAN_DURATION[] = "lastScanDuration";
    static constexpr char LAST_SCANNED_SNAPSHOT_ID[] = "lastScannedSnapshotId";
    static constexpr char LAST_SCANNED_MANIFESTS[] = "lastScannedManifests";
    static constexpr char LAST_SCAN_SKIPPED_TABLE_FILES[] = "lastScanSkippedTableFiles";
    static constexpr char LAST_SCAN_RESULTED_TABLE_FILES[] = "lastScanResultedTableFiles";
};

}  // namespace paimon
