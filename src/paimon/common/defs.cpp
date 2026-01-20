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

#include "paimon/defs.h"

namespace paimon {

const char Options::FIELDS_SEPARATOR[] = ",";
const char Options::FIELDS_PREFIX[] = "fields";
const char Options::AGG_FUNCTION[] = "aggregate-function";
const char Options::DEFAULT_AGG_FUNCTION[] = "default-aggregate-function";
const char Options::IGNORE_RETRACT[] = "ignore-retract";
const char Options::SEQUENCE_GROUP[] = "sequence-group";

const char Options::BUCKET[] = "bucket";
const char Options::BUCKET_KEY[] = "bucket-key";
const char Options::FILE_FORMAT[] = "file.format";
const char Options::FILE_SYSTEM[] = "file-system";
const char Options::TARGET_FILE_SIZE[] = "target-file-size";
const char Options::BLOB_TARGET_FILE_SIZE[] = "blob.target-file-size";
const char Options::PAGE_SIZE[] = "page-size";
const char Options::PARTITION_DEFAULT_NAME[] = "partition.default-name";
const char Options::FILE_COMPRESSION[] = "file.compression";
const char Options::FILE_COMPRESSION_ZSTD_LEVEL[] = "file.compression.zstd-level";
const char Options::MANIFEST_TARGET_FILE_SIZE[] = "manifest.target-file-size";
const char Options::MANIFEST_FORMAT[] = "manifest.format";
const char Options::MANIFEST_COMPRESSION[] = "manifest.compression";
const char Options::MANIFEST_MERGE_MIN_COUNT[] = "manifest.merge-min-count";
const char Options::MANIFEST_FULL_COMPACTION_FILE_SIZE[] =
    "manifest.full-compaction-threshold-size";
const char Options::SOURCE_SPLIT_TARGET_SIZE[] = "source.split.target-size";
const char Options::SOURCE_SPLIT_OPEN_FILE_COST[] = "source.split.open-file-cost";
const char Options::SCAN_SNAPSHOT_ID[] = "scan.snapshot-id";
const char Options::SCAN_MODE[] = "scan.mode";
const char Options::READ_BATCH_SIZE[] = "read.batch-size";
const char Options::WRITE_BATCH_SIZE[] = "write.batch-size";
const char Options::WRITE_BUFFER_SIZE[] = "write-buffer-size";
const char Options::SNAPSHOT_NUM_RETAINED_MIN[] = "snapshot.num-retained.min";
const char Options::SNAPSHOT_NUM_RETAINED_MAX[] = "snapshot.num-retained.max";
const char Options::SNAPSHOT_TIME_RETAINED[] = "snapshot.time-retained";
const char Options::SNAPSHOT_EXPIRE_LIMIT[] = "snapshot.expire.limit";
const char Options::SNAPSHOT_CLEAN_EMPTY_DIRECTORIES[] = "snapshot.clean-empty-directories";
const char Options::COMMIT_TIMEOUT[] = "commit.timeout";
const char Options::COMMIT_MAX_RETRIES[] = "commit.max-retries";
const char Options::SEQUENCE_FIELD[] = "sequence.field";
const char Options::SEQUENCE_FIELD_SORT_ORDER[] = "sequence.field.sort-order";
const char Options::MERGE_ENGINE[] = "merge-engine";
const char Options::SORT_ENGINE[] = "sort-engine";
const char Options::IGNORE_DELETE[] = "ignore-delete";
const char Options::FIELDS_DEFAULT_AGG_FUNC[] = "fields.default-aggregate-function";
const char Options::DELETION_VECTORS_ENABLED[] = "deletion-vectors.enabled";
const char Options::CHANGELOG_PRODUCER[] = "changelog-producer";
const char Options::FORCE_LOOKUP[] = "force-lookup";
const char Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE[] =
    "partial-update.remove-record-on-delete";
const char Options::PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP[] =
    "partial-update.remove-record-on-sequence-group";
const char Options::SCAN_FALLBACK_BRANCH[] = "scan.fallback-branch";
const char Options::BRANCH[] = "branch";
const char Options::FILE_INDEX_READ_ENABLED[] = "file-index.read.enabled";
const char Options::DATA_FILE_EXTERNAL_PATHS[] = "data-file.external-paths";
const char Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY[] = "data-file.external-paths.strategy";
const char Options::DATA_FILE_PREFIX[] = "data-file.prefix";
const char Options::INDEX_FILE_IN_DATA_FILE_DIR[] = "index-file-in-data-file-dir";
const char Options::ROW_TRACKING_ENABLED[] = "row-tracking.enabled";
const char Options::DATA_EVOLUTION_ENABLED[] = "data-evolution.enabled";
const char Options::PARTITION_GENERATE_LEGACY_NAME[] = "partition.legacy-name";
const char Options::BLOB_AS_DESCRIPTOR[] = "blob-as-descriptor";
const char Options::GLOBAL_INDEX_ENABLED[] = "global-index.enabled";
const char Options::GLOBAL_INDEX_EXTERNAL_PATH[] = "global-index.external-path";
}  // namespace paimon
