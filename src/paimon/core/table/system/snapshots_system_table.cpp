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

#include "paimon/core/table/system/snapshots_system_table.h"

#include <algorithm>
#include <utility>

#include "arrow/api.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/utils/snapshot_manager.h"

namespace paimon {

SnapshotsSystemTable::SnapshotsSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                           std::string branch)
    : InMemorySystemTable(table_path),
      context_(SystemTableUtils::CreateContext(std::move(fs), std::move(table_path),
                                               std::move(branch))) {}

std::string SnapshotsSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> SnapshotsSystemTable::ArrowSchema() const {
    return arrow::schema({
        arrow::field("snapshot_id", arrow::int64(), /*nullable=*/false),
        arrow::field("schema_id", arrow::int64(), /*nullable=*/false),
        arrow::field("commit_user", arrow::utf8(), /*nullable=*/false),
        arrow::field("commit_identifier", arrow::int64(), /*nullable=*/false),
        arrow::field("commit_kind", arrow::utf8(), /*nullable=*/false),
        arrow::field("commit_time", arrow::timestamp(arrow::TimeUnit::MILLI),
                     /*nullable=*/false),
        arrow::field("base_manifest_list", arrow::utf8(), /*nullable=*/false),
        arrow::field("delta_manifest_list", arrow::utf8(), /*nullable=*/false),
        arrow::field("changelog_manifest_list", arrow::utf8(), /*nullable=*/true),
        arrow::field("total_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("delta_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("changelog_record_count", arrow::int64(), /*nullable=*/true),
        arrow::field("watermark", arrow::int64(), /*nullable=*/true),
        arrow::field("next_row_id", arrow::int64(), /*nullable=*/true),
    });
}

Result<std::vector<GenericRow>> SnapshotsSystemTable::BuildRows() const {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> schema, ArrowSchema());
    SnapshotManager snapshot_manager(context_.fs, context_.table_path, context_.branch);
    PAIMON_ASSIGN_OR_RAISE(std::vector<Snapshot> snapshots, snapshot_manager.GetAllSnapshots());
    std::sort(snapshots.begin(), snapshots.end(),
              [](const Snapshot& lhs, const Snapshot& rhs) { return lhs.Id() < rhs.Id(); });
    std::vector<GenericRow> rows;
    rows.reserve(snapshots.size());

    for (const auto& snapshot : snapshots) {
        GenericRow row(schema->num_fields());
        row.SetField(0, snapshot.Id());
        row.SetField(1, snapshot.SchemaId());
        row.SetField(2, SystemTableUtils::StringValue(snapshot.CommitUser()));
        row.SetField(3, snapshot.CommitIdentifier());
        row.SetField(4, SystemTableUtils::StringValue(
                            Snapshot::CommitKind::ToString(snapshot.GetCommitKind())));
        PAIMON_ASSIGN_OR_RAISE(VariantType commit_time,
                               SystemTableUtils::LocalTimestampMillisValue(snapshot.TimeMillis()));
        row.SetField(5, commit_time);
        row.SetField(6, SystemTableUtils::StringValue(snapshot.BaseManifestList()));
        row.SetField(7, SystemTableUtils::StringValue(snapshot.DeltaManifestList()));
        row.SetField(8, SystemTableUtils::OptionalStringValue(snapshot.ChangelogManifestList()));
        row.SetField(9, SystemTableUtils::OptionalInt64Value(snapshot.TotalRecordCount()));
        row.SetField(10, SystemTableUtils::OptionalInt64Value(snapshot.DeltaRecordCount()));
        row.SetField(11, SystemTableUtils::OptionalInt64Value(snapshot.ChangelogRecordCount()));
        row.SetField(12, SystemTableUtils::OptionalInt64Value(snapshot.Watermark()));
        row.SetField(13, SystemTableUtils::OptionalInt64Value(snapshot.NextRowId()));
        rows.push_back(std::move(row));
    }

    return rows;
}

}  // namespace paimon
