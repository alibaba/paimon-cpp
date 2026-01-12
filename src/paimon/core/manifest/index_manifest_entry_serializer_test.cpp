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

#include "paimon/core/manifest/index_manifest_entry_serializer.h"

#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/manifest/file_kind.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
TEST(IndexManifestEntrySerializerTest, TestSerialize) {
    auto pool = GetDefaultPool();
    auto bytes = std::make_shared<Bytes>("apple", pool.get());
    LinkedHashMap<std::string, DeletionVectorMeta> deletion_vectors_ranges;
    deletion_vectors_ranges.insert_or_assign(
        "my_file_name1", DeletionVectorMeta("my_file_name1", rand(), rand(), std::nullopt));
    deletion_vectors_ranges.insert_or_assign(
        "my_file_name2", DeletionVectorMeta("my_file_name2", rand(), rand(), std::nullopt));
    std::vector<IndexManifestEntry> entries = {
        IndexManifestEntry(
            FileKind::Add(), /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
            /*bucket=*/0,
            std::make_shared<IndexFileMeta>(
                "bsi", "bsi.index", /*file_size=*/110, /*row_count=*/210,
                /*dv_ranges=*/std::nullopt,
                /*external_path=*/std::nullopt,
                GlobalIndexMeta(/*row_range_start=*/41, /*row_range_end=*/71, /*index_field_id=*/2,
                                /*extra_field_ids=*/std::nullopt, /*index_meta=*/nullptr))),
        IndexManifestEntry(
            FileKind::Add(), /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool.get()),
            /*bucket=*/0,
            std::make_shared<IndexFileMeta>(
                "bitmap", "bitmap.index", /*file_size=*/100, /*row_count=*/200,
                /*dv_ranges=*/std::nullopt,
                /*external_path=*/std::optional<std::string>("FILE:/tmp/external/bitmap.index"),
                GlobalIndexMeta(/*row_range_start=*/30, /*row_range_end=*/70, /*index_field_id=*/0,
                                /*extra_field_ids=*/std::optional<std::vector<int32_t>>({3, 4}),
                                /*index_meta=*/bytes))),
        IndexManifestEntry(
            FileKind::Delete(), /*partition=*/BinaryRowGenerator::GenerateRow({30}, pool.get()),
            /*bucket=*/3,
            std::make_shared<IndexFileMeta>("deletion_vector", "dev.index", /*file_size=*/200,
                                            /*row_count=*/20, deletion_vectors_ranges,
                                            /*external_path=*/std::nullopt)),
        IndexManifestEntry(
            FileKind::Add(), /*partition=*/BinaryRowGenerator::GenerateRow({20}, pool.get()),
            /*bucket=*/2,
            std::make_shared<IndexFileMeta>("deletion_vector", "dev.index1", /*file_size=*/210,
                                            /*row_count=*/40, deletion_vectors_ranges,
                                            /*external_path=*/"oss:///tmp"))};
    IndexManifestEntrySerializer serializer(pool);
    for (const auto& entry : entries) {
        ASSERT_OK_AND_ASSIGN(auto row, serializer.ToRow(entry));
        ASSERT_OK_AND_ASSIGN(auto result_entry, serializer.FromRow(row));
        ASSERT_EQ(result_entry, entry);
        ASSERT_EQ(result_entry.ToString(), entry.ToString());
    }
}
}  // namespace paimon::test
