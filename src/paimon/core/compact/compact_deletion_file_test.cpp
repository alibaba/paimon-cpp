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

#include "paimon/core/compact/compact_deletion_file.h"

#include <map>
#include <memory>
#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "paimon/core/deletionvectors/bitmap_deletion_vector.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/testing/mock/mock_index_path_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

std::shared_ptr<BucketedDvMaintainer> CreateMaintainer(
    const std::shared_ptr<FileSystem>& fs,
    const std::shared_ptr<MockIndexPathFactory>& path_factory,
    const std::shared_ptr<MemoryPool>& pool,
    const std::map<std::string, std::shared_ptr<DeletionVector>>& deletion_vectors) {
    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);
    return std::make_shared<BucketedDvMaintainer>(dv_index_file, deletion_vectors);
}

class TestGeneratedDeletionFile : public GeneratedDeletionFile {
 public:
    using GeneratedDeletionFile::GeneratedDeletionFile;

    void Clean() override {
        cleaned_ = true;
    }

    bool Cleaned() const {
        return cleaned_;
    }

 private:
    bool cleaned_ = false;
};

class NonGeneratedCompactDeletionFile : public CompactDeletionFile {
 public:
    std::optional<std::shared_ptr<IndexFileMeta>> GetOrCompute() override {
        return std::nullopt;
    }

    Result<std::shared_ptr<CompactDeletionFile>> MergeOldFile(
        const std::shared_ptr<CompactDeletionFile>&) override {
        return Status::Invalid("not used in this test");
    }

    void Clean() override {}
};

}  // namespace

TEST(CompactDeletionFileTest, GenerateFilesShouldReturnFileWhenModified) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    RoaringBitmap32 roaring;
    roaring.Add(1);
    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    deletion_vectors["data-a"] = std::make_shared<BitmapDeletionVector>(roaring);

    auto maintainer = CreateMaintainer(fs, path_factory, pool, deletion_vectors);
    maintainer->RemoveDeletionVectorOf("data-a");

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CompactDeletionFile> generated,
                         CompactDeletionFile::GenerateFiles(maintainer));
    auto file = generated->GetOrCompute();
    ASSERT_TRUE(file.has_value());
    ASSERT_NE(file.value(), nullptr);
    ASSERT_EQ(file.value()->IndexType(), DeletionVectorsIndexFile::DELETION_VECTORS_INDEX);
}

TEST(CompactDeletionFileTest, GenerateFilesShouldReturnNulloptWhenNotModified) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    std::map<std::string, std::shared_ptr<DeletionVector>> deletion_vectors;
    auto maintainer = CreateMaintainer(fs, path_factory, pool, deletion_vectors);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CompactDeletionFile> generated,
                         CompactDeletionFile::GenerateFiles(maintainer));
    auto file = generated->GetOrCompute();
    ASSERT_FALSE(file.has_value());
}

TEST(CompactDeletionFileTest, MergeOldFileShouldRejectNonGeneratedType) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);
    auto meta = std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "index-0", 1, 0,
        LinkedHashMap<std::string, DeletionVectorMeta>(), std::nullopt);

    auto current = std::make_shared<GeneratedDeletionFile>(meta, dv_index_file);
    auto old = std::make_shared<NonGeneratedCompactDeletionFile>();

    ASSERT_NOK_WITH_MSG(current->MergeOldFile(old), "old should be a GeneratedDeletionFile");
}

TEST(CompactDeletionFileTest, MergeOldFileShouldRejectInvokedOldFile) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);
    auto current_meta = std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "index-current", 1, 0,
        LinkedHashMap<std::string, DeletionVectorMeta>(), std::nullopt);
    auto old_meta = std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "index-old", 1, 0,
        LinkedHashMap<std::string, DeletionVectorMeta>(), std::nullopt);

    auto current = std::make_shared<GeneratedDeletionFile>(current_meta, dv_index_file);
    auto old = std::make_shared<GeneratedDeletionFile>(old_meta, dv_index_file);
    ASSERT_TRUE(old->GetOrCompute().has_value());

    ASSERT_NOK_WITH_MSG(current->MergeOldFile(old), "old should not be get");
}

TEST(CompactDeletionFileTest, MergeOldFileShouldReturnOldWhenCurrentIsNull) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);
    auto old_meta = std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "index-old", 1, 0,
        LinkedHashMap<std::string, DeletionVectorMeta>(), std::nullopt);

    auto current = std::make_shared<GeneratedDeletionFile>(nullptr, dv_index_file);
    auto old = std::make_shared<TestGeneratedDeletionFile>(old_meta, dv_index_file);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CompactDeletionFile> merged, current->MergeOldFile(old));
    ASSERT_EQ(merged.get(), old.get());
    ASSERT_FALSE(old->Cleaned());
}

TEST(CompactDeletionFileTest, MergeOldFileShouldCleanOldAndReturnCurrent) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);
    auto current_meta = std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "index-current", 1, 0,
        LinkedHashMap<std::string, DeletionVectorMeta>(), std::nullopt);
    auto old_meta = std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "index-old", 1, 0,
        LinkedHashMap<std::string, DeletionVectorMeta>(), std::nullopt);

    auto current = std::make_shared<GeneratedDeletionFile>(current_meta, dv_index_file);
    auto old = std::make_shared<TestGeneratedDeletionFile>(old_meta, dv_index_file);

    ASSERT_OK_AND_ASSIGN(std::shared_ptr<CompactDeletionFile> merged, current->MergeOldFile(old));
    ASSERT_EQ(merged.get(), current.get());
    ASSERT_TRUE(old->Cleaned());
}

TEST(CompactDeletionFileTest, CleanShouldDeleteIndexFile) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<FileSystem> fs,
                         FileSystemFactory::Get("local", dir->Str(), {}));
    auto path_factory = std::make_shared<MockIndexPathFactory>(dir->Str());
    auto pool = GetDefaultPool();

    auto dv_index_file =
        std::make_shared<DeletionVectorsIndexFile>(fs, path_factory, /*bitmap64=*/false, pool);

    auto file_meta = std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "index-clean-target", 1, 0,
        LinkedHashMap<std::string, DeletionVectorMeta>(), std::nullopt);
    const std::string file_path = PathUtil::JoinPath(dir->Str(), file_meta->FileName());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<OutputStream> out,
                         fs->Create(file_path, /*overwrite=*/true));
    ASSERT_OK(out->Close());

    ASSERT_OK_AND_ASSIGN(bool exists_before, dv_index_file->Exists(file_meta));
    ASSERT_TRUE(exists_before);

    GeneratedDeletionFile generated(file_meta, dv_index_file);
    generated.Clean();

    ASSERT_OK_AND_ASSIGN(bool exists_after, dv_index_file->Exists(file_meta));
    ASSERT_FALSE(exists_after);
}

}  // namespace paimon::test
