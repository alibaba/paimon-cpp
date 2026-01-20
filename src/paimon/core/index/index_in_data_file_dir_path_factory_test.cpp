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

#include "paimon/core/index/index_in_data_file_dir_path_factory.h"

#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(IndexInDataFileDirPathFactoryTest, TestSimple) {
    auto count = std::make_shared<std::atomic<int32_t>>();
    count->store(0);

    auto data_file_path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(data_file_path_factory->Init(/*parent=*/"/tmp/p0=1/p1=0/bucket-0",
                                           /*format_identifier=*/"txt",
                                           /*data_file_prefix=*/"data-",
                                           /*external_path_provider=*/nullptr));
    IndexInDataFileDirPathFactory factory(/*uuid=*/"uuid", count,
                                          std::move(data_file_path_factory));

    ASSERT_EQ(factory.NewPath(), "/tmp/p0=1/p1=0/bucket-0/index-uuid-0");
    // test ToPath with IndexFileMeta
    auto meta =
        std::make_shared<IndexFileMeta>(/*index_type=*/"DELETION_VECTOR", "deletion_file",
                                        /*file_size=*/500, /*row_count=*/1,
                                        /*dv_ranges=*/std::nullopt, /*external_path=*/std::nullopt);
    ASSERT_EQ(factory.ToPath(meta), "/tmp/p0=1/p1=0/bucket-0/deletion_file");

    // test ToPath with file_name
    ASSERT_EQ(factory.ToPath("bitmap.index"), "/tmp/p0=1/p1=0/bucket-0/bitmap.index");
    // test external path
    ASSERT_FALSE(factory.IsExternalPath());
}

TEST(IndexInDataFileDirPathFactoryTest, TestWithExternalPath) {
    auto count = std::make_shared<std::atomic<int32_t>>();
    count->store(1);

    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<ExternalPathProvider> external_path_provider,
        ExternalPathProvider::Create({"/tmp/external_path/"}, "p0=1/p1=0/bucket-0"));

    auto data_file_path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(
        data_file_path_factory->Init(/*parent=*/"/tmp", /*format_identifier=*/"txt",
                                     /*data_file_prefix=*/"data-",
                                     /*external_path_provider=*/std::move(external_path_provider)));
    IndexInDataFileDirPathFactory factory(/*uuid=*/"uuid", count,
                                          std::move(data_file_path_factory));
    std::string external_path = "/tmp/external_path/p0=1/p1=0/bucket-0/index-uuid-1";
    ASSERT_EQ(factory.NewPath(), external_path);
    auto meta = std::make_shared<IndexFileMeta>(
        /*index_type=*/"DELETION_VECTOR", "deletion_file",
        /*file_size=*/500, /*row_count=*/1,
        /*dv_ranges=*/std::nullopt, external_path);
    ASSERT_EQ(factory.ToPath(meta), external_path);
    ASSERT_TRUE(factory.IsExternalPath());
}

}  // namespace paimon::test
