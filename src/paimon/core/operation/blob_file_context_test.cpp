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

#include "paimon/core/operation/blob_file_context.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/data/blob.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {

class BlobFileContextTest : public ::testing::Test {
 protected:
    static std::shared_ptr<arrow::Schema> MakeSchema(const std::vector<std::string>& normal_fields,
                                                     const std::vector<std::string>& blob_fields) {
        std::vector<std::shared_ptr<arrow::Field>> fields;
        for (const auto& name : normal_fields) {
            fields.push_back(arrow::field(name, arrow::int32()));
        }
        for (const auto& name : blob_fields) {
            fields.push_back(BlobUtils::ToArrowField(name));
        }
        return arrow::schema(fields);
    }
};

TEST_F(BlobFileContextTest, NoBlobFields) {
    auto schema = MakeSchema({"id", "name"}, {});
    std::map<std::string, std::string> opts_map;
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(opts_map));
    auto context = BlobFileContext::Create(schema, options);
    ASSERT_FALSE(context);
}

TEST_F(BlobFileContextTest, AllInlineNoExternalStorage) {
    auto schema = MakeSchema({"id"}, {"image", "video"});
    std::map<std::string, std::string> opts_map = {
        {Options::BLOB_DESCRIPTOR_FIELD, "image"},
        {Options::BLOB_VIEW_FIELD, "video"},
    };
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(opts_map));
    auto context = BlobFileContext::Create(schema, options);
    // All blobs are inline but context is still valid for callers to query inline fields
    ASSERT_TRUE(context);
    ASSERT_EQ(context->GetInlineFields(), std::set<std::string>({"image", "video"}));
    ASSERT_TRUE(context->GetBlobFileFields().empty());
    ASSERT_FALSE(context->RequireBlobFileWriter());
    ASSERT_FALSE(context->RequireExternalStorageWriter());
}

TEST_F(BlobFileContextTest, MixedInlineAndBlobFile) {
    auto schema = MakeSchema({"id"}, {"image", "video", "audio"});
    std::map<std::string, std::string> opts_map = {
        {Options::BLOB_DESCRIPTOR_FIELD, "image"},
        // video and audio are not configured as inline -> go to .blob files
    };
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(opts_map));
    auto context = BlobFileContext::Create(schema, options);
    ASSERT_TRUE(context);

    // descriptor fields
    ASSERT_EQ(context->GetDescriptorFields(), std::set<std::string>({"image"}));

    // view fields (none configured)
    ASSERT_TRUE(context->GetViewFields().empty());

    // inline = descriptor ∪ view
    ASSERT_EQ(context->GetInlineFields(), std::set<std::string>({"image"}));

    // blob file fields = non-inline blob fields
    ASSERT_EQ(context->GetBlobFileFields(), std::set<std::string>({"video", "audio"}));

    // Query methods
    ASSERT_TRUE(context->IsInlineField("image"));
    ASSERT_TRUE(context->IsDescriptorField("image"));
    ASSERT_FALSE(context->IsViewField("image"));
    ASSERT_FALSE(context->IsBlobFileField("image"));

    ASSERT_FALSE(context->IsInlineField("video"));
    ASSERT_TRUE(context->IsBlobFileField("video"));

    ASSERT_FALSE(context->IsInlineField("audio"));
    ASSERT_TRUE(context->IsBlobFileField("audio"));

    // Requires blob file writer for video and audio
    ASSERT_TRUE(context->RequireBlobFileWriter());
    ASSERT_FALSE(context->RequireExternalStorageWriter());
}

TEST_F(BlobFileContextTest, ExternalStorageFields) {
    auto schema = MakeSchema({"id"}, {"image", "video"});
    std::map<std::string, std::string> opts_map = {
        {Options::BLOB_DESCRIPTOR_FIELD, "image,video"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "image"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, "oss://bucket/blob/"},
    };
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(opts_map));
    auto context = BlobFileContext::Create(schema, options);
    ASSERT_TRUE(context);

    ASSERT_EQ(context->GetDescriptorFields(), std::set<std::string>({"image", "video"}));
    ASSERT_EQ(context->GetInlineFields(), std::set<std::string>({"image", "video"}));
    ASSERT_EQ(context->GetExternalStorageFields(), std::set<std::string>({"image"}));
    ASSERT_TRUE(context->GetExternalStoragePath());
    ASSERT_EQ(context->GetExternalStoragePath(), "oss://bucket/blob/");
    ASSERT_TRUE(context->GetBlobFileFields().empty());

    ASSERT_TRUE(context->IsExternalStorageField("image"));
    ASSERT_FALSE(context->IsExternalStorageField("video"));

    ASSERT_FALSE(context->RequireBlobFileWriter());
    ASSERT_TRUE(context->RequireExternalStorageWriter());
}

TEST_F(BlobFileContextTest, ViewFields) {
    auto schema = MakeSchema({"id"}, {"ref_image", "raw_blob"});
    std::map<std::string, std::string> opts_map = {
        {Options::BLOB_VIEW_FIELD, "ref_image"},
        // raw_blob not configured -> goes to .blob file
    };
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(opts_map));
    auto context = BlobFileContext::Create(schema, options);
    ASSERT_TRUE(context);

    ASSERT_TRUE(context->GetDescriptorFields().empty());
    ASSERT_EQ(context->GetViewFields(), std::set<std::string>({"ref_image"}));
    ASSERT_EQ(context->GetInlineFields(), std::set<std::string>({"ref_image"}));
    ASSERT_EQ(context->GetBlobFileFields(), std::set<std::string>({"raw_blob"}));

    ASSERT_TRUE(context->IsInlineField("ref_image"));
    ASSERT_TRUE(context->IsViewField("ref_image"));
    ASSERT_FALSE(context->IsDescriptorField("ref_image"));

    ASSERT_TRUE(context->RequireBlobFileWriter());
    ASSERT_FALSE(context->RequireExternalStorageWriter());
}

TEST_F(BlobFileContextTest, DescriptorAndViewTogether) {
    auto schema = MakeSchema({"id"}, {"desc_blob", "view_blob", "normal_blob"});
    std::map<std::string, std::string> opts_map = {
        {Options::BLOB_DESCRIPTOR_FIELD, "desc_blob"},
        {Options::BLOB_VIEW_FIELD, "view_blob"},
        {Options::BLOB_EXTERNAL_STORAGE_FIELD, "desc_blob"},
        {Options::BLOB_EXTERNAL_STORAGE_PATH, "/tmp/ext/"},
    };
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(opts_map));
    auto context = BlobFileContext::Create(schema, options);
    ASSERT_TRUE(context);

    ASSERT_EQ(context->GetDescriptorFields(), std::set<std::string>({"desc_blob"}));
    ASSERT_EQ(context->GetViewFields(), std::set<std::string>({"view_blob"}));
    ASSERT_EQ(context->GetInlineFields(), std::set<std::string>({"desc_blob", "view_blob"}));
    ASSERT_EQ(context->GetExternalStorageFields(), std::set<std::string>({"desc_blob"}));
    ASSERT_TRUE(context->GetExternalStoragePath());
    ASSERT_EQ(context->GetExternalStoragePath(), "/tmp/ext/");
    ASSERT_EQ(context->GetBlobFileFields(), std::set<std::string>({"normal_blob"}));

    ASSERT_TRUE(context->IsDescriptorField("desc_blob"));
    ASSERT_TRUE(context->IsExternalStorageField("desc_blob"));
    ASSERT_TRUE(context->IsInlineField("desc_blob"));
    ASSERT_FALSE(context->IsBlobFileField("desc_blob"));

    ASSERT_TRUE(context->IsViewField("view_blob"));
    ASSERT_TRUE(context->IsInlineField("view_blob"));
    ASSERT_FALSE(context->IsDescriptorField("view_blob"));

    ASSERT_FALSE(context->IsInlineField("normal_blob"));
    ASSERT_TRUE(context->IsBlobFileField("normal_blob"));

    // Non-existent field
    ASSERT_FALSE(context->IsInlineField("not_exist"));
    ASSERT_FALSE(context->IsBlobFileField("not_exist"));

    ASSERT_TRUE(context->RequireBlobFileWriter());
    ASSERT_TRUE(context->RequireExternalStorageWriter());
}

}  // namespace paimon
