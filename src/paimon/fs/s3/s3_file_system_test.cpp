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

#include "paimon/fs/s3/s3_file_system.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <utility>
#include <vector>

#include "paimon/common/utils/string_utils.h"
#include "paimon/fs/s3/s3_file_system_factory.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::s3 {
namespace {

class MockHttpClient : public HttpClient {
 public:
    Result<HttpResponse> Execute(const HttpRequest& request,
                                 const HttpBodyConsumer& consumer) const override {
        request_ = request;
        HttpResponse response;
        response.status_code = status_code_;
        response.headers = response_headers_;
        if (!body_.empty()) {
            PAIMON_RETURN_NOT_OK(consumer(body_.data(), body_.size()));
            response.body_size = body_.size();
        }
        return response;
    }

    mutable HttpRequest request_;
    int32_t status_code_ = 200;
    HttpHeaders response_headers_;
    std::string body_;
};

class ScopedEnvironmentVariable {
 public:
    ScopedEnvironmentVariable(const char* name, std::optional<std::string> value) : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            previous_ = previous;
        }
        if (value) {
            setenv(name, value->c_str(), 1);
        } else {
            unsetenv(name);
        }
    }

    ~ScopedEnvironmentVariable() {
        if (previous_) {
            setenv(name_.c_str(), previous_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

 private:
    std::string name_;
    std::optional<std::string> previous_;
};

std::map<std::string, std::string> StaticOptions() {
    return {{kS3AccessKeyOption, "access"},
            {kS3SecretKeyOption, "secret"},
            {kS3SessionTokenOption, "token"},
            {kS3RegionOption, "ap-northeast-2"}};
}

const std::string* FindHeader(const HttpHeaders& headers, const std::string& name) {
    std::string normalized_name = StringUtils::ToLowerCase(name);
    for (const auto& [header_name, value] : headers) {
        if (StringUtils::ToLowerCase(header_name) == normalized_name) {
            return &value;
        }
    }
    return nullptr;
}

TEST(S3ObjectStoreClientTest, TestHeadAndSigning) {
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(StaticOptions(), http));
    ASSERT_OK_AND_ASSIGN(auto metadata, client->HeadObject({"bucket", "a b/file"}));
    ASSERT_EQ(metadata.size, 12);
    ASSERT_EQ(http->request_.url, "https://bucket.s3.ap-northeast-2.amazonaws.com/a%20b/file");
    const std::string* authorization = FindHeader(http->request_.headers, "authorization");
    ASSERT_NE(authorization, nullptr);
    ASSERT_NE(authorization->find("/ap-northeast-2/s3/aws4_request"), std::string::npos);
    ASSERT_NE(authorization->find(
                  "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token"),
              std::string::npos);
    ASSERT_EQ(http->request_.headers["host"], "bucket.s3.ap-northeast-2.amazonaws.com");
    const std::string* session_token = FindHeader(http->request_.headers, "x-amz-security-token");
    ASSERT_NE(session_token, nullptr);
    ASSERT_EQ(*session_token, "token");
}

TEST(S3ObjectStoreClientTest, TestDottedBucketUsesPathStyleForDefaultEndpoint) {
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(StaticOptions(), http));
    ASSERT_OK(client->HeadObject({"paimon.prod.data", "file"}));
    ASSERT_EQ(http->request_.url, "https://s3.ap-northeast-2.amazonaws.com/paimon.prod.data/file");
    ASSERT_EQ(http->request_.headers["host"], "s3.ap-northeast-2.amazonaws.com");

    auto path_style_options = StaticOptions();
    path_style_options[kS3PathStyleAccessOption] = "false";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> path_style_client,
                         MakeS3ObjectStoreClient(path_style_options, http));
    ASSERT_OK(path_style_client->HeadObject({"paimon.prod.data", "file"}));
    ASSERT_EQ(http->request_.url, "https://s3.ap-northeast-2.amazonaws.com/paimon.prod.data/file");
}

TEST(S3ObjectStoreClientTest, TestCustomEndpointAddressing) {
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";

    auto https_options = StaticOptions();
    https_options[kS3EndpointOption] = "https://s3.example.com";
    https_options[kS3PathStyleAccessOption] = "false";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> https_client,
                         MakeS3ObjectStoreClient(https_options, http));
    ASSERT_OK(https_client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(http->request_.url, "https://s3.example.com/bucket/file");
    ASSERT_OK(https_client->HeadObject({"paimon.prod.data", "file"}));
    ASSERT_EQ(http->request_.url, "https://s3.example.com/paimon.prod.data/file");

    auto http_options = StaticOptions();
    http_options[kS3EndpointOption] = "http://s3.example.com";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> http_client,
                         MakeS3ObjectStoreClient(http_options, http));
    ASSERT_OK(http_client->HeadObject({"paimon.prod.data", "file"}));
    ASSERT_EQ(http->request_.url, "http://paimon.prod.data.s3.example.com/file");

    auto ip_options = StaticOptions();
    ip_options[kS3EndpointOption] = "http://127.0.0.1:9000";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> ip_client,
                         MakeS3ObjectStoreClient(ip_options, http));
    ASSERT_OK(ip_client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(http->request_.url, "http://127.0.0.1:9000/bucket/file");

    auto base_path_options = StaticOptions();
    base_path_options[kS3EndpointOption] = "HTTPS://s3.example.com/storage";
    base_path_options[kS3PathStyleAccessOption] = "true";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> base_path_client,
                         MakeS3ObjectStoreClient(base_path_options, http));
    ASSERT_OK(base_path_client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(http->request_.url, "https://s3.example.com/storage/bucket/file");
}

TEST(S3ObjectStoreClientTest, TestInvalidCustomEndpoint) {
    for (const char* endpoint :
         {"ftp://s3.example.com", "https://user@s3.example.com", "https://s3.example.com?query",
          "https://s3.example.com#fragment"}) {
        auto options = StaticOptions();
        options[kS3EndpointOption] = endpoint;
        ASSERT_NOK(MakeS3ObjectStoreClient(options, std::make_shared<MockHttpClient>()));
    }
}

TEST(S3ObjectStoreClientTest, TestNonVirtualHostableBucketUsesPathStyle) {
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(StaticOptions(), http));
    ASSERT_OK(client->HeadObject({"aa", "file"}));
    ASSERT_EQ(http->request_.url, "https://s3.ap-northeast-2.amazonaws.com/aa/file");

    auto options = StaticOptions();
    options[kS3EndpointOption] = "http://s3.example.com";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> custom_client,
                         MakeS3ObjectStoreClient(options, http));
    ASSERT_OK(custom_client->HeadObject({"BucketName", "file"}));
    ASSERT_EQ(http->request_.url, "http://s3.example.com/BucketName/file");
}

TEST(S3ObjectStoreClientTest, TestPathStyleOptionUsesCommonBooleanParser) {
    for (const auto& [value, path_style] :
         std::vector<std::pair<std::string, bool>>{{"t", true},
                                                   {"y", true},
                                                   {"yes", true},
                                                   {"1", true},
                                                   {"f", false},
                                                   {"n", false},
                                                   {"no", false},
                                                   {"0", false}}) {
        auto options = StaticOptions();
        options[kS3PathStyleAccessOption] = value;
        auto http = std::make_shared<MockHttpClient>();
        http->response_headers_["content-length"] = "12";
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                             MakeS3ObjectStoreClient(options, http));
        ASSERT_OK(client->HeadObject({"bucket", "file"}));
        ASSERT_EQ(http->request_.url, path_style
                                          ? "https://s3.ap-northeast-2.amazonaws.com/bucket/file"
                                          : "https://bucket.s3.ap-northeast-2.amazonaws.com/file");
    }
}

TEST(S3ObjectStoreClientTest, TestOptionAliases) {
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    std::map<std::string, std::string> python_options{
        {"fs.s3.accessKeyId", "python-access"},  {"fs.s3.accessKeySecret", "python-secret"},
        {"fs.s3.securityToken", "python-token"}, {"fs.s3.endpoint", "http://s3.example.com"},
        {"fs.s3.region", "us-west-2"},           {"fs.s3.path.style.access", "true"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> python_client,
                         MakeS3ObjectStoreClient(python_options, http));
    ASSERT_OK(python_client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(http->request_.url, "http://s3.example.com/bucket/file");
    const std::string* authorization = FindHeader(http->request_.headers, "authorization");
    ASSERT_NE(authorization, nullptr);
    ASSERT_NE(authorization->find("Credential=python-access/"), std::string::npos);
    const std::string* token = FindHeader(http->request_.headers, "x-amz-security-token");
    ASSERT_NE(token, nullptr);
    ASSERT_EQ(*token, "python-token");

    std::map<std::string, std::string> java_options{{"s3a.access.key", "java-access"},
                                                    {"s3a.secret.key", "java-secret"},
                                                    {"s3a.session.token", "java-token"},
                                                    {"s3a.region", "eu-west-1"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> java_client,
                         MakeS3ObjectStoreClient(java_options, http));
    ASSERT_OK(java_client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(http->request_.url, "https://bucket.s3.eu-west-1.amazonaws.com/file");
    authorization = FindHeader(http->request_.headers, "authorization");
    ASSERT_NE(authorization, nullptr);
    ASSERT_NE(authorization->find("Credential=java-access/"), std::string::npos);
    token = FindHeader(http->request_.headers, "x-amz-security-token");
    ASSERT_NE(token, nullptr);
    ASSERT_EQ(*token, "java-token");
}

TEST(S3ObjectStoreClientTest, TestCanonicalOptionsTakePrecedenceOverAliases) {
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    std::map<std::string, std::string> options{{kS3AccessKeyOption, "canonical-access"},
                                               {kS3SecretKeyOption, "canonical-secret"},
                                               {"s3a.access.key", "alias-access"},
                                               {"s3a.secret.key", "alias-secret"},
                                               {kS3RegionOption, "ap-northeast-2"}};
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(options, http));
    ASSERT_OK(client->HeadObject({"bucket", "file"}));
    const std::string* authorization = FindHeader(http->request_.headers, "authorization");
    ASSERT_NE(authorization, nullptr);
    ASSERT_NE(authorization->find("Credential=canonical-access/"), std::string::npos);
}

TEST(S3ObjectStoreClientTest, TestInvalidContentLength) {
    for (const std::string content_length :
         {"", "invalid", "-1", "12abc", "999999999999999999999999"}) {
        auto http = std::make_shared<MockHttpClient>();
        http->response_headers_["content-length"] = content_length;
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                             MakeS3ObjectStoreClient(StaticOptions(), http));
        ASSERT_TRUE(client->HeadObject({"bucket", "file"}).status().IsIOError());
    }
}

TEST(S3ObjectStoreClientTest, TestInvalidModificationTime) {
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    http->response_headers_["last-modified"] = "invalid";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(StaticOptions(), http));
    ASSERT_OK_AND_ASSIGN(auto metadata, client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(metadata.modification_time, 0);

    http->body_ =
        "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>file</Key>"
        "<LastModified>invalid</LastModified><Size>12</Size></Contents></ListBucketResult>";
    ASSERT_OK_AND_ASSIGN(auto result, client->ListObjects({"bucket", ""}, "", 0));
    ASSERT_EQ(result.objects[0].modification_time, 0);
}

TEST(S3ObjectStoreClientTest, TestRegionFromEnvironment) {
    ScopedEnvironmentVariable region("AWS_REGION", "eu-west-1");
    auto options = StaticOptions();
    options.erase(kS3RegionOption);
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(options, http));
    ASSERT_OK(client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(http->request_.url, "https://bucket.s3.eu-west-1.amazonaws.com/file");
}

TEST(S3ObjectStoreClientTest, TestDefaultEndpointsForAwsPartitions) {
    for (const auto& [region, endpoint] : std::vector<std::pair<std::string, std::string>>{
             {"ap-northeast-2", "https://bucket.s3.ap-northeast-2.amazonaws.com/file"},
             {"cn-north-1", "https://bucket.s3.cn-north-1.amazonaws.com.cn/file"},
             {"eusc-de-east-1", "https://bucket.s3.eusc-de-east-1.amazonaws.eu/file"},
             {"us-iso-east-1", "https://bucket.s3.us-iso-east-1.c2s.ic.gov/file"},
             {"us-isob-east-1", "https://bucket.s3.us-isob-east-1.sc2s.sgov.gov/file"},
             {"eu-isoe-west-1", "https://bucket.s3.eu-isoe-west-1.cloud.adc-e.uk/file"},
             {"us-isof-south-1", "https://bucket.s3.us-isof-south-1.csp.hci.ic.gov/file"},
             {"us-gov-west-1", "https://bucket.s3.us-gov-west-1.amazonaws.com/file"}}) {
        auto options = StaticOptions();
        options[kS3RegionOption] = region;
        auto http = std::make_shared<MockHttpClient>();
        http->response_headers_["content-length"] = "12";
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                             MakeS3ObjectStoreClient(options, http));
        ASSERT_OK(client->HeadObject({"bucket", "file"}));
        ASSERT_EQ(http->request_.url, endpoint);
    }
}

TEST(S3ObjectStoreClientTest, TestRegionFromProfile) {
    auto test_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_dir);
    std::filesystem::path config_path = std::filesystem::path(test_dir->Str()) / "region-config";
    {
        std::ofstream config(config_path);
        config << "[profile test-profile]\nregion = ap-south-1\n";
    }
    ScopedEnvironmentVariable region("AWS_REGION", std::nullopt);
    ScopedEnvironmentVariable default_region("AWS_DEFAULT_REGION", std::nullopt);
    ScopedEnvironmentVariable config_file("AWS_CONFIG_FILE", config_path.string());
    auto options = StaticOptions();
    options.erase(kS3RegionOption);
    options[kS3ProfileOption] = "test-profile";
    auto http = std::make_shared<MockHttpClient>();
    http->response_headers_["content-length"] = "12";
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(options, http));
    ASSERT_OK(client->HeadObject({"bucket", "file"}));
    ASSERT_EQ(http->request_.url, "https://bucket.s3.ap-south-1.amazonaws.com/file");
}

TEST(S3ObjectStoreClientTest, TestCredentialsFromEnvironmentProfile) {
    auto test_dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(test_dir);
    std::filesystem::path credentials_path =
        std::filesystem::path(test_dir->Str()) / "credentials-config";
    {
        std::ofstream credentials(credentials_path);
        credentials << "[environment-profile]\n"
                       "aws_access_key_id = profile-access\n"
                       "aws_secret_access_key = profile-secret\n"
                       "aws_session_token = profile-token\n"
                       "[default]\n"
                       "aws_access_key_id = default-access\n"
                       "aws_secret_access_key = default-secret\n"
                       "aws_session_token = default-token\n";
    }
    ScopedEnvironmentVariable credentials_file("AWS_SHARED_CREDENTIALS_FILE",
                                               credentials_path.string());
    ScopedEnvironmentVariable access_key("AWS_ACCESS_KEY_ID", std::nullopt);
    ScopedEnvironmentVariable secret_key("AWS_SECRET_ACCESS_KEY", std::nullopt);
    ScopedEnvironmentVariable session_token("AWS_SESSION_TOKEN", std::nullopt);
    {
        ScopedEnvironmentVariable profile("AWS_PROFILE", "environment-profile");
        auto http = std::make_shared<MockHttpClient>();
        http->response_headers_["content-length"] = "12";
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                             MakeS3ObjectStoreClient({{kS3RegionOption, "ap-northeast-2"}}, http));
        ASSERT_OK(client->HeadObject({"bucket", "file"}));
        const std::string* authorization = FindHeader(http->request_.headers, "authorization");
        ASSERT_NE(authorization, nullptr);
        ASSERT_NE(authorization->find("Credential=profile-access/"), std::string::npos);
        const std::string* token = FindHeader(http->request_.headers, "x-amz-security-token");
        ASSERT_NE(token, nullptr);
        ASSERT_EQ(*token, "profile-token");
    }
    {
        ScopedEnvironmentVariable profile("AWS_PROFILE", std::nullopt);
        auto http = std::make_shared<MockHttpClient>();
        http->response_headers_["content-length"] = "12";
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                             MakeS3ObjectStoreClient({{kS3RegionOption, "ap-northeast-2"}}, http));
        ASSERT_OK(client->HeadObject({"bucket", "file"}));
        const std::string* authorization = FindHeader(http->request_.headers, "authorization");
        ASSERT_NE(authorization, nullptr);
        ASSERT_NE(authorization->find("Credential=default-access/"), std::string::npos);
        const std::string* token = FindHeader(http->request_.headers, "x-amz-security-token");
        ASSERT_NE(token, nullptr);
        ASSERT_EQ(*token, "default-token");
    }
}

TEST(S3ObjectStoreClientTest, TestRangeAndListObjects) {
    auto http = std::make_shared<MockHttpClient>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(StaticOptions(), http));
    http->body_ = "data";
    char buffer[4];
    ASSERT_OK_AND_ASSIGN(auto size, client->GetObjectRange({"bucket", "key"}, 2, 4, buffer));
    ASSERT_EQ(size, 4);
    ASSERT_EQ(std::string(buffer, sizeof(buffer)), "data");
    ASSERT_EQ(http->request_.headers["range"], "bytes=2-5");

    http->body_ =
        "<ListBucketResult><IsTruncated>true</IsTruncated>"
        "<Contents><Key>dir/a&amp;b</Key><LastModified>2026-01-01T00:00:00Z</LastModified>"
        "<Size>7</Size></Contents><Contents><Key>dir/a&amp;lt;b</Key><Size>8</Size></Contents>"
        "<CommonPrefixes><Prefix>dir/sub/</Prefix></CommonPrefixes>"
        "<NextContinuationToken>next token</NextContinuationToken></ListBucketResult>";
    ASSERT_OK_AND_ASSIGN(auto result, client->ListObjects({"bucket", "dir/"}, "old token", 10));
    ASSERT_TRUE(result.is_truncated);
    ASSERT_EQ(result.continuation_token, "next token");
    ASSERT_EQ(result.objects[0].key, "dir/a&b");
    ASSERT_EQ(result.objects[1].key, "dir/a&lt;b");
    ASSERT_EQ(result.common_prefixes[0], "dir/sub/");
    ASSERT_NE(http->request_.url.find("amazonaws.com/?list-type=2"), std::string::npos);
    ASSERT_NE(http->request_.url.find("encoding-type=url"), std::string::npos);
    ASSERT_NE(http->request_.url.find("continuation-token=old%20token"), std::string::npos);
}

TEST(S3ObjectStoreClientTest, TestUrlEncodedListObjects) {
    auto http = std::make_shared<MockHttpClient>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(StaticOptions(), http));
    http->body_ =
        "<ListBucketResult><IsTruncated>true</IsTruncated>"
        "<Contents><Key>dir/a%26b%0D</Key><Size>7</Size></Contents>"
        "<CommonPrefixes><Prefix>dir/sub%25/</Prefix></CommonPrefixes>"
        "<NextContinuationToken>next%2Ftoken</NextContinuationToken></ListBucketResult>";
    ASSERT_OK_AND_ASSIGN(auto result, client->ListObjects({"bucket", "dir/"}, "", 0));
    ASSERT_EQ(result.objects[0].key, "dir/a&b\r");
    ASSERT_EQ(result.common_prefixes[0], "dir/sub%/");
    ASSERT_EQ(result.continuation_token, "next%2Ftoken");

    http->body_ = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
    ASSERT_OK(client->ListObjects({"bucket", "dir/"}, result.continuation_token, 0));
    ASSERT_NE(http->request_.url.find("continuation-token=next%252Ftoken"), std::string::npos);

    http->body_ =
        "<ListBucketResult><IsTruncated>false</IsTruncated>"
        "<Contents><Key>dir/invalid%2</Key><Size>7</Size></Contents></ListBucketResult>";
    ASSERT_TRUE(client->ListObjects({"bucket", "dir/"}, "", 0).status().IsIOError());
}

TEST(S3ObjectStoreClientTest, TestInvalidListObjectsResponse) {
    auto http = std::make_shared<MockHttpClient>();
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<ObjectStoreClient> client,
                         MakeS3ObjectStoreClient(StaticOptions(), http));
    for (const std::string size : {"", "abc", "-1", "12abc", "9223372036854775808"}) {
        http->body_ =
            "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>key</Key>"
            "<Size>" +
            size + "</Size></Contents></ListBucketResult>";
        ASSERT_TRUE(client->ListObjects({"bucket", ""}, "", 0).status().IsIOError());
    }
    http->body_ =
        "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>key</Key>"
        "<Size>1</Size></ListBucketResult>";
    ASSERT_TRUE(client->ListObjects({"bucket", ""}, "", 0).status().IsIOError());
    http->body_ =
        "<ListBucketResult><Contents><Key>key</Key><Size>1</Size></Contents></ListBucketResult>";
    ASSERT_TRUE(client->ListObjects({"bucket", ""}, "", 0).status().IsIOError());
}

TEST(S3FileSystemFactoryTest, TestOptionValidation) {
    S3FileSystemFactory factory;
    ASSERT_TRUE(
        factory.Create("s3://bucket", {{kS3AccessKeyOption, ""}, {kS3SecretKeyOption, "secret"}})
            .status()
            .IsInvalid());
    ASSERT_TRUE(
        factory.Create("s3://bucket", {{kS3PathStyleAccessOption, "treu"}}).status().IsInvalid());
    ASSERT_TRUE(
        factory.Create("s3://bucket", {{kS3PathStyleAccessOption, "on"}}).status().IsInvalid());
    ASSERT_TRUE(
        factory.Create("s3://bucket", {{kS3SessionTokenOption, "token"}}).status().IsInvalid());

    auto http = std::make_shared<MockHttpClient>();
    ASSERT_TRUE(
        MakeS3ObjectStoreClient({{kS3AccessKeyOption, "access"}}, http).status().IsInvalid());
    ASSERT_TRUE(
        MakeS3ObjectStoreClient({{kS3PathStyleAccessOption, "treu"}}, http).status().IsInvalid());
}

}  // namespace
}  // namespace paimon::s3
