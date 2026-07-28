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

#include <map>
#include <memory>
#include <string>

#include "paimon/common/fs/http_client.h"
#include "paimon/common/fs/object_store_file_system.h"

namespace paimon::s3 {

inline constexpr char kS3RegionOption[] = "s3.region";
inline constexpr char kS3EndpointOption[] = "s3.endpoint";
inline constexpr char kS3PathStyleAccessOption[] = "s3.path-style-access";
inline constexpr char kS3ProfileOption[] = "s3.profile";
inline constexpr char kS3AccessKeyOption[] = "s3.access-key";
inline constexpr char kS3SecretKeyOption[] = "s3.secret-key";
inline constexpr char kS3SessionTokenOption[] = "s3.session.token";

Status ValidateS3Options(const std::map<std::string, std::string>& options);
Result<std::shared_ptr<ObjectStoreClient>> MakeS3ObjectStoreClient(
    const std::map<std::string, std::string>& options, std::shared_ptr<HttpClient> http_client);

class S3FileSystem : public ObjectStoreFileSystem {
 public:
    using ObjectStoreFileSystem::Create;

    static Result<std::unique_ptr<FileSystem>> Create(
        const std::map<std::string, std::string>& options);
    ~S3FileSystem() override = default;

 private:
    explicit S3FileSystem(std::shared_ptr<ObjectStoreClient> client);
};

}  // namespace paimon::s3
