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

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Input of one request signature.
struct RestAuthParameter {
    std::string method;
    std::string resource_path;
    /// Query parameters with url-encoded values.
    std::map<std::string, std::string> parameters;
    /// Request body, empty when the request carries none.
    std::string data;

    /// Builds the parameter of one request. `query_params` are stored url-encoded, since
    /// a signing provider signs the query string as it is sent; building the parameter
    /// only through here keeps a request from being signed over unencoded values.
    static RestAuthParameter Create(const std::string& method, const std::string& resource_path,
                                    const std::map<std::string, std::string>& query_params,
                                    const std::string& data);
};

/// Generates authentication headers for REST catalog requests.
class AuthProvider {
 public:
    virtual ~AuthProvider() = default;

    /// Returns `base_header` merged with the authentication headers of this provider.
    virtual Result<std::map<std::string, std::string>> MergeAuthHeader(
        const std::map<std::string, std::string>& base_header,
        const RestAuthParameter& parameter) const = 0;

    /// Creates the provider configured by `CatalogOptions::TOKEN_PROVIDER`.
    static Result<std::unique_ptr<AuthProvider>> Create(
        const std::map<std::string, std::string>& options);
};

/// Adds `Authorization: Bearer <token>`.
class BearTokenAuthProvider : public AuthProvider {
 public:
    explicit BearTokenAuthProvider(const std::string& token) : token_(token) {}

    Result<std::map<std::string, std::string>> MergeAuthHeader(
        const std::map<std::string, std::string>& base_header,
        const RestAuthParameter& parameter) const override;

 private:
    std::string token_;
};

}  // namespace paimon
