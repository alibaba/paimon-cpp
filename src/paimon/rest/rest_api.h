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
#include <vector>

#include "paimon/catalog/identifier.h"
#include "paimon/core/snapshot.h"
#include "paimon/rest/resource_paths.h"
#include "paimon/rest/rest_auth.h"
#include "paimon/rest/rest_http_client.h"
#include "paimon/rest/rest_messages.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

/// Carries the code of a failed rest request (the parsed error body's code, falling back
/// to the http status) so callers can tell e.g. an authentication failure (401/403) from
/// another IO error.
class RestErrorDetail : public StatusDetail {
 public:
    static constexpr const char* kTypeId = "rest-error-detail";

    explicit RestErrorDetail(int64_t code) : code_(code) {}

    const char* type_id() const override {
        return kTypeId;
    }

    std::string ToString() const override {
        return "rest error code " + std::to_string(code_);
    }

    int64_t GetCode() const {
        return code_;
    }

 private:
    int64_t code_;
};

/// The client of the REST catalog server. This layer only talks HTTP + JSON and never
/// touches the file system.
class RestApi {
 public:
    static constexpr const char* kQueryParamPageToken = "pageToken";
    static constexpr const char* kQueryParamWarehouse = "warehouse";
    /// Option key of the url path prefix inserted after "/v1", usually pushed down by
    /// "/v1/config".
    static constexpr const char* kOptionUrlPrefix = "prefix";
    /// Options with this prefix are sent as http headers (with the prefix stripped).
    static constexpr const char* kHeaderOptionPrefix = "header.";

    /// Creates the api client.
    ///
    /// @param options Client side options; `CatalogOptions::URI` and
    ///                `CatalogOptions::TOKEN_PROVIDER` are required.
    /// @param warehouse Warehouse sent as query parameter of "/v1/config"; may be empty.
    /// @param config_required When true, fetch "/v1/config" and merge the server
    ///                        defaults/overrides into `options` with the precedence
    ///                        overrides > client options > defaults.
    /// @param http_config Transport level settings, mainly overridable for tests.
    static Result<std::unique_ptr<RestApi>> Create(
        const std::map<std::string, std::string>& options, const std::string& warehouse,
        bool config_required, const RestHttpClient::Config& http_config = RestHttpClient::Config());

    /// Options merged with the server side config.
    const std::map<std::string, std::string>& GetMergedOptions() const {
        return options_;
    }

    Result<std::vector<std::string>> ListDatabases() const;
    Status CreateDatabase(const std::string& name,
                          const std::map<std::string, std::string>& options) const;
    Result<GetDatabaseResponse> GetDatabase(const std::string& name) const;
    Status DropDatabase(const std::string& name) const;

    Result<std::vector<std::string>> ListTables(const std::string& database_name) const;
    Result<GetTableResponse> GetTable(const Identifier& identifier) const;
    /// `schema_json` uses the schema JSON layout of the protocol
    /// (fields/partitionKeys/primaryKeys/options/comment).
    Status CreateTable(const Identifier& identifier, const std::string& schema_json) const;
    Status DropTable(const Identifier& identifier) const;
    Status RenameTable(const Identifier& from_table, const Identifier& to_table) const;

    Result<std::vector<Snapshot>> ListSnapshots(const Identifier& identifier) const;

    /// Maps a non-successful http response to a status: 404 becomes `NotExist`, 409
    /// becomes `Exist`, 400 becomes `Invalid`, 501 becomes `NotImplemented` and the
    /// other codes become `IOError`. The status carries a `RestErrorDetail` with the
    /// mapped code.
    static Status ErrorToStatus(const RestHttpClient::Response& response);

 private:
    RestApi(std::unique_ptr<RestHttpClient> client, std::unique_ptr<AuthProvider> auth_provider,
            const std::map<std::string, std::string>& base_headers,
            const std::map<std::string, std::string>& options, const ResourcePaths& paths);

    /// Executes one request with the authentication headers merged in, mapping a
    /// non-successful response to an error status.
    Result<RestHttpClient::Response> Execute(const std::string& method, const std::string& path,
                                             const std::map<std::string, std::string>& query_params,
                                             const std::string& body) const;

    template <typename ResponseT>
    Result<ResponseT> GetEntity(const std::string& path,
                                const std::map<std::string, std::string>& query_params) const;

    /// Fetches all pages, stopping at an empty page or a missing/empty next page token.
    template <typename ResponseT>
    Result<std::vector<typename ResponseT::ItemType>> ListAllPages(const std::string& path) const;

    std::unique_ptr<RestHttpClient> client_;
    std::unique_ptr<AuthProvider> auth_provider_;
    std::map<std::string, std::string> base_headers_;
    std::map<std::string, std::string> options_;
    ResourcePaths resource_paths_;
};

}  // namespace paimon
