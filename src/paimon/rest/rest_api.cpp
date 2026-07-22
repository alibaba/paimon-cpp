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

#include "paimon/rest/rest_api.h"

#include <memory>
#include <string>
#include <utility>

#include "fmt/format.h"
#include "paimon/catalog_options.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/sensitive_config_utils.h"
#include "paimon/logging.h"
#include "paimon/rest/rest_util.h"

namespace paimon {

namespace {
// The `PAIMON_ASSIGN_OR_RAISE` macro cannot take a declaration type containing a comma.
using StringMap = std::map<std::string, std::string>;

// A successful response body may carry credentials (e.g. a token response), so neither it
// nor the parse failure quoting it is echoed into the error message; only the request path
// is reported.
template <typename ResponseT>
Status ParseResponseBody(const std::string& body, const std::string& path, ResponseT* entity) {
    if (!RapidJsonUtil::FromJsonString(body, entity).ok()) {
        return Status::Invalid(
            fmt::format("failed to deserialize the response of {} from the rest server", path));
    }
    return Status::OK();
}
}  // namespace

RestApi::RestApi(std::unique_ptr<RestHttpClient> client,
                 std::unique_ptr<AuthProvider> auth_provider,
                 const std::map<std::string, std::string>& base_headers,
                 const std::map<std::string, std::string>& options, const ResourcePaths& paths)
    : client_(std::move(client)),
      auth_provider_(std::move(auth_provider)),
      base_headers_(base_headers),
      options_(options),
      resource_paths_(paths) {}

Result<std::unique_ptr<RestApi>> RestApi::Create(const std::map<std::string, std::string>& options,
                                                 const std::string& warehouse, bool config_required,
                                                 const RestHttpClient::Config& http_config) {
    auto uri_iter = options.find(CatalogOptions::URI);
    if (uri_iter == options.end() || uri_iter->second.empty()) {
        return Status::Invalid(fmt::format("option '{}' must be configured for the rest catalog",
                                           CatalogOptions::URI));
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<RestHttpClient> client,
                           RestHttpClient::Create(uri_iter->second, http_config));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<AuthProvider> auth_provider,
                           AuthProvider::Create(options));

    std::map<std::string, std::string> merged_options = options;
    std::map<std::string, std::string> base_headers =
        RestUtil::ExtractPrefixMap(options, kHeaderOptionPrefix);
    if (config_required) {
        std::map<std::string, std::string> query_params;
        if (!warehouse.empty()) {
            query_params[kQueryParamWarehouse] = warehouse;
        }
        RestAuthParameter auth_parameter =
            RestAuthParameter::Create("GET", ResourcePaths::Config(), query_params, "");
        PAIMON_ASSIGN_OR_RAISE(StringMap headers,
                               auth_provider->MergeAuthHeader(base_headers, auth_parameter));
        PAIMON_ASSIGN_OR_RAISE(
            RestHttpClient::Response response,
            client->Execute("GET", ResourcePaths::Config(), query_params, headers, ""));
        if (!response.IsSuccessful()) {
            return ErrorToStatus(response);
        }
        ConfigResponse config;
        PAIMON_RETURN_NOT_OK(ParseResponseBody(response.body, ResourcePaths::Config(), &config));
        merged_options = config.Merge(options);
        for (const auto& [key, value] :
             RestUtil::ExtractPrefixMap(merged_options, kHeaderOptionPrefix)) {
            base_headers[key] = value;
        }
        std::shared_ptr<Logger> logger = Logger::GetLogger("RestApi");
        PAIMON_LOG_DEBUG(logger,
                         "merged %zu client options with the /v1/config response into %zu options",
                         options.size(), merged_options.size());
    }
    std::string prefix;
    auto prefix_iter = merged_options.find(kOptionUrlPrefix);
    if (prefix_iter != merged_options.end()) {
        prefix = prefix_iter->second;
    }
    return std::unique_ptr<RestApi>(new RestApi(std::move(client), std::move(auth_provider),
                                                base_headers, merged_options,
                                                ResourcePaths(prefix)));
}

Status RestApi::ErrorToStatus(const RestHttpClient::Response& response) {
    // The code of the parsed error body takes precedence over the http status, which
    // a gateway may have rewritten.
    int64_t code = response.code;
    std::string message;
    std::string resource_info;
    bool body_parsed = false;
    if (!response.body.empty()) {
        ErrorResponse error;
        if (RapidJsonUtil::FromJsonString(response.body, &error).ok()) {
            body_parsed = true;
            // The message may embed secrets (e.g. "password=..."), so it is redacted.
            message = SensitiveConfigUtils::RedactText(error.GetMessage());
            if (!error.GetResourceType().empty()) {
                resource_info = fmt::format(" (resource type: {}, resource name: {})",
                                            error.GetResourceType(), error.GetResourceName());
            }
            if (error.GetCode() != 0) {
                code = error.GetCode();
            }
        }
    }
    if (message.empty()) {
        // The body is never echoed (it may carry credentials), so a server answering with
        // something other than an error object is told apart by the message alone.
        message = body_parsed ? fmt::format("empty error message (http status {})", response.code)
                              : fmt::format("unparsable error response body (http status {})",
                                            response.code);
    }
    message += resource_info;
    std::string request_id = RestUtil::ExtractRequestId(response.headers);
    if (request_id != RestUtil::kUnknownRequestId) {
        message += fmt::format(" requestId:{}", request_id);
    }
    Status status;
    switch (code) {
        case HttpStatus::kBadRequest:
            status = Status::Invalid(message);
            break;
        case HttpStatus::kUnauthorized:
            status = Status::IOError("not authorized: ", message);
            break;
        case HttpStatus::kForbidden:
            status = Status::IOError("forbidden: ", message);
            break;
        case HttpStatus::kNotFound:
            status = Status::NotExist(message);
            break;
        case HttpStatus::kConflict:
            status = Status::Exist(message);
            break;
        case HttpStatus::kInternalServerError:
            status = Status::IOError("server error: ", message);
            break;
        case HttpStatus::kNotImplemented:
            status = Status::NotImplemented(message);
            break;
        case HttpStatus::kServiceUnavailable:
            status = Status::IOError("service unavailable: ", message);
            break;
        default:
            status =
                Status::IOError(fmt::format("rest request failed with code {}: {}", code, message));
            break;
    }
    return status.WithDetail(std::make_shared<RestErrorDetail>(code));
}

Result<RestHttpClient::Response> RestApi::Execute(
    const std::string& method, const std::string& path,
    const std::map<std::string, std::string>& query_params, const std::string& body) const {
    RestAuthParameter auth_parameter = RestAuthParameter::Create(method, path, query_params, body);
    StringMap request_headers = base_headers_;
    if (!body.empty()) {
        // Set before merging the auth headers so a provider that signs headers covers it.
        request_headers["Content-Type"] = "application/json";
    }
    PAIMON_ASSIGN_OR_RAISE(StringMap headers,
                           auth_provider_->MergeAuthHeader(request_headers, auth_parameter));
    PAIMON_ASSIGN_OR_RAISE(RestHttpClient::Response response,
                           client_->Execute(method, path, query_params, headers, body));
    if (!response.IsSuccessful()) {
        return ErrorToStatus(response);
    }
    return response;
}

template <typename ResponseT>
Result<ResponseT> RestApi::GetEntity(const std::string& path,
                                     const std::map<std::string, std::string>& query_params) const {
    PAIMON_ASSIGN_OR_RAISE(RestHttpClient::Response response,
                           Execute("GET", path, query_params, ""));
    ResponseT entity;
    PAIMON_RETURN_NOT_OK(ParseResponseBody(response.body, path, &entity));
    return entity;
}

template <typename ResponseT>
Result<std::vector<typename ResponseT::ItemType>> RestApi::ListAllPages(
    const std::string& path) const {
    std::vector<typename ResponseT::ItemType> items;
    std::map<std::string, std::string> query_params;
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(ResponseT response, GetEntity<ResponseT>(path, query_params));
        const auto& data = response.Data();
        items.insert(items.end(), data.begin(), data.end());
        const std::optional<std::string>& next_page_token = response.NextPageToken();
        if (!next_page_token || next_page_token.value().empty() || data.empty()) {
            return items;
        }
        query_params[kQueryParamPageToken] = next_page_token.value();
    }
}

Result<std::vector<std::string>> RestApi::ListDatabases() const {
    return ListAllPages<ListDatabasesResponse>(resource_paths_.Databases());
}

Status RestApi::CreateDatabase(const std::string& name,
                               const std::map<std::string, std::string>& options) const {
    CreateDatabaseRequest request(name, options);
    PAIMON_ASSIGN_OR_RAISE(std::string body, request.ToJsonString());
    return Execute("POST", resource_paths_.Databases(), {}, body).status();
}

Result<GetDatabaseResponse> RestApi::GetDatabase(const std::string& name) const {
    return GetEntity<GetDatabaseResponse>(resource_paths_.Database(name), {});
}

Status RestApi::DropDatabase(const std::string& name) const {
    return Execute("DELETE", resource_paths_.Database(name), {}, "").status();
}

Result<std::vector<std::string>> RestApi::ListTables(const std::string& database_name) const {
    return ListAllPages<ListTablesResponse>(resource_paths_.Tables(database_name));
}

Result<GetTableResponse> RestApi::GetTable(const Identifier& identifier) const {
    return GetEntity<GetTableResponse>(
        resource_paths_.Table(identifier.GetDatabaseName(), identifier.GetTableName()), {});
}

Status RestApi::CreateTable(const Identifier& identifier, const std::string& schema_json) const {
    CreateTableRequest request(identifier.GetDatabaseName(), identifier.GetTableName(),
                               schema_json);
    PAIMON_ASSIGN_OR_RAISE(std::string body, request.ToJsonString());
    return Execute("POST", resource_paths_.Tables(identifier.GetDatabaseName()), {}, body).status();
}

Status RestApi::DropTable(const Identifier& identifier) const {
    return Execute("DELETE",
                   resource_paths_.Table(identifier.GetDatabaseName(), identifier.GetTableName()),
                   {}, "")
        .status();
}

Status RestApi::RenameTable(const Identifier& from_table, const Identifier& to_table) const {
    RenameTableRequest request(from_table.GetDatabaseName(), from_table.GetTableName(),
                               to_table.GetDatabaseName(), to_table.GetTableName());
    PAIMON_ASSIGN_OR_RAISE(std::string body, request.ToJsonString());
    return Execute("POST", resource_paths_.RenameTable(), {}, body).status();
}

Result<std::vector<Snapshot>> RestApi::ListSnapshots(const Identifier& identifier) const {
    return ListAllPages<ListSnapshotsResponse>(
        resource_paths_.Snapshots(identifier.GetDatabaseName(), identifier.GetTableName()));
}

}  // namespace paimon
