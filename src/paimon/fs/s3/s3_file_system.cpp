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

#include <aws/auth/auth.h>
#include <aws/auth/credentials.h>
#include <aws/auth/signable.h>
#include <aws/auth/signing.h>
#include <aws/auth/signing_result.h>
#include <aws/common/clock.h>
#include <aws/common/string.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/sdkutils/aws_profile.h>
#include <curl/curl.h>

#include <algorithm>
#include <cctype>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <optional>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/fs/http_client.h"
#include "paimon/common/utils/options_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/executor.h"

namespace paimon::s3 {
namespace {

Result<std::string> PercentEncode(std::string_view value, bool preserve_slash) {
    if (value.size() > std::numeric_limits<int>::max()) {
        return Status::IOError("S3 URL component is too large to encode");
    }
    char* encoded = curl_easy_escape(nullptr, value.data(), static_cast<int>(value.size()));
    if (encoded == nullptr) {
        return Status::IOError("failed to URL encode S3 component");
    }
    ScopeGuard free_encoded([encoded] { curl_free(encoded); });
    std::string result(encoded);
    if (preserve_slash) {
        result = StringUtils::Replace(result, "%2F", "/");
    }
    return result;
}

Result<std::string> PercentDecode(std::string_view value, const std::string& field) {
    for (size_t position = 0; position < value.size(); ++position) {
        if (value[position] == '%' &&
            (position + 2 >= value.size() ||
             !std::isxdigit(static_cast<unsigned char>(value[position + 1])) ||
             !std::isxdigit(static_cast<unsigned char>(value[position + 2])))) {
            return Status::IOError(fmt::format("invalid URL encoding in S3 {}", field));
        }
    }
    if (value.size() > std::numeric_limits<int>::max()) {
        return Status::IOError(fmt::format("S3 {} is too large to URL decode", field));
    }
    int decoded_size = 0;
    char* decoded =
        curl_easy_unescape(nullptr, value.data(), static_cast<int>(value.size()), &decoded_size);
    if (decoded == nullptr) {
        return Status::IOError(fmt::format("failed to URL decode S3 {}", field));
    }
    ScopeGuard free_decoded([decoded] { curl_free(decoded); });
    std::string result(decoded, decoded_size);
    return result;
}

Result<int64_t> ParseNonNegativeInt64(const std::string& value, const std::string& field) {
    std::optional<int64_t> result = StringUtils::StringToValue<int64_t>(value);
    if (!result || *result < 0 || (!value.empty() && value.front() == '-')) {
        return Status::IOError(fmt::format("S3 {} is not a non-negative integer", field));
    }
    return *result;
}

int64_t ParseModificationTime(const std::string& value) {
    time_t seconds = curl_getdate(value.c_str(), nullptr);
    return seconds == static_cast<time_t>(-1) ? 0 : static_cast<int64_t>(seconds) * 1000;
}

std::string XmlUnescape(const std::string& value) {
    const std::pair<const char*, const char*> entities[] = {
        {"&amp;", "&"}, {"&lt;", "<"}, {"&gt;", ">"}, {"&quot;", "\""}, {"&apos;", "'"}};

    std::string result;
    result.reserve(value.size());
    for (size_t position = 0; position < value.size();) {
        bool matched = false;
        if (value[position] == '&') {
            for (const auto& [entity, replacement] : entities) {
                size_t entity_size = std::strlen(entity);
                if (value.compare(position, entity_size, entity) == 0) {
                    result.append(replacement);
                    position += entity_size;
                    matched = true;
                    break;
                }
            }
        }
        if (!matched) {
            result.push_back(value[position++]);
        }
    }
    return result;
}

std::optional<std::string> TagValue(const std::string& xml, const std::string& tag,
                                    size_t offset = 0) {
    std::string begin = "<" + tag + ">";
    std::string end = "</" + tag + ">";
    size_t begin_position = xml.find(begin, offset);
    if (begin_position == std::string::npos) {
        return std::nullopt;
    }
    begin_position += begin.size();
    size_t end_position = xml.find(end, begin_position);
    if (end_position == std::string::npos) {
        return std::nullopt;
    }
    return XmlUnescape(xml.substr(begin_position, end_position - begin_position));
}

Result<std::vector<std::string>> TagBlocks(const std::string& xml, const std::string& tag) {
    std::vector<std::string> blocks;
    std::string begin = "<" + tag + ">";
    std::string end = "</" + tag + ">";
    size_t position = 0;
    while (true) {
        size_t begin_position = xml.find(begin, position);
        size_t unexpected_end = xml.find(end, position);
        if (begin_position == std::string::npos) {
            if (unexpected_end != std::string::npos) {
                return Status::IOError(fmt::format("malformed S3 XML element {}", tag));
            }
            break;
        }
        if (unexpected_end != std::string::npos && unexpected_end < begin_position) {
            return Status::IOError(fmt::format("malformed S3 XML element {}", tag));
        }
        size_t end_position = xml.find(end, begin_position + begin.size());
        if (end_position == std::string::npos) {
            return Status::IOError(fmt::format("malformed S3 XML element {}", tag));
        }
        size_t nested_begin = xml.find(begin, begin_position + begin.size());
        if (nested_begin != std::string::npos && nested_begin < end_position) {
            return Status::IOError(fmt::format("malformed S3 XML element {}", tag));
        }
        end_position += end.size();
        blocks.push_back(xml.substr(begin_position, end_position - begin_position));
        position = end_position;
    }
    return blocks;
}

class AwsAuthRuntime {
 public:
    static Result<std::unique_ptr<AwsAuthRuntime>> Create() {
        std::unique_ptr<AwsAuthRuntime> runtime(new AwsAuthRuntime());
        PAIMON_RETURN_NOT_OK(runtime->Initialize());
        return runtime;
    }

    ~AwsAuthRuntime() {
        if (tls_context_ != nullptr) {
            aws_tls_ctx_release(tls_context_);
        }
        if (bootstrap_ != nullptr) {
            aws_client_bootstrap_release(bootstrap_);
        }
        if (resolver_ != nullptr) {
            aws_host_resolver_release(resolver_);
        }
        if (event_loop_group_ != nullptr) {
            aws_event_loop_group_release(event_loop_group_);
        }
        if (library_initialized_) {
            aws_auth_library_clean_up();
        }
    }

    aws_allocator* allocator() const {
        return allocator_;
    }
    aws_client_bootstrap* bootstrap() const {
        return bootstrap_;
    }
    aws_tls_ctx* tls_context() const {
        return tls_context_;
    }

 private:
    AwsAuthRuntime() : allocator_(aws_default_allocator()) {}

    Status Initialize() {
        aws_auth_library_init(allocator_);
        library_initialized_ = true;
        event_loop_group_ = aws_event_loop_group_new_default(allocator_, 1, nullptr);
        if (event_loop_group_ == nullptr) {
            return InitializationError("event loop group");
        }
        aws_host_resolver_default_options resolver_options{};
        resolver_options.el_group = event_loop_group_;
        resolver_options.max_entries = 8;
        resolver_ = aws_host_resolver_new_default(allocator_, &resolver_options);
        if (resolver_ == nullptr) {
            return InitializationError("host resolver");
        }
        aws_client_bootstrap_options bootstrap_options{};
        bootstrap_options.event_loop_group = event_loop_group_;
        bootstrap_options.host_resolver = resolver_;
        bootstrap_ = aws_client_bootstrap_new(allocator_, &bootstrap_options);
        if (bootstrap_ == nullptr) {
            return InitializationError("client bootstrap");
        }
        aws_tls_ctx_options tls_options;
        aws_tls_ctx_options_init_default_client(&tls_options, allocator_);
        tls_context_ = aws_tls_client_ctx_new(allocator_, &tls_options);
        aws_tls_ctx_options_clean_up(&tls_options);
        if (tls_context_ == nullptr) {
            return InitializationError("TLS context");
        }
        return Status::OK();
    }

    Status InitializationError(const std::string& component) const {
        return Status::IOError(fmt::format("failed to initialize AWS {}: {}", component,
                                           aws_error_debug_str(aws_last_error())));
    }

    aws_allocator* allocator_;
    aws_event_loop_group* event_loop_group_ = nullptr;
    aws_host_resolver* resolver_ = nullptr;
    aws_client_bootstrap* bootstrap_ = nullptr;
    aws_tls_ctx* tls_context_ = nullptr;
    bool library_initialized_ = false;
};

Result<AwsAuthRuntime*> GetAwsAuthRuntime() {
    static const Result<AwsAuthRuntime*> runtime = [] {
        Result<std::unique_ptr<AwsAuthRuntime>> created = AwsAuthRuntime::Create();
        if (!created.ok()) {
            return Result<AwsAuthRuntime*>(created.status());
        }
        return Result<AwsAuthRuntime*>(std::move(created).value().release());
    }();
    return runtime;
}

aws_byte_cursor Cursor(const std::string& value) {
    return aws_byte_cursor_from_array(value.data(), value.size());
}

const char* CanonicalS3OptionName(const std::string& option) {
    if (option == "access-key" || option == "access.key" || option == "accessKeyId") {
        return kS3AccessKeyOption;
    }
    if (option == "secret-key" || option == "secret.key" || option == "accessKeySecret") {
        return kS3SecretKeyOption;
    }
    if (option == "session.token" || option == "session-token" || option == "security.token" ||
        option == "security-token" || option == "securityToken") {
        return kS3SessionTokenOption;
    }
    if (option == "endpoint") {
        return kS3EndpointOption;
    }
    if (option == "region") {
        return kS3RegionOption;
    }
    if (option == "path-style-access" || option == "path.style.access") {
        return kS3PathStyleAccessOption;
    }
    if (option == "profile") {
        return kS3ProfileOption;
    }
    return nullptr;
}

std::map<std::string, std::string> NormalizeS3Options(
    const std::map<std::string, std::string>& options) {
    std::map<std::string, std::string> normalized = options;
    for (const auto& [key, value] : options) {
        for (const char* prefix : {"s3a.", "fs.s3.", "fs.s3a."}) {
            if (!StringUtils::StartsWith(key, prefix)) {
                continue;
            }
            const char* canonical = CanonicalS3OptionName(key.substr(std::strlen(prefix)));
            if (canonical != nullptr && normalized.find(canonical) == normalized.end()) {
                normalized.emplace(canonical, value);
            }
            break;
        }
        if (StringUtils::StartsWith(key, "s3.")) {
            const char* canonical = CanonicalS3OptionName(key.substr(std::strlen("s3.")));
            if (canonical != nullptr && normalized.find(canonical) == normalized.end()) {
                normalized.emplace(canonical, value);
            }
        }
    }
    return normalized;
}

std::shared_ptr<aws_credentials_provider> WrapProvider(aws_credentials_provider* provider) {
    return std::shared_ptr<aws_credentials_provider>(provider, aws_credentials_provider_release);
}

Result<std::string> ResolveRegion(const std::map<std::string, std::string>& options) {
    auto region = options.find(kS3RegionOption);
    if (region != options.end() && !region->second.empty()) {
        return region->second;
    }
    const char* environment_region = std::getenv("AWS_REGION");
    if (environment_region != nullptr && environment_region[0] != '\0') {
        return std::string(environment_region);
    }
    environment_region = std::getenv("AWS_DEFAULT_REGION");
    if (environment_region != nullptr && environment_region[0] != '\0') {
        return std::string(environment_region);
    }

    PAIMON_ASSIGN_OR_RAISE(AwsAuthRuntime * runtime, GetAwsAuthRuntime());
    aws_byte_cursor profile_override{};
    const aws_byte_cursor* profile_override_ptr = nullptr;
    auto profile = options.find(kS3ProfileOption);
    if (profile != options.end() && !profile->second.empty()) {
        profile_override = Cursor(profile->second);
        profile_override_ptr = &profile_override;
    }
    aws_string* config_path = aws_get_config_file_path(runtime->allocator(), nullptr);
    aws_string* profile_name = aws_get_profile_name(runtime->allocator(), profile_override_ptr);
    aws_profile_collection* profiles = config_path == nullptr
                                           ? nullptr
                                           : aws_profile_collection_new_from_file(
                                                 runtime->allocator(), config_path, AWS_PST_CONFIG);
    const aws_profile* selected_profile =
        profiles == nullptr || profile_name == nullptr
            ? nullptr
            : aws_profile_collection_get_profile(profiles, profile_name);
    aws_string* region_name = aws_string_new_from_c_str(runtime->allocator(), "region");
    const aws_profile_property* property =
        selected_profile == nullptr || region_name == nullptr
            ? nullptr
            : aws_profile_get_property(selected_profile, region_name);
    const aws_string* value =
        property == nullptr ? nullptr : aws_profile_property_get_value(property);
    std::string resolved = value == nullptr ? "" : aws_string_c_str(value);
    aws_string_destroy(region_name);
    aws_profile_collection_release(profiles);
    aws_string_destroy(profile_name);
    aws_string_destroy(config_path);
    return resolved.empty() ? "us-east-1" : resolved;
}

Result<std::shared_ptr<aws_credentials_provider>> MakeCredentialsProvider(
    const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(AwsAuthRuntime * runtime, GetAwsAuthRuntime());
    auto access = options.find(kS3AccessKeyOption);
    if (access != options.end()) {
        const std::string& secret = options.at(kS3SecretKeyOption);
        std::string token;
        auto configured_token = options.find(kS3SessionTokenOption);
        if (configured_token != options.end()) {
            token = configured_token->second;
        }
        aws_credentials_provider_static_options static_options{};
        static_options.access_key_id = Cursor(access->second);
        static_options.secret_access_key = Cursor(secret);
        static_options.session_token = Cursor(token);
        return WrapProvider(
            aws_credentials_provider_new_static(runtime->allocator(), &static_options));
    }

    aws_byte_cursor profile_override{};
    auto profile_iter = options.find(kS3ProfileOption);
    if (profile_iter != options.end() && !profile_iter->second.empty()) {
        profile_override = Cursor(profile_iter->second);
    }
    std::string region;
    auto region_iter = options.find(kS3RegionOption);
    if (region_iter != options.end()) {
        region = region_iter->second;
    }

    std::vector<aws_credentials_provider*> providers;
    aws_credentials_provider_environment_options environment_options{};
    providers.push_back(
        aws_credentials_provider_new_environment(runtime->allocator(), &environment_options));

    aws_credentials_provider_profile_options profile_options{};
    profile_options.profile_name_override = profile_override;
    profile_options.bootstrap = runtime->bootstrap();
    profile_options.tls_ctx = runtime->tls_context();
    providers.push_back(
        aws_credentials_provider_new_profile(runtime->allocator(), &profile_options));

    aws_credentials_provider_sts_web_identity_options web_options{};
    web_options.profile_name_override = profile_override;
    web_options.region = Cursor(region);
    web_options.bootstrap = runtime->bootstrap();
    web_options.tls_ctx = runtime->tls_context();
    providers.push_back(
        aws_credentials_provider_new_sts_web_identity(runtime->allocator(), &web_options));

    aws_credentials_provider_sso_options sso_options{};
    sso_options.profile_name_override = profile_override;
    sso_options.bootstrap = runtime->bootstrap();
    sso_options.tls_ctx = runtime->tls_context();
    providers.push_back(aws_credentials_provider_new_sso(runtime->allocator(), &sso_options));

    aws_credentials_provider_login_options login_options{};
    login_options.profile_name_override = profile_override;
    login_options.login_region = Cursor(region);
    login_options.bootstrap = runtime->bootstrap();
    login_options.tls_ctx = runtime->tls_context();
    providers.push_back(aws_credentials_provider_new_login(runtime->allocator(), &login_options));

    aws_credentials_provider_chain_default_options default_options{};
    default_options.profile_name_override = profile_override;
    default_options.bootstrap = runtime->bootstrap();
    default_options.tls_ctx = runtime->tls_context();
    default_options.skip_environment_credentials_provider = true;
    providers.push_back(
        aws_credentials_provider_new_chain_default(runtime->allocator(), &default_options));

    providers.erase(std::remove(providers.begin(), providers.end(), nullptr), providers.end());
    aws_credentials_provider_chain_options chain_options{};
    chain_options.providers = providers.data();
    chain_options.provider_count = providers.size();
    aws_credentials_provider* chain =
        aws_credentials_provider_new_chain(runtime->allocator(), &chain_options);
    for (aws_credentials_provider* provider : providers) {
        aws_credentials_provider_release(provider);
    }
    if (chain == nullptr) {
        return std::shared_ptr<aws_credentials_provider>();
    }
    aws_credentials_provider_cached_options cached_options{};
    cached_options.source = chain;
    cached_options.refresh_time_in_milliseconds = 15 * 60 * 1000;
    aws_credentials_provider* cached =
        aws_credentials_provider_new_cached(runtime->allocator(), &cached_options);
    aws_credentials_provider_release(chain);
    return WrapProvider(cached);
}

struct Endpoint {
    std::string scheme;
    std::string authority;
    std::string base_path;
};

Result<Endpoint> ParseEndpoint(std::string endpoint) {
    if (endpoint.find("://") == std::string::npos) {
        endpoint = "https://" + endpoint;
    }
    CURLU* url = curl_url();
    if (url == nullptr) {
        return Status::IOError("failed to create S3 endpoint parser");
    }
    ScopeGuard cleanup_url([url] { curl_url_cleanup(url); });
    CURLUcode code = curl_url_set(url, CURLUPART_URL, endpoint.c_str(), 0);
    if (code != CURLUE_OK) {
        return Status::Invalid(
            fmt::format("invalid S3 endpoint {}: code {}", endpoint, static_cast<int>(code)));
    }
    auto get_part = [url, &endpoint](CURLUPart part, CURLUcode no_value,
                                     const char* name) -> Result<std::optional<std::string>> {
        char* value = nullptr;
        CURLUcode result = curl_url_get(url, part, &value, 0);
        if (result == no_value) {
            return std::optional<std::string>();
        }
        if (result != CURLUE_OK) {
            return Status::Invalid(fmt::format("invalid S3 endpoint {} {}: code {}", endpoint, name,
                                               static_cast<int>(result)));
        }
        ScopeGuard free_value([value] { curl_free(value); });
        return std::optional<std::string>(value);
    };
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> scheme,
                           get_part(CURLUPART_SCHEME, CURLUE_NO_SCHEME, "scheme"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> host,
                           get_part(CURLUPART_HOST, CURLUE_NO_HOST, "host"));
    std::string normalized_scheme = scheme ? StringUtils::ToLowerCase(*scheme) : "";
    if (!host || (normalized_scheme != "http" && normalized_scheme != "https")) {
        return Status::Invalid(fmt::format("invalid S3 endpoint {}", endpoint));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> user,
                           get_part(CURLUPART_USER, CURLUE_NO_USER, "user"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> password,
                           get_part(CURLUPART_PASSWORD, CURLUE_NO_PASSWORD, "password"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> query,
                           get_part(CURLUPART_QUERY, CURLUE_NO_QUERY, "query"));
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> fragment,
                           get_part(CURLUPART_FRAGMENT, CURLUE_NO_FRAGMENT, "fragment"));
    if (user || password || query || fragment) {
        return Status::Invalid(fmt::format(
            "S3 endpoint {} must not contain user, password, query, or fragment", endpoint));
    }
    PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> port,
                           get_part(CURLUPART_PORT, CURLUE_NO_PORT, "port"));
    char* path = nullptr;
    code = curl_url_get(url, CURLUPART_PATH, &path, 0);
    if (code != CURLUE_OK) {
        return Status::Invalid(
            fmt::format("invalid S3 endpoint {} path: code {}", endpoint, static_cast<int>(code)));
    }
    ScopeGuard free_path([path] { curl_free(path); });
    std::string authority = *host;
    if (authority.find(':') != std::string::npos) {
        authority = "[" + authority + "]";
    }
    if (port) {
        authority += ":" + *port;
    }
    return Endpoint{std::move(normalized_scheme), std::move(authority), path};
}

bool IsVirtualHostableS3Bucket(const std::string& bucket, bool allow_subdomains) {
    if (bucket.size() < 3 || bucket.size() > 63) {
        return false;
    }
    bool label_start = true;
    for (size_t index = 0; index < bucket.size(); ++index) {
        const auto character = static_cast<unsigned char>(bucket[index]);
        if (std::islower(character) || std::isdigit(character)) {
            label_start = false;
            continue;
        }
        if (character == '-') {
            if (label_start || index + 1 == bucket.size() || bucket[index + 1] == '.') {
                return false;
            }
            continue;
        }
        if (character == '.') {
            if (!allow_subdomains || label_start || index + 1 == bucket.size()) {
                return false;
            }
            label_start = true;
            continue;
        }
        return false;
    }
    return !label_start;
}

bool IsIpAddressAuthority(const std::string& authority) {
    if (!authority.empty() && authority.front() == '[') {
        return true;
    }
    std::string_view host(authority);
    size_t port_separator = host.find(':');
    if (port_separator != std::string_view::npos) {
        host = host.substr(0, port_separator);
    }
    size_t component_start = 0;
    int component_count = 0;
    while (component_start < host.size()) {
        size_t component_end = host.find('.', component_start);
        std::string_view component = host.substr(component_start, component_end - component_start);
        if (component.empty() || component.size() > 3) {
            return false;
        }
        int value = 0;
        for (unsigned char character : component) {
            if (!std::isdigit(character)) {
                return false;
            }
            value = value * 10 + character - '0';
        }
        if (value > 255) {
            return false;
        }
        ++component_count;
        if (component_end == std::string_view::npos) {
            break;
        }
        component_start = component_end + 1;
    }
    return component_count == 4;
}

const char* AwsDnsSuffixForRegion(const std::string& region) {
    if (region.rfind("cn-", 0) == 0) {
        return "amazonaws.com.cn";
    }
    if (region.rfind("eusc-de-", 0) == 0) {
        return "amazonaws.eu";
    }
    if (region.rfind("us-iso-", 0) == 0) {
        return "c2s.ic.gov";
    }
    if (region.rfind("us-isob-", 0) == 0) {
        return "sc2s.sgov.gov";
    }
    if (region.rfind("eu-isoe-", 0) == 0) {
        return "cloud.adc-e.uk";
    }
    if (region.rfind("us-isof-", 0) == 0) {
        return "csp.hci.ic.gov";
    }
    return "amazonaws.com";
}

struct SigningContext {
    SigningContext(aws_allocator* allocator, aws_http_message* message)
        : allocator(allocator), message(message) {}

    aws_allocator* allocator;
    aws_http_message* message;
    std::mutex mutex;
    std::condition_variable condition;
    int error_code = AWS_ERROR_SUCCESS;
    bool complete = false;
};

void OnSigningComplete(aws_signing_result* result, int error_code, void* user_data) {
    auto* context = static_cast<SigningContext*>(user_data);
    if (error_code == AWS_ERROR_SUCCESS &&
        aws_apply_signing_result_to_http_request(context->message, context->allocator, result)) {
        error_code = aws_last_error();
    }
    {
        std::scoped_lock lock(context->mutex);
        context->error_code = error_code;
        context->complete = true;
    }
    context->condition.notify_one();
}

class S3ObjectStoreClient : public ObjectStoreClient,
                            public std::enable_shared_from_this<S3ObjectStoreClient> {
 public:
    static Result<std::shared_ptr<ObjectStoreClient>> Create(
        const std::map<std::string, std::string>& options, std::shared_ptr<HttpClient> http_client,
        std::shared_ptr<aws_credentials_provider> credentials, std::unique_ptr<Executor> executor) {
        PAIMON_ASSIGN_OR_RAISE(std::string region, ResolveRegion(options));
        auto endpoint = options.find(kS3EndpointOption);
        bool use_default_endpoint = endpoint == options.end() || endpoint->second.empty();
        PAIMON_ASSIGN_OR_RAISE(
            Endpoint parsed_endpoint,
            ParseEndpoint(use_default_endpoint ? fmt::format("https://s3.{}.{}", region,
                                                             AwsDnsSuffixForRegion(region))
                                               : endpoint->second));
        auto path_style = options.find(kS3PathStyleAccessOption);
        bool use_path_style =
            path_style != options.end() &&
            OptionsUtils::GetValueFromMap<bool>(options, kS3PathStyleAccessOption).value();
        return std::shared_ptr<ObjectStoreClient>(new S3ObjectStoreClient(
            std::move(http_client), std::move(credentials), std::move(executor),
            std::move(parsed_endpoint), std::move(region), use_path_style, use_default_endpoint));
    }

    Result<ObjectMetadata> HeadObject(const ObjectStorePath& path) const override {
        PAIMON_ASSIGN_OR_RAISE(HttpResponse response,
                               Execute(path, HttpMethod::HEAD, "", {}, nullptr));
        if (response.status_code == 404) {
            return Status::NotExist(
                fmt::format("s3://{}/{} does not exist", path.bucket, path.key));
        }
        PAIMON_RETURN_NOT_OK(CheckResponse(response, "HeadObject", path));
        auto length = response.headers.find("content-length");
        if (length == response.headers.end()) {
            return Status::IOError("HeadObject response is missing Content-Length");
        }
        int64_t modification_time = 0;
        auto modified = response.headers.find("last-modified");
        if (modified != response.headers.end()) {
            modification_time = ParseModificationTime(modified->second);
        }
        PAIMON_ASSIGN_OR_RAISE(int64_t object_size,
                               ParseNonNegativeInt64(length->second, "Content-Length"));
        return ObjectMetadata{path.key, object_size, modification_time};
    }

    Result<ListObjectsResult> ListObjects(const ObjectStorePath& path,
                                          const std::string& continuation_token,
                                          int32_t max_keys) const override {
        std::string query = "list-type=2&delimiter=%2F&encoding-type=url";
        if (!path.key.empty()) {
            PAIMON_ASSIGN_OR_RAISE(std::string encoded_prefix, PercentEncode(path.key, false));
            query += "&prefix=" + encoded_prefix;
        }
        if (!continuation_token.empty()) {
            PAIMON_ASSIGN_OR_RAISE(std::string encoded_token,
                                   PercentEncode(continuation_token, false));
            query += "&continuation-token=" + encoded_token;
        }
        if (max_keys > 0) {
            query += "&max-keys=" + std::to_string(max_keys);
        }
        std::string body;
        HttpBodyConsumer consumer = [&body](const char* data, int64_t size) {
            body.append(data, static_cast<size_t>(size));
            return Status::OK();
        };
        ObjectStorePath bucket_path{path.bucket, ""};
        PAIMON_ASSIGN_OR_RAISE(HttpResponse response,
                               Execute(bucket_path, HttpMethod::GET, query, {}, consumer));
        if (response.status_code == 404) {
            return Status::NotExist(fmt::format("S3 bucket {} does not exist", path.bucket));
        }
        PAIMON_RETURN_NOT_OK(CheckResponse(response, "ListObjectsV2", path));
        if (body.find("<ListBucketResult") == std::string::npos ||
            body.find("</ListBucketResult>") == std::string::npos) {
            return Status::IOError("malformed S3 ListObjectsV2 XML response");
        }
        ListObjectsResult result;
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> contents, TagBlocks(body, "Contents"));
        for (const std::string& block : contents) {
            auto key = TagValue(block, "Key");
            auto size = TagValue(block, "Size");
            if (!key || !size) {
                return Status::IOError("S3 ListObjectsV2 Contents is missing Key or Size");
            }
            PAIMON_ASSIGN_OR_RAISE(std::string decoded_key, PercentDecode(*key, "Key"));
            PAIMON_ASSIGN_OR_RAISE(int64_t object_size,
                                   ParseNonNegativeInt64(*size, "ListObjectsV2 Size"));
            int64_t modified = 0;
            auto last_modified = TagValue(block, "LastModified");
            if (last_modified) {
                modified = ParseModificationTime(*last_modified);
            }
            result.objects.push_back(ObjectMetadata{decoded_key, object_size, modified});
        }
        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> common_prefixes,
                               TagBlocks(body, "CommonPrefixes"));
        for (const std::string& block : common_prefixes) {
            auto prefix = TagValue(block, "Prefix");
            if (!prefix) {
                return Status::IOError("S3 ListObjectsV2 CommonPrefixes is missing Prefix");
            }
            PAIMON_ASSIGN_OR_RAISE(std::string decoded_prefix, PercentDecode(*prefix, "Prefix"));
            result.common_prefixes.push_back(std::move(decoded_prefix));
        }
        auto is_truncated = TagValue(body, "IsTruncated");
        if (!is_truncated || (*is_truncated != "true" && *is_truncated != "false")) {
            return Status::IOError("S3 ListObjectsV2 response has invalid IsTruncated");
        }
        result.is_truncated = *is_truncated == "true";
        auto token = TagValue(body, "NextContinuationToken");
        if (token) {
            result.continuation_token = std::move(*token);
        }
        return result;
    }

    Result<int64_t> GetObjectRange(const ObjectStorePath& path, int64_t offset, int64_t size,
                                   char* buffer) const override {
        if (size == 0) {
            return 0;
        }
        int64_t copied = 0;
        HttpHeaders headers{{"range", fmt::format("bytes={}-{}", offset, offset + size - 1)}};
        HttpBodyConsumer consumer = [&copied, buffer, size](const char* data, int64_t length) {
            if (length > size - copied) {
                return Status::IOError("S3 range response exceeds the requested length");
            }
            std::memcpy(buffer + copied, data, static_cast<size_t>(length));
            copied += length;
            return Status::OK();
        };
        PAIMON_ASSIGN_OR_RAISE(HttpResponse response,
                               Execute(path, HttpMethod::GET, "", headers, consumer));
        if (response.status_code == 404) {
            return Status::NotExist(
                fmt::format("s3://{}/{} does not exist", path.bucket, path.key));
        }
        PAIMON_RETURN_NOT_OK(CheckResponse(response, "GetObject", path));
        if (copied != size) {
            return Status::IOError(
                fmt::format("GetObject read {} bytes for s3://{}/{}, expected {}", copied,
                            path.bucket, path.key, size));
        }
        return copied;
    }

    void GetObjectRangeAsync(const ObjectStorePath& path, int64_t offset, int64_t size,
                             char* buffer, std::function<void(Status)>&& callback) const override {
        auto self = shared_from_this();
        executor_->Add([self = std::move(self), path, offset, size, buffer,
                        callback = std::move(callback)]() mutable {
            Result<int64_t> result = self->GetObjectRange(path, offset, size, buffer);
            callback(result.ok() ? Status::OK() : result.status());
        });
    }

 private:
    S3ObjectStoreClient(std::shared_ptr<HttpClient> http_client,
                        std::shared_ptr<aws_credentials_provider> credentials,
                        std::unique_ptr<Executor> executor, Endpoint endpoint, std::string region,
                        bool path_style, bool use_default_endpoint)
        : http_client_(std::move(http_client)),
          credentials_(std::move(credentials)),
          endpoint_(std::move(endpoint)),
          region_(std::move(region)),
          path_style_(path_style),
          use_default_endpoint_(use_default_endpoint),
          executor_(std::move(executor)) {}
    Status CheckResponse(const HttpResponse& response, const std::string& operation,
                         const ObjectStorePath& path) const {
        if (response.status_code >= 200 && response.status_code < 300) {
            return Status::OK();
        }
        return Status::IOError(fmt::format("{} failed for s3://{}/{}: HTTP {}", operation,
                                           path.bucket, path.key, response.status_code));
    }

    Result<HttpResponse> Execute(const ObjectStorePath& object, HttpMethod method,
                                 const std::string& query, const HttpHeaders& headers,
                                 const HttpBodyConsumer& consumer) const {
        std::string authority = endpoint_.authority;
        std::string request_path = endpoint_.base_path;
        if (request_path.empty() || request_path.back() != '/') {
            request_path += '/';
        }
        bool use_path_style =
            path_style_ ||
            (use_default_endpoint_
                 ? !IsVirtualHostableS3Bucket(object.bucket, false)
                 : endpoint_.scheme != "http" || IsIpAddressAuthority(endpoint_.authority) ||
                       !IsVirtualHostableS3Bucket(object.bucket, true));
        if (use_path_style) {
            PAIMON_ASSIGN_OR_RAISE(std::string encoded_bucket, PercentEncode(object.bucket, false));
            request_path += encoded_bucket + "/";
        } else {
            authority = object.bucket + "." + authority;
        }
        PAIMON_ASSIGN_OR_RAISE(std::string encoded_key, PercentEncode(object.key, true));
        request_path += encoded_key;
        if (!query.empty()) {
            request_path += "?" + query;
        }

        PAIMON_ASSIGN_OR_RAISE(AwsAuthRuntime * runtime, GetAwsAuthRuntime());
        aws_http_message* message = aws_http_message_new_request(runtime->allocator());
        if (message == nullptr) {
            return Status::IOError("failed to create S3 HTTP request");
        }
        ScopeGuard release_message([message] { aws_http_message_release(message); });
        std::string method_name = method == HttpMethod::HEAD ? "HEAD" : "GET";
        aws_http_message_set_request_method(message, Cursor(method_name));
        aws_http_message_set_request_path(message, Cursor(request_path));
        aws_http_header host_header{};
        host_header.name = aws_byte_cursor_from_c_str("host");
        host_header.value = Cursor(authority);
        aws_http_message_add_header(message, host_header);
        for (const auto& [name, value] : headers) {
            aws_http_header header{};
            header.name = Cursor(name);
            header.value = Cursor(value);
            aws_http_message_add_header(message, header);
        }
        aws_signable* signable = aws_signable_new_http_request(runtime->allocator(), message);
        if (signable == nullptr) {
            return Status::IOError("failed to create S3 signable request");
        }
        ScopeGuard destroy_signable([signable] { aws_signable_destroy(signable); });
        aws_signing_config_aws config{};
        config.config_type = AWS_SIGNING_CONFIG_AWS;
        config.algorithm = AWS_SIGNING_ALGORITHM_V4;
        config.signature_type = AWS_ST_HTTP_REQUEST_HEADERS;
        config.region = Cursor(region_);
        config.service = aws_byte_cursor_from_c_str("s3");
        aws_date_time_init_now(&config.date);
        config.flags.use_double_uri_encode = false;
        config.flags.should_normalize_uri_path = false;
        config.signed_body_value = g_aws_signed_body_value_unsigned_payload;
        config.signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;
        config.credentials_provider = credentials_.get();

        SigningContext context(runtime->allocator(), message);
        int result = aws_sign_request_aws(runtime->allocator(), signable,
                                          reinterpret_cast<aws_signing_config_base*>(&config),
                                          OnSigningComplete, &context);
        if (result == AWS_OP_SUCCESS) {
            std::unique_lock<std::mutex> lock(context.mutex);
            context.condition.wait(lock, [&context] { return context.complete; });
        }
        if (result != AWS_OP_SUCCESS || context.error_code != AWS_ERROR_SUCCESS) {
            int error = result == AWS_OP_SUCCESS ? context.error_code : aws_last_error();
            return Status::IOError(
                fmt::format("failed to sign S3 request: {}", aws_error_debug_str(error)));
        }

        HttpRequest request;
        request.method = method;
        request.url = endpoint_.scheme + "://" + authority + request_path;
        aws_http_headers* signed_headers = aws_http_message_get_headers(message);
        for (size_t i = 0; i < aws_http_headers_count(signed_headers); ++i) {
            aws_http_header header;
            aws_http_headers_get_index(signed_headers, i, &header);
            request.headers[std::string(reinterpret_cast<const char*>(header.name.ptr),
                                        header.name.len)] =
                std::string(reinterpret_cast<const char*>(header.value.ptr), header.value.len);
        }
        HttpBodyConsumer body_consumer = consumer;
        if (!body_consumer) {
            body_consumer = [](const char*, int64_t) { return Status::OK(); };
        }
        return http_client_->Execute(request, body_consumer);
    }

    std::shared_ptr<HttpClient> http_client_;
    std::shared_ptr<aws_credentials_provider> credentials_;
    Endpoint endpoint_;
    std::string region_;
    bool path_style_ = false;
    bool use_default_endpoint_ = false;
    std::unique_ptr<Executor> executor_;
};

}  // namespace

Status ValidateS3Options(const std::map<std::string, std::string>& options) {
    auto access = options.find(kS3AccessKeyOption);
    auto secret = options.find(kS3SecretKeyOption);
    auto token = options.find(kS3SessionTokenOption);
    bool has_access = access != options.end();
    bool has_secret = secret != options.end();
    bool has_token = token != options.end();
    if (has_access != has_secret) {
        return Status::Invalid(fmt::format("{} and {} must be configured together",
                                           kS3AccessKeyOption, kS3SecretKeyOption));
    }
    if (has_token && !has_access) {
        return Status::Invalid(fmt::format("{} requires {} and {}", kS3SessionTokenOption,
                                           kS3AccessKeyOption, kS3SecretKeyOption));
    }
    if (has_access && access->second.empty()) {
        return Status::Invalid(fmt::format("{} must not be empty", kS3AccessKeyOption));
    }
    if (has_secret && secret->second.empty()) {
        return Status::Invalid(fmt::format("{} must not be empty", kS3SecretKeyOption));
    }
    if (options.find(kS3PathStyleAccessOption) == options.end()) {
        return Status::OK();
    }
    Result<bool> parsed = OptionsUtils::GetValueFromMap<bool>(options, kS3PathStyleAccessOption);
    if (!parsed.ok()) {
        return Status::Invalid(
            fmt::format("{} {}", kS3PathStyleAccessOption, parsed.status().message()));
    }
    return Status::OK();
}

S3FileSystem::S3FileSystem(std::shared_ptr<ObjectStoreClient> client)
    : ObjectStoreFileSystem("s3", std::move(client)) {}

Result<std::unique_ptr<FileSystem>> S3FileSystem::Create(
    const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ObjectStoreClient> client,
                           MakeS3ObjectStoreClient(options, std::make_shared<CurlHttpClient>()));
    return std::unique_ptr<FileSystem>(new S3FileSystem(std::move(client)));
}

Result<std::shared_ptr<ObjectStoreClient>> MakeS3ObjectStoreClient(
    const std::map<std::string, std::string>& options, std::shared_ptr<HttpClient> http_client) {
    std::map<std::string, std::string> normalized_options = NormalizeS3Options(options);
    PAIMON_RETURN_NOT_OK(ValidateS3Options(normalized_options));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<aws_credentials_provider> credentials,
                           MakeCredentialsProvider(normalized_options));
    if (!credentials) {
        return Status::IOError("failed to initialize S3 credentials provider");
    }
    std::unique_ptr<Executor> executor = CreateDefaultExecutor();
    return S3ObjectStoreClient::Create(normalized_options, std::move(http_client),
                                       std::move(credentials), std::move(executor));
}

}  // namespace paimon::s3
