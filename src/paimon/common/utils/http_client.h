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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

enum class HttpMethod { HEAD, GET };
using HttpHeaders = std::map<std::string, std::string>;
using HttpBodyConsumer = std::function<Status(const char*, int64_t)>;

/// Ensures libcurl's global state is initialized; the returned guard keeps it alive.
PAIMON_EXPORT std::shared_ptr<void> EnsureCurlGlobalInit();

/// Parses one raw HTTP header line into `headers`, lower-casing the name and trimming
/// HTTP whitespace around the name and value; lines without a ':' or with an empty
/// name are ignored.
PAIMON_EXPORT void ParseHttpHeaderLine(const char* data, size_t size, HttpHeaders* headers);

struct HttpRequest {
    HttpMethod method = HttpMethod::GET;
    std::string url;
    HttpHeaders headers;
};

struct HttpResponse {
    int32_t status_code = 0;
    HttpHeaders headers;
    int64_t body_size = 0;
};

class PAIMON_EXPORT HttpClient {
 public:
    virtual ~HttpClient() = default;
    virtual Result<HttpResponse> Execute(const HttpRequest& request,
                                         const HttpBodyConsumer& consumer) const = 0;
};

class PAIMON_EXPORT CurlHttpClient : public HttpClient {
 public:
    CurlHttpClient();
    ~CurlHttpClient() override;

    Result<HttpResponse> Execute(const HttpRequest& request,
                                 const HttpBodyConsumer& consumer) const override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
