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

#include "paimon/rest/rest_http_client.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/rest/mock_rest_server.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {
RestHttpClient::Config FastRetryConfig(int32_t max_retries) {
    RestHttpClient::Config config;
    config.max_retries = max_retries;
    config.retry_base_delay_ms = 1;
    return config;
}
}  // namespace

TEST(RestHttpClientTest, NormalizeUri) {
    ASSERT_EQ("http://localhost:80", RestHttpClient::NormalizeUri("localhost:80/"));
    ASSERT_EQ("http://localhost", RestHttpClient::NormalizeUri("http://localhost//"));
    ASSERT_EQ("https://foo.bar", RestHttpClient::NormalizeUri(" https://foo.bar "));
}

TEST(RestHttpClientTest, BuildQueryString) {
    ASSERT_EQ("a=1&b=x+y%2Fz", RestHttpClient::BuildQueryString({{"a", "1"}, {"b", "x y/z"}}));
}

TEST(RestHttpClientTest, GetWithHeadersAndQuery) {
    // the handler runs on the mock server's accept thread, so every state it shares with
    // the test body is mutex guarded, here and in the tests below
    std::mutex mutex;
    MockRestServer::Request last_request;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             std::lock_guard<std::mutex> lock(mutex);
                             last_request = request;
                             MockRestServer::Response response;
                             response.body = R"({"ok": true})";
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri()));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/v1/config", {{"warehouse", "wh 1"}},
                                         {{"Authorization", "Bearer token1"}}, ""));
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(R"({"ok": true})", response.body);
    ASSERT_EQ("application/json", response.headers.at("content-type"));
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ("GET", last_request.method);
    ASSERT_EQ("/v1/config", last_request.path);
    ASSERT_EQ("wh 1", last_request.query_params.at("warehouse"));
    ASSERT_EQ("Bearer token1", last_request.headers.at("authorization"));
}

TEST(RestHttpClientTest, PostBody) {
    std::mutex mutex;
    MockRestServer::Request last_request;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             std::lock_guard<std::mutex> lock(mutex);
                             last_request = request;
                             return MockRestServer::Response();
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri()));
    ASSERT_OK_AND_ASSIGN(
        RestHttpClient::Response response,
        client->Execute("POST", "/v1/databases", {}, {{"Content-Type", "application/json"}},
                        R"({"name": "db1"})"));
    ASSERT_EQ(200, response.code);
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ("POST", last_request.method);
    ASSERT_EQ(R"({"name": "db1"})", last_request.body);
    ASSERT_EQ("application/json", last_request.headers.at("content-type"));
}

TEST(RestHttpClientTest, NotFoundIsNotRetried) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             response.code = 404;
                             response.body = R"({"message": "not found", "code": 404})";
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/v1/databases/db1", {}, {}, ""));
    ASSERT_EQ(404, response.code);
    ASSERT_FALSE(response.IsSuccessful());
    ASSERT_EQ(1, request_count.load());
}

TEST(RestHttpClientTest, ServiceUnavailableIsRetried) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             if (request_count++ < 2) {
                                 response.code = 503;
                             } else {
                                 response.body = R"({"ok": true})";
                             }
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    // 429/503 responses are retried even for non-idempotent POST.
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("POST", "/v1/databases", {}, {}, "{}"));
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(3, request_count.load());
}

TEST(RestHttpClientTest, RetryAfterHeaderIsHonored) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             if (request_count++ == 0) {
                                 response.code = 429;
                                 response.headers["Retry-After"] = "1";
                             }
                             return response;
                         }));
    // the backoff base is large so that the elapsed time below proves the Retry-After
    // header took precedence: without it this test would sleep for a minute
    RestHttpClient::Config config;
    config.max_retries = 5;
    config.retry_base_delay_ms = 60 * 1000;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), config));
    auto start = std::chrono::steady_clock::now();
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/v1/databases", {}, {}, ""));
    auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(2, request_count.load());
    ASSERT_GE(elapsed, std::chrono::seconds(1));
    ASSERT_LT(elapsed, std::chrono::seconds(30));
}

TEST(RestHttpClientTest, ComputeRetryDelayMs) {
    RestHttpClient::Config config;
    config.retry_base_delay_ms = 1000;
    constexpr int64_t kBudgetMs = 1000 * 1000;
    // without a usable Retry-After header: exponential backoff with up to 10% jitter,
    // the multiplier capped at 2^6
    std::optional<int64_t> delay =
        RestHttpClient::ComputeRetryDelayMs(config, 1, nullptr, 0, kBudgetMs);
    ASSERT_TRUE(delay);
    ASSERT_GE(delay.value(), 1000);
    ASSERT_LE(delay.value(), 1100);
    delay = RestHttpClient::ComputeRetryDelayMs(config, 3, nullptr, 0, kBudgetMs);
    ASSERT_TRUE(delay);
    ASSERT_GE(delay.value(), 4000);
    ASSERT_LE(delay.value(), 4400);
    // the capped multiplier lands exactly on the default per-retry bound, so the
    // jitter is clamped away
    delay = RestHttpClient::ComputeRetryDelayMs(config, 100, nullptr, 0, kBudgetMs);
    ASSERT_EQ(64000, delay);
    // an out-of-contract execution count degrades to the first-attempt delay instead
    // of shifting by a negative amount
    delay = RestHttpClient::ComputeRetryDelayMs(config, 0, nullptr, 0, kBudgetMs);
    ASSERT_TRUE(delay);
    ASSERT_GE(delay.value(), 1000);
    ASSERT_LE(delay.value(), 1100);

    // the delta-seconds form takes precedence over the backoff, without jitter
    RestHttpClient::Response response;
    response.headers["retry-after"] = "7";
    ASSERT_EQ(7000, RestHttpClient::ComputeRetryDelayMs(config, 1, &response, 0, kBudgetMs));

    // the HTTP-date form is resolved against the given "now"
    constexpr int64_t kDateEpoch = 1445412480;  // Wed, 21 Oct 2015 07:28:00 GMT
    response.headers["retry-after"] = "Wed, 21 Oct 2015 07:28:00 GMT";
    ASSERT_EQ(5000,
              RestHttpClient::ComputeRetryDelayMs(config, 1, &response, kDateEpoch - 5, kBudgetMs));

    // a date in the past falls back to the backoff
    delay = RestHttpClient::ComputeRetryDelayMs(config, 1, &response, kDateEpoch + 1, kBudgetMs);
    ASSERT_TRUE(delay);
    ASSERT_GE(delay.value(), 1000);
    ASSERT_LE(delay.value(), 1100);
}

TEST(RestHttpClientTest, RetryDelayBounds) {
    RestHttpClient::Config config;
    config.retry_base_delay_ms = 1000;
    config.retry_max_delay_ms = 10 * 1000;
    RestHttpClient::Response response;

    // the per-retry bound applies to the backoff only: a Retry-After beyond it is
    // still honored as long as it fits the remaining budget
    response.headers["retry-after"] = "11";
    ASSERT_EQ(11000, RestHttpClient::ComputeRetryDelayMs(config, 1, &response, 0, 1000 * 1000));
    // a Retry-After beyond the remaining budget stops retrying instead of sleeping
    // less than the server requested
    response.headers["retry-after"] = "9";
    ASSERT_FALSE(RestHttpClient::ComputeRetryDelayMs(config, 1, &response, 0, 8000));
    ASSERT_EQ(9000, RestHttpClient::ComputeRetryDelayMs(config, 1, &response, 0, 9000));

    // the backoff is clamped to the per-retry bound...
    ASSERT_EQ(10000, RestHttpClient::ComputeRetryDelayMs(config, 100, nullptr, 0, 1000 * 1000));
    // ...and stops retrying when even the clamped delay does not fit the budget
    ASSERT_FALSE(RestHttpClient::ComputeRetryDelayMs(config, 100, nullptr, 0, 9999));
    // an exhausted budget stops retrying before any header is consulted
    ASSERT_FALSE(RestHttpClient::ComputeRetryDelayMs(config, 1, &response, 0, 0));
}

TEST(RestHttpClientTest, ExcessiveRetryAfterStopsRetrying) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             response.code = 503;
                             // an hour is far beyond the default overall retry budget
                             response.headers["Retry-After"] = "3600";
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/v1/databases", {}, {}, ""));
    // the 503 is returned as-is after a single attempt instead of five shortened
    // sleeps the server did not ask for
    ASSERT_EQ(503, response.code);
    ASSERT_EQ(1, request_count.load());
}

TEST(RestHttpClientTest, InvalidRetryAfterFallsBackToBackoff) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             int32_t count = request_count++;
                             if (count == 0) {
                                 response.code = 429;
                                 // non-positive values are ignored
                                 response.headers["Retry-After"] = "0";
                             } else if (count == 2) {
                                 response.code = 429;
                                 // neither delta-seconds nor a valid http date
                                 response.headers["Retry-After"] = "Sat, 32 Foo 2015 99:99:99 GMT";
                             }
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/v1/databases", {}, {}, ""));
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(2, request_count.load());
    ASSERT_OK_AND_ASSIGN(response, client->Execute("GET", "/v1/databases", {}, {}, ""));
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(4, request_count.load());
}

TEST(RestHttpClientTest, RetriesExhausted) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             response.code = 503;
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(2)));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/v1/databases", {}, {}, ""));
    ASSERT_EQ(503, response.code);
    // initial attempt + 2 retries
    ASSERT_EQ(3, request_count.load());
}

TEST(RestHttpClientTest, EmptyReplyIsNotRetried) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             // closing without a response maps to CURLE_GOT_NOTHING, a
                             // non-retriable transport error that, unlike timeouts or
                             // TLS failures, is deterministic in a unit test
                             response.close_without_response = true;
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_NOK(client->Execute("GET", "/v1/databases", {}, {}, "").status());
    ASSERT_EQ(1, request_count.load());
}

TEST(RestHttpClientTest, ConnectionRefusedIsNotRetried) {
    // a closed port yields CURLE_COULDNT_CONNECT, another of the non-retriable
    // transport errors; the port of a stopped mock server is known to be closed
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([](const MockRestServer::Request& request) {
                             return MockRestServer::Response();
                         }));
    std::string base_uri = server->GetBaseUri();
    server->Stop();
    // the backoff base is large so that the elapsed time below proves the failure was
    // not retried: a single retry would sleep for a minute
    RestHttpClient::Config config;
    config.max_retries = 5;
    config.retry_base_delay_ms = 60 * 1000;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(base_uri, config));
    auto start = std::chrono::steady_clock::now();
    ASSERT_NOK(client->Execute("GET", "/v1/databases", {}, {}, "").status());
    auto elapsed = std::chrono::steady_clock::now() - start;
    ASSERT_LT(elapsed, std::chrono::seconds(30));
}

TEST(RestHttpClientTest, DeleteWithBodyIsSent) {
    std::mutex mutex;
    MockRestServer::Request last_request;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             std::lock_guard<std::mutex> lock(mutex);
                             last_request = request;
                             return MockRestServer::Response();
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri()));
    // a body on DELETE keeps the DELETE method line instead of degrading to POST
    ASSERT_OK_AND_ASSIGN(
        RestHttpClient::Response response,
        client->Execute("DELETE", "/v1/databases/db1", {}, {{"Content-Type", "application/json"}},
                        R"({"purge": true})"));
    ASSERT_EQ(200, response.code);
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ("DELETE", last_request.method);
    ASSERT_EQ(R"({"purge": true})", last_request.body);
}

TEST(RestHttpClientTest, ReusedHandleDoesNotCarryRequestState) {
    // the client pools its curl handles to keep connections alive, so a request must not
    // inherit the method or the body of the request that used the handle before it
    std::mutex mutex;
    std::vector<MockRestServer::Request> requests;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             std::lock_guard<std::mutex> lock(mutex);
                             requests.push_back(request);
                             return MockRestServer::Response();
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri()));
    ASSERT_OK(client->Execute("POST", "/v1/databases", {}, {}, R"({"name": "db1"})").status());
    ASSERT_OK(
        client->Execute("DELETE", "/v1/databases/db1", {}, {}, R"({"purge": true})").status());
    ASSERT_OK(client->Execute("GET", "/v1/databases", {}, {}, "").status());
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ(3, requests.size());
    ASSERT_EQ("GET", requests[2].method);
    ASSERT_TRUE(requests[2].body.empty()) << requests[2].body;
}

TEST(RestHttpClientTest, TransportErrorOmitsUrlAndQuery) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             response.close_without_response = true;
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri()));
    Status status =
        client->Execute("GET", "/v1/secret-path", {{"sig", "topsecret1"}}, {}, "").status();
    ASSERT_NOK(status);
    // the error must not echo the path or query values, which may carry credentials
    // (e.g. a presigned url)
    ASSERT_EQ(std::string::npos, status.ToString().find("secret-path")) << status.ToString();
    ASSERT_EQ(std::string::npos, status.ToString().find("topsecret1")) << status.ToString();
}

TEST(RestHttpClientTest, TruncatedResponseIsRetriedForGet) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             response.body = R"({"ok": true})";
                             if (request_count++ == 0) {
                                 // cutting the response off mid-body is a retriable
                                 // transport error, and GET is idempotent
                                 response.missing_body_bytes = 5;
                             }
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/v1/databases", {}, {}, ""));
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(R"({"ok": true})", response.body);
    ASSERT_EQ(2, request_count.load());
}

TEST(RestHttpClientTest, TruncatedResponseIsRetriedForDelete) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             if (request_count++ == 0) {
                                 // DELETE is the other idempotent method (see the GET
                                 // test above)
                                 response.missing_body_bytes = 5;
                                 response.body = R"({"ok": true})";
                             }
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("DELETE", "/v1/databases/db1", {}, {}, ""));
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(2, request_count.load());
}

TEST(RestHttpClientTest, TruncatedResponseIsNotRetriedForPost) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             response.body = R"({"ok": true})";
                             // the same retriable transport error kind as in the GET
                             // test above, but POST is not idempotent
                             response.missing_body_bytes = 5;
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_NOK(client->Execute("POST", "/v1/databases", {}, {}, "{}").status());
    ASSERT_EQ(1, request_count.load());
}

TEST(RestHttpClientTest, RedirectIsFollowed) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             if (request_count++ == 0) {
                                 response.code = 302;
                                 response.headers["Location"] = "/v1/config";
                             } else {
                                 response.body = R"({"ok": true})";
                             }
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri()));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("GET", "/old", {}, {}, ""));
    ASSERT_EQ(200, response.code);
    ASSERT_EQ(R"({"ok": true})", response.body);
    ASSERT_EQ(2, request_count.load());
    // only the final response's headers are reported, not the redirect's
    ASSERT_EQ(0, response.headers.count("location"));
}

TEST(RestHttpClientTest, PostRedirectKeepsMethodAndBody) {
    std::mutex mutex;
    MockRestServer::Request last_request;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             MockRestServer::Response response;
                             if (request.path == "/old") {
                                 // 302 is one of the codes that would degrade a POST
                                 // to a bodyless GET without CURLOPT_POSTREDIR
                                 response.code = 302;
                                 response.headers["Location"] = "/v1/databases";
                                 return response;
                             }
                             std::lock_guard<std::mutex> lock(mutex);
                             last_request = request;
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri()));
    ASSERT_OK_AND_ASSIGN(RestHttpClient::Response response,
                         client->Execute("POST", "/old", {}, {}, R"({"name": "db1"})"));
    ASSERT_EQ(200, response.code);
    std::lock_guard<std::mutex> lock(mutex);
    ASSERT_EQ("/v1/databases", last_request.path);
    ASSERT_EQ("POST", last_request.method);
    ASSERT_EQ(R"({"name": "db1"})", last_request.body);
}

TEST(RestHttpClientTest, RedirectLoopIsNotRetried) {
    std::atomic<int32_t> request_count{0};
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<MockRestServer> server,
                         MockRestServer::Start([&](const MockRestServer::Request& request) {
                             request_count++;
                             MockRestServer::Response response;
                             response.code = 302;
                             response.headers["Location"] = "/loop";
                             return response;
                         }));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create(server->GetBaseUri(), FastRetryConfig(5)));
    ASSERT_NOK(client->Execute("GET", "/loop", {}, {}, "").status());
    // one attempt follows at most CURLOPT_MAXREDIRS (50) redirects and the resulting
    // CURLE_TOO_MANY_REDIRECTS is a permanent transport error: retrying it would
    // multiply the request count by the retry schedule
    ASSERT_LE(request_count.load(), 51);
}

TEST(RestHttpClientTest, InvalidHeadersAreRejected) {
    // headers are validated before anything is sent, so no server is needed
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create("http://127.0.0.1:1"));
    ASSERT_NOK_WITH_MSG(client->Execute("GET", "/", {}, {{"Bad Name", "v"}}, "").status(),
                        "invalid http header name");
    ASSERT_NOK_WITH_MSG(
        client->Execute("GET", "/", {}, {{"Evil\r\nInjected: x", "v"}}, "").status(),
        "invalid http header name");
    ASSERT_NOK_WITH_MSG(client->Execute("GET", "/", {}, {{"", "v"}}, "").status(),
                        "invalid http header name");
    ASSERT_NOK_WITH_MSG(
        client->Execute("GET", "/", {}, {{"X-Ok", "a\r\nInjected: b"}}, "").status(),
        "invalid http header value for 'X-Ok'");
    ASSERT_NOK_WITH_MSG(
        client->Execute("GET", "/", {}, {{"X-Ok", std::string("a\0b", 3)}}, "").status(),
        "invalid http header value for 'X-Ok'");
}

TEST(RestHttpClientTest, UnsupportedMethod) {
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RestHttpClient> client,
                         RestHttpClient::Create("http://127.0.0.1:1"));
    ASSERT_NOK_WITH_MSG(client->Execute("PATCH", "/", {}, {}, "").status(),
                        "unsupported http method");
}

}  // namespace paimon::test
