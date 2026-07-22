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

#include "paimon/rest/mock_rest_server.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/common/utils/url_utils.h"

namespace paimon {

namespace {

bool ReceiveAll(int32_t fd, std::string* buffer, size_t min_size) {
    char chunk[4096];
    while (buffer->size() < min_size) {
        ssize_t received = ::recv(fd, chunk, sizeof(chunk), 0);
        if (received <= 0) {
            return false;
        }
        buffer->append(chunk, static_cast<size_t>(received));
    }
    return true;
}

std::map<std::string, std::string> ParseQuery(const std::string& query) {
    std::map<std::string, std::string> params;
    for (const std::string& pair : StringUtils::Split(query, "&", /*ignore_empty=*/true)) {
        size_t eq = pair.find('=');
        if (eq == std::string::npos) {
            params[UrlUtils::DecodeString(pair)] = "";
        } else {
            params[UrlUtils::DecodeString(pair.substr(0, eq))] =
                UrlUtils::DecodeString(pair.substr(eq + 1));
        }
    }
    return params;
}

}  // namespace

MockRestServer::MockRestServer(Handler handler, int32_t listen_fd, int32_t port)
    : handler_(std::move(handler)), listen_fd_(listen_fd), port_(port) {
    accept_thread_ = std::thread([this] { AcceptLoop(); });
}

Result<std::unique_ptr<MockRestServer>> MockRestServer::Start(Handler handler) {
    int32_t listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        return Status::IOError("mock rest server: failed to create socket: ", std::strerror(errno));
    }
    int32_t reuse = 1;
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in address;
    std::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(listen_fd, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) < 0) {
        ::close(listen_fd);
        return Status::IOError("mock rest server: failed to bind: ", std::strerror(errno));
    }
    if (::listen(listen_fd, 16) < 0) {
        ::close(listen_fd);
        return Status::IOError("mock rest server: failed to listen: ", std::strerror(errno));
    }
    socklen_t address_len = sizeof(address);
    if (::getsockname(listen_fd, reinterpret_cast<struct sockaddr*>(&address), &address_len) < 0) {
        ::close(listen_fd);
        return Status::IOError("mock rest server: failed to get port: ", std::strerror(errno));
    }
    int32_t port = ntohs(address.sin_port);
    return std::unique_ptr<MockRestServer>(new MockRestServer(std::move(handler), listen_fd, port));
}

MockRestServer::~MockRestServer() {
    Stop();
}

void MockRestServer::Stop() {
    if (stopped_.exchange(true)) {
        return;
    }
    // `shutdown` wakes the blocked `accept`; the fd is closed only after the accept
    // thread has joined, so it cannot be recycled by another thread while the accept
    // thread might still use it.
    ::shutdown(listen_fd_, SHUT_RDWR);
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    ::close(listen_fd_);
}

std::string MockRestServer::GetBaseUri() const {
    return fmt::format("http://127.0.0.1:{}", port_);
}

void MockRestServer::AcceptLoop() {
    while (!stopped_.load()) {
        int32_t connection_fd = ::accept(listen_fd_, nullptr, nullptr);
        if (connection_fd < 0) {
            if (stopped_.load()) {
                return;
            }
            continue;
        }
        // The connection is handled on the accept thread; the socket timeouts keep a
        // wedged peer from blocking `Stop()` indefinitely.
        struct timeval timeout;
        timeout.tv_sec = 30;
        timeout.tv_usec = 0;
        ::setsockopt(connection_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        ::setsockopt(connection_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        HandleConnection(connection_fd);
        ::close(connection_fd);
    }
}

void MockRestServer::HandleConnection(int32_t connection_fd) {
    std::string buffer;
    size_t header_end;
    while ((header_end = buffer.find("\r\n\r\n")) == std::string::npos) {
        if (!ReceiveAll(connection_fd, &buffer, buffer.size() + 1)) {
            return;
        }
    }

    Request request;
    std::string header_part = buffer.substr(0, header_end);
    std::vector<std::string> lines = StringUtils::Split(header_part, "\r\n",
                                                        /*ignore_empty=*/true);
    if (lines.empty()) {
        return;
    }
    std::vector<std::string> request_line = StringUtils::Split(lines[0], " ",
                                                               /*ignore_empty=*/true);
    if (request_line.size() < 2) {
        return;
    }
    request.method = request_line[0];
    std::string target = request_line[1];
    size_t question = target.find('?');
    if (question == std::string::npos) {
        request.path = UrlUtils::DecodeString(target);
    } else {
        request.path = UrlUtils::DecodeString(target.substr(0, question));
        request.query_params = ParseQuery(target.substr(question + 1));
    }
    size_t content_length = 0;
    for (size_t i = 1; i < lines.size(); i++) {
        size_t colon = lines[i].find(':');
        if (colon == std::string::npos) {
            continue;
        }
        std::string name = lines[i].substr(0, colon);
        std::string value = lines[i].substr(colon + 1);
        StringUtils::Trim(&name);
        StringUtils::Trim(&value);
        request.headers[StringUtils::ToLowerCase(name)] = value;
    }
    auto length_iter = request.headers.find("content-length");
    if (length_iter != request.headers.end()) {
        content_length =
            static_cast<size_t>(std::strtoul(length_iter->second.c_str(), nullptr, 10));
    }
    size_t body_begin = header_end + 4;
    if (!ReceiveAll(connection_fd, &buffer, body_begin + content_length)) {
        return;
    }
    request.body = buffer.substr(body_begin, content_length);

    Response response = handler_(request);
    if (response.close_without_response) {
        return;
    }
    std::string extra_headers;
    for (const auto& [name, value] : response.headers) {
        extra_headers += fmt::format("{}: {}\r\n", name, value);
    }
    std::string payload = fmt::format(
        "HTTP/1.1 {} MOCK\r\nContent-Type: {}\r\nContent-Length: {}\r\n{}Connection: "
        "close\r\n\r\n{}",
        response.code, response.content_type, response.body.size() + response.missing_body_bytes,
        extra_headers, response.body);
    size_t sent = 0;
    while (sent < payload.size()) {
        ssize_t written = ::send(connection_fd, payload.data() + sent, payload.size() - sent, 0);
        if (written <= 0) {
            return;
        }
        sent += static_cast<size_t>(written);
    }
}

}  // namespace paimon
