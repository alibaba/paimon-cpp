/*
 * Copyright 2024-present Alibaba Inc.
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

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

// Adapted from Apache Arrow
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/status.cc

#include "paimon/status.h"

#include <cassert>
#include <cstdlib>
#include <iostream>

namespace paimon {

Status::Status(StatusCode code, const std::string& msg) : Status::Status(code, msg, nullptr) {}

Status::Status(StatusCode code, std::string msg, std::shared_ptr<StatusDetail> detail) {
    // should not construct ok status with message
    assert(code != StatusCode::OK);
    state_ = new State;
    state_->code = code;
    state_->msg = std::move(msg);
    if (detail != nullptr) {
        state_->detail = std::move(detail);
    }
}

void Status::CopyFrom(const Status& s) {
    delete state_;
    if (s.state_ == nullptr) {
        state_ = nullptr;
    } else {
        state_ = new State(*s.state_);
    }
}

std::string Status::CodeAsString() const {
    if (state_ == nullptr) {
        return "OK";
    }
    return CodeAsString(code());
}

std::string Status::CodeAsString(StatusCode code) {
    const char* type;
    switch (code) {
        case StatusCode::OK:
            type = "OK";
            break;
        case StatusCode::OutOfMemory:
            type = "Out of memory";
            break;
        case StatusCode::KeyError:
            type = "Key error";
            break;
        case StatusCode::TypeError:
            type = "Type error";
            break;
        case StatusCode::Invalid:
            type = "Invalid";
            break;
        case StatusCode::IOError:
            type = "IOError";
            break;
        case StatusCode::CapacityError:
            type = "Capacity error";
            break;
        case StatusCode::IndexError:
            type = "Index error";
            break;
        case StatusCode::UnknownError:
            type = "Unknown error";
            break;
        case StatusCode::NotImplemented:
            type = "NotImplemented";
            break;
        case StatusCode::SerializationError:
            type = "Serialization error";
            break;
        default:
            type = "Unknown";
            break;
    }
    return std::string(type);
}

std::string Status::ToString() const {
    std::string result(CodeAsString());
    if (state_ == nullptr) {
        return result;
    }
    result += ": ";
    result += state_->msg;
    if (state_->detail != nullptr) {
        result += ". Detail: ";
        result += state_->detail->ToString();
    }

    return result;
}

void Status::Abort() const {
    Abort(std::string());
}

void Status::Abort(const std::string& message) const {
    std::cerr << "-- Arrow Fatal Error --\n";
    if (!message.empty()) {
        std::cerr << message << "\n";
    }
    std::cerr << ToString() << std::endl;
    std::abort();
}

#ifdef PAIMON_EXTRA_ERROR_CONTEXT
void Status::AddContextLine(const char* filename, int line, const char* function_name,
                            const char* expr) {
    assert(!ok() && "Cannot add context line to ok status");
    std::stringstream ss;
    ss << "\nIn " << filename << ":" << line << ", function: " << function_name
       << ", code: " << expr;
    state_->msg += ss.str();
}
#endif

}  // namespace paimon
