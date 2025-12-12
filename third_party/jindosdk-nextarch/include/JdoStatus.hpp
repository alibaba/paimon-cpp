/*
 * Copyright 2024-present Alibaba Cloud.
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

#include <string>
#include <sstream>
#include "jdo_error.h"
#include "jdo_common.h"

#ifdef __has_builtin
#define JDO_HAS_BUILTIN(x) __has_builtin(x)
#else
#define JDO_HAS_BUILTIN(x) 0
#endif

#if (!defined(__NVCC__)) && \
    (JDO_HAS_BUILTIN(__builtin_expect) || (defined(__GNUC__) && __GNUC__ >= 3))
#define JDO_PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define JDO_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define JDO_PREDICT_FALSE(x) (x)
#define JDO_PREDICT_TRUE(x) (x)
#endif

#define JDO_RETURN_IF_ERROR(...)                          \
  do {                                                    \
    JdoStatus _status = (__VA_ARGS__);                    \
    if (JDO_PREDICT_FALSE(!_status.ok())) return _status; \
  } while (0)

class JDO_EXPORT JdoStatus {
public:
    uint64_t _tid = 0;
    int _errCode  = 0;
    std::string _errMsg;

    JdoStatus() {
    }

    virtual ~JdoStatus() {
    }

    JdoStatus(int errCode, std::string errMsg) {
        _errCode = errCode;
        _errMsg  = std::move(errMsg);
    }

    JdoStatus(int errCode, const char* errMsg) {
        _errCode = errCode;
        _errMsg  = std::string(errMsg);
    }

    inline bool hasErr() {
        return _errCode != 0;
    }

    inline void clear() {
        _errCode = 0;
        _errMsg.clear();
    }

    inline void setErrCode(int errCode) {
        _errCode = errCode;
    }

    inline int code() {
        return _errCode;
    }

    inline int getErrCode() {
        return _errCode;
    }

    inline void setErrMsg(std::string errMsg) {
        _errMsg = std::move(errMsg);
    }

    inline std::string errMsg() {
        return _errMsg;
    }

    inline std::string getErrMsg() {
        return _errMsg;
    }

    inline bool ok() {
        return _errCode == 0;
    }

    inline bool isOk() {
        return _errCode == 0;
    }

    friend std::ostream& operator<<(std::ostream &os, const JdoStatus &status) {
        os << "code: " << status._errCode << " msg: " << status._errMsg;
        return os;
    }

    static JdoStatus OK() {
        return JdoStatus();
    }

    template<typename...Args>
    static JdoStatus InitError(Args...args) {
        return JdoStatus(JDO_CLIENT_ERROR, concat("init failed ", args...));
    }

    template<typename...Args>
    static JdoStatus InvalidArgument(Args...args) {
        return JdoStatus(JDO_CLIENT_ERROR, concat("invalid argument ", args...));
    }

    template<typename...Args>
    static JdoStatus NotFound(Args...args) {
        return JdoStatus(JDO_FILE_NOT_FOUND_ERROR, concat(args...));
    }

    template<typename...Args>
    static JdoStatus IOError(Args...args) {
        return JdoStatus(JDO_IO_ERROR, concat(args...));
    }

    template<typename...Args>
    static JdoStatus InternalError(int32_t code, Args...args) {
        return JdoStatus(code, concat(args...));
    }

private:
    template<typename...Args>
    static std::string concat(Args...args) {
        std::stringstream os;
        (os << ... << args);
        return os.str();
    }
};
