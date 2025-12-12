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

#include <inttypes.h>
#include "jdo_defines.h"
#include "jdo_common.h"

class JDO_EXPORT JdoFileInfo {
public:
    std::string path;
    std::string user;
    std::string group;
    int8_t type    = JDO_FILE_TYPE_UNKNOWN;
    int16_t perm   = 0755;
    int64_t length = -1;
    int64_t mtime  = 0;
    int64_t atime  = 0;

    void setPath(const std::string& path) {
        this->path = path;
    }

    std::string getPath() const {
        return path;
    }

    void setUser(const std::string& user) {
        this->user = user;
    }

    std::string getUser() const {
        return user;
    }

    void setGroup(const std::string& group) {
        this->group = group;
    }

    std::string getGroup() const {
        return group;
    }

    void setType(int8_t type) {
        this->type = type;
    }

    int8_t getType() const {
        return type;
    }

    bool isDir() const {
        return type == JDO_FILE_TYPE_DIRECTORY;
    }

    void setPerm(int16_t perm) {
        this->perm = perm;
    }

    int16_t getPerm() const {
        return perm;
    }

    void setLength(int64_t length) {
        this->length = length;
    }

    int64_t getLength() const {
        return length;
    }

    void setMtime(int64_t mtime) {
        this->mtime = mtime;
    }

    int64_t getMtime() const {
        return mtime;
    }

    void setAtime(int64_t atime) {
        this->atime = atime;
    }

    int64_t getAtime() const {
        return atime;
    }
};
