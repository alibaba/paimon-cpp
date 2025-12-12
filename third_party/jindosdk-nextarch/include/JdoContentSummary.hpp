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

#include "jdo_common.h"

class JDO_EXPORT JdoContentSummary {
public:
    int64_t fileSize = 0;
    int64_t fileCount = 0;
    int64_t dirCount = 0;

    void setFileSize(int64_t fileSize) {
        this->fileSize = fileSize;
    }

    int64_t getFileSize() const {
        return fileSize;
    }

    void setFileCount(int64_t fileCount) {
        this->fileCount = fileCount;
    }

    int64_t getFileCount() const {
        return fileCount;
    }

    void setDirCount(int64_t dirCount) {
        this->dirCount = dirCount;
    }

    int64_t getDirCount() const {
        return dirCount;
    }
};
