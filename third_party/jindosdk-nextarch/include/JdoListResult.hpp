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

#include "JdoFileInfo.hpp"
#include "jdo_common.h"

class JDO_EXPORT JdoListResult {
public:
    std::vector<JdoFileInfo> infos;
    bool truncated = false;
    std::string nextMarker;

    void setFileInfos(const std::vector<JdoFileInfo>& infos) {
        this->infos = infos;
    }

    std::vector<JdoFileInfo> getFileInfos() const {
        return infos;
    }

    void setTruncated(bool truncated) {
        this->truncated = truncated;
    }

    bool isTruncated() const {
        return truncated;
    }

    void setNextMarker(const std::string& nextMarker) {
        this->nextMarker = nextMarker;
    }

    std::string getNextMarker() const {
        return nextMarker;
    }
};
