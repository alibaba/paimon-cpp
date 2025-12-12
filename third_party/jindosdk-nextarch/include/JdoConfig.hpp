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

#include <map>
#include <string>
#include "jdo_common.h"

class JDO_EXPORT JdoConfig {
protected:
    std::map<std::string, std::string> _options;

public:
    JdoConfig();
    JdoConfig(std::map<std::string, std::string>& options);

    std::string getString(const std::string& key, const std::string& def);
    void setString(const std::string& key, const std::string& val);
    int64_t getInt64(const std::string& key, int64_t def);
    void setInt64(const std::string& key, int64_t val);
    int32_t getInt32(const std::string& key, int32_t def);
    void setInt32(const std::string& key, int32_t val);
    double getDouble(const std::string& key, double def);
    void setDouble(const std::string& key, double val);
    bool getBool(const std::string& key, bool def);
    void setBool(const std::string& key, bool val);

    bool contains(const std::string& key);
    std::map<std::string, std::string> getAll();
};
