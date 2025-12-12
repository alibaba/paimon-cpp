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

#include "JdoConfig.hpp"
#include <sstream>
#include <iostream>
#include <limits>
#include <cassert>
#include <cstring>

static int32_t StrToInt32(const char* str, int32_t def) {
    int64_t retval;
    char* end = NULL;
    errno = 0;
    retval = strtol(str, &end, 0);

    if (EINVAL == errno || 0 != *end) {
        std::stringstream ss;
        std::cerr << "Invalid int32_t type: " << str << std::endl;
        return def;
    }

    if (ERANGE == errno || retval > std::numeric_limits<int32_t>::max() ||
        retval < std::numeric_limits<int32_t>::min()) {
        std::cerr << "Underflow/Overflow int32_t type: " << str << std::endl;
        return def;
    }

    return retval;
}

static int64_t StrToInt64(const char* str, int64_t def) {
    int64_t retval;
    char* end = NULL;
    errno = 0;
    retval = strtoll(str, &end, 0);

    if (EINVAL == errno || 0 != *end) {
        std::cerr << "Invalid int64_t type: " << str << std::endl;
        return def;
    }

    if (ERANGE == errno || retval > std::numeric_limits<int64_t>::max() ||
        retval < std::numeric_limits<int64_t>::min()) {
        std::cerr << "Underflow/Overflow int64_t type: " << str << std::endl;
        return def;
    }

    return retval;
}

static double StrToDouble(const char* str, int64_t def) {
    double retval;
    char* end = NULL;
    errno = 0;
    retval = strtod(str, &end);

    if (EINVAL == errno || 0 != *end) {
        std::cerr << "Invalid double type: " << str;
        return def;
    }

    if (ERANGE == errno || retval > std::numeric_limits<double>::max() || retval < std::numeric_limits<double>::min()) {
        std::cerr << "Underflow/Overflow int64_t type: " << str << std::endl;
        return def;
    }

    return retval;
}

static bool StrToBool(const char* str, bool def) {
    bool retval = false;

    if (!strcasecmp(str, "true") || !strcmp(str, "1")) {
        retval = true;
    } else if (!strcasecmp(str, "false") || !strcmp(str, "0")) {
        retval = false;
    } else {
        std::cerr << "Invalid bool type: " << str << std::endl;
        return def;
    }

    return retval;
}

static double StrToDouble(const char* str, double def) {
    double retval;
    char* end = NULL;
    errno = 0;
    retval = strtod(str, &end);

    if (EINVAL == errno || 0 != *end) {
        std::cerr << "Invalid double type: " << str << std::endl;
        return def;
    }

    if (ERANGE == errno || retval > std::numeric_limits<double>::max() || retval < std::numeric_limits<double>::min()) {
        std::cerr << "Underflow/Overflow int64_t type: " << str << std::endl;
        return def;
    }

    return retval;
}

JdoConfig::JdoConfig() {

}

JdoConfig::JdoConfig(std::map<std::string, std::string>& options) {
    _options = options;
}

std::string JdoConfig::getString(const std::string& key, const std::string& def) {
    auto it = _options.find(key);

    if (_options.end() == it) {
        return def;
    } else {
        return it->second;
    }
}

void JdoConfig::setString(const std::string& key, const std::string& val) {
    _options[key] = val;
}

int64_t JdoConfig::getInt64(const std::string& key, int64_t def) {
    int64_t retval;
    auto it = _options.find(key);

    if (_options.end() == it) {
        return def;
    }

    return StrToInt64(it->second.c_str(), def);
}

void JdoConfig::setInt64(const std::string& key, int64_t val) {
    _options[key] = std::to_string(val);
}

int32_t JdoConfig::getInt32(const std::string& key, int32_t def) {
    int32_t retval;
    auto it = _options.find(key);

    if (_options.end() == it) {
        return def;
    }

    return StrToInt32(it->second.c_str(), def);
}

void JdoConfig::setInt32(const std::string& key, int32_t val) {
    _options[key] = std::to_string(val);
}

double JdoConfig::getDouble(const std::string& key, double def) {
    double retval;
    auto it = _options.find(key);

    if (_options.end() == it) {
        return def;
    }

    return StrToDouble(it->second.c_str(), def);
}

void JdoConfig::setDouble(const std::string& key, double val) {
    _options[key] = std::to_string(val);
}

bool JdoConfig::getBool(const std::string& key, bool def) {
    bool retval;
    auto it = _options.find(key);

    if (_options.end() == it) {
        return def;
    }

    return StrToBool(it->second.c_str(), def);
}

void JdoConfig::setBool(const std::string& key, bool val) {
    _options[key] = std::to_string(val);
}

bool JdoConfig::contains(const std::string& key) {
    if (_options.find(key) == _options.end()) {
        return false;
    }
    return true;
}

std::map<std::string, std::string> JdoConfig::getAll() {
    return _options;
}
