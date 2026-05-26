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

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace paimon::benchmark {

inline bool ConsumeCliOption(const std::string& arg, const std::string& option_name,
                             std::string* value_out) {
    const std::string prefix = option_name + "=";
    if (arg.rfind(prefix, 0) != 0) {
        return false;
    }
    *value_out = arg.substr(prefix.size());
    return true;
}

inline std::vector<std::string> ParseCsvColumns(const std::string& csv,
                                                const std::string& option_name) {
    if (csv.empty()) {
        throw std::runtime_error("missing value for " + option_name);
    }

    std::vector<std::string> columns;
    std::string current;
    bool last_delimiter_was_comma = false;
    for (char c : csv) {
        if (c == ',') {
            if (current.empty()) {
                throw std::runtime_error("invalid " + option_name + ": empty column name");
            }
            columns.push_back(current);
            current.clear();
            last_delimiter_was_comma = true;
            continue;
        }
        if (c == ' ' || c == '\t') {
            if (!current.empty()) {
                columns.push_back(current);
                current.clear();
            }
            continue;
        }

        current.push_back(c);
        last_delimiter_was_comma = false;
    }

    if (current.empty()) {
        if (!columns.empty() && !last_delimiter_was_comma) {
            return columns;
        }
        throw std::runtime_error("invalid " + option_name + ": empty column name");
    }

    columns.push_back(current);
    return columns;
}

inline std::vector<std::pair<std::string, std::string>> ParseDelimitedOptions(
    const std::string& input, const std::string& option_name) {
    if (input.empty()) {
        throw std::runtime_error("missing value for " + option_name);
    }

    std::vector<std::pair<std::string, std::string>> parsed;
    std::string token;
    for (size_t i = 0; i <= input.size(); ++i) {
        const bool at_end = (i == input.size());
        if (!at_end && input[i] != ';') {
            token.push_back(input[i]);
            continue;
        }

        if (token.empty()) {
            throw std::runtime_error("invalid " + option_name + ": empty option segment");
        }

        const auto sep = token.find(':');
        if (sep == std::string::npos || sep == 0 || sep + 1 >= token.size()) {
            throw std::runtime_error("invalid " + option_name + ": expected key:value");
        }

        parsed.emplace_back(token.substr(0, sep), token.substr(sep + 1));
        token.clear();
    }
    return parsed;
}

inline bool ParseStringOptionArg(int* i, int argc, char** argv, const std::string& arg,
                                 const std::string& option_name, std::string* value_out) {
    std::string parsed_value;
    if (ConsumeCliOption(arg, option_name, &parsed_value)) {
        *value_out = std::move(parsed_value);
        return true;
    }

    if (arg != option_name) {
        return false;
    }

    if (*i + 1 >= argc) {
        throw std::runtime_error("missing value for " + option_name);
    }
    *value_out = argv[++(*i)];
    return true;
}

inline bool ParseCsvOptionArg(int* i, int argc, char** argv, const std::string& arg,
                              const std::string& option_name,
                              std::vector<std::string>* columns_out) {
    std::string parsed_value;
    if (ConsumeCliOption(arg, option_name, &parsed_value)) {
        *columns_out = ParseCsvColumns(parsed_value, option_name);
        return true;
    }

    if (arg != option_name) {
        return false;
    }

    if (*i + 1 >= argc) {
        throw std::runtime_error("missing value for " + option_name);
    }
    *columns_out = ParseCsvColumns(std::string(argv[++(*i)]), option_name);
    return true;
}

inline bool ParseDelimitedRepeatableOptionArg(
    int* i, int argc, char** argv, const std::string& arg, const std::string& option_name,
    std::vector<std::pair<std::string, std::string>>* options_out) {
    std::string parsed_value;
    if (ConsumeCliOption(arg, option_name, &parsed_value)) {
        const auto parsed_options = ParseDelimitedOptions(parsed_value, option_name);
        options_out->insert(options_out->end(), parsed_options.begin(), parsed_options.end());
        return true;
    }

    if (arg != option_name) {
        return false;
    }

    if (*i + 1 >= argc) {
        throw std::runtime_error("missing value for " + option_name);
    }

    const std::string option_arg = argv[++(*i)];
    const auto parsed_options = ParseDelimitedOptions(option_arg, option_name);
    options_out->insert(options_out->end(), parsed_options.begin(), parsed_options.end());
    return true;
}

}  // namespace paimon::benchmark
