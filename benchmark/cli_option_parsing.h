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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon::benchmark {

using ParsedOptions = std::vector<std::pair<std::string, std::string>>;

inline bool ConsumeCliOption(const std::string& arg, const std::string& option_name,
                             std::string* value_out) {
    const std::string prefix = option_name + "=";
    if (arg.rfind(prefix, 0) != 0) {
        return false;
    }
    *value_out = arg.substr(prefix.size());
    return true;
}

inline std::string TrimAsciiWhitespace(const std::string& value) {
    const auto first = value.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) {
        return "";
    }
    const auto last = value.find_last_not_of(" \t\n\r");
    return value.substr(first, last - first + 1);
}

inline Result<std::vector<std::string>> ParseCommaSeparatedColumns(const std::string& input,
                                                                   const std::string& option_name) {
    if (input.empty()) {
        return Status::Invalid("missing value for ", option_name);
    }

    std::vector<std::string> columns;
    size_t segment_start = 0;
    for (size_t index = 0; index <= input.size(); ++index) {
        if (index != input.size() && input[index] != ',') {
            continue;
        }

        const std::string column =
            TrimAsciiWhitespace(input.substr(segment_start, index - segment_start));
        if (column.empty()) {
            return Status::Invalid("invalid ", option_name, ": empty column name");
        }
        columns.push_back(column);
        segment_start = index + 1;
    }
    return columns;
}

inline Result<ParsedOptions> ParseDelimitedOptions(const std::string& input,
                                                   const std::string& option_name) {
    if (input.empty()) {
        return Status::Invalid("missing value for ", option_name);
    }

    ParsedOptions parsed;
    std::string token;
    for (size_t index = 0; index <= input.size(); ++index) {
        const bool at_end = (index == input.size());
        if (!at_end && input[index] != ';') {
            token.push_back(input[index]);
            continue;
        }

        const std::string segment = TrimAsciiWhitespace(token);
        if (segment.empty()) {
            return Status::Invalid("invalid ", option_name, ": empty option segment");
        }

        const auto separator = segment.find(':');
        if (separator == std::string::npos) {
            return Status::Invalid("invalid ", option_name, ": expected key:value");
        }

        const std::string key = TrimAsciiWhitespace(segment.substr(0, separator));
        const std::string value = TrimAsciiWhitespace(segment.substr(separator + 1));
        if (key.empty() || value.empty()) {
            return Status::Invalid("invalid ", option_name, ": expected key:value");
        }

        parsed.emplace_back(key, value);
        token.clear();
    }
    return parsed;
}

inline Result<bool> ParseStringOptionArg(int32_t argc, char** argv, const std::string& arg,
                                         const std::string& option_name, int32_t* arg_index,
                                         std::string* value_out) {
    std::string parsed_value;
    if (ConsumeCliOption(arg, option_name, &parsed_value)) {
        *value_out = std::move(parsed_value);
        return true;
    }

    if (arg != option_name) {
        return false;
    }

    if (*arg_index + 1 >= argc) {
        return Status::Invalid("missing value for ", option_name);
    }
    *value_out = argv[++(*arg_index)];
    return true;
}

inline Result<bool> ParseCommaSeparatedOptionArg(int32_t argc, char** argv, const std::string& arg,
                                                 const std::string& option_name, int32_t* arg_index,
                                                 std::vector<std::string>* columns_out) {
    std::string parsed_value;
    if (ConsumeCliOption(arg, option_name, &parsed_value)) {
        PAIMON_ASSIGN_OR_RAISE(*columns_out, ParseCommaSeparatedColumns(parsed_value, option_name));
        return true;
    }

    if (arg != option_name) {
        return false;
    }

    if (*arg_index + 1 >= argc) {
        return Status::Invalid("missing value for ", option_name);
    }
    PAIMON_ASSIGN_OR_RAISE(
        *columns_out, ParseCommaSeparatedColumns(std::string(argv[++(*arg_index)]), option_name));
    return true;
}

inline Result<bool> ParseDelimitedRepeatableOptionArg(
    int32_t argc, char** argv, const std::string& arg, const std::string& option_name,
    int32_t* arg_index, std::vector<std::pair<std::string, std::string>>* options_out) {
    std::string parsed_value;
    if (ConsumeCliOption(arg, option_name, &parsed_value)) {
        ParsedOptions parsed_options;
        PAIMON_ASSIGN_OR_RAISE(parsed_options, ParseDelimitedOptions(parsed_value, option_name));
        options_out->insert(options_out->end(), parsed_options.begin(), parsed_options.end());
        return true;
    }

    if (arg != option_name) {
        return false;
    }

    if (*arg_index + 1 >= argc) {
        return Status::Invalid("missing value for ", option_name);
    }

    const std::string option_arg = argv[++(*arg_index)];
    ParsedOptions parsed_options;
    PAIMON_ASSIGN_OR_RAISE(parsed_options, ParseDelimitedOptions(option_arg, option_name));
    options_out->insert(options_out->end(), parsed_options.begin(), parsed_options.end());
    return true;
}

}  // namespace paimon::benchmark
