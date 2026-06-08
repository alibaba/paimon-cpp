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

#include "benchmark/cli_option_parsing.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::testing {
namespace {

struct ArgvHolder {
    std::vector<std::string> args;
    std::vector<char*> argv;

    explicit ArgvHolder(std::vector<std::string> in_args) : args(std::move(in_args)) {
        argv.reserve(args.size());
        for (auto& arg : args) {
            argv.push_back(arg.data());
        }
    }

    int32_t argc() const {
        return static_cast<int32_t>(argv.size());
    }
};

TEST(CliOptionParsingTest, ConsumeCliOptionWorks) {
    std::string value;
    ASSERT_TRUE(paimon::benchmark::ConsumeCliOption("--foo=bar", "--foo", &value));
    ASSERT_EQ(value, "bar");

    value.clear();
    ASSERT_FALSE(paimon::benchmark::ConsumeCliOption("--foo", "--foo", &value));
}

TEST(CliOptionParsingTest, ParseCommaSeparatedColumnsWorks) {
    ASSERT_OK_AND_ASSIGN(auto parsed,
                         paimon::benchmark::ParseCommaSeparatedColumns("id, name,age", "--cols"));
    ASSERT_EQ(parsed.size(), 3U);
    ASSERT_EQ(parsed[0], "id");
    ASSERT_EQ(parsed[1], "name");
    ASSERT_EQ(parsed[2], "age");
}

TEST(CliOptionParsingTest, ParseCommaSeparatedColumnsRejectsInvalidInput) {
    ASSERT_NOK(paimon::benchmark::ParseCommaSeparatedColumns("", "--cols"));
    ASSERT_NOK(paimon::benchmark::ParseCommaSeparatedColumns("id,", "--cols"));
    ASSERT_NOK(paimon::benchmark::ParseCommaSeparatedColumns("id,,name", "--cols"));
}

TEST(CliOptionParsingTest, ParseDelimitedOptionsWorks) {
    ASSERT_OK_AND_ASSIGN(
        auto parsed, paimon::benchmark::ParseDelimitedOptions("k1:v1;k2:v2", "--paimon_option"));
    ASSERT_EQ(parsed.size(), 2U);
    ASSERT_EQ(parsed[0], std::make_pair(std::string("k1"), std::string("v1")));
    ASSERT_EQ(parsed[1], std::make_pair(std::string("k2"), std::string("v2")));
}

TEST(CliOptionParsingTest, ParseDelimitedOptionsTrimsKeyAndValue) {
    ASSERT_OK_AND_ASSIGN(auto parsed, paimon::benchmark::ParseDelimitedOptions(" k1 : v1 ; k2: v2 ",
                                                                               "--paimon_option"));
    ASSERT_EQ(parsed.size(), 2U);
    ASSERT_EQ(parsed[0], std::make_pair(std::string("k1"), std::string("v1")));
    ASSERT_EQ(parsed[1], std::make_pair(std::string("k2"), std::string("v2")));
}

TEST(CliOptionParsingTest, ParseDelimitedOptionsRejectsInvalidInput) {
    ASSERT_NOK(paimon::benchmark::ParseDelimitedOptions("", "--paimon_option"));
    ASSERT_NOK(paimon::benchmark::ParseDelimitedOptions("k1:v1;", "--paimon_option"));
    ASSERT_NOK(paimon::benchmark::ParseDelimitedOptions("k1:", "--paimon_option"));
    ASSERT_NOK(paimon::benchmark::ParseDelimitedOptions(":v1", "--paimon_option"));
    ASSERT_NOK(paimon::benchmark::ParseDelimitedOptions("k1:   ", "--paimon_option"));
}

TEST(CliOptionParsingTest, ParseStringOptionArgWorksForEqualsAndSeparatedForms) {
    {
        ArgvHolder argv_holder({"prog", "--foo=bar"});
        int32_t arg_index = 1;
        std::string value;
        ASSERT_OK_AND_ASSIGN(bool is_parsed,
                             paimon::benchmark::ParseStringOptionArg(
                                 argv_holder.argc(), argv_holder.argv.data(),
                                 argv_holder.args[arg_index], "--foo", &arg_index, &value));
        ASSERT_TRUE(is_parsed);
        ASSERT_EQ(arg_index, 1);
        ASSERT_EQ(value, "bar");
    }

    {
        ArgvHolder argv_holder({"prog", "--foo", "bar"});
        int32_t arg_index = 1;
        std::string value;
        ASSERT_OK_AND_ASSIGN(bool is_parsed,
                             paimon::benchmark::ParseStringOptionArg(
                                 argv_holder.argc(), argv_holder.argv.data(),
                                 argv_holder.args[arg_index], "--foo", &arg_index, &value));
        ASSERT_TRUE(is_parsed);
        ASSERT_EQ(arg_index, 2);
        ASSERT_EQ(value, "bar");
    }
}

TEST(CliOptionParsingTest, ParseStringOptionArgRejectsMissingValue) {
    ArgvHolder argv_holder({"prog", "--foo"});
    int32_t arg_index = 1;
    std::string value;
    ASSERT_NOK(paimon::benchmark::ParseStringOptionArg(argv_holder.argc(), argv_holder.argv.data(),
                                                       argv_holder.args[arg_index], "--foo",
                                                       &arg_index, &value));
}

TEST(CliOptionParsingTest, ParseStringOptionArgIgnoresOtherOptions) {
    ArgvHolder argv_holder({"prog", "--bar=baz"});
    int32_t arg_index = 1;
    std::string value;
    ASSERT_OK_AND_ASSIGN(bool is_parsed,
                         paimon::benchmark::ParseStringOptionArg(
                             argv_holder.argc(), argv_holder.argv.data(),
                             argv_holder.args[arg_index], "--foo", &arg_index, &value));
    ASSERT_FALSE(is_parsed);
    ASSERT_EQ(arg_index, 1);
    ASSERT_TRUE(value.empty());
}

TEST(CliOptionParsingTest, ParseCommaSeparatedOptionArgAndDelimitedRepeatableOptionArgWorks) {
    {
        ArgvHolder argv_holder({"prog", "--cols", "id,name"});
        int32_t arg_index = 1;
        std::vector<std::string> columns;
        ASSERT_OK_AND_ASSIGN(bool is_parsed,
                             paimon::benchmark::ParseCommaSeparatedOptionArg(
                                 argv_holder.argc(), argv_holder.argv.data(),
                                 argv_holder.args[arg_index], "--cols", &arg_index, &columns));
        ASSERT_TRUE(is_parsed);
        ASSERT_EQ(arg_index, 2);
        ASSERT_EQ(columns.size(), 2U);
        ASSERT_EQ(columns[0], "id");
        ASSERT_EQ(columns[1], "name");
    }

    {
        ArgvHolder argv_holder({"prog", "--paimon_option", "k1:v1;k2:v2"});
        int32_t arg_index = 1;
        std::vector<std::pair<std::string, std::string>> options;
        ASSERT_OK_AND_ASSIGN(bool is_parsed, paimon::benchmark::ParseDelimitedRepeatableOptionArg(
                                                 argv_holder.argc(), argv_holder.argv.data(),
                                                 argv_holder.args[arg_index], "--paimon_option",
                                                 &arg_index, &options));
        ASSERT_TRUE(is_parsed);
        ASSERT_EQ(arg_index, 2);
        ASSERT_EQ(options.size(), 2U);
        ASSERT_EQ(options[0], std::make_pair(std::string("k1"), std::string("v1")));
        ASSERT_EQ(options[1], std::make_pair(std::string("k2"), std::string("v2")));
    }
}

}  // namespace
}  // namespace paimon::testing
