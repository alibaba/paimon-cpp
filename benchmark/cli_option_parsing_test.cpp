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

#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

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

    int argc() const {
        return static_cast<int>(argv.size());
    }
};

TEST(CliOptionParsingTest, ConsumeCliOptionWorks) {
    std::string value;
    EXPECT_TRUE(paimon::benchmark::ConsumeCliOption("--foo=bar", "--foo", &value));
    EXPECT_EQ(value, "bar");

    value.clear();
    EXPECT_FALSE(paimon::benchmark::ConsumeCliOption("--foo", "--foo", &value));
}

TEST(CliOptionParsingTest, ParseCsvColumnsWorks) {
    const auto parsed = paimon::benchmark::ParseCsvColumns("id, name\tage", "--cols");
    ASSERT_EQ(parsed.size(), 3U);
    EXPECT_EQ(parsed[0], "id");
    EXPECT_EQ(parsed[1], "name");
    EXPECT_EQ(parsed[2], "age");
}

TEST(CliOptionParsingTest, ParseCsvColumnsRejectsInvalidInput) {
    EXPECT_THROW((void)paimon::benchmark::ParseCsvColumns("", "--cols"), std::runtime_error);
    EXPECT_THROW((void)paimon::benchmark::ParseCsvColumns("id,", "--cols"), std::runtime_error);
}

TEST(CliOptionParsingTest, ParseDelimitedOptionsWorks) {
    const auto parsed = paimon::benchmark::ParseDelimitedOptions("k1:v1;k2:v2", "--paimon_option");
    ASSERT_EQ(parsed.size(), 2U);
    EXPECT_EQ(parsed[0], std::make_pair(std::string("k1"), std::string("v1")));
    EXPECT_EQ(parsed[1], std::make_pair(std::string("k2"), std::string("v2")));
}

TEST(CliOptionParsingTest, ParseDelimitedOptionsRejectsInvalidInput) {
    EXPECT_THROW((void)paimon::benchmark::ParseDelimitedOptions("", "--paimon_option"),
                 std::runtime_error);
    EXPECT_THROW((void)paimon::benchmark::ParseDelimitedOptions("k1:v1;", "--paimon_option"),
                 std::runtime_error);
}

TEST(CliOptionParsingTest, ParseStringOptionArgWorksForEqualsAndSeparatedForms) {
    {
        ArgvHolder argv_holder({"prog", "--foo=bar"});
        int i = 1;
        std::string value;
        EXPECT_TRUE(paimon::benchmark::ParseStringOptionArg(
            &i, argv_holder.argc(), argv_holder.argv.data(), argv_holder.args[i], "--foo", &value));
        EXPECT_EQ(i, 1);
        EXPECT_EQ(value, "bar");
    }

    {
        ArgvHolder argv_holder({"prog", "--foo", "bar"});
        int i = 1;
        std::string value;
        EXPECT_TRUE(paimon::benchmark::ParseStringOptionArg(
            &i, argv_holder.argc(), argv_holder.argv.data(), argv_holder.args[i], "--foo", &value));
        EXPECT_EQ(i, 2);
        EXPECT_EQ(value, "bar");
    }
}

TEST(CliOptionParsingTest, ParseStringOptionArgRejectsMissingValue) {
    ArgvHolder argv_holder({"prog", "--foo"});
    int i = 1;
    std::string value;
    EXPECT_THROW(
        (void)paimon::benchmark::ParseStringOptionArg(
            &i, argv_holder.argc(), argv_holder.argv.data(), argv_holder.args[i], "--foo", &value),
        std::runtime_error);
}

TEST(CliOptionParsingTest, ParseCsvOptionArgAndDelimitedRepeatableOptionArgWorks) {
    {
        ArgvHolder argv_holder({"prog", "--cols", "id,name"});
        int i = 1;
        std::vector<std::string> columns;
        EXPECT_TRUE(paimon::benchmark::ParseCsvOptionArg(&i, argv_holder.argc(),
                                                         argv_holder.argv.data(),
                                                         argv_holder.args[i], "--cols", &columns));
        EXPECT_EQ(i, 2);
        ASSERT_EQ(columns.size(), 2U);
        EXPECT_EQ(columns[0], "id");
        EXPECT_EQ(columns[1], "name");
    }

    {
        ArgvHolder argv_holder({"prog", "--paimon_option", "k1:v1;k2:v2"});
        int i = 1;
        std::vector<std::pair<std::string, std::string>> options;
        EXPECT_TRUE(paimon::benchmark::ParseDelimitedRepeatableOptionArg(
            &i, argv_holder.argc(), argv_holder.argv.data(), argv_holder.args[i], "--paimon_option",
            &options));
        EXPECT_EQ(i, 2);
        ASSERT_EQ(options.size(), 2U);
        EXPECT_EQ(options[0], std::make_pair(std::string("k1"), std::string("v1")));
        EXPECT_EQ(options[1], std::make_pair(std::string("k2"), std::string("v2")));
    }
}

}  // namespace
}  // namespace paimon::testing
