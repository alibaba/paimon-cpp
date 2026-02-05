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
#include <chrono>

#include "cppjieba/Jieba.hpp"
#include "gtest/gtest.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lucene::test {
class JiebaInterfaceTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(JiebaInterfaceTest, TestSimple) {
    ASSERT_OK_AND_ASSIGN(std::string dictionary_dir, LuceneUtils::GetJiebaDictionaryDir());
    cppjieba::Jieba jieba(dictionary_dir + "/jieba.dict.utf8", dictionary_dir + "/hmm_model.utf8",
                          dictionary_dir + "/user.dict.utf8", dictionary_dir + "/idf.utf8",
                          dictionary_dir + "/stop_words.utf8");

    {
        std::vector<std::string> words;
        jieba.CutForSearch("我爱机器学习", words);
        std::vector<std::string> expected = {"我", "爱", "机器", "学习"};
        ASSERT_EQ(expected, words);
    }
    {
        std::vector<std::string> words;
        jieba.CutForSearch("我爱机器学习 工作 good work nice Day，price12345，12345", words);
        std::vector<std::string> expected = {"我", "爱",   "机器", "学习",       " ",  "工作",
                                             " ",  "good", " ",    "work",       " ",  "nice",
                                             " ",  "Day",  "，",   "price12345", "，", "12345"};
        ASSERT_EQ(expected, words);
    }
}

TEST_F(JiebaInterfaceTest, TestMP) {
    ASSERT_OK_AND_ASSIGN(std::string dictionary_dir, LuceneUtils::GetJiebaDictionaryDir());
    cppjieba::Jieba jieba(dictionary_dir + "/jieba.dict.utf8", dictionary_dir + "/hmm_model.utf8",
                          dictionary_dir + "/user.dict.utf8", dictionary_dir + "/idf.utf8",
                          dictionary_dir + "/stop_words.utf8");

    {
        std::vector<std::string> words;
        jieba.CutSmall("我爱机器学习", words, /*max_word_len=*/5);
        std::vector<std::string> expected = {"我", "爱", "机器", "学习"};
        ASSERT_EQ(expected, words);
    }
    {
        std::vector<std::string> words;
        jieba.CutSmall("我爱机器学习", words, /*max_word_len=*/1);
        std::vector<std::string> expected = {"我", "爱", "机", "器", "学", "习"};
        ASSERT_EQ(expected, words);
    }
}

}  // namespace paimon::lucene::test
