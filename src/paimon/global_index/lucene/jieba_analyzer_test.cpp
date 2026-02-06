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
#include "paimon/global_index/lucene/jieba_analyzer.h"

#include "cppjieba/Jieba.hpp"
#include "gtest/gtest.h"
#include "lucene++/LuceneHeaders.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lucene::test {
class JiebaAnalyzerTest : public ::testing::Test, public ::testing::WithParamInterface<int32_t> {
 public:
    void SetUp() override {}
    void TearDown() override {}
    Lucene::TokenStreamPtr CreateJiebaTokenizer(bool with_position) const {
        return CreateJiebaTokenizer(with_position, L"我爱机器学习");
    }

    Lucene::TokenStreamPtr CreateJiebaTokenizer(bool with_position,
                                                const Lucene::String& text) const {
        auto pool = GetDefaultPool();
        std::string dictionary_dir = LuceneUtils::GetJiebaDictionaryDir().value();
        auto jieba = std::make_shared<cppjieba::Jieba>(
            dictionary_dir + "/jieba.dict.utf8", dictionary_dir + "/hmm_model.utf8",
            dictionary_dir + "/user.dict.utf8", dictionary_dir + "/idf.utf8",
            dictionary_dir + "/stop_words.utf8");
        auto reader = Lucene::newLucene<Lucene::StringReader>(text);
        int32_t buffer_size = GetParam();
        JiebaTokenizerContext context(/*tokenize_mode=*/"query", with_position, jieba, pool,
                                      buffer_size);
        auto analyzer = Lucene::newLucene<JiebaAnalyzer>(context);
        return analyzer->tokenStream(/*field_name*/ L"f0", reader);
    }
};

TEST_P(JiebaAnalyzerTest, TestSimple) {
    auto tokenizer = CreateJiebaTokenizer(/*with_position=*/false);

    auto term_att = tokenizer->addAttribute<Lucene::TermAttribute>();

    tokenizer->reset();
    std::vector<Lucene::String> results;
    while (tokenizer->incrementToken()) {
        results.push_back(term_att->term());
    }
    tokenizer->end();
    tokenizer->close();
    std::vector<Lucene::String> expected = {L"爱", L"机器", L"学习"};
    ASSERT_EQ(expected, results);
}

TEST_P(JiebaAnalyzerTest, TestWithPosition) {
    auto tokenizer = CreateJiebaTokenizer(/*with_position=*/true);

    auto term_att = tokenizer->addAttribute<Lucene::TermAttribute>();
    auto pos_att = tokenizer->addAttribute<Lucene::PositionIncrementAttribute>();

    tokenizer->reset();
    std::vector<Lucene::String> results;
    std::vector<int32_t> result_pos;
    int32_t pos = 0;
    while (tokenizer->incrementToken()) {
        pos += pos_att->getPositionIncrement();
        result_pos.push_back(pos);
        results.push_back(term_att->term());
    }
    tokenizer->end();
    tokenizer->close();

    std::vector<Lucene::String> expected = {L"爱", L"机器", L"学习"};
    std::vector<int32_t> expected_pos = {1, 2, 3};
    ASSERT_EQ(expected, results);
    ASSERT_EQ(expected_pos, result_pos);
}

TEST_P(JiebaAnalyzerTest, TestNormalize) {
    auto tokenizer = CreateJiebaTokenizer(
        /*with_position=*/false,
        L"由于购买了Iphone14，我越来越热爱网上学习了！Happy work, happy day!");

    auto term_att = tokenizer->addAttribute<Lucene::TermAttribute>();

    tokenizer->reset();
    std::vector<Lucene::String> results;
    while (tokenizer->incrementToken()) {
        results.push_back(term_att->term());
    }
    tokenizer->end();
    tokenizer->close();
    std::vector<Lucene::String> expected = {L"购买", L"iphone14", L"越来", L"越来越",
                                            L"热爱", L"网上",     L"学习", L"happy",
                                            L"work", L"happy",    L"day"};
    ASSERT_EQ(expected, results);
}

INSTANTIATE_TEST_SUITE_P(ReadBufferSize, JiebaAnalyzerTest,
                         ::testing::ValuesIn(std::vector<int32_t>({2, 5, 10, 100})));

}  // namespace paimon::lucene::test
