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
#include "cppjieba/Jieba.hpp"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/MiscUtils.h"
#include "lucene++/PositionIncrementAttribute.h"
#include "lucene++/TermAttribute.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/memory/memory_pool.h"
namespace paimon::lucene {
struct JiebaTokenizerContext {
    JiebaTokenizerContext(const std::string& _tokenize_mode, bool _with_position,
                          const std::shared_ptr<cppjieba::Jieba> _jieba,
                          const std::shared_ptr<MemoryPool>& _pool,
                          int32_t _buffer_size = kReadBufferSize);

    std::shared_ptr<MemoryPool> pool;
    std::string tokenize_mode;
    bool with_position;
    int32_t buffer_size;
    std::shared_ptr<cppjieba::Jieba> jieba;

    static inline const int32_t kReadBufferSize = 5 * 1024 * 1024;
    static inline const int32_t kMaxWordLen = 1024;
};

class JiebaTokenizer : public Lucene::Tokenizer {
 public:
    JiebaTokenizer(const JiebaTokenizerContext& context, const Lucene::ReaderPtr& input);

    ~JiebaTokenizer() override;

    bool incrementToken() override;

    void reset(const Lucene::ReaderPtr& input) override;

    void reset() override;

    static void CutWithMode(const std::string& tokenize_mode, const cppjieba::Jieba* jieba,
                            const std::string& str, std::vector<std::string>* terms_ptr);

    // In-place converts each string in `input` to lowercase to avoid data copying.
    static void Normalize(const std::unordered_set<std::string>& stop_words,
                          std::vector<std::string>* input, std::vector<std::string_view>* output);

 private:
    void InnerReset();

 private:
    JiebaTokenizerContext context_;
    size_t term_index_ = 0;
    std::vector<std::string> terms_;
    std::vector<std::string_view> normalized_terms_;
    wchar_t* buffer_;
    Lucene::TermAttributePtr term_att_;
    Lucene::PositionIncrementAttributePtr pos_att_;
};

class JiebaAnalyzer : public Lucene::Analyzer {
 public:
    explicit JiebaAnalyzer(const JiebaTokenizerContext& context) : context_(context) {}

    ~JiebaAnalyzer() override = default;

    Lucene::TokenStreamPtr tokenStream(const Lucene::String& field_name,
                                       const Lucene::ReaderPtr& reader) override {
        return Lucene::newLucene<JiebaTokenizer>(context_, reader);
    }

 private:
    JiebaTokenizerContext context_;
};
}  // namespace paimon::lucene
