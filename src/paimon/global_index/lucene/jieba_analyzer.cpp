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

#include "paimon/global_index/lucene/lucene_utils.h"

namespace paimon::lucene {
JiebaTokenizerContext::JiebaTokenizerContext(const std::string& _tokenize_mode, bool _with_position,
                                             const std::shared_ptr<cppjieba::Jieba> _jieba,
                                             const std::shared_ptr<MemoryPool>& _pool,
                                             int32_t _buffer_size)
    : pool(_pool),
      tokenize_mode(_tokenize_mode),
      with_position(_with_position),
      buffer_size(_buffer_size),
      jieba(_jieba) {}

JiebaTokenizer::JiebaTokenizer(const JiebaTokenizerContext& context, const Lucene::ReaderPtr& input)
    : Lucene::Tokenizer(input), context_(context) {
    term_att_ = addAttribute<Lucene::TermAttribute>();
    pos_att_ = addAttribute<Lucene::PositionIncrementAttribute>();
    buffer_ = static_cast<wchar_t*>(
        context_.pool->Malloc(context_.buffer_size * sizeof(wchar_t), /*alignment=*/8));
}

JiebaTokenizer::~JiebaTokenizer() {
    if (buffer_) {
        context_.pool->Free(reinterpret_cast<void*>(buffer_),
                            context_.buffer_size * sizeof(wchar_t),
                            /*alignment=*/8);
        buffer_ = nullptr;
    }
}

bool JiebaTokenizer::incrementToken() {
    if (term_index_ >= normalized_terms_.size()) {
        return false;
    }

    const auto& term = normalized_terms_[term_index_++];
    clearAttributes();

    term_att_->setTermBuffer(LuceneUtils::StringToWstring(term));

    if (context_.with_position) {
        pos_att_->setPositionIncrement(1);
    } else {
        pos_att_->setPositionIncrement(0);
    }
    return true;
}

void JiebaTokenizer::CutWithMode(const std::string& tokenize_mode, const cppjieba::Jieba* jieba,
                                 const std::string& str, std::vector<std::string>* terms_ptr) {
    auto& terms = *terms_ptr;
    if (tokenize_mode == "mp") {
        jieba->CutSmall(str, terms, /*max_word_len=*/JiebaTokenizerContext::kMaxWordLen);
    } else if (tokenize_mode == "hmm") {
        jieba->CutHMM(str, terms);
    } else if (tokenize_mode == "mix") {
        jieba->Cut(str, terms, /*hmm=*/true);
    } else if (tokenize_mode == "full") {
        jieba->CutAll(str, terms);
    } else if (tokenize_mode == "query") {
        jieba->CutForSearch(str, terms, /*hmm=*/true);
    } else {
        throw Lucene::IllegalArgumentException(
            L"only support mp/hmm/mix/full/query in jieba tokenizer");
    }
}

void JiebaTokenizer::Normalize(const std::unordered_set<std::string>& stop_words,
                               std::vector<std::string>* input_ptr,
                               std::vector<std::string_view>* output_ptr) {
    auto& input = *input_ptr;
    auto& output = *output_ptr;
    output.clear();
    output.reserve(input.size());
    for (auto& term : input) {
        // remove stop words
        if (stop_words.find(term) != stop_words.end()) {
            continue;
        }

        // to lower case
        bool is_alphanumeric = true;
        for (const auto& c : term) {
            if (!std::isalnum(static_cast<unsigned char>(c))) {
                is_alphanumeric = false;
                break;
            }
        }
        if (is_alphanumeric && !term.empty()) {
            std::transform(term.begin(), term.end(), term.begin(), [](char ch) {
                return static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            });
        }
        output.emplace_back(term.data(), term.length());
    }
}

void JiebaTokenizer::reset() {
    Lucene::Tokenizer::reset();
    InnerReset();
}

void JiebaTokenizer::reset(const Lucene::ReaderPtr& input) {
    Lucene::Tokenizer::reset(input);
    InnerReset();
}

void JiebaTokenizer::InnerReset() {
    terms_.clear();
    normalized_terms_.clear();
    term_index_ = 0;

    // read wchar from input
    Lucene::String wstr;
    wstr.reserve(context_.buffer_size);
    while (true) {
        int32_t length = input->read(buffer_, /*offset=*/0, context_.buffer_size);
        if (length <= 0) {
            break;
        }
        wstr.append(buffer_, length);
    }

    // jieba tokenize
    std::string doc_str = LuceneUtils::WstringToString(wstr);
    // TODO(xinyu.lxy): support porter2 stemmer
    CutWithMode(context_.tokenize_mode, context_.jieba.get(), doc_str, &terms_);
    Normalize(context_.jieba->extractor.GetStopWords(), &terms_, &normalized_terms_);
}

}  // namespace paimon::lucene
