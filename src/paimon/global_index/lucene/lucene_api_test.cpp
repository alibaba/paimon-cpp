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
#include "gtest/gtest.h"
#include "lucene++/FileUtils.h"
#include "lucene++/LuceneHeaders.h"
#include "lucene++/MiscUtils.h"
#include "paimon/global_index/lucene/jieba_analyzer.h"
#include "paimon/global_index/lucene/lucene_directory.h"
#include "paimon/global_index/lucene/lucene_utils.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::lucene::test {
class LuceneInterfaceTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}

    class TestDocIdSetIterator : public Lucene::DocIdSetIterator {
     public:
        explicit TestDocIdSetIterator(const std::vector<int32_t>& ids)
            : Lucene::DocIdSetIterator(), ids_(ids) {}

        int32_t advance(int32_t target) override {
            int32_t doc_id = nextDoc();
            while (doc_id < target) {
                doc_id = nextDoc();
            }
            return doc_id;
        }
        int32_t docID() override {
            if (cursor_ >= ids_.size()) {
                return Lucene::DocIdSetIterator::NO_MORE_DOCS;
            }
            return ids_[cursor_];
        }
        int32_t nextDoc() override {
            if (cursor_ >= ids_.size()) {
                return Lucene::DocIdSetIterator::NO_MORE_DOCS;
            }
            return ids_[cursor_++];
        }

     private:
        size_t cursor_ = 0;
        std::vector<int32_t> ids_;
    };

    class TestDocIdSet : public Lucene::DocIdSet {
     public:
        explicit TestDocIdSet(const std::vector<int32_t>& ids) : DocIdSet(), ids_(ids) {}

        Lucene::DocIdSetIteratorPtr iterator() override {
            return Lucene::newLucene<TestDocIdSetIterator>(ids_);
        }
        bool isCacheable() override {
            return true;
        }

     private:
        std::vector<int32_t> ids_;
    };

    class TestFilter : public Lucene::Filter {
     public:
        explicit TestFilter(const std::vector<int32_t>& ids) : ids_(ids) {}

        Lucene::DocIdSetPtr getDocIdSet(const Lucene::IndexReaderPtr& reader) override {
            return Lucene::newLucene<TestDocIdSet>(ids_);
        }

     private:
        std::vector<int32_t> ids_;
    };

    struct WriteContext {
        Lucene::IndexWriterPtr writer;
        Lucene::DocumentPtr doc;
        Lucene::FieldPtr field;
        Lucene::FieldPtr doc_id_field;
    };

    Lucene::AnalyzerPtr CreateJiebaAnalyzer() const {
        auto pool = GetDefaultPool();
        std::string dictionary_dir = LuceneUtils::GetJiebaDictionaryDir().value();
        auto jieba = std::make_shared<cppjieba::Jieba>(
            dictionary_dir + "/jieba.dict.utf8", dictionary_dir + "/hmm_model.utf8",
            dictionary_dir + "/user.dict.utf8", dictionary_dir + "/idf.utf8",
            dictionary_dir + "/stop_words.utf8");
        JiebaTokenizerContext context(/*tokenize_mode=*/"query", /*with_position=*/true, jieba,
                                      pool);
        return Lucene::newLucene<JiebaAnalyzer>(context);
    }

    WriteContext CreateWriteContext(const Lucene::DirectoryPtr& lucene_dir,
                                    const Lucene::AnalyzerPtr& analyzer) const {
        auto lucene_analyzer = analyzer ? analyzer
                                        : Lucene::newLucene<Lucene::StandardAnalyzer>(
                                              Lucene::LuceneVersion::LUCENE_CURRENT);
        Lucene::IndexWriterPtr writer = Lucene::newLucene<Lucene::IndexWriter>(
            lucene_dir, lucene_analyzer,
            /*create=*/true, Lucene::IndexWriter::MaxFieldLengthLIMITED);

        Lucene::DocumentPtr doc = Lucene::newLucene<Lucene::Document>();
        auto field = Lucene::newLucene<Lucene::Field>(L"content", L"", Lucene::Field::STORE_NO,
                                                      Lucene::Field::INDEX_ANALYZED_NO_NORMS);
        auto doc_id_field = Lucene::newLucene<Lucene::Field>(
            L"id", L"", Lucene::Field::STORE_YES, Lucene::Field::INDEX_NOT_ANALYZED_NO_NORMS);

        field->setOmitTermFreqAndPositions(false);
        doc_id_field->setOmitTermFreqAndPositions(true);
        doc->add(field);
        doc->add(doc_id_field);
        return {writer, doc, field, doc_id_field};
    }

    void AddDocument(const std::wstring& doc_str, int32_t doc_id, WriteContext* context) const {
        context->field->setValue(doc_str);
        context->doc_id_field->setValue(LuceneUtils::StringToWstring(std::to_string(doc_id)));
        context->writer->addDocument(context->doc);
    }

    struct ReadContext {
        Lucene::IndexReaderPtr reader;
        Lucene::IndexSearcherPtr searcher;
        Lucene::QueryParserPtr parser;
    };

    ReadContext CreateReadContext(const Lucene::DirectoryPtr& lucene_dir,
                                  const Lucene::AnalyzerPtr& analyzer) const {
        auto lucene_analyzer = analyzer ? analyzer
                                        : Lucene::newLucene<Lucene::StandardAnalyzer>(
                                              Lucene::LuceneVersion::LUCENE_CURRENT);
        Lucene::IndexReaderPtr reader = Lucene::IndexReader::open(lucene_dir, /*read_only=*/true);
        Lucene::IndexSearcherPtr searcher = Lucene::newLucene<Lucene::IndexSearcher>(reader);
        Lucene::QueryParserPtr parser = Lucene::newLucene<Lucene::QueryParser>(
            Lucene::LuceneVersion::LUCENE_CURRENT, L"content", lucene_analyzer);
        parser->setAllowLeadingWildcard(true);
        return {reader, searcher, parser};
    }

    void Search(const std::wstring& query_str, int32_t limit,
                const std::optional<std::vector<int32_t>> selected_id,
                const std::vector<int32_t>& expected_doc_id_vec,
                const std::vector<std::wstring>& expected_doc_id_content_vec,
                ReadContext* context) const {
        Lucene::QueryPtr query = context->parser->parse(query_str);
        Lucene::TopDocsPtr results;
        if (selected_id) {
            Lucene::FilterPtr lucene_filter = Lucene::newLucene<TestFilter>(selected_id.value());
            results = context->searcher->search(query, lucene_filter, limit);
        } else {
            results = context->searcher->search(query, limit);
        }
        ASSERT_EQ(expected_doc_id_vec.size(), results->scoreDocs.size());

        std::vector<int32_t> result_doc_id_vec;
        std::vector<std::wstring> result_doc_id_content_vec;
        for (auto score_doc : results->scoreDocs) {
            Lucene::DocumentPtr result_doc = context->searcher->doc(score_doc->doc);
            result_doc_id_vec.push_back(score_doc->doc);
            result_doc_id_content_vec.push_back(result_doc->get(L"id"));
        }
        ASSERT_EQ(result_doc_id_vec, expected_doc_id_vec);
        ASSERT_EQ(result_doc_id_content_vec, expected_doc_id_content_vec);
    }
};

TEST_F(LuceneInterfaceTest, TestSimple) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    std::string index_path = dir->Str() + "/lucene_test";
    auto lucene_dir = Lucene::FSDirectory::open(LuceneUtils::StringToWstring(index_path),
                                                Lucene::NoLockFactory::getNoLockFactory());
    // write
    auto write_context = CreateWriteContext(lucene_dir, /*analyzer=*/nullptr);

    AddDocument(L"This is an test document.", 0, &write_context);
    AddDocument(L"This is an new document document document.", 1, &write_context);
    AddDocument(L"Document document document document test.", 2, &write_context);
    AddDocument(L"unordered user-defined doc id", 5, &write_context);
    AddDocument(L"", 6, &write_context);  // add a null doc

    write_context.writer->optimize();
    write_context.writer->close();

    // read
    auto read_context = CreateReadContext(lucene_dir, /*analyzer=*/nullptr);

    // result is sorted by tf-idf score
    Search(L"document", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({2, 1, 0}),
           std::vector<std::wstring>({L"2", L"1", L"0"}), &read_context);
    Search(L"document", /*limit=*/1, /*selected_id=*/std::nullopt, std::vector<int32_t>({2}),
           std::vector<std::wstring>({L"2"}), &read_context);
    Search(L"test AND document", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({2, 0}), std::vector<std::wstring>({L"2", L"0"}), &read_context);
    Search(L"test OR new", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({1, 0, 2}), std::vector<std::wstring>({L"1", L"0", L"2"}),
           &read_context);
    Search(L"\"test document\"", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({0}), std::vector<std::wstring>({L"0"}), &read_context);
    Search(L"unordered", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({3}),
           std::vector<std::wstring>({L"5"}), &read_context);
    Search(L"*orDer*", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({3}),
           std::vector<std::wstring>({L"5"}), &read_context);

    // test filter
    Search(L"document", /*limit=*/10, /*selected_id=*/std::vector<int32_t>({0, 1}),
           std::vector<int32_t>({1, 0}), std::vector<std::wstring>({L"1", L"0"}), &read_context);
    Search(L"document OR unordered", /*limit=*/10,
           /*selected_id=*/std::vector<int32_t>({0, 1, 3}), std::vector<int32_t>({3, 1, 0}),
           std::vector<std::wstring>({L"5", L"1", L"0"}), &read_context);
    Search(L"unordered", /*limit=*/10, /*selected_id=*/std::vector<int32_t>({0}),
           std::vector<int32_t>(), std::vector<std::wstring>(), &read_context);

    read_context.reader->close();
    lucene_dir->close();
}

TEST_F(LuceneInterfaceTest, TestWithAnalyzer) {
    auto dir = paimon::test::UniqueTestDirectory::Create("local");
    std::string index_path = dir->Str() + "/lucene_test";
    auto lucene_dir = Lucene::FSDirectory::open(LuceneUtils::StringToWstring(index_path),
                                                Lucene::NoLockFactory::getNoLockFactory());
    // write
    auto analyzer = CreateJiebaAnalyzer();
    auto write_context = CreateWriteContext(lucene_dir, analyzer);

    AddDocument(L"我爱机器学习", 0, &write_context);
    AddDocument(L"机器会学习吗？", 1, &write_context);
    AddDocument(L"我爱工作", 2, &write_context);
    AddDocument(L"Have a nice day", 3, &write_context);

    write_context.writer->optimize();
    write_context.writer->close();

    // read
    auto read_context = CreateReadContext(lucene_dir, analyzer);

    // result is sorted by tf-idf score
    Search(L"机器", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({0, 1}),
           std::vector<std::wstring>({L"0", L"1"}), &read_context);
    Search(L"机器 AND 学习", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({0, 1}), std::vector<std::wstring>({L"0", L"1"}), &read_context);
    Search(L"\"机器学习\"", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({0}),
           std::vector<std::wstring>({L"0"}), &read_context);
    Search(L"我爱", /*limit=*/10, /*selected_id=*/std::nullopt, std::vector<int32_t>({0, 2}),
           std::vector<std::wstring>({L"0", L"2"}), &read_context);
    Search(L"爱 OR nice", /*limit=*/10, /*selected_id=*/std::nullopt,
           std::vector<int32_t>({3, 0, 2}), std::vector<std::wstring>({L"3", L"0", L"2"}),
           &read_context);

    read_context.reader->close();
    lucene_dir->close();
}

}  // namespace paimon::lucene::test
