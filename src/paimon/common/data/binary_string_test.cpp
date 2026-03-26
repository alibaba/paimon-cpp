/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/common/data/binary_string.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon::test {
class BinaryStringTest : public testing::Test {
 private:
    BinaryString FromString(const std::string& str) {
        auto pool = GetDefaultPool();
        return BinaryString::FromString(str, pool.get());
    }

    void CheckBasic(const std::string& str, int32_t len) {
        BinaryString s1 = FromString(str);
        auto pool = GetDefaultPool();
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(str, pool.get());
        BinaryString s2 = BinaryString::FromBytes(bytes);
        ASSERT_EQ(len, s1.NumChars());
        ASSERT_EQ(len, s2.NumChars());

        ASSERT_EQ(str, s1.ToString());
        ASSERT_EQ(str, s2.ToString());
        ASSERT_TRUE(s1 == s2);

        ASSERT_EQ(s2.HashCode(), s1.HashCode());

        ASSERT_TRUE(s1.Contains(s2));
        ASSERT_TRUE(s2.Contains(s1));
        ASSERT_TRUE(s1.StartsWith(s1));
        ASSERT_TRUE(s1.EndsWith(s1));
        ASSERT_TRUE(s2.StartsWith(s2));
        ASSERT_TRUE(s2.EndsWith(s2));
    }
};

TEST_F(BinaryStringTest, TestBasic) {
    CheckBasic("", 0);
    CheckBasic(",", 1);
    CheckBasic("hello", 5);
    CheckBasic("hello world", 11);
    CheckBasic("Paimon中文社区", 10);
    CheckBasic("中 文 社 区", 7);

    CheckBasic("¡", 1);       // 2 bytes char
    CheckBasic("ку", 2);      // 2 * 2 bytes chars
    CheckBasic("︽﹋％", 3);  // 3 * 3 bytes chars
    // CheckBasic("\uD83E\uDD19", 1);  // 4 bytes char
}

TEST_F(BinaryStringTest, EmptyStringTest) {
    ASSERT_EQ(FromString(""), BinaryString::EmptyUtf8());
    std::string empty_str;
    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(empty_str, pool.get());
    ASSERT_EQ(BinaryString::FromBytes(bytes), BinaryString::EmptyUtf8());
    ASSERT_EQ(BinaryString::EmptyUtf8().NumChars(), 0);
    ASSERT_EQ(BinaryString::EmptyUtf8().GetSizeInBytes(), 0);
}

TEST_F(BinaryStringTest, TestCompareTo) {
    auto pool = GetDefaultPool();
    ASSERT_EQ(FromString("   ").CompareTo(BinaryString::BlankString(3, pool.get())), 0);
    ASSERT_TRUE(FromString("").CompareTo(FromString("a")) < 0);
    ASSERT_TRUE(FromString("abc").CompareTo(FromString("ABC")) > 0);
    ASSERT_TRUE(FromString("abc0").CompareTo(FromString("abc")) > 0);
    ASSERT_EQ(FromString("abcabcabc").CompareTo(FromString("abcabcabc")), 0);
    ASSERT_TRUE(FromString("aBcabcabc").CompareTo(FromString("Abcabcabc")) > 0);
    ASSERT_TRUE(FromString("Abcabcabc").CompareTo(FromString("abcabcabC")) < 0);
    ASSERT_TRUE(FromString("abcabcabc").CompareTo(FromString("abcabcabC")) > 0);

    ASSERT_TRUE(FromString("abc").CompareTo(FromString("世界")) < 0);
    ASSERT_TRUE(FromString("你好").CompareTo(FromString("世界")) > 0);
    ASSERT_TRUE(FromString("你好123").CompareTo(FromString("你好122")) > 0);
}

TEST_F(BinaryStringTest, TestSingleSegment) {
    // prepare
    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> data1 = Bytes::AllocateBytes("aaaaaabcde", pool.get());
    MemorySegment seg1 = MemorySegment::Wrap(data1);

    std::shared_ptr<Bytes> data2 = Bytes::AllocateBytes("abcdeb", pool.get());
    MemorySegment seg2 = MemorySegment::Wrap(data2);

    // test compare
    BinaryString binary_string1 = BinaryString::FromAddress(seg1, 0, 10);
    BinaryString binary_string2 = BinaryString::FromAddress(seg2, 0, 6);
    ASSERT_EQ(binary_string1.ToString(), "aaaaaabcde");
    ASSERT_EQ(binary_string2.ToString(), "abcdeb");
    ASSERT_EQ(binary_string1.CompareTo(binary_string2), -1);
    ASSERT_EQ(binary_string1, binary_string1);
    ASSERT_TRUE(binary_string1 < binary_string2);

    // test equal length compare
    binary_string1 = BinaryString::FromAddress(seg1, 5, 5);
    binary_string2 = BinaryString::FromAddress(seg2, 0, 5);
    ASSERT_EQ(binary_string1.ToString(), "abcde");
    ASSERT_EQ(binary_string2.ToString(), "abcde");
    ASSERT_EQ(binary_string1, binary_string2);

    // test not equal
    binary_string1 = BinaryString::FromAddress(seg1, 0, 5);
    binary_string2 = BinaryString::FromAddress(seg2, 0, 5);
    ASSERT_EQ(binary_string1.ToString(), "aaaaa");
    ASSERT_EQ(binary_string2.ToString(), "abcde");
    ASSERT_EQ(binary_string1.CompareTo(binary_string2), -1);
    ASSERT_EQ(binary_string2.CompareTo(binary_string1), 1);

    // test with offset in single segment
    std::shared_ptr<Bytes> data3 = Bytes::AllocateBytes(10, pool.get());
    MemorySegment seg3 = MemorySegment::Wrap(data3);
    seg3.Put(4, Bytes("abcdeb", pool.get()), 0, 6);
    binary_string2 = BinaryString::FromAddress(seg3, 4, 6);
    ASSERT_EQ(binary_string2.ToString(), "abcdeb");
    ASSERT_EQ(binary_string1.CompareTo(binary_string2), -1);
    ASSERT_EQ(binary_string2.CompareTo(binary_string1), 1);
}

TEST_F(BinaryStringTest, TestContains) {
    ASSERT_TRUE(BinaryString::EmptyUtf8().Contains(BinaryString::EmptyUtf8()));
    ASSERT_TRUE(FromString("hello").Contains(FromString("ello")));
    ASSERT_FALSE(FromString("hello").Contains(FromString("vello")));
    ASSERT_FALSE(FromString("hello").Contains(FromString("hellooo")));
    ASSERT_TRUE(FromString("大千世界").Contains(FromString("千世界")));
    ASSERT_FALSE(FromString("大千世界").Contains(FromString("世千")));
    ASSERT_FALSE(FromString("大千世界").Contains(FromString("大千世界好")));
}

TEST_F(BinaryStringTest, TestStartsWith) {
    ASSERT_TRUE(BinaryString::EmptyUtf8().StartsWith(BinaryString::EmptyUtf8()));
    ASSERT_TRUE(FromString("hello").StartsWith(FromString("hell")));
    ASSERT_FALSE(FromString("hello").StartsWith(FromString("ell")));
    ASSERT_FALSE(FromString("hello").StartsWith(FromString("hellooo")));
    ASSERT_TRUE(FromString("数据砖头").StartsWith(FromString("数据")));
    ASSERT_FALSE(FromString("大千世界").StartsWith(FromString("千")));
    ASSERT_FALSE(FromString("大千世界").StartsWith(FromString("大千世界好")));
}

TEST_F(BinaryStringTest, TestEndsWith) {
    ASSERT_TRUE(BinaryString::EmptyUtf8().EndsWith(BinaryString::EmptyUtf8()));
    ASSERT_TRUE(FromString("hello").EndsWith(FromString("ello")));
    ASSERT_FALSE(FromString("hello").EndsWith(FromString("ellov")));
    ASSERT_FALSE(FromString("hello").EndsWith(FromString("hhhello")));
    ASSERT_TRUE(FromString("大千世界").EndsWith(FromString("世界")));
    ASSERT_FALSE(FromString("大千世界").EndsWith(FromString("世")));
    ASSERT_FALSE(FromString("数据砖头").EndsWith(FromString("我的数据砖头")));
}

TEST_F(BinaryStringTest, TestSubstring) {
    auto pool = GetDefaultPool();
    ASSERT_EQ(FromString("hello").Substring(0, 0, pool.get()), BinaryString::EmptyUtf8());
    ASSERT_EQ(FromString("hello").Substring(1, 3, pool.get()), FromString("el"));
    ASSERT_EQ(FromString("数据砖头").Substring(0, 1, pool.get()), FromString("数"));
    ASSERT_EQ(FromString("数据砖头").Substring(1, 3, pool.get()), FromString("据砖"));
    ASSERT_EQ(FromString("数据砖头").Substring(3, 5, pool.get()), FromString("头"));
    ASSERT_EQ(FromString("ߵ梷").Substring(0, 2, pool.get()), FromString("ߵ梷"));
}

TEST_F(BinaryStringTest, TestSubStringAndCopyBinaryString) {
    auto pool = GetDefaultPool();
    std::string combined = "hello world!nice to meet you!";
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(combined, pool.get());
    MemorySegment seg = MemorySegment::Wrap(bytes);
    BinaryString binary_string = BinaryString(seg, 0, combined.size());
    int32_t left = 6, right = 20;

    // Substring [left, right), The right is not included
    ASSERT_EQ(binary_string.Substring(left, right, pool.get()),
              FromString(combined.substr(left, right - left)));
    // CopyBinaryString [left, right], The right is included
    ASSERT_EQ(binary_string.CopyBinaryString(left, right, pool.get()),
              FromString(combined.substr(left, right - left + 1)));
    ASSERT_EQ(binary_string.CopyBinaryString(0, 11, pool.get()), FromString("hello world!"));
}

TEST_F(BinaryStringTest, TestIndexOf) {
    {
        ASSERT_EQ(BinaryString::EmptyUtf8().IndexOf(BinaryString::EmptyUtf8(), 0), 0);
        ASSERT_EQ(BinaryString::EmptyUtf8().IndexOf(FromString("l"), 0), -1);
        ASSERT_EQ(FromString("hello").IndexOf(BinaryString::EmptyUtf8(), 0), 0);
        ASSERT_EQ(FromString("hello").IndexOf(FromString("l"), 0), 2);
        ASSERT_EQ(FromString("hello").IndexOf(FromString("l"), 3), 3);
        ASSERT_EQ(FromString("hello").IndexOf(FromString("a"), 0), -1);
        ASSERT_EQ(FromString("hello").IndexOf(FromString("ll"), 0), 2);
        ASSERT_EQ(FromString("hello").IndexOf(FromString("ll"), 4), -1);
        ASSERT_EQ(FromString("数据砖头").IndexOf(FromString("据砖"), 0), 1);
        ASSERT_EQ(FromString("数据砖头").IndexOf(FromString("数"), 3), -1);
        ASSERT_EQ(FromString("数据砖头").IndexOf(FromString("数"), 0), 0);
        ASSERT_EQ(FromString("数据砖头").IndexOf(FromString("头"), 0), 3);
    }
    {
        auto pool = GetDefaultPool();
        std::string combined = "Strive not to be a success, but rather to be of value.";
        auto bytes = std::make_shared<Bytes>(combined, pool.get());
        MemorySegment seg = MemorySegment::Wrap(bytes);
        auto binary_string = BinaryString::FromAddress(seg, /*offset=*/0,
                                                       /*num_bytes=*/combined.length());
        ASSERT_EQ(combined, binary_string.ToString());
        ASSERT_EQ(binary_string.IndexOf(FromString("value"), 0), 48);
        ASSERT_EQ(binary_string.IndexOf(FromString("value"), 5), 48);
        ASSERT_EQ(binary_string.IndexOf(FromString("vvalue"), 0), -1);
        ASSERT_EQ(binary_string.IndexOf(FromString("!"), 0), -1);
    }
}

TEST_F(BinaryStringTest, TestToUpperLowerCase) {
    auto pool = GetDefaultPool();
    ASSERT_EQ(FromString("我是中国人").ToLowerCase(pool.get()), FromString("我是中国人"));
    ASSERT_EQ(FromString("我是中国人").ToUpperCase(pool.get()), FromString("我是中国人"));
    ASSERT_EQ(BinaryString::EmptyUtf8().ToUpperCase(pool.get()), BinaryString::EmptyUtf8());

    ASSERT_EQ(FromString("aBcDeFg").ToLowerCase(pool.get()), FromString("abcdefg"));
    ASSERT_EQ(FromString("aBcDeFg").ToUpperCase(pool.get()), FromString("ABCDEFG"));

    ASSERT_EQ(FromString("!@#$%^*").ToLowerCase(pool.get()), FromString("!@#$%^*"));
    ASSERT_EQ(FromString("!@#$%^*").ToLowerCase(pool.get()), FromString("!@#$%^*"));
    ASSERT_EQ(BinaryString::EmptyUtf8().ToLowerCase(pool.get()), BinaryString::EmptyUtf8());
}

TEST_F(BinaryStringTest, TestEmptyString) {
    BinaryString str2 = FromString("hahahahah");
    BinaryString str3;
    auto pool = GetDefaultPool();
    {
        std::shared_ptr<Bytes> bytes0 = Bytes::AllocateBytes(10, pool.get());
        MemorySegment seg0 = MemorySegment::Wrap(bytes0);
        str3 = BinaryString::FromAddress(seg0, /*offset=*/5, /*num_bytes=*/0);
    }

    ASSERT_TRUE(BinaryString::EmptyUtf8().CompareTo(str2) < 0);
    ASSERT_TRUE(str2.CompareTo(BinaryString::EmptyUtf8()) > 0);

    ASSERT_EQ(BinaryString::EmptyUtf8().CompareTo(str3), 0);
    ASSERT_EQ(str3.CompareTo(BinaryString::EmptyUtf8()), 0);

    ASSERT_FALSE(str2 == BinaryString::EmptyUtf8());
    ASSERT_FALSE(BinaryString::EmptyUtf8() == str2);

    ASSERT_EQ(str3, BinaryString::EmptyUtf8());
    ASSERT_EQ(BinaryString::EmptyUtf8(), str3);
}

TEST_F(BinaryStringTest, TestSkipWrongFirstByte) {
    auto pool = GetDefaultPool();
    std::vector<int32_t> wrong_first_bytes = {0x80, 0x9F,
                                              0xBF,  // Skip Continuation bytes
                                              0xC0,
                                              0xC2,  // 0xC0..0xC1 - disallowed in UTF-8
                                              // 0xF5..0xFF - disallowed in UTF-8
                                              0xF5, 0xF6, 0xF7, 0xF8, 0xF9, 0xFA, 0xFB, 0xFC, 0xFD,
                                              0xFE, 0xFF};
    std::shared_ptr<Bytes> c = Bytes::AllocateBytes(1, pool.get());
    for (int32_t wrong_first_byte : wrong_first_bytes) {
        (*c)[0] = static_cast<char>(wrong_first_byte);
        ASSERT_EQ(1, BinaryString::FromBytes(c).NumChars());
    }
}

TEST_F(BinaryStringTest, TestFromBytes) {
    auto pool = GetDefaultPool();
    std::string s = "hahahe";
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(s, pool.get());
    ASSERT_TRUE(BinaryString::FromBytes(bytes, 0, 6) == BinaryString::FromString(s, pool.get()));
}

TEST_F(BinaryStringTest, TestCopy) {
    auto pool = GetDefaultPool();
    std::string s = "hahahe";
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(s, pool.get());
    BinaryString binary_string = BinaryString::FromBytes(bytes, 0, 6);
    BinaryString copy_binary_string = binary_string.Copy(pool.get());
    ASSERT_EQ(binary_string, copy_binary_string);
    ASSERT_EQ(copy_binary_string.ByteAt(2), 'h');
}

TEST_F(BinaryStringTest, TestByteAt) {
    auto pool = GetDefaultPool();
    std::string combined = "helloworld!";
    auto bytes = std::make_shared<Bytes>(combined, pool.get());
    MemorySegment seg = MemorySegment::Wrap(bytes);
    auto binary_string = BinaryString::FromAddress(seg, /*offset=*/2,
                                                   /*num_bytes=*/combined.length() - 2);
    ASSERT_EQ(binary_string.ByteAt(0), 'l');
    ASSERT_EQ(binary_string.ByteAt(5), 'r');
}

TEST_F(BinaryStringTest, TestNumChars) {
    auto pool = GetDefaultPool();
    {
        auto bytes = std::make_shared<Bytes>("hello", pool.get());
        MemorySegment seg = MemorySegment::Wrap(bytes);
        auto binary_string = BinaryString::FromAddress(seg, /*offset=*/0,
                                                       /*num_bytes=*/5);
        ASSERT_EQ(5, binary_string.NumChars());
    }
    {
        auto bytes = std::make_shared<Bytes>("helloworld", pool.get());
        MemorySegment seg = MemorySegment::Wrap(bytes);
        auto binary_string = BinaryString::FromAddress(seg, /*offset=*/0,
                                                       /*num_bytes=*/10);
        ASSERT_EQ(10, binary_string.NumChars());
    }
}

TEST_F(BinaryStringTest, TestMatchAt) {
    auto pool = GetDefaultPool();
    {
        // abc
        std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes("abc", pool.get());
        MemorySegment seg1 = MemorySegment::Wrap(bytes1);
        auto binary_string1 = BinaryString::FromAddress(seg1, /*offset=*/0,
                                                        /*num_bytes=*/3);
        // bc
        std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes("bc", pool.get());
        MemorySegment seg2 = MemorySegment::Wrap(bytes2);
        auto binary_string2 = BinaryString::FromAddress(seg2, /*offset=*/0,
                                                        /*num_bytes=*/2);
        ASSERT_TRUE(binary_string1.MatchAt(binary_string2, /*pos=*/1));
        ASSERT_FALSE(binary_string1.MatchAt(binary_string2, /*pos=*/0));
    }
    {
        // abcdef
        std::shared_ptr<Bytes> bytes1 = Bytes::AllocateBytes("abcdef", pool.get());
        MemorySegment seg1 = MemorySegment::Wrap(bytes1);
        auto binary_string1 = BinaryString::FromAddress(seg1, /*offset=*/0,
                                                        /*num_bytes=*/6);
        // bc
        std::shared_ptr<Bytes> bytes2 = Bytes::AllocateBytes("bc", pool.get());
        MemorySegment seg2 = MemorySegment::Wrap(bytes2);
        auto binary_string2 = BinaryString::FromAddress(seg2, /*offset=*/0,
                                                        /*num_bytes=*/2);
        ASSERT_TRUE(binary_string1.MatchAt(binary_string2, /*pos=*/1));
        ASSERT_FALSE(binary_string1.MatchAt(binary_string2, /*pos=*/0));
    }
}

}  // namespace paimon::test
