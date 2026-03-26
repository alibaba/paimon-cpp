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

    template <typename T, typename U>
    void InnerCheckEqual(T&& expected, U&& actual) {
        ASSERT_EQ(std::forward<T>(expected), std::forward<U>(actual));
    }

    template <typename T>
    void InnerCheck(T&& expected) {
        ASSERT_TRUE(std::forward<T>(expected));
    }

    template <typename T>
    void InnerCheckFalse(T&& expected) {
        ASSERT_FALSE(std::forward<T>(expected));
    }

    void CheckBasic(const std::string& str, int32_t len) {
        BinaryString s1 = FromString(str);
        auto pool = GetDefaultPool();
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(str, pool.get());
        BinaryString s2 = BinaryString::FromBytes(bytes);
        InnerCheckEqual(len, s1.NumChars());
        InnerCheckEqual(len, s2.NumChars());

        InnerCheckEqual(str, s1.ToString());
        InnerCheckEqual(str, s2.ToString());
        InnerCheck(s1 == s2);

        InnerCheckEqual(s2.HashCode(), s1.HashCode());

        InnerCheck(s1.Contains(s2));
        InnerCheck(s2.Contains(s1));
        InnerCheck(s1.StartsWith(s1));
        InnerCheck(s1.EndsWith(s1));
        InnerCheck(s2.StartsWith(s2));
        InnerCheck(s2.EndsWith(s2));
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
    InnerCheckEqual(FromString(""), BinaryString::EmptyUtf8());
    std::string empty_str;
    auto pool = GetDefaultPool();
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(empty_str, pool.get());
    InnerCheckEqual(BinaryString::FromBytes(bytes), BinaryString::EmptyUtf8());
    InnerCheckEqual(BinaryString::EmptyUtf8().NumChars(), 0);
    InnerCheckEqual(BinaryString::EmptyUtf8().GetSizeInBytes(), 0);
}

TEST_F(BinaryStringTest, TestCompareTo) {
    auto pool = GetDefaultPool();
    InnerCheckEqual(FromString("   ").CompareTo(BinaryString::BlankString(3, pool.get())), 0);
    InnerCheck(FromString("").CompareTo(FromString("a")) < 0);
    InnerCheck(FromString("abc").CompareTo(FromString("ABC")) > 0);
    InnerCheck(FromString("abc0").CompareTo(FromString("abc")) > 0);
    InnerCheckEqual(FromString("abcabcabc").CompareTo(FromString("abcabcabc")), 0);
    InnerCheck(FromString("aBcabcabc").CompareTo(FromString("Abcabcabc")) > 0);
    InnerCheck(FromString("Abcabcabc").CompareTo(FromString("abcabcabC")) < 0);
    InnerCheck(FromString("abcabcabc").CompareTo(FromString("abcabcabC")) > 0);

    InnerCheck(FromString("abc").CompareTo(FromString("世界")) < 0);
    InnerCheck(FromString("你好").CompareTo(FromString("世界")) > 0);
    InnerCheck(FromString("你好123").CompareTo(FromString("你好122")) > 0);
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
    InnerCheckEqual(binary_string1.ToString(), "aaaaaabcde");
    InnerCheckEqual(binary_string2.ToString(), "abcdeb");
    InnerCheckEqual(binary_string1.CompareTo(binary_string2), -1);
    InnerCheckEqual(binary_string1, binary_string1);
    InnerCheck(binary_string1 < binary_string2);

    // test equal length compare
    binary_string1 = BinaryString::FromAddress(seg1, 5, 5);
    binary_string2 = BinaryString::FromAddress(seg2, 0, 5);
    InnerCheckEqual(binary_string1.ToString(), "abcde");
    InnerCheckEqual(binary_string2.ToString(), "abcde");
    InnerCheckEqual(binary_string1, binary_string2);

    // test not equal
    binary_string1 = BinaryString::FromAddress(seg1, 0, 5);
    binary_string2 = BinaryString::FromAddress(seg2, 0, 5);
    InnerCheckEqual(binary_string1.ToString(), "aaaaa");
    InnerCheckEqual(binary_string2.ToString(), "abcde");
    InnerCheckEqual(binary_string1.CompareTo(binary_string2), -1);
    InnerCheckEqual(binary_string2.CompareTo(binary_string1), 1);

    // test with offset in single segment
    std::shared_ptr<Bytes> data3 = Bytes::AllocateBytes(10, pool.get());
    MemorySegment seg3 = MemorySegment::Wrap(data3);
    seg3.Put(4, Bytes("abcdeb", pool.get()), 0, 6);
    binary_string2 = BinaryString::FromAddress(seg3, 4, 6);
    InnerCheckEqual(binary_string2.ToString(), "abcdeb");
    InnerCheckEqual(binary_string1.CompareTo(binary_string2), -1);
    InnerCheckEqual(binary_string2.CompareTo(binary_string1), 1);
}

TEST_F(BinaryStringTest, TestContains) {
    InnerCheck(BinaryString::EmptyUtf8().Contains(BinaryString::EmptyUtf8()));
    InnerCheck(FromString("hello").Contains(FromString("ello")));
    InnerCheckFalse(FromString("hello").Contains(FromString("vello")));
    InnerCheckFalse(FromString("hello").Contains(FromString("hellooo")));
    InnerCheck(FromString("大千世界").Contains(FromString("千世界")));
    InnerCheckFalse(FromString("大千世界").Contains(FromString("世千")));
    InnerCheckFalse(FromString("大千世界").Contains(FromString("大千世界好")));
}

TEST_F(BinaryStringTest, TestStartsWith) {
    InnerCheck(BinaryString::EmptyUtf8().StartsWith(BinaryString::EmptyUtf8()));
    InnerCheck(FromString("hello").StartsWith(FromString("hell")));
    InnerCheckFalse(FromString("hello").StartsWith(FromString("ell")));
    InnerCheckFalse(FromString("hello").StartsWith(FromString("hellooo")));
    InnerCheck(FromString("数据砖头").StartsWith(FromString("数据")));
    InnerCheckFalse(FromString("大千世界").StartsWith(FromString("千")));
    InnerCheckFalse(FromString("大千世界").StartsWith(FromString("大千世界好")));
}

TEST_F(BinaryStringTest, TestEndsWith) {
    InnerCheck(BinaryString::EmptyUtf8().EndsWith(BinaryString::EmptyUtf8()));
    InnerCheck(FromString("hello").EndsWith(FromString("ello")));
    InnerCheckFalse(FromString("hello").EndsWith(FromString("ellov")));
    InnerCheckFalse(FromString("hello").EndsWith(FromString("hhhello")));
    InnerCheck(FromString("大千世界").EndsWith(FromString("世界")));
    InnerCheckFalse(FromString("大千世界").EndsWith(FromString("世")));
    InnerCheckFalse(FromString("数据砖头").EndsWith(FromString("我的数据砖头")));
}

TEST_F(BinaryStringTest, TestSubstring) {
    auto pool = GetDefaultPool();
    InnerCheckEqual(FromString("hello").Substring(0, 0, pool.get()), BinaryString::EmptyUtf8());
    InnerCheckEqual(FromString("hello").Substring(1, 3, pool.get()), FromString("el"));
    InnerCheckEqual(FromString("数据砖头").Substring(0, 1, pool.get()), FromString("数"));
    InnerCheckEqual(FromString("数据砖头").Substring(1, 3, pool.get()), FromString("据砖"));
    InnerCheckEqual(FromString("数据砖头").Substring(3, 5, pool.get()), FromString("头"));
    InnerCheckEqual(FromString("ߵ梷").Substring(0, 2, pool.get()), FromString("ߵ梷"));
}

TEST_F(BinaryStringTest, TestSubStringAndCopyBinaryString) {
    auto pool = GetDefaultPool();
    std::string combined = "hello world!nice to meet you!";
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(combined, pool.get());
    MemorySegment seg = MemorySegment::Wrap(bytes);
    BinaryString binary_string = BinaryString(seg, 0, combined.size());
    int32_t left = 6, right = 20;

    // Substring [left, right), The right is not included
    InnerCheckEqual(binary_string.Substring(left, right, pool.get()),
                    FromString(combined.substr(left, right - left)));
    // CopyBinaryString [left, right], The right is included
    InnerCheckEqual(binary_string.CopyBinaryString(left, right, pool.get()),
                    FromString(combined.substr(left, right - left + 1)));
    InnerCheckEqual(binary_string.CopyBinaryString(0, 11, pool.get()), FromString("hello world!"));
}

TEST_F(BinaryStringTest, TestIndexOf) {
    {
        InnerCheckEqual(BinaryString::EmptyUtf8().IndexOf(BinaryString::EmptyUtf8(), 0), 0);
        InnerCheckEqual(BinaryString::EmptyUtf8().IndexOf(FromString("l"), 0), -1);
        InnerCheckEqual(FromString("hello").IndexOf(BinaryString::EmptyUtf8(), 0), 0);
        InnerCheckEqual(FromString("hello").IndexOf(FromString("l"), 0), 2);
        InnerCheckEqual(FromString("hello").IndexOf(FromString("l"), 3), 3);
        InnerCheckEqual(FromString("hello").IndexOf(FromString("a"), 0), -1);
        InnerCheckEqual(FromString("hello").IndexOf(FromString("ll"), 0), 2);
        InnerCheckEqual(FromString("hello").IndexOf(FromString("ll"), 4), -1);
        InnerCheckEqual(FromString("数据砖头").IndexOf(FromString("据砖"), 0), 1);
        InnerCheckEqual(FromString("数据砖头").IndexOf(FromString("数"), 3), -1);
        InnerCheckEqual(FromString("数据砖头").IndexOf(FromString("数"), 0), 0);
        InnerCheckEqual(FromString("数据砖头").IndexOf(FromString("头"), 0), 3);
    }
    {
        auto pool = GetDefaultPool();
        std::string combined = "Strive not to be a success, but rather to be of value.";
        auto bytes = std::make_shared<Bytes>(combined, pool.get());
        MemorySegment seg = MemorySegment::Wrap(bytes);
        auto binary_string = BinaryString::FromAddress(seg, /*offset=*/0,
                                                       /*num_bytes=*/combined.length());
        InnerCheckEqual(combined, binary_string.ToString());
        InnerCheckEqual(binary_string.IndexOf(FromString("value"), 0), 48);
        InnerCheckEqual(binary_string.IndexOf(FromString("value"), 5), 48);
        InnerCheckEqual(binary_string.IndexOf(FromString("vvalue"), 0), -1);
        InnerCheckEqual(binary_string.IndexOf(FromString("!"), 0), -1);
    }
}

TEST_F(BinaryStringTest, TestToUpperLowerCase) {
    auto pool = GetDefaultPool();
    InnerCheckEqual(FromString("我是中国人").ToLowerCase(pool.get()), FromString("我是中国人"));
    InnerCheckEqual(FromString("我是中国人").ToUpperCase(pool.get()), FromString("我是中国人"));
    InnerCheckEqual(BinaryString::EmptyUtf8().ToUpperCase(pool.get()), BinaryString::EmptyUtf8());

    InnerCheckEqual(FromString("aBcDeFg").ToLowerCase(pool.get()), FromString("abcdefg"));
    InnerCheckEqual(FromString("aBcDeFg").ToUpperCase(pool.get()), FromString("ABCDEFG"));

    InnerCheckEqual(FromString("!@#$%^*").ToLowerCase(pool.get()), FromString("!@#$%^*"));
    InnerCheckEqual(FromString("!@#$%^*").ToLowerCase(pool.get()), FromString("!@#$%^*"));
    InnerCheckEqual(BinaryString::EmptyUtf8().ToLowerCase(pool.get()), BinaryString::EmptyUtf8());
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

    InnerCheck(BinaryString::EmptyUtf8().CompareTo(str2) < 0);
    InnerCheck(str2.CompareTo(BinaryString::EmptyUtf8()) > 0);

    InnerCheckEqual(BinaryString::EmptyUtf8().CompareTo(str3), 0);
    InnerCheckEqual(str3.CompareTo(BinaryString::EmptyUtf8()), 0);

    InnerCheckFalse(str2 == BinaryString::EmptyUtf8());
    InnerCheckFalse(BinaryString::EmptyUtf8() == str2);

    InnerCheckEqual(str3, BinaryString::EmptyUtf8());
    InnerCheckEqual(BinaryString::EmptyUtf8(), str3);
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
        InnerCheckEqual(1, BinaryString::FromBytes(c).NumChars());
    }
}

TEST_F(BinaryStringTest, TestFromBytes) {
    auto pool = GetDefaultPool();
    std::string s = "hahahe";
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(s, pool.get());
    InnerCheck(BinaryString::FromBytes(bytes, 0, 6) == BinaryString::FromString(s, pool.get()));
}

TEST_F(BinaryStringTest, TestCopy) {
    auto pool = GetDefaultPool();
    std::string s = "hahahe";
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(s, pool.get());
    BinaryString binary_string = BinaryString::FromBytes(bytes, 0, 6);
    BinaryString copy_binary_string = binary_string.Copy(pool.get());
    InnerCheckEqual(binary_string, copy_binary_string);
    InnerCheckEqual(copy_binary_string.ByteAt(2), 'h');
}

TEST_F(BinaryStringTest, TestByteAt) {
    auto pool = GetDefaultPool();
    std::string combined = "helloworld!";
    auto bytes = std::make_shared<Bytes>(combined, pool.get());
    MemorySegment seg = MemorySegment::Wrap(bytes);
    auto binary_string = BinaryString::FromAddress(seg, /*offset=*/2,
                                                   /*num_bytes=*/combined.length() - 2);
    InnerCheckEqual(binary_string.ByteAt(0), 'l');
    InnerCheckEqual(binary_string.ByteAt(5), 'r');
}

TEST_F(BinaryStringTest, TestNumChars) {
    auto pool = GetDefaultPool();
    {
        auto bytes = std::make_shared<Bytes>("hello", pool.get());
        MemorySegment seg = MemorySegment::Wrap(bytes);
        auto binary_string = BinaryString::FromAddress(seg, /*offset=*/0,
                                                       /*num_bytes=*/5);
        InnerCheckEqual(5, binary_string.NumChars());
    }
    {
        auto bytes = std::make_shared<Bytes>("helloworld", pool.get());
        MemorySegment seg = MemorySegment::Wrap(bytes);
        auto binary_string = BinaryString::FromAddress(seg, /*offset=*/0,
                                                       /*num_bytes=*/10);
        InnerCheckEqual(10, binary_string.NumChars());
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
        InnerCheck(binary_string1.MatchAt(binary_string2, /*pos=*/1));
        InnerCheckFalse(binary_string1.MatchAt(binary_string2, /*pos=*/0));
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
        InnerCheck(binary_string1.MatchAt(binary_string2, /*pos=*/1));
        InnerCheckFalse(binary_string1.MatchAt(binary_string2, /*pos=*/0));
    }
}

}  // namespace paimon::test
