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

#include "paimon/common/types/data_type_json_parser.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <map>
#include <sstream>
#include <utility>
#include <vector>

#include "fmt/format.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/rapidjson_util.h"
#include "paimon/common/utils/string_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"

namespace paimon {
namespace {
static constexpr char CHAR_BEGIN_SUBTYPE = '<';
static constexpr char CHAR_END_SUBTYPE = '>';
static constexpr char CHAR_BEGIN_PARAMETER = '(';
static constexpr char CHAR_END_PARAMETER = ')';
static constexpr char CHAR_LIST_SEPARATOR = ',';
static constexpr char CHAR_STRING = '\'';
static constexpr char CHAR_IDENTIFIER = '`';
static constexpr char CHAR_DOT = '.';

enum class TokenType : int32_t {
    // e.g. "ROW<"
    BEGIN_SUBTYPE = 1,
    // e.g. "ROW<..>"
    END_SUBTYPE,
    // e.g. "CHAR("
    BEGIN_PARAMETER,
    // e.g. "CHAR(...)"
    END_PARAMETER,
    // e.g. "ROW<INT,"
    LIST_SEPARATOR,
    // e.g. "ROW<name INT 'Comment'"
    LITERAL_STRING,
    // CHAR(12
    LITERAL_INT,
    // e.g. "CHAR" or "TO"
    KEYWORD,
    // e.g. "ROW<name" or "myCatalog.myDatabase"
    IDENTIFIER,
    // e.g. "myCatalog.myDatabase."
    IDENTIFIER_SEPARATOR
};

struct Token {
    Token(const TokenType& _type, int32_t _cursor_position, const std::string& _value)
        : type(_type), cursor_position(_cursor_position), value(_value) {}
    TokenType type;
    int32_t cursor_position;
    std::string value;
};

// nullptr is returned in the case of parsing failed
Result<std::shared_ptr<arrow::DataType>> ParseAtomicType(const std::string& str, bool* nullable,
                                                         bool* is_blob);
std::vector<Token> Tokenize(const std::string& chars);
bool IsWhitespace(char character);
bool IsDelimiter(char character);
bool IsDigit(char c) {
    return c >= '0' && c <= '9';
}
int32_t ConsumeEscaped(const std::string& chars, int32_t cursor, char delimiter,
                       std::ostringstream& builder);
int32_t ConsumeInt(const std::string& chars, int32_t cursor, std::ostringstream& builder);
int32_t ConsumeIdentifier(const std::string& chars, int32_t cursor, std::ostringstream& builder);

// noted that keyword appears in Keyword is supposed to appear in KEYWORDS
enum class Keyword : int32_t {
    INVALID = 0,
    CHAR,
    VARCHAR,
    STRING,
    BOOLEAN,
    BINARY,
    VARBINARY,
    BYTES,
    DECIMAL,
    NUMERIC,
    DEC,
    TINYINT,
    SMALLINT,
    INT,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    PRECISION,
    DATE,
    TIME,
    WITH,
    WITHOUT,
    LOCAL,
    ZONE,
    TIMESTAMP,
    TIMESTAMP_LTZ,
    INTERVAL,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE,
    SECOND,
    TO,
    ARRAY,
    MULTISET,
    MAP,
    ROW,
    BLOB,
    // NULL is keyword in c++
    NULL_,
    RAW,
    LEGACY,
    NOT
};

const std::map<std::string, Keyword>& Keywords() {
    static const std::map<std::string, Keyword> kKeywords = {
        {"CHAR", Keyword::CHAR},
        {"VARCHAR", Keyword::VARCHAR},
        {"STRING", Keyword::STRING},
        {"BOOLEAN", Keyword::BOOLEAN},
        {"BINARY", Keyword::BINARY},
        {"VARBINARY", Keyword::VARBINARY},
        {"BYTES", Keyword::BYTES},
        {"DECIMAL", Keyword::DECIMAL},
        {"NUMERIC", Keyword::NUMERIC},
        {"DEC", Keyword::DEC},
        {"TINYINT", Keyword::TINYINT},
        {"SMALLINT", Keyword::SMALLINT},
        {"INT", Keyword::INT},
        {"INTEGER", Keyword::INTEGER},
        {"BIGINT", Keyword::BIGINT},
        {"FLOAT", Keyword::FLOAT},
        {"DOUBLE", Keyword::DOUBLE},
        {"PRECISION", Keyword::PRECISION},
        {"DATE", Keyword::DATE},
        {"TIME", Keyword::TIME},
        {"WITH", Keyword::WITH},
        {"WITHOUT", Keyword::WITHOUT},
        {"LOCAL", Keyword::LOCAL},
        {"ZONE", Keyword::ZONE},
        {"TIMESTAMP", Keyword::TIMESTAMP},
        {"TIMESTAMP_LTZ", Keyword::TIMESTAMP_LTZ},
        {"INTERVAL", Keyword::INTERVAL},
        {"YEAR", Keyword::YEAR},
        {"MONTH", Keyword::MONTH},
        {"DAY", Keyword::DAY},
        {"HOUR", Keyword::HOUR},
        {"MINUTE", Keyword::MINUTE},
        {"SECOND", Keyword::SECOND},
        {"TO", Keyword::TO},
        {"ARRAY", Keyword::ARRAY},
        {"MULTISET", Keyword::MULTISET},
        {"MAP", Keyword::MAP},
        {"ROW", Keyword::ROW},
        {"BLOB", Keyword::BLOB},
        {"NULL", Keyword::NULL_},
        {"RAW", Keyword::RAW},
        {"LEGACY", Keyword::LEGACY},
        {"NOT", Keyword::NOT}};
    return kKeywords;
}

class TokenParser {
 public:
    TokenParser(const std::string& input_string, const std::vector<Token>& tokens)
        : input_string_(input_string), tokens_(tokens) {}

    Result<std::shared_ptr<arrow::DataType>> ParseTokens(bool* nullable, bool* is_blob);

 private:
    inline const Token& GetToken() const {
        return tokens_[current_token_];
    }
    int32_t TokenAsInt() const {
        return std::stoi(GetToken().value);
    }
    Keyword TokenAsKeyword() const {
        return TokenAsKeyword(GetToken());
    }
    Keyword TokenAsKeyword(const Token& token) const {
        auto iter = Keywords().find(token.value);
        if (iter != Keywords().end()) {
            return iter->second;
        }
        return Keyword::INVALID;
    }
    bool HasRemainingTokens() const {
        return current_token_ + 1 < static_cast<int32_t>(tokens_.size());
    }

    Status NextToken();
    Status NextToken(TokenType type);
    Status NextToken(Keyword keyword);
    bool HasNextToken(const std::vector<TokenType>& types) const;
    bool HasNextToken(const std::vector<Keyword>& keywords) const;
    Result<bool> ParseNullability();
    Result<std::shared_ptr<arrow::DataType>> ParseTypeWithNullability(bool* nullable,
                                                                      bool* is_blob);
    Result<std::shared_ptr<arrow::DataType>> ParseTypeByKeyword(bool* is_blob);
    Result<int32_t> ParseStringLength();
    template <typename T>
    Result<std::shared_ptr<arrow::DataType>> ParseStringType();
    Result<std::shared_ptr<arrow::DataType>> ParseDecimalType();
    Result<std::shared_ptr<arrow::DataType>> ParseDoubleType();
    Result<std::shared_ptr<arrow::DataType>> ParseTimestampType();
    Result<std::shared_ptr<arrow::DataType>> ParseTimestampLtzType();
    Result<int32_t> ParseOptionalPrecision(int32_t default_precision);

 private:
    std::string input_string_;
    std::vector<Token> tokens_;
    int32_t last_valid_token_ = -1;
    int32_t current_token_ = -1;
};

Result<std::shared_ptr<arrow::DataType>> ParseAtomicType(const std::string& str, bool* nullable,
                                                         bool* is_blob) {
    try {
        std::vector<Token> tokens = Tokenize(str);
        TokenParser converter(str, tokens);
        return converter.ParseTokens(nullable, is_blob);
    } catch (...) {
        return Status::Invalid("parse atomic type failed.");
    }
}

std::vector<Token> Tokenize(const std::string& chars) {
    std::vector<Token> tokens;
    std::ostringstream builder;
    for (size_t cursor = 0; cursor < chars.length(); cursor++) {
        const auto& cur_char = chars[cursor];
        switch (cur_char) {
            case CHAR_BEGIN_SUBTYPE:
                tokens.emplace_back(TokenType::BEGIN_SUBTYPE, cursor,
                                    std::to_string(CHAR_BEGIN_SUBTYPE));
                break;
            case CHAR_END_SUBTYPE:
                tokens.emplace_back(TokenType::END_SUBTYPE, cursor,
                                    std::to_string(CHAR_END_SUBTYPE));
            case CHAR_BEGIN_PARAMETER:
                tokens.emplace_back(TokenType::BEGIN_PARAMETER, cursor,
                                    std::to_string(CHAR_BEGIN_PARAMETER));
                break;
            case CHAR_END_PARAMETER:
                tokens.emplace_back(TokenType::END_PARAMETER, cursor,
                                    std::to_string(CHAR_END_PARAMETER));
                break;
            case CHAR_LIST_SEPARATOR:
                tokens.emplace_back(TokenType::LIST_SEPARATOR, cursor,
                                    std::to_string(CHAR_LIST_SEPARATOR));
                break;
            case CHAR_DOT:
                tokens.emplace_back(TokenType::IDENTIFIER_SEPARATOR, cursor,
                                    std::to_string(CHAR_DOT));
                break;
            case CHAR_STRING:
                builder.str("");
                builder.clear();
                cursor = ConsumeEscaped(chars, cursor, CHAR_STRING, builder);
                tokens.emplace_back(TokenType::LITERAL_STRING, cursor, builder.str());
                break;
            case CHAR_IDENTIFIER:
                builder.str("");
                builder.clear();
                cursor = ConsumeEscaped(chars, cursor, CHAR_IDENTIFIER, builder);
                tokens.emplace_back(TokenType::IDENTIFIER, cursor, builder.str());
                break;
            default:
                if (IsWhitespace(cur_char)) {
                    continue;
                }
                if (IsDigit(cur_char)) {
                    builder.str("");
                    builder.clear();
                    cursor = ConsumeInt(chars, cursor, builder);
                    tokens.emplace_back(TokenType::LITERAL_INT, cursor, builder.str());
                    break;
                }
                builder.str("");
                builder.clear();
                cursor = ConsumeIdentifier(chars, cursor, builder);
                auto token = builder.str();
                auto normalized_token = token;
                std::transform(normalized_token.begin(), normalized_token.end(),
                               normalized_token.begin(),
                               [](unsigned char c) { return std::toupper(c); });
                if (Keywords().find(normalized_token) != Keywords().end()) {
                    tokens.emplace_back(TokenType::KEYWORD, cursor, normalized_token);
                } else {
                    tokens.emplace_back(TokenType::IDENTIFIER, cursor, token);
                }
        }
    }
    return tokens;
}

bool IsWhitespace(char character) {
    return std::isspace(static_cast<unsigned char>(character));
}

bool IsDelimiter(char character) {
    return IsWhitespace(character) || character == CHAR_BEGIN_SUBTYPE ||
           character == CHAR_END_SUBTYPE || character == CHAR_BEGIN_PARAMETER ||
           character == CHAR_END_PARAMETER || character == CHAR_LIST_SEPARATOR ||
           character == CHAR_DOT;
}

int32_t ConsumeEscaped(const std::string& chars, int32_t cursor, char delimiter,
                       std::ostringstream& builder) {
    // skip delimiter
    cursor++;
    for (; cursor < static_cast<int32_t>(chars.length()); cursor++) {
        const char& cur_char = chars[cursor];
        if (cur_char == delimiter && cursor + 1 < static_cast<int32_t>(chars.length()) &&
            chars[cursor + 1] == delimiter) {
            // escaping of the escaping char e.g. "'Hello '' World'"
            cursor++;
            builder << cur_char;
        } else if (cur_char == delimiter) {
            break;
        } else {
            builder << cur_char;
        }
    }
    return cursor;
}

int32_t ConsumeInt(const std::string& chars, int32_t cursor, std::ostringstream& builder) {
    for (; cursor < static_cast<int32_t>(chars.length()) && IsDigit(chars[cursor]); cursor++) {
        builder << chars[cursor];
    }
    return cursor - 1;
}

int32_t ConsumeIdentifier(const std::string& chars, int32_t cursor, std::ostringstream& builder) {
    for (; cursor < static_cast<int32_t>(chars.length()) && !IsDelimiter(chars[cursor]); cursor++) {
        builder << chars[cursor];
    }
    return cursor - 1;
}

Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseTokens(bool* nullable, bool* is_blob) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> type,
                           ParseTypeWithNullability(nullable, is_blob));
    if (HasRemainingTokens()) {
        PAIMON_RETURN_NOT_OK(NextToken());
        return Status::Invalid(fmt::format("Unexpected token: {}", GetToken().value));
    }
    return type;
}

Status TokenParser::NextToken() {
    current_token_++;
    if (current_token_ >= static_cast<int32_t>(tokens_.size())) {
        return Status::Invalid("Unexpected end.");
    }
    last_valid_token_ = current_token_ - 1;
    return Status::OK();
}

Status TokenParser::NextToken(TokenType type) {
    PAIMON_RETURN_NOT_OK(NextToken());
    const auto& token = GetToken();
    if (token.type != type) {
        return Status::Invalid(fmt::format("< {} > expected but was < {} >.",
                                           static_cast<int32_t>(type),
                                           static_cast<int32_t>(token.type)));
    }
    return Status::OK();
}

Status TokenParser::NextToken(Keyword keyword) {
    PAIMON_RETURN_NOT_OK(NextToken(TokenType::KEYWORD));
    const auto& token = GetToken();
    if (Keywords().find(token.value) == Keywords().end() || keyword != TokenAsKeyword(token)) {
        return Status::Invalid(fmt::format("Keyword '{}' expected but was '{}'.",
                                           static_cast<int32_t>(keyword), token.value));
    }
    return Status::OK();
}

bool TokenParser::HasNextToken(const std::vector<TokenType>& types) const {
    if (current_token_ + types.size() + 1 > tokens_.size()) {
        return false;
    }
    for (size_t i = 0; i < types.size(); i++) {
        const auto& look_ahead = tokens_[current_token_ + i + 1];
        if (look_ahead.type != types[i]) {
            return false;
        }
    }
    return true;
}

bool TokenParser::HasNextToken(const std::vector<Keyword>& keywords) const {
    if (current_token_ + keywords.size() + 1 > tokens_.size()) {
        return false;
    }
    for (size_t i = 0; i < keywords.size(); i++) {
        const auto& look_ahead = tokens_[current_token_ + i + 1];
        if (look_ahead.type != TokenType::KEYWORD || keywords[i] != TokenAsKeyword(look_ahead)) {
            return false;
        }
    }
    return true;
}

Result<bool> TokenParser::ParseNullability() {
    // "NOT NULL"
    if (HasNextToken({Keyword::NOT, Keyword::NULL_})) {
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::NOT));
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::NULL_));
        return false;
    } else if (HasNextToken({Keyword::NULL_})) {
        // explicit "NULL"
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::NULL_));
        return true;
    }
    // implicit "NULL"
    return true;
}

Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseTypeWithNullability(bool* nullable,
                                                                               bool* is_blob) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> data_type, ParseTypeByKeyword(is_blob));
    PAIMON_ASSIGN_OR_RAISE(*nullable, ParseNullability());
    // special case: suffix notation for ARRAY types
    if (HasNextToken({Keyword::ARRAY}) || HasNextToken({Keyword::MULTISET})) {
        return Status::NotImplemented("not support for old version schema");
    }
    return data_type;
}

Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseTypeByKeyword(bool* is_blob) {
    PAIMON_RETURN_NOT_OK(NextToken(TokenType::KEYWORD));
    switch (TokenAsKeyword()) {
        case Keyword::BYTES:
            return arrow::binary();
        case Keyword::BLOB: {
            *is_blob = true;
            return arrow::large_binary();
        }
        case Keyword::STRING:
            return arrow::utf8();
        case Keyword::BOOLEAN:
            return arrow::boolean();
        case Keyword::DECIMAL:
        case Keyword::NUMERIC:
        case Keyword::DEC:
            return ParseDecimalType();
        case Keyword::TINYINT:
            return arrow::int8();
        case Keyword::SMALLINT:
            return arrow::int16();
        case Keyword::INT:
        case Keyword::INTEGER:
            return arrow::int32();
        case Keyword::BIGINT:
            return arrow::int64();
        case Keyword::FLOAT:
            return arrow::float32();
        case Keyword::DOUBLE:
            return ParseDoubleType();
        case Keyword::DATE:
            return arrow::date32();
        case Keyword::TIMESTAMP:
            return ParseTimestampType();
        case Keyword::TIMESTAMP_LTZ:
            return ParseTimestampLtzType();
        default:
            return Status::Invalid(fmt::format("Unsupported type: {}", GetToken().value));
    }
}

Result<int32_t> TokenParser::ParseStringLength() {
    // explicit length
    if (HasNextToken({TokenType::BEGIN_PARAMETER})) {
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::BEGIN_PARAMETER));
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::LITERAL_INT));
        auto length = TokenAsInt();
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::END_PARAMETER));
        return length;
    }
    // implicit length
    return -1;
}

template <typename T>
Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseStringType() {
    PAIMON_ASSIGN_OR_RAISE([[maybe_unused]] int32_t length, ParseStringLength());
    return std::make_shared<T>();
}

Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseDecimalType() {
    int32_t precision = Decimal::DEFAULT_PRECISION;
    int32_t scale = Decimal::DEFAULT_SCALE;
    if (HasNextToken({TokenType::BEGIN_PARAMETER})) {
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::BEGIN_PARAMETER));
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::LITERAL_INT));
        precision = TokenAsInt();
        if (HasNextToken({TokenType::LIST_SEPARATOR})) {
            PAIMON_RETURN_NOT_OK(NextToken(TokenType::LIST_SEPARATOR));
            PAIMON_RETURN_NOT_OK(NextToken(TokenType::LITERAL_INT));
            scale = TokenAsInt();
        }
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::END_PARAMETER));
    }
    return arrow::decimal128(precision, scale);
}

Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseDoubleType() {
    if (HasNextToken({Keyword::PRECISION})) {
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::PRECISION));
    }
    return arrow::float64();
}

Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseTimestampType() {
    PAIMON_ASSIGN_OR_RAISE(int32_t precision, ParseOptionalPrecision(Timestamp::DEFAULT_PRECISION));
    bool with_timezone = false;
    if (HasNextToken({Keyword::WITHOUT})) {
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::WITHOUT));
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::TIME));
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::ZONE));
    } else if (HasNextToken({Keyword::WITH})) {
        PAIMON_RETURN_NOT_OK(NextToken(Keyword::WITH));
        if (HasNextToken({Keyword::LOCAL})) {
            PAIMON_RETURN_NOT_OK(NextToken(Keyword::LOCAL));
            PAIMON_RETURN_NOT_OK(NextToken(Keyword::TIME));
            PAIMON_RETURN_NOT_OK(NextToken(Keyword::ZONE));
            with_timezone = true;
        }
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> ts_type,
                           DateTimeUtils::GetTypeFromPrecision(precision, with_timezone));
    return ts_type;
}

Result<std::shared_ptr<arrow::DataType>> TokenParser::ParseTimestampLtzType() {
    PAIMON_ASSIGN_OR_RAISE(int32_t precision, ParseOptionalPrecision(Timestamp::DEFAULT_PRECISION));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> ts_type,
                           DateTimeUtils::GetTypeFromPrecision(precision, /*with_timezone=*/true));
    return ts_type;
}

Result<int32_t> TokenParser::ParseOptionalPrecision(int32_t default_precision) {
    auto precision = default_precision;
    if (HasNextToken({TokenType::BEGIN_PARAMETER})) {
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::BEGIN_PARAMETER));
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::LITERAL_INT));
        precision = TokenAsInt();
        PAIMON_RETURN_NOT_OK(NextToken(TokenType::END_PARAMETER));
    }
    return precision;
}
}  // namespace

Result<std::shared_ptr<arrow::Field>> DataTypeJsonParser::ParseType(
    const std::string& name, const rapidjson::Value& type_json_value) {
    if (type_json_value.IsString()) {
        return ParseAtomicTypeField(name, type_json_value);
    } else if (type_json_value.IsObject()) {
        return ParseComplexTypeField(name, type_json_value);
    }

    return Status::Invalid("cannot parse data type");
}

Result<std::shared_ptr<arrow::Field>> DataTypeJsonParser::ParseAtomicTypeField(
    const std::string& name, const rapidjson::Value& type_json_value) {
    bool nullable = true;
    bool is_blob = false;
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::DataType> type,
                           ParseAtomicType(type_json_value.GetString(), &nullable, &is_blob));
    if (is_blob) {
        return BlobUtils::ToArrowField(name, nullable);
    } else {
        return arrow::field(name, type, nullable);
    }
}

Result<std::shared_ptr<arrow::Field>> DataTypeJsonParser::ParseComplexTypeField(
    const std::string& name, const rapidjson::Value& type_json_value) {
    if (!type_json_value.HasMember("type")) {
        return Status::Invalid("complex data type must have type");
    }

    std::string type_str = type_json_value["type"].GetString();
    bool nullable = true;
    if (type_str.find("NOT NULL") != std::string::npos) {
        nullable = false;
    }

    if (StringUtils::StartsWith(type_str, "ARRAY")) {
        return ParseArrayType(name, type_json_value, nullable);
    } else if (StringUtils::StartsWith(type_str, "MAP")) {
        return ParseMapType(name, type_json_value, nullable);
    } else if (StringUtils::StartsWith(type_str, "ROW")) {
        return ParseRowType(name, type_json_value, nullable);
    } else if (StringUtils::StartsWith(type_str, "MULTISET")) {
        return Status::NotImplemented("MULTISET is not supported");
    }

    return Status::Invalid("unknown complex data type: " + type_str);
}

Result<std::shared_ptr<arrow::Field>> DataTypeJsonParser::ParseArrayType(
    const std::string& name, const rapidjson::Value& type_json_value, bool nullable) {
    if (!type_json_value.HasMember("element")) {
        return Status::Invalid("array data type must have element");
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> element_field,
                           ParseType("item", type_json_value["element"]));
    return arrow::field(name, arrow::list(element_field), nullable);
}

Result<std::shared_ptr<arrow::Field>> DataTypeJsonParser::ParseMapType(
    const std::string& name, const rapidjson::Value& type_json_value, bool nullable) {
    if (!type_json_value.HasMember("key") || !type_json_value.HasMember("value")) {
        return Status::Invalid("map data type must have key and value");
    }

    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> key,
                           ParseType("key", type_json_value["key"]));

    // NOTE: Unlike Java Paimon, this C++ implementation does not support nullable keys in
    // MapType. This is a limitation of Apache Arrow, which does not allow null keys in its
    // MapType. As a result, we explicitly set `nullable = false` for the map key, regardless of
    // the original schema. Please be aware of this behavioral difference when migrating or
    // interoperating with Java Paimon.
    key = key->WithNullable(false);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Field> value,
                           ParseType("value", type_json_value["value"]));
    return arrow::field(name, std::make_shared<arrow::MapType>(key, value), nullable);
}

Result<std::shared_ptr<arrow::Field>> DataTypeJsonParser::ParseRowType(
    const std::string& name, const rapidjson::Value& type_json_value, bool nullable) {
    auto data_fields =
        RapidJsonUtil::DeserializeKeyValue<std::vector<DataField>>(type_json_value, "fields");

    auto struct_type = DataField::ConvertDataFieldsToArrowStructType(data_fields);
    return arrow::field(name, struct_type, nullable);
}

}  // namespace paimon
