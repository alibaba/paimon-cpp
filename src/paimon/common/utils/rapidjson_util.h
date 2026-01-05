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

#pragma once

#include <cassert>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>

#include "paimon/common/utils/string_utils.h"
#include "paimon/status.h"
#include "paimon/traits.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/encodings.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace paimon {

class RapidJsonUtil {
 public:
    RapidJsonUtil() = delete;
    ~RapidJsonUtil() = delete;

    // if T is custom type, T must have ToJson()
    template <typename T>
    static inline Status ToJsonString(const T& obj, std::string* json_str) {
        rapidjson::Document doc;
        rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();
        rapidjson::Value value;
        try {
            if constexpr (is_pointer<T>::value) {
                value = obj->ToJson(&allocator);
            } else if constexpr (std::is_same_v<T, std::map<std::string, std::string>>) {
                *json_str = MapToJsonString(obj);
                return Status::OK();
            } else {
                value = obj.ToJson(&allocator);
            }
        } catch (const std::invalid_argument& e) {
            return Status::Invalid("json serialize failed:", e.what());
        } catch (...) {
            return Status::Invalid("json serialize failed, unknown error");
        }
        if (!ToJson(value, json_str)) {
            return Status::Invalid("serialize failed");
        }
        return Status::OK();
    }

    // if T is custom type, T must have FromJson()
    template <typename T>
    static inline Status FromJsonString(const std::string& json_str, T* obj) {
        if (!obj) {
            return Status::Invalid("deserialize failed: obj is nullptr");
        }
        if constexpr (std::is_same_v<T, std::map<std::string, std::string>>) {
            PAIMON_ASSIGN_OR_RAISE(*obj, MapFromJsonString(json_str));
        } else {
            rapidjson::Document doc;
            if (!FromJson(json_str, &doc)) {
                return Status::Invalid("deserialize failed: ", json_str);
            }
            try {
                obj->FromJson(doc);
            } catch (const std::invalid_argument& e) {
                return Status::Invalid("deserialize failed, possibly type incompatible: ",
                                       e.what());
            } catch (...) {
                return Status::Invalid("deserialize failed, reason unknown: ", json_str);
            }
        }
        return Status::OK();
    }

    // if T is std::nullopt, will use rapid_json null_value
    template <typename T>
    static rapidjson::Value SerializeValue(const T& obj,
                                           rapidjson::Document::AllocatorType* allocator);

    // condition1 : has key & value -> return value
    // condition2 : no key or null value -> return default value
    template <typename T>
    static T DeserializeKeyValue(const rapidjson::Value& value, const std::string& key,
                                 const T& default_value);

    // condition1 : has key & value -> return value
    // condition2 : no key or null value, T is optional -> return std::nullopt
    // condition3 : no key or null value, T is not optional -> throw exception
    template <typename T>
    static T DeserializeKeyValue(const rapidjson::Value& value, const std::string& key);

    template <typename T>
    static T DeserializeValue(const rapidjson::Value& value);

 private:
    static inline bool ToJson(const rapidjson::Value& value, std::string* json_str) {
        assert(json_str);
        rapidjson::StringBuffer buffer;
        rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
        if (!value.Accept(writer)) {
            return false;
        }
        *json_str = buffer.GetString();
        return true;
    }

    static inline bool FromJson(const std::string& json_str, rapidjson::Document* doc) {
        doc->Parse(json_str.c_str());
        if (doc->HasParseError()) {
            return false;
        }
        return true;
    }

    template <typename T>
    static rapidjson::Value SerializeMap(const T& map,
                                         rapidjson::Document::AllocatorType* allocator);

    template <typename T>
    static rapidjson::Value SerializeVector(const T& vec,
                                            rapidjson::Document::AllocatorType* allocator);

    template <typename T>
    static T DeserializeVector(const rapidjson::Value& value);

    template <typename T>
    static T DeserializeMap(const rapidjson::Value& value);

    template <typename T>
    static T GetValue(const rapidjson::Value& value);

    static std::string MapToJsonString(const std::map<std::string, std::string>& map) {
        rapidjson::Document d;
        d.SetObject();
        rapidjson::Document::AllocatorType& allocator = d.GetAllocator();

        for (const auto& kv : map) {
            d.AddMember(rapidjson::Value(kv.first.c_str(), allocator),
                        rapidjson::Value(kv.second.c_str(), allocator), allocator);
        }

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        d.Accept(writer);

        return buffer.GetString();
    }
    static Result<std::map<std::string, std::string>> MapFromJsonString(
        const std::string& json_str) {
        rapidjson::Document doc;
        doc.Parse(json_str.c_str());
        if (doc.HasParseError() || !doc.IsObject()) {
            return Status::Invalid("deserialize failed: parse error or not JSON object: ",
                                   json_str);
        }

        std::map<std::string, std::string> result;
        for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
            if (!it->name.IsString() || !it->value.IsString()) {
                return Status::Invalid(
                    "deserialize failed: non-string key or value in JSON object: ", json_str);
            }
            result[it->name.GetString()] = it->value.GetString();
        }
        return result;
    }
};

template <typename T>
inline rapidjson::Value RapidJsonUtil::SerializeValue(
    const T& obj, rapidjson::Document::AllocatorType* allocator) {
    if constexpr (is_optional<T>::value) {
        if (obj == std::nullopt) {
            rapidjson::Value null_value;
            null_value.SetNull();
            return null_value;
        } else {
            return SerializeValue(obj.value(), allocator);
        }
    } else {
        if constexpr (std::is_same_v<T, std::string>) {
            return rapidjson::Value(obj.c_str(), *allocator);
        } else if constexpr (std::is_arithmetic_v<T>) {
            return rapidjson::Value(obj);
        } else if constexpr (is_map<T>::value) {
            return SerializeMap(obj, allocator);
        } else if constexpr (is_vector<T>::value) {
            return SerializeVector(obj, allocator);
        } else {
            // custom type
            return obj.ToJson(allocator);
        }
    }
}

template <typename T>
inline rapidjson::Value RapidJsonUtil::SerializeMap(const T& map,
                                                    rapidjson::Document::AllocatorType* allocator) {
    rapidjson::Value value(rapidjson::kObjectType);
    using K = typename T::key_type;
    for (const auto& kv : map) {
        rapidjson::Value key;
        if constexpr (std::is_same_v<K, std::string>) {
            key = SerializeValue(kv.first, allocator);
        } else if constexpr (std::is_same_v<T, bool>) {
            throw std::invalid_argument("map key cannot be bool");
        } else {
            std::string key_str = std::to_string(kv.first);
            key = SerializeValue(key_str, allocator);
        }
        rapidjson::Value val = SerializeValue(kv.second, allocator);
        value.AddMember(key, val, *allocator);
    }
    return value;
}

template <typename T>
inline rapidjson::Value RapidJsonUtil::SerializeVector(
    const T& vec, rapidjson::Document::AllocatorType* allocator) {
    rapidjson::Value value(rapidjson::kArrayType);
    using V = typename T::value_type;
    for (const V& item : vec) {
        value.PushBack(SerializeValue(item, allocator), *allocator);
    }
    return value;
}

template <typename T>
inline T RapidJsonUtil::DeserializeKeyValue(const rapidjson::Value& value, const std::string& key) {
    if (!value.IsObject()) {
        throw std::invalid_argument("value must be an object");
    }
    if constexpr (is_optional<T>::value) {
        if (!value.HasMember(key.c_str()) || value[key].IsNull()) {
            return std::nullopt;
        } else {
            return DeserializeValue<typename T::value_type>(value[key]);
        }
    } else {
        if (!value.HasMember(key.c_str()) || value[key].IsNull()) {
            throw std::invalid_argument("key must exist");
        }
        return DeserializeValue<T>(value[key]);
    }
}

template <typename T>
inline T RapidJsonUtil::DeserializeKeyValue(const rapidjson::Value& value, const std::string& key,
                                            const T& default_value) {
    if (!value.IsObject()) {
        throw std::invalid_argument("value must be an object");
    }
    if (!value.HasMember(key.c_str()) || value[key].IsNull()) {
        return default_value;
    }
    if constexpr (is_optional<T>::value) {
        return DeserializeValue<typename T::value_type>(value[key]);
    } else {
        return DeserializeValue<T>(value[key]);
    }
}

template <typename T>
inline T RapidJsonUtil::DeserializeValue(const rapidjson::Value& value) {
    if constexpr (is_vector<T>::value) {
        return DeserializeVector<T>(value);
    } else if constexpr (is_map<T>::value) {
        return DeserializeMap<T>(value);
    } else {
        // arithmetic or string or custom type
        return GetValue<T>(value);
    }
}

template <typename T>
inline T RapidJsonUtil::DeserializeVector(const rapidjson::Value& value) {
    if (!value.IsArray()) {
        throw std::invalid_argument("value must be an array");
    }
    T obj;
    obj.reserve(value.Size());
    using V = typename T::value_type;
    for (const auto& item : value.GetArray()) {
        obj.push_back(DeserializeValue<V>(item));
    }
    return obj;
}

template <typename T>
inline T RapidJsonUtil::DeserializeMap(const rapidjson::Value& value) {
    if (!value.IsObject()) {
        throw std::invalid_argument("value must be an object");
    }
    using K = typename T::key_type;
    using V = typename T::mapped_type;
    T obj;
    for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
        K key;
        if constexpr (std::is_same_v<K, std::string>) {
            key = DeserializeValue<K>(it->name);
        } else {
            auto key_str = DeserializeValue<std::string>(it->name);
            auto optional_key = StringUtils::StringToValue<K>(key_str);
            if (!optional_key) {
                throw std::invalid_argument("key cannot be parse from string");
            }
            key = optional_key.value();
        }
        obj.emplace(key, DeserializeValue<V>(it->value));
    }
    return obj;
}

template <>
inline bool RapidJsonUtil::GetValue<bool>(const rapidjson::Value& value) {
    if (!value.IsBool()) {
        throw std::invalid_argument("value must be bool");
    }
    return value.GetBool();
}

template <>
inline int8_t RapidJsonUtil::GetValue<int8_t>(const rapidjson::Value& value) {
    if (!value.IsInt()) {
        throw std::invalid_argument("value must be int");
    }
    return static_cast<int8_t>(value.GetInt());
}

template <>
inline uint8_t RapidJsonUtil::GetValue<uint8_t>(const rapidjson::Value& value) {
    if (!value.IsUint()) {
        throw std::invalid_argument("value must be uint");
    }
    return static_cast<uint8_t>(value.GetUint());
}

template <>
inline int16_t RapidJsonUtil::GetValue<int16_t>(const rapidjson::Value& value) {
    if (!value.IsInt()) {
        throw std::invalid_argument("value must be int");
    }
    return static_cast<int16_t>(value.GetInt());
}

template <>
inline uint16_t RapidJsonUtil::GetValue<uint16_t>(const rapidjson::Value& value) {
    if (!value.IsUint()) {
        throw std::invalid_argument("value must be uint");
    }
    return static_cast<uint16_t>(value.GetUint());
}

template <>
inline int32_t RapidJsonUtil::GetValue<int32_t>(const rapidjson::Value& value) {
    if (!value.IsInt()) {
        throw std::invalid_argument("value must be int");
    }
    return value.GetInt();
}

template <>
inline uint32_t RapidJsonUtil::GetValue<uint32_t>(const rapidjson::Value& value) {
    if (!value.IsUint()) {
        throw std::invalid_argument("value must be uint");
    }
    return value.GetUint();
}

template <>
inline int64_t RapidJsonUtil::GetValue<int64_t>(const rapidjson::Value& value) {
    if (!value.IsInt64()) {
        throw std::invalid_argument("value must be int64");
    }
    return value.GetInt64();
}

template <>
inline uint64_t RapidJsonUtil::GetValue<uint64_t>(const rapidjson::Value& value) {
    if (!value.IsUint64()) {
        throw std::invalid_argument("value must be uint64");
    }
    return value.GetUint64();
}

template <>
inline double RapidJsonUtil::GetValue<double>(const rapidjson::Value& value) {
    if (!value.IsDouble()) {
        throw std::invalid_argument("value must be double");
    }
    return value.GetDouble();
}

template <>
inline float RapidJsonUtil::GetValue<float>(const rapidjson::Value& value) {
    if (!value.IsDouble()) {
        throw std::invalid_argument("value must be double");
    }
    return static_cast<float>(value.GetDouble());
}

template <>
inline std::string RapidJsonUtil::GetValue<std::string>(const rapidjson::Value& value) {
    if (!value.IsString()) {
        throw std::invalid_argument("value must be string");
    }
    return std::string(value.GetString(), value.GetStringLength());
}

template <typename T>
inline T RapidJsonUtil::GetValue(const rapidjson::Value& value) {
    // custom type
    T obj;
    obj.FromJson(value);
    return obj;
}

}  // namespace paimon
