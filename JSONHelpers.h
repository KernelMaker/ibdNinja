/*
 * Copyright (c) [2025-2026] [Zhao Song]
 *
 * Internal header for JSON parsing helper functions.
 * Used by Column.cc, Index.cc, and Table.cc for SDI parsing.
 */
#ifndef JSONHELPERS_H_
#define JSONHELPERS_H_

#include <rapidjson/document.h>
#include <string>
#include <algorithm>
#include <cstdint>

namespace ibd_ninja {

// Template functions for reading values from RapidJSON
template <typename GV>
bool ReadValue(bool* ap, const GV& gv) {
  if (!gv.IsBool()) {
    return false;
  }
  *ap = gv.GetBool();
  return true;
}

template <typename GV>
bool ReadValue(int32_t* ap, const GV& gv) {
  if (!gv.IsInt()) {
    return false;
  }
  *ap = gv.GetInt();
  return true;
}

template <typename GV>
bool ReadValue(uint32_t* ap, const GV& gv) {
  if (!gv.IsUint()) {
    return false;
  }
  *ap = gv.GetUint();
  return true;
}

template <typename GV>
bool ReadValue(int64_t* ap, const GV& gv) {
  if (!gv.IsInt64()) {
    return false;
  }
  *ap = gv.GetInt64();
  return true;
}

template <typename GV>
bool ReadValue(uint64_t* ap, const GV& gv) {
  if (!gv.IsUint64()) {
    return false;
  }
  *ap = gv.GetUint64();
  return true;
}

template <typename GV>
bool ReadValue(std::string* ap, const GV& gv) {
  if (!gv.IsString()) {
    return false;
  }
  *ap = gv.GetString();
  return true;
}

template <typename T, typename GV>
bool Read(T* ap, const GV& gv, const char* key) {
  if (!gv.HasMember(key)) {
    return false;
  }
  return ReadValue(ap, gv[key]);
}

template <typename ENUM_T, typename GV>
bool ReadEnum(ENUM_T* ap, const GV& gv, const char* key) {
  uint64_t v = 0;
  if (!Read(&v, gv, key)) {
    return false;
  }
  *ap = static_cast<ENUM_T>(v);
  return true;
}

// GetValue functions for Properties parsing
inline bool GetValue(const std::string& value_str, std::string* value) {
  *value = value_str;
  return true;
}

inline bool GetValue(const std::string& value_str, bool* value) {
  if (value_str == "true") {
    *value = true;
    return true;
  }
  if (value_str == "false" || value_str == "0") {
    *value = false;
    return true;
  }
  size_t pos = 0;
  if (value_str[pos] == '+' || value_str[pos] == '-') {
    pos += 1;
  }
  bool is_digit = std::all_of(value_str.begin() + pos, value_str.end(),
                              ::isdigit);
  if (is_digit) {
    *value = true;
  } else {
    return false;
  }
  return true;
}

inline bool GetValue(const std::string& value_str, int32_t* value) {
  *value = strtol(value_str.c_str(), nullptr, 10);
  return true;
}

inline bool GetValue(const std::string& value_str, uint32_t* value) {
  *value = strtoul(value_str.c_str(), nullptr, 10);
  return true;
}

inline bool GetValue(const std::string& value_str, int64_t* value) {
  *value = strtoll(value_str.c_str(), nullptr, 10);
  return true;
}

inline bool GetValue(const std::string& value_str, uint64_t* value) {
  *value = strtoull(value_str.c_str(), nullptr, 10);
  return true;
}

template <typename ENUM_T>
bool GetValue(const std::string& value_str, ENUM_T* value) {
  uint64_t v = 0;
  GetValue(value_str, &v);
  *value = static_cast<ENUM_T>(v);
  return true;
}

template <typename PP, typename GV>
bool ReadProperties(PP* pp, const GV& gv, const char* key) {
  std::string opt_string;
  if (Read(&opt_string, gv, key)) {
    return pp->InsertValues(opt_string);
  } else {
    return false;
  }
}

}  // namespace ibd_ninja

#endif  // JSONHELPERS_H_
