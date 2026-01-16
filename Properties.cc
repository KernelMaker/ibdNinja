/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */

#include "Properties.h"

#include <iostream>
#include <cassert>

namespace ibd_ninja {

bool Properties::ValidKey(const std::string& key) const {
  bool ret = (keys_.empty() || keys_.find(key) != keys_.end());
  return ret;
}

bool Properties::Exists(const std::string& key) const {
  if (!ValidKey(key)) {
    return false;
  }
  const auto& iter = kvs_.find(key);
  if (iter != kvs_.end()) {
    return true;
  } else {
    return false;
  }
}

bool Properties::InsertValues(const std::string& opt_string) {
  assert(kvs_.empty());
  bool found_key = false;
  std::string key = "";
  bool found_value = false;
  std::string value = "";
  size_t last_pos = 0;
  for (size_t pos = 0; pos < opt_string.length(); pos++) {
    if (!found_key && opt_string[pos] != '=') {
      continue;
    } else if (opt_string[pos] == '=') {
      found_key = true;
      if (pos - last_pos > 1) {
        if (last_pos == 0) {
          key = std::string(&opt_string[last_pos], pos - last_pos);
        } else {
          key = std::string(&opt_string[last_pos + 1], pos - last_pos - 1);
        }
      }
      last_pos = pos;
      continue;
    }

    if (!found_value && opt_string[pos] != ';') {
      continue;
    } else if (opt_string[pos] == ';') {
      found_value = true;
      if (pos - last_pos > 1) {
        if (last_pos == 0) {
          value = std::string(&opt_string[last_pos], pos - last_pos);
        } else {
          value = std::string(&opt_string[last_pos + 1], pos - last_pos - 1);
        }
      }
      last_pos = pos;
    }

    assert(found_key && found_value);
    if (key.empty()) {
      std::cerr << "[SDI]Found empty Properties::key" << std::endl;
      return false;
    }

    if (ValidKey(key)) {
      kvs_[key] = value;
      key.clear();
      found_key = false;
      value.clear();
      found_value = false;
    } else {
      std::cerr << "[SDI]Found invalid Properties::key, "
                << key << std::endl;
      return false;
    }
  }
  return true;
}

}  // namespace ibd_ninja
