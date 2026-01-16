/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#ifndef PROPERTIES_H_
#define PROPERTIES_H_

#include "JSONHelpers.h"

#include <iostream>
#include <string>
#include <set>
#include <map>

namespace ibd_ninja {

class Properties {
 public:
  Properties() = default;
  explicit Properties(const std::set<std::string>& keys) : keys_(keys) {
  }
  bool InsertValues(const std::string& opt_string);
  void DebugDump(int space = 0) {
    std::string space_str(space, ' ');

    std::cout << space_str << "[" << std::endl;
    std::cout << space_str << "Dump Properties:" << std::endl;

    for (const auto& iter : kvs_) {
      std::cout << space_str << "  " << iter.first << ": "
                << iter.second << std::endl;
    }

    std::cout << space_str << "]" << std::endl;
  }

  template <typename T>
  bool Get(const std::string& key, T* value) const {
    std::string value_str;
    if (!ValidKey(key)) {
      assert(false);
      return false;
    }
    const auto& iter = kvs_.find(key);
    if (iter == kvs_.end()) {
      return false;
    } else {
      value_str = iter->second;
    }
    GetValue(value_str, value);
    return true;
  }
  bool Exists(const std::string& key) const;

 private:
  bool ValidKey(const std::string& key) const;
  std::set<std::string> keys_;
  std::map<std::string, std::string> kvs_;
};

}  // namespace ibd_ninja

#endif  // PROPERTIES_H_
