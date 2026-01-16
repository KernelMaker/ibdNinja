/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#ifndef INDEX_H_
#define INDEX_H_

#include "Column.h"

#include <rapidjson/document.h>

#include <iostream>
#include <vector>
#include <string>
#include <set>

namespace ibd_ninja {

constexpr uint32_t DICT_CLUSTERED = 1;
constexpr uint32_t DICT_UNIQUE = 2;
constexpr uint32_t DICT_FTS = 32;
constexpr uint32_t DICT_SPATIAL = 64;
const uint8_t MAX_ROW_VERSION = 64;

class Table;
class Index {
 public:
  static Index* CreateIndex(const rapidjson::Value& dd_index_obj,
                            const std::vector<Column*>& columns,
                            Table* table);
  ~Index() {
    if (IsClustered()) {
      for (auto* iter : ib_fields_) {
        if (iter->se_explicit()) {
          delete iter;
        }
      }
    }
    for (auto* iter : dd_elements_) {
      delete iter;
    }
  }
  void DebugDump(int space = 0) {
    std::string space_str(space, ' ');
    std::cout << space_str << "[" << std::endl;
    std::cout << space_str << "Dump Index:" << std::endl;
    std::cout << space_str << "  " << "name: "
              << dd_name_ << std::endl
              << space_str << "  " << "hidden: "
              << dd_hidden_ << std::endl
              << space_str << "  " << "is_generated: "
              << dd_is_generated_ << std::endl
              << space_str << "  " << "ordinal_position: "
              << dd_ordinal_position_ << std::endl
              << space_str << "  " << "comment: "
              << dd_comment_ << std::endl
              << space_str << "  " << "options: " << std::endl;
    dd_options_.DebugDump(space + 4);
    std::cout << space_str << "  " << "se_private_data: " << std::endl;
    dd_se_private_data_.DebugDump(space + 4);

    std::cout << space_str << "  " << "type: "
              << dd_type_ << std::endl
              << space_str << "  " << "algorithm: "
              << dd_algorithm_ << std::endl
              << space_str << "  " << "is_algorithm_explicit: "
              << dd_is_algorithm_explicit_ << std::endl
              << space_str << "  " << "is_visible: "
              << dd_is_visible_ << std::endl
              << space_str << "  " << "engine: "
              << dd_engine_ << std::endl
              << space_str << "  " << "engine_attribute: "
              << dd_engine_attribute_ << std::endl
              << space_str << "  " << "secondary_engine_attribute: "
              << dd_secondary_engine_attribute_ << std::endl
              << space_str << "  " << "tablespace_ref: "
              << dd_tablespace_ref_ << std::endl;

    std:: cout << space_str << "  " << "elements: " << std::endl;
    for (const auto& iter : dd_elements_) {
      iter->DebugDump(space + 4);
    }

    std::cout << space_str << "]" << std::endl;
  }
  enum enum_index_type {
    IT_PRIMARY = 1,
    IT_UNIQUE,
    IT_MULTIPLE,
    IT_FULLTEXT,
    IT_SPATIAL
  };
  enum enum_index_algorithm {
    IA_SE_SPECIFIC = 1,
    IA_BTREE,
    IA_RTREE,
    IA_HASH,
    IA_FULLTEXT
  };

  static const std::set<std::string> default_valid_option_keys;

  const std::string& name() const {
    return dd_name_;
  }
  enum_index_type type() const {
    return dd_type_;
  }
  const Properties& se_private_data() const {
    return dd_se_private_data_;
  }

  /* ------TABLE SHARE------ */
  bool FillIndex(uint32_t ind);
  uint32_t s_user_defined_key_parts() {
    return s_user_defined_key_parts_;
  }
  uint32_t s_key_length() {
    return s_key_length_;
  }
  uint32_t s_flags() {
    return s_flags_;
  }

  /* ------SE------ */
  bool FillSeIndex(uint32_t ind);
  bool IndexAddCol(Column* column, uint32_t prefix_len);
  bool IsClustered() {
    return (ib_type_ & DICT_CLUSTERED);
  }
  uint32_t ib_id() {
    return ib_id_;
  }
  // root page
  uint32_t ib_page() {
    return ib_page_;
  }
  uint32_t ib_n_fields() {
    return ib_n_fields_;
  }
  uint32_t ib_n_uniq() {
    return ib_n_uniq_;
  }
  uint32_t ib_type() {
    return ib_type_;
  }
  uint32_t ib_n_def() {
    return ib_n_def_;
  }
  uint32_t ib_n_nullable() {
    return ib_n_nullable_;
  }
  std::vector<IndexColumn*>* ib_fields() {
    return &ib_fields_;
  }
  bool IsIndexUnique() {
    return (ib_type_ & DICT_UNIQUE);
  }

  std::vector<uint16_t>* ib_fields_array() {
    return &ib_fields_array_;
  }
  uint32_t* ib_nullables() {
    return ib_nullables_;
  }
  bool ib_row_versions() {
    return ib_row_versions_;
  }
  bool ib_instant_cols() {
    return ib_instant_cols_;
  }
  uint32_t ib_n_instant_nullable() {
    return ib_n_instant_nullable_;
  }

  uint32_t GetNFields() const;
  uint32_t ib_n_total_fields() {
    return ib_n_total_fields_;
  }
  Table* table() const {
    return table_;
  }
  uint32_t GetNOriginalFields();
  uint32_t GetNNullableBefore(uint32_t nth);
  uint32_t CalculateNInstantNullable(uint32_t n_fields);
  bool HasInstantColsOrRowVersions();
  uint32_t GetNullableInVersion(uint8_t version);
  uint16_t GetNullableBeforeInstantAddDrop();
  uint16_t GetNUniqueInTree();
  uint16_t GetNUniqueInTreeNonleaf();
  IndexColumn* GetPhysicalField(size_t pos);

  bool IsIndexSupported();
  std::string UnsupportedReason();
  bool IsIndexParsingPageSupported();
  bool IsIndexParsingRecSupported();

 private:
  explicit Index(Table* table) :
            dd_options_(default_valid_option_keys),
            dd_se_private_data_(),
            unsupported_reason_(0),
            ib_type_(0),
            table_(table) {
  }
  bool Init(const rapidjson::Value& dd_index_obj,
            const std::vector<Column*>& columns);
  std::string dd_name_;
  bool dd_hidden_;
  bool dd_is_generated_;
  uint32_t dd_ordinal_position_;
  std::string dd_comment_;
  Properties dd_options_;
  Properties dd_se_private_data_;
  enum_index_type dd_type_;
  enum_index_algorithm dd_algorithm_;
  bool dd_is_algorithm_explicit_;
  bool dd_is_visible_;
  std::string dd_engine_;
  std::string dd_engine_attribute_;
  std::string dd_secondary_engine_attribute_;
  std::vector<IndexColumn*> dd_elements_;
  std::string dd_tablespace_ref_;

  /* ------TABLE SHARE------ */
  uint32_t s_user_defined_key_parts_;
  uint32_t s_key_length_;
  uint32_t s_flags_;

  /* ------SE------ */
  void PreCheck();
  uint32_t unsupported_reason_;
  uint32_t ib_id_;
  uint32_t ib_page_;  // root page
  uint32_t ib_n_fields_;
  uint32_t ib_n_uniq_;
  uint32_t ib_type_;
  uint32_t ib_n_def_;
  uint32_t ib_n_nullable_;
  uint32_t ib_n_user_defined_cols_;
  std::vector<IndexColumn*> ib_fields_;
  std::vector<uint16_t> ib_fields_array_;
  uint32_t ib_nullables_[MAX_ROW_VERSION + 1] = {0};
  bool ib_row_versions_;
  bool ib_instant_cols_;
  uint32_t ib_n_instant_nullable_;
  uint32_t ib_n_total_fields_;
  Table* table_;
};

}  // namespace ibd_ninja

#endif  // INDEX_H_
