/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#ifndef TABLE_H_
#define TABLE_H_

#include "Column.h"
#include "Index.h"

#include <rapidjson/document.h>

#include <iostream>
#include <vector>
#include <string>
#include <set>

namespace ibd_ninja {

class Table {
 public:
  static Table* CreateTable(const rapidjson::Value& dd_obj,
                            unsigned char* sdi_data);
  ~Table() {
    for (auto iter : indexes_) {
      delete iter;
    }
    for (auto iter : ib_cols_) {
      if (iter->se_explicit()) {
        delete iter;
      }
    }
    for (auto iter : columns_) {
      delete iter;
    }
    delete[] sdi_data_;
  }
  void DebugDump() {
    std::cout << "Dump Table:" << std::endl
              << "  name: " << dd_name_ << std::endl
              << "  mysql_version_id: " << dd_mysql_version_id_ << std::endl
              << "  created: " << dd_created_ << std::endl
              << "  last_altered: " << dd_last_altered_ << std::endl
              << "  hidden: " << dd_hidden_ << std::endl
              << "  options: " << std::endl;
    dd_options_.DebugDump(4);
    std::cout << "  schema_ref: "
              << dd_schema_ref_ << std::endl
              << "  se_private_id: "
              << dd_se_private_id_ << std::endl
              << "  engine: "
              << dd_engine_ << std::endl
              << "  comment: "
              << dd_comment_ << std::endl
              << "  last_checked_for_upgrade_version_id: "
              << dd_last_checked_for_upgrade_version_id_ << std::endl
              << "  se_private_data: " << std::endl;
    dd_se_private_data_.DebugDump(4);

    std::cout << "  engine_attribute: "
              << dd_engine_attribute_ << std::endl
              << "  secondary_engine_attribute: "
              << dd_secondary_engine_attribute_ << std::endl
              << "  row_format: "
              << dd_row_format_ << std::endl
              << "  partition_type: "
              << dd_partition_type_ << std::endl
              << "  partition_expression: "
              << dd_partition_expression_ << std::endl
              << "  partition_expression_utf8: "
              << dd_partition_expression_utf8_ << std::endl
              << "  default_partitioning: "
              << dd_default_partitioning_ << std::endl
              << "  subpartition_type: "
              << dd_subpartition_type_ << std::endl
              << "  subpartition_expression: "
              << dd_subpartition_expression_ << std::endl
              << "  subpartition_expression_utf8: "
              << dd_subpartition_expression_utf8_ << std::endl
              << "  default_subpartitioning: "
              << dd_default_subpartitioning_ << std::endl
              << "  collation_id: "
              << dd_collation_id_ << std::endl;

    std::cout << "  columns: " << std::endl;
    for (const auto& iter : columns_) {
      iter->DebugDump(4);
    }
    std::cout << "  indexes: " << std::endl;
    for (const auto& iter : indexes_) {
      iter->DebugDump(4);
    }
    std::cout << "--------INTERNAL TABLE--------" << std::endl;
    std::cout << "------TABLE_SHARE------" << std::endl;
    std::cout << "fields: " << s_fields_ << std::endl
              << "null_fields: " << s_null_fields_ << std::endl;
    std::cout << "------SE------" << std::endl;
    std::cout << "id: "
              << ib_id_ << std::endl
              << "n_cols: "
              << ib_n_cols_ << std::endl
              << "n_v_cols: "
              << ib_n_v_cols_ << std::endl
              << "n_m_v_cols: "
              << ib_n_m_v_cols_ << std::endl
              << "n_t_cols: "
              << ib_n_t_cols_ << std::endl
              << "n_instant_cols: "
              << ib_n_instant_cols_ << std::endl
              << "m_upgraded_instant: "
              << ib_m_upgraded_instant_ << std::endl
              << "initial_col_count: "
              << ib_initial_col_count_ << std::endl
              << "current_col_count: "
              << ib_current_col_count_ << std::endl
              << "total_col_count: "
              << ib_total_col_count_ << std::endl
              << "current_row_version: "
              << ib_current_row_version_ << std::endl
              << "n_def: "
              << ib_n_def_ << std::endl
              << "n_v_def: "
              << ib_n_v_def_ << std::endl
              << "cols:" << std::endl;
    for (auto* iter : ib_cols_) {
      std::cout << "  " << "------" << std::endl
                << "  " << "name: "
                << iter->name() << std::endl;
      std::cout << "  " << "------SHARE_TABLE------" << std::endl
                << "  " << "Field::type(): "
                << iter->FieldType() << std::endl
                << "  " << "Field::binary(): "
                << iter->IsBinary() << std::endl;
      std::cout << "  " << "------SE------" << std::endl
                << "  " << "ind: "
                << iter->ib_ind() << std::endl
                << "  " << "mtype: "
                << iter->ib_mtype() << std::endl
                << "  " << "is_visible: "
                << iter->ib_is_visible() << std::endl
                << "  " << "version_added: "
                << iter->ib_version_added() << std::endl
                << "  " << "version_dropped: "
                << iter->ib_version_dropped() << std::endl
                << "  " << "phy_pos: "
                << iter->ib_phy_pos() << std::endl
                << "  " << "col_len: "
                << iter->ib_col_len() << std::endl
                << "  " << "instant_default: "
                << iter->ib_instant_default() << std::endl;
    }
    std::cout << "indexes:" << std::endl;
    for (auto* iter : indexes_) {
      std::cout << "  " << "------" << std::endl;
      std::cout << "  " << "name: "
                << iter->name() << std::endl
                << "  " << "------TABLE SHARE------" << std::endl
                << "  " << "user_defined_key_parts: "
                << iter->s_user_defined_key_parts() << std::endl
                << "  " << "key_length: "
                << iter->s_key_length() << std::endl
                << "  " << "flags: "
                << iter->s_flags() << std::endl;
      std::cout << "  " << "------SE------" << std::endl
                << "  " << "id: "
                << iter->ib_id() << std::endl
                << "  " << "page: "
                << iter->ib_page() << std::endl
                << "  " << "n_fields: "
                << iter->ib_n_fields() << std::endl
                << "  " << "n_uniq: "
                << iter->ib_n_uniq() << std::endl
                << "  " << "type: "
                << iter->ib_type() << std::endl
                << "  " << "n_def: "
                << iter->ib_n_def() << std::endl
                << "  " << "n_nullable: "
                << iter->ib_n_nullable() << std::endl
                << "  " << "row_versions: "
                << iter->ib_row_versions() << std::endl
                << "  " << "instant_cols: "
                << iter->ib_instant_cols() << std::endl
                << "  " << "n_instant_nullable: "
                << iter->ib_n_instant_nullable() << std::endl
                << "  " << "n_total_fields: "
                << iter->ib_n_total_fields() << std::endl
                << "  " << "ib_fields_array: " << std::endl
                << "    ";
      if (HasRowVersions() && iter->IsClustered()) {
        for (uint32_t i = 0; i < iter->ib_n_def(); i++) {
          std::cout << (*(iter->ib_fields_array()))[i] << " ";
        }
        std::cout << std::endl;
      } else {
        std::cout << "NULL" << std::endl;
      }
      std::cout << "  " << "ib_nullables: " << std::endl
                << "    ";
      if (HasRowVersions() && iter->IsClustered()) {
        for (uint32_t i = 0; i < ib_current_row_version_; i++) {
          std::cout << (iter->ib_nullables())[i] << " ";
        }
        std::cout << std::endl;
      } else {
        std::cout << "NULL" << std::endl;
      }
      std::cout << "  " << "fields: " << std::endl;
      for (auto* field : *(iter->ib_fields())) {
        std::cout << "    " << "------" << std::endl
                  << "    " << "name: "
                  << field->column()->name() << std::endl
                  << "    " << "fixed_len: "
                  << field->ib_fixed_len() << std::endl
                  << "    " << "phy_pos: "
                  << field->column()->ib_phy_pos() << std::endl;
      }
    }
  }
  enum enum_hidden_type {
    HT_VISIBLE = 1,
    HT_HIDDEN_SYSTEM,
    HT_HIDDEN_SE,
    HT_HIDDEN_DDL
  };

  enum enum_row_format {
    RF_FIXED = 1,
    RF_DYNAMIC,
    RF_COMPRESSED,
    RF_REDUNDANT,
    RF_COMPACT,
    RF_PAGED
  };
  std::string RowFormatString() {
    switch (dd_row_format_) {
      case RF_FIXED:
        return "FIXED";
      case RF_DYNAMIC:
        return "DYNAMIC";
      case RF_COMPRESSED:
        return "COMPRESSED";
      case RF_REDUNDANT:
        return "REDUNDANT";
      case RF_COMPACT:
        return "COMPACT";
      case RF_PAGED:
        return "PAGED";
      default:
        return "UNKNOWN";
    }
  }

  enum enum_partition_type {
    PT_NONE = 0,
    PT_HASH,
    PT_KEY_51,
    PT_KEY_55,
    PT_LINEAR_HASH,
    PT_LINEAR_KEY_51,
    PT_LINEAR_KEY_55,
    PT_RANGE,
    PT_LIST,
    PT_RANGE_COLUMNS,
    PT_LIST_COLUMNS,
    PT_AUTO,
    PT_AUTO_LINEAR,
  };

  enum enum_subpartition_type {
    ST_NONE = 0,
    ST_HASH,
    ST_KEY_51,
    ST_KEY_55,
    ST_LINEAR_HASH,
    ST_LINEAR_KEY_51,
    ST_LINEAR_KEY_55
  };

  enum enum_default_partitioning {
    DP_NONE = 0,
    DP_NO,
    DP_YES,
    DP_NUMBER
  };

  static const std::set<std::string> default_valid_option_keys;

  const std::string& name() const {
    return dd_name_;
  }
  enum_hidden_type hidden() {
    return dd_hidden_;
  }
  std::string& schema_ref() {
    return dd_schema_ref_;
  }
  uint64_t se_private_id() {
    return dd_se_private_id_;
  }
  enum_row_format row_format() {
    return dd_row_format_;
  }
  enum_partition_type partition_type() {
    return dd_partition_type_;
  }
  uint32_t ib_id() {
    return ib_id_;
  }
  uint32_t ib_n_cols() {
    return ib_n_cols_;
  }
  std::vector<Column*>* ib_cols() {
    return &ib_cols_;
  }
  bool ib_is_system_table() {
    return ib_is_system_table_;
  }
  uint32_t ib_current_row_version() {
    return ib_current_row_version_;
  }
  bool ib_m_upgraded_instant() {
    return ib_m_upgraded_instant_;
  }
  std::vector<Index*>& indexes() {
    return indexes_;
  }
  Index* clust_index() {
    return clust_index_;
  }
  void set_clust_index(Index* index) {
    clust_index_ = index;
  }
  bool HasRowVersions() {
    return (ib_current_row_version_ > 0);
  }
  uint32_t GetTotalCols() {
    if (!HasRowVersions()) {
      return ib_n_cols_;
    }
    assert(ib_total_col_count_ + DATA_N_SYS_COLS ==
           ib_n_cols_ + GetNInstantDropCols());
    return (ib_n_cols_ + GetNInstantDropCols());
  }
  uint32_t GetNInstantAddCols() {
    return ib_total_col_count_ - ib_initial_col_count_;
  }
  bool HasInstantAddCols() {
    return GetNInstantAddCols() > 0;
  }
  uint32_t GetNInstantDropCols() {
    return ib_total_col_count_ - ib_current_col_count_;
  }
  bool HasInstantDropCols() {
    return GetNInstantDropCols() > 0;
  }
  uint32_t GetNInstantAddedColV1() {
    uint32_t n_cols_dropped = GetNInstantDropCols();
    uint32_t n_cols_added = GetNInstantAddCols();
    uint32_t n_instant_added_cols =
      ib_n_cols_ + n_cols_dropped - n_cols_added - ib_n_instant_cols_;
    return n_instant_added_cols;
  }
  bool IsCompact() {
    return (dd_row_format_ != enum_row_format::RF_REDUNDANT);
  }
  bool HasInstantCols() {
    if (ib_m_upgraded_instant_ || ib_n_instant_cols_ < ib_n_cols_) {
      return true;
    } else {
      return false;
    }
  }

  bool IsTableSupported();
  std::string UnsupportedReason();
  bool IsTableParsingRecSupported();

 private:
  explicit Table(unsigned char* sdi_data) : sdi_data_(sdi_data),
            dd_options_(default_valid_option_keys),
            dd_se_private_data_(),
            s_fields_(0), s_null_fields_(0),
            unsupported_reason_(0),
            clust_index_(nullptr) {
    columns_.clear();
    indexes_.clear();
  }
  bool Init(const rapidjson::Value& dd_obj);
  unsigned char* sdi_data_;
  /* DD */
  std::string dd_name_;
  uint32_t dd_mysql_version_id_;
  uint64_t dd_created_;
  uint64_t dd_last_altered_;
  enum_hidden_type dd_hidden_;
  Properties dd_options_;
  std::vector<Column*> columns_;
  std::string dd_schema_ref_;
  uint64_t dd_se_private_id_;
  std::string dd_engine_;
  std::string dd_comment_;
  uint32_t dd_last_checked_for_upgrade_version_id_;
  Properties dd_se_private_data_;
  std::string dd_engine_attribute_;
  std::string dd_secondary_engine_attribute_;
  enum_row_format dd_row_format_;
  enum_partition_type dd_partition_type_;
  std::string dd_partition_expression_;
  std::string dd_partition_expression_utf8_;
  enum_default_partitioning dd_default_partitioning_;
  enum_subpartition_type dd_subpartition_type_;
  std::string dd_subpartition_expression_;
  std::string dd_subpartition_expression_utf8_;
  enum_default_partitioning dd_default_subpartitioning_;
  std::vector<Index*> indexes_;
  // dd_foreign_keys
  // dd_check_constraints
  // dd_partitions
  uint64_t dd_collation_id_;

  /* TABLE_SHARE */
  uint32_t s_fields_;
  uint32_t s_null_fields_;
  std::vector<Column*> s_field_;

  /* SE */
  Column* FindColumn(const std::string& col_name);
  void PreCheck();
  bool InitSeTable();
  bool ContainFulltext();
  uint32_t unsupported_reason_;
  uint32_t ib_id_;
  uint32_t ib_n_cols_;
  uint32_t ib_n_v_cols_;
  uint32_t ib_n_m_v_cols_;
  uint32_t ib_n_t_cols_;
  uint32_t ib_n_instant_cols_;
  bool ib_m_upgraded_instant_;
  uint32_t ib_initial_col_count_;
  uint32_t ib_current_col_count_;
  uint32_t ib_total_col_count_;
  uint32_t ib_current_row_version_;

  uint32_t ib_n_def_;
  uint32_t ib_n_v_def_;
  uint32_t ib_n_t_def_;
  std::vector<Column*> ib_cols_;
  Column* row_id_col_;

  bool ib_is_system_table_;
  Index* clust_index_;
};

}  // namespace ibd_ninja

#endif  // TABLE_H_
