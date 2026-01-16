/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */

#include "Table.h"
#include "JSONHelpers.h"

#include <cassert>
#include <iostream>

namespace ibd_ninja {

#define ninja_error(format, ...) \
      fprintf(stderr, "[ERROR] %s:%d - " format "\n", \
              __FILE__, __LINE__, ##__VA_ARGS__)

#define FTS_DOC_ID_COL_NAME "FTS_DOC_ID"

/* ------ Table ------ */
const std::set<std::string> Table::default_valid_option_keys = {
  "avg_row_length",
  "checksum",
  "compress",
  "connection_string",
  "delay_key_write",
  "encrypt_type",
  "explicit_tablespace",
  "key_block_size",
  "keys_disabled",
  "max_rows",
  "min_rows",
  "pack_keys",
  "pack_record",
  "plugin_version",
  "row_type",
  "secondary_engine",
  "secondary_load",
  "server_i_s_table",
  "server_p_s_table",
  "stats_auto_recalc",
  "stats_persistent",
  "stats_sample_pages",
  "storage",
  "tablespace",
  "timestamp",
  "view_valid",
  "gipk"
};

bool Table::Init(const rapidjson::Value& dd_obj) {
  // Table
  Read(&dd_name_, dd_obj, "name");
  Read(&dd_mysql_version_id_, dd_obj, "mysql_version_id");
  Read(&dd_created_, dd_obj, "created");
  Read(&dd_last_altered_, dd_obj, "last_altered");
  ReadEnum(&dd_hidden_, dd_obj, "hidden");
  ReadProperties(&dd_options_, dd_obj, "options");

  // Columns
  if (!dd_obj.HasMember("columns") || !dd_obj["columns"].IsArray()) {
    std::cerr << "[SDI]Can't find columns" << std::endl;
    return false;
  }
  const rapidjson::Value& columns = dd_obj["columns"].GetArray();
  for (rapidjson::SizeType i = 0; i < columns.Size(); i++) {
    if (!columns[i].IsObject()) {
      std::cerr << "[SDI]Column isn't an object" << std::endl;
      return false;
    }
    Column* column = Column::CreateColumn(columns[i]);
    if (column == nullptr) {
      return false;
    }
    columns_.push_back(column);
  }

  // Table
  Read(&dd_schema_ref_, dd_obj, "schema_ref");
  Read(&dd_se_private_id_, dd_obj, "se_private_id");
  Read(&dd_engine_, dd_obj, "engine");
  Read(&dd_last_checked_for_upgrade_version_id_, dd_obj,
       "last_checked_for_upgrade_version_id");
  Read(&dd_comment_, dd_obj, "comment");
  ReadProperties(&dd_se_private_data_, dd_obj, "se_private_data");
  Read(&dd_engine_attribute_, dd_obj, "engine_attribute");
  Read(&dd_secondary_engine_attribute_, dd_obj, "secondary_engine_attribute");
  ReadEnum(&dd_row_format_, dd_obj, "row_format");
  ReadEnum(&dd_partition_type_, dd_obj, "partition_type");
  Read(&dd_partition_expression_, dd_obj, "partition_expression");
  Read(&dd_partition_expression_utf8_, dd_obj, "partition_expression_utf8");
  ReadEnum(&dd_default_partitioning_, dd_obj, "default_partitioning");
  ReadEnum(&dd_subpartition_type_, dd_obj, "subpartition_type");
  Read(&dd_subpartition_expression_, dd_obj, "subpartition_expression");
  Read(&dd_subpartition_expression_utf8_, dd_obj,
       "subpartition_expression_utf8");
  ReadEnum(&dd_default_subpartitioning_, dd_obj, "default_subpartitioning");
  ReadEnum(&dd_collation_id_, dd_obj, "collation_id");

  // Indexes
  if (!dd_obj.HasMember("indexes") || !dd_obj["indexes"].IsArray()) {
    std::cerr << "Can't find indexes" << std::endl;
    return false;
  }
  const rapidjson::Value& indexes = dd_obj["indexes"].GetArray();
  for (rapidjson::SizeType i = 0; i < indexes.Size(); i++) {
    if (!indexes[i].IsObject()) {
      std::cerr << "Index isn't an object" << std::endl;
      return false;
    }
    Index* index = Index::CreateIndex(indexes[i], columns_, this);
    if (index == nullptr) {
      return false;
    }
    indexes_.push_back(index);
  }

  /* TABLE_SHARE */
  s_field_.clear();
  for (auto* iter : columns_) {
    if (iter->IsSeHidden()) {
      continue;
    }
    if (iter->is_nullable()) {
      s_null_fields_++;
    }
    s_fields_++;
    s_field_.push_back(iter);
  }

  /* SE */
  if (!InitSeTable()) {
    return false;
  }
  return true;
}

Column* Table::FindColumn(const std::string& col_name) {
  for (const auto& iter : columns_) {
    if (iter->name() == col_name) {
      return iter;
    }
  }
  return nullptr;
}

bool Table::ContainFulltext() {
  for (const auto& iter : indexes_) {
    if (iter->type() == Index::enum_index_type::IT_FULLTEXT) {
      return true;
    }
  }
  return false;
}

#define UNSUPP_TABLE_MASK 0x1F
#define UNSUPP_TABLE_MASK_PARTITION 0x1
#define UNSUPP_TABLE_MASK_ENCRYPT 0x2
#define UNSUPP_TABLE_MASK_FTS_AUX_INDEX 0x4
#define UNSUPP_TABLE_MASK_FTS_COM_INDEX 0x8
#define UNSUPP_TABLE_MASK_VERSION 0x10

void Table::PreCheck() {
  // TODO(Zhao): Support more MySQL versions
  if (dd_mysql_version_id_ < 80016 || dd_mysql_version_id_ > 80040) {
    unsupported_reason_ |= UNSUPP_TABLE_MASK_VERSION;
  }

  if (dd_partition_type_ != enum_partition_type::PT_NONE) {
    unsupported_reason_ |= UNSUPP_TABLE_MASK_PARTITION;
  }

  if (dd_options_.Exists("encrypt_type")) {
    std::string encrypted;
    dd_options_.Get("encrypt_type", &encrypted);
    if (encrypted != "" && encrypted != "N" && encrypted != "n") {
      unsupported_reason_ |= UNSUPP_TABLE_MASK_ENCRYPT;
    }
  }

  for (const auto& index : indexes_) {
    if (index->name() == "FTS_INDEX_TABLE_IND" &&
        dd_hidden_ == enum_hidden_type::HT_HIDDEN_SE) {
      unsupported_reason_ |= UNSUPP_TABLE_MASK_FTS_AUX_INDEX;
      continue;
    }
    if (index->name() == "FTS_COMMON_TABLE_IND" &&
        dd_hidden_ == enum_hidden_type::HT_HIDDEN_SE) {
      unsupported_reason_ |= UNSUPP_TABLE_MASK_FTS_COM_INDEX;
      continue;
    }
  }
}

bool Table::IsTableSupported() {
  return (unsupported_reason_ & UNSUPP_TABLE_MASK) == 0;
}

std::string Table::UnsupportedReason() {
  assert(!IsTableSupported());
  std::string reason = "";
  if (unsupported_reason_ & UNSUPP_TABLE_MASK_PARTITION) {
    reason += "[Partition table]";
  }
  if (unsupported_reason_ & UNSUPP_TABLE_MASK_ENCRYPT) {
    reason += "[Encrpted table]";
  }
  if (unsupported_reason_ & UNSUPP_TABLE_MASK_FTS_AUX_INDEX) {
    reason += "[FTS Auxiliary index table]";
  }
  if (unsupported_reason_ & UNSUPP_TABLE_MASK_FTS_COM_INDEX) {
    reason += "[FTS Common index table]";
  }
  if (unsupported_reason_ & UNSUPP_TABLE_MASK_VERSION) {
    reason += ("[Table was created in unsupported version " +
                std::to_string(dd_mysql_version_id_) +
                ", expected in [80016, 80040] ]");
  }
  return reason;
}

bool Table::InitSeTable() {
  PreCheck();
  if (!IsTableSupported()) {
    return true;
  }
  std::string norm_name = dd_schema_ref_ + "/" + dd_name_;

  if (dd_schema_ref_ == "mysql" && dd_schema_ref_ == "information_schema" &&
      dd_schema_ref_ == "performance_schema") {
    ib_is_system_table_ = true;
  } else {
    ib_is_system_table_ = false;
  }

  [[maybe_unused]] bool is_encrypted = false;
  if (dd_options_.Exists("encrypt_type")) {
    std::string encrypted;
    dd_options_.Get("encrypt_type", &encrypted);
    if (encrypted != "" && encrypted != "N" && encrypted != "n") {
      is_encrypted = true;
    }
  }

  bool has_doc_id = false;
  const Column* col = FindColumn(FTS_DOC_ID_COL_NAME);
  if (col != 0 && col->type() == Column::enum_column_types::LONGLONG &&
      !col->is_nullable()) {
    has_doc_id = true;
  }
  bool add_doc_id = false;
  if (has_doc_id && col->IsSeHidden()) {
    add_doc_id = true;
  }

  // TODO(Zhao): Support partition table

  uint32_t n_cols = s_fields_ + (add_doc_id ? 1 : 0);
  uint32_t n_v_cols = 0;
  uint32_t n_m_v_cols = 0;
  for (const auto& iter : columns_) {
    if (iter->IsSeHidden()) {
      continue;
    }
    if (iter->is_virtual()) {
      n_v_cols++;
      if (iter->options().Exists("is_array")) {
        bool is_array = false;
        iter->options().Get("is_array", &is_array);
        if (is_array) {
          n_m_v_cols++;
        }
      }
    }
  }

  uint32_t current_row_version = 0;
  uint32_t n_current_cols = 0;
  uint32_t n_initial_cols = 0;
  uint32_t n_total_cols = 0;

  uint32_t n_dropped_cols = 0;
  uint32_t n_added_cols = 0;
  uint32_t n_added_and_dropped_cols = 0;
  bool has_row_version = false;
  for (const auto& iter : columns_) {
    if (iter->IsSystemColumn() || iter->is_virtual()) {
      continue;
    }

    if (!has_row_version) {
      if (iter->se_private_data().Exists("physical_pos")) {
        has_row_version = true;
      }
    }

    if (iter->IsColumnDropped()) {
      n_dropped_cols++;
      if (iter->IsColumnAdded()) {
        n_added_and_dropped_cols++;
      }
      uint32_t v_dropped = iter->GetVersionDropped();
      current_row_version = std::max(current_row_version, v_dropped);
      continue;
    }

    if (iter->IsColumnAdded()) {
      n_added_cols++;
      uint32_t v_added = iter->GetVersionAdded();
      current_row_version = std::max(current_row_version, v_added);
    }
    n_current_cols++;
  }
  n_initial_cols = (n_current_cols - n_added_cols) +
                   (n_dropped_cols - n_added_and_dropped_cols);
  n_total_cols = n_current_cols + n_dropped_cols;

  ib_n_t_cols_ = n_cols + DATA_N_SYS_COLS;
  ib_n_v_cols_ = n_v_cols;
  ib_n_m_v_cols_ = n_m_v_cols;
  ib_n_cols_ = ib_n_t_cols_ - ib_n_v_cols_;
  ib_n_instant_cols_ = ib_n_cols_;
  ib_initial_col_count_ = n_initial_cols;
  ib_current_col_count_ = n_current_cols;
  ib_total_col_count_ = n_total_cols;
  ib_current_row_version_ = current_row_version;
  ib_m_upgraded_instant_ = false;

  ib_id_ = dd_se_private_id_;

  if (dd_se_private_data_.Exists("instant_col")) {
    uint32_t n_inst_cols = 0;
    if (dd_partition_type_ != enum_partition_type::PT_NONE) {
      // TODO(Zhao): Support partition table
    } else {
      dd_se_private_data_.Get("instant_col", &n_inst_cols);
      ib_n_instant_cols_ = n_inst_cols + DATA_N_SYS_COLS;
      ib_m_upgraded_instant_ = true;
    }
  }

  ib_cols_.clear();
  ib_n_def_ = 0;
  ib_n_v_def_ = 0;
  ib_n_t_def_ = 0;

  for (auto* iter : s_field_) {
    iter->set_ib_mtype(iter->FieldType2SeType());
    ib_n_t_def_++;
    if (!iter->is_virtual()) {
      iter->set_ib_ind(ib_n_def_);
      ib_n_def_++;
      uint32_t v_added = iter->GetVersionAdded();
      uint32_t phy_pos = UINT32_UNDEFINED;
      bool is_hidden_by_system =
        (iter->hidden() == Column::enum_hidden_type::HT_HIDDEN_SE ||
         iter->hidden() == Column::enum_hidden_type::HT_HIDDEN_SQL);
      if (has_row_version) {
        if (iter->se_private_data().Exists("physical_pos")) {
          iter->se_private_data().Get("physical_pos", &phy_pos);
          assert(phy_pos != UINT32_UNDEFINED);
        }
      }
      iter->set_ib_is_visible(!is_hidden_by_system);
      iter->set_ib_version_added(v_added);
      iter->set_ib_version_dropped(UINT8_UNDEFINED);
      iter->set_ib_phy_pos(phy_pos);
      if (iter->FieldType() == Column::enum_field_types::MYSQL_TYPE_VARCHAR) {
        // The col_len of VARCHAR in InnoDB does not include the length header.
        uint32_t col_len = iter->PackLength() - iter->VarcharLenBytes();
        iter->set_ib_col_len(col_len);
      } else {
        iter->set_ib_col_len(iter->PackLength());
      }

      ib_cols_.push_back(iter);
    } else {
      ib_n_v_def_++;
    }
  }

  if (add_doc_id) {
    Column* doc_id_col = new Column(FTS_DOC_ID_COL_NAME, ib_n_def_, true);
    ib_n_t_def_++;
    ib_n_def_++;
    doc_id_col->set_type(Column::enum_column_types::LONGLONG);
    doc_id_col->set_ib_mtype(DATA_INT);
    doc_id_col->set_ib_col_len(sizeof(uint64_t));
    ib_cols_.push_back(doc_id_col);
  }

  Column* row_id_col = FindColumn("DB_ROW_ID");
  if (row_id_col != nullptr) {
    // DB_ROW_ID is already in the columns_
    ib_n_t_def_++;
    row_id_col->set_ib_ind(ib_n_def_);
    ib_n_def_++;
    row_id_col->set_ib_mtype(DATA_SYS);
    row_id_col->set_ib_is_visible(false);
    row_id_col->set_ib_version_added(0);
    row_id_col->set_ib_version_dropped(0);
    uint32_t phy_pos = UINT32_UNDEFINED;
    if (has_row_version) {
      if (row_id_col->se_private_data().Exists("physical_pos")) {
        row_id_col->se_private_data().Get("physical_pos", &phy_pos);
      }
    }
    row_id_col->set_ib_phy_pos(phy_pos);
    row_id_col->set_ib_col_len(DATA_ROW_ID_LEN);
    ib_cols_.push_back(row_id_col);
  } else {
    row_id_col_ = new Column("DB_ROW_ID", ib_n_def_);
    ib_n_t_def_++;
    ib_n_def_++;
    row_id_col_->set_ib_col_len(DATA_ROW_ID_LEN);
    row_id_col_->set_type(Column::enum_column_types::INT24);
    ib_cols_.push_back(row_id_col_);
  }

  Column* trx_id_col = FindColumn("DB_TRX_ID");
  assert(trx_id_col != nullptr);
  if (trx_id_col != nullptr) {
    ib_n_t_def_++;
    trx_id_col->set_ib_ind(ib_n_def_);
    ib_n_def_++;
    trx_id_col->set_ib_mtype(DATA_SYS);
    trx_id_col->set_ib_is_visible(false);
    trx_id_col->set_ib_version_added(0);
    trx_id_col->set_ib_version_dropped(0);
    uint32_t phy_pos = UINT32_UNDEFINED;
    if (has_row_version) {
      if (trx_id_col->se_private_data().Exists("physical_pos")) {
        trx_id_col->se_private_data().Get("physical_pos", &phy_pos);
      }
    }
    trx_id_col->set_ib_phy_pos(phy_pos);
    trx_id_col->set_ib_col_len(DATA_TRX_ID_LEN);
    ib_cols_.push_back(trx_id_col);
  }

  Column* roll_ptr_col = FindColumn("DB_ROLL_PTR");
  assert(roll_ptr_col != nullptr);
  if (roll_ptr_col != nullptr) {
    ib_n_t_def_++;
    roll_ptr_col->set_ib_ind(ib_n_def_);
    ib_n_def_++;
    roll_ptr_col->set_ib_mtype(DATA_SYS);
    roll_ptr_col->set_ib_is_visible(false);
    roll_ptr_col->set_ib_version_added(0);
    roll_ptr_col->set_ib_version_dropped(0);
    uint32_t phy_pos = UINT32_UNDEFINED;
    if (has_row_version) {
      if (roll_ptr_col->se_private_data().Exists("physical_pos")) {
        roll_ptr_col->se_private_data().Get("physical_pos", &phy_pos);
      }
    }
    roll_ptr_col->set_ib_phy_pos(phy_pos);
    roll_ptr_col->set_ib_col_len(DATA_ROLL_PTR_LEN);
    ib_cols_.push_back(roll_ptr_col);
  }

  if (HasInstantDropCols()) {
    for (auto* iter : columns_) {
      if (iter->IsSystemColumn()) {
        continue;
      }
      if (iter->IsColumnDropped()) {
        iter->set_ib_mtype(iter->FieldType2SeType());
        iter->set_ib_ind(ib_n_def_);
        ib_n_def_++;
        ib_n_t_def_++;
        uint32_t v_added = iter->GetVersionAdded();
        uint32_t v_dropped = iter->GetVersionDropped();
        uint32_t phy_pos = UINT32_UNDEFINED;
        assert(iter->se_private_data().Exists("physical_pos"));
        iter->se_private_data().Get("physical_pos", &phy_pos);
        assert(phy_pos != UINT32_UNDEFINED);
        iter->set_ib_is_visible(false);
        iter->set_ib_version_added(v_added);
        iter->set_ib_version_dropped(v_dropped);
        iter->set_ib_phy_pos(phy_pos);
        if (iter->FieldType() == Column::enum_field_types::MYSQL_TYPE_VARCHAR) {
          // The col_len of VARCHAR in InnoDB does not include the length header.
          uint32_t col_len = iter->PackLength() - iter->VarcharLenBytes();
          iter->set_ib_col_len(col_len);
        } else {
          iter->set_ib_col_len(iter->PackLength());
        }
        ib_cols_.push_back(iter);
      }
    }
  }

  if (HasInstantCols() || HasRowVersions()) {
    // TODO(Zhao): Support partition table
    for (auto* iter : columns_) {
      iter->set_ib_instant_default(false);
      if (iter->is_virtual() || iter->IsSystemColumn()) {
        continue;
      }
      if (iter->IsColumnDropped()) {
        continue;
      }
      if (!iter->se_private_data().Exists("default_null") &&
          !iter->se_private_data().Exists("default")) {
        // This is not INSTANT ADD column
        continue;
      }

      if (iter->se_private_data().Exists("default_null")) {
        iter->set_ib_instant_default(false);
      } else if (iter->se_private_data().Exists("default")) {
        iter->set_ib_instant_default(true);
        // TODO(Zhao): parse the default value
      }
    }
  }

  assert(!indexes_.empty());
  uint32_t ind = 0;
  for (auto* iter : indexes_) {
    iter->FillIndex(ind);
    ind++;
  }

  return true;
}

Table* Table::CreateTable(const rapidjson::Value& dd_obj,
                          unsigned char* sdi_data) {
  Table* table = new Table(sdi_data);
  bool init_ret = table->Init(dd_obj);
  if (!init_ret) {
    delete table;
    table = nullptr;
  }
  return table;
}

bool Table::IsTableParsingRecSupported() {
  if (!IsTableSupported()) {
    return false;
  }
  if (dd_row_format_ != RF_DYNAMIC &&
      dd_row_format_ != RF_COMPACT) {
    ninja_error("Parsing of record with row format %s is not yet supported",
                 RowFormatString().c_str());
    return false;
  }
  return true;
}

}  // namespace ibd_ninja
