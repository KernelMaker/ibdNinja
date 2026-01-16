/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */

#include "Index.h"
#include "Table.h"
#include "JSONHelpers.h"

#include <cassert>
#include <cstring>
#include <iostream>

namespace ibd_ninja {

/* ------ Index ------ */
const std::set<std::string> Index::default_valid_option_keys = {
  "block_size",
  "flags",
  "parser_name",
  "gipk" /* generated implicit primary key */
};

bool Index::Init(const rapidjson::Value& dd_index_obj,
                 const std::vector<Column*>& columns) {
  Read(&dd_name_, dd_index_obj, "name");
  Read(&dd_hidden_, dd_index_obj, "hidden");
  Read(&dd_is_generated_, dd_index_obj, "is_generated");
  Read(&dd_ordinal_position_, dd_index_obj, "ordinal_position");
  Read(&dd_comment_, dd_index_obj, "comment");
  ReadProperties(&dd_options_, dd_index_obj, "options");
  ReadProperties(&dd_se_private_data_, dd_index_obj, "se_private_data");
  ReadEnum(&dd_type_, dd_index_obj, "type");
  ReadEnum(&dd_algorithm_, dd_index_obj, "algorithm");
  Read(&dd_is_algorithm_explicit_, dd_index_obj, "is_algorithm_explicit");
  Read(&dd_is_visible_, dd_index_obj, "is_visible");
  Read(&dd_engine_, dd_index_obj, "engine");
  Read(&dd_engine_attribute_, dd_index_obj, "engine_attribute");
  Read(&dd_secondary_engine_attribute_, dd_index_obj,
      "secondary_engine_attribute");
  if (!dd_index_obj.HasMember("elements") ||
      !dd_index_obj["elements"].IsArray()) {
    std::cerr << "[SDI]Can't find index elements" << std::endl;
    return false;
  }
  const rapidjson::Value& elements = dd_index_obj["elements"].GetArray();
  for (rapidjson::SizeType i = 0; i < elements.Size(); i++) {
    if (!elements[i].IsObject()) {
      std::cerr << "[SDI]Index element isn't an object" << std::endl;
      return false;
    }
    IndexColumn* element = IndexColumn::CreateIndexColumn(elements[i], columns);
    if (element == nullptr) {
      return false;
    }
    dd_elements_.push_back(element);
  }
  Read(&dd_tablespace_ref_, dd_index_obj, "tablespace_ref");
  return true;
}

Index* Index::CreateIndex(const rapidjson::Value& dd_index_obj,
                          const std::vector<Column*>& columns,
                          Table* table) {
  Index* index = new Index(table);
  bool init_ret = index->Init(dd_index_obj, columns);
  if (!init_ret) {
    delete index;
    index = nullptr;
  }
  return index;
}

#define HA_NOSAME 1
#define HA_FULLTEXT (1 << 7)
#define HA_SPATIAL (1 << 10)

bool Index::FillIndex(uint32_t ind) {
  s_user_defined_key_parts_ = 0;
  s_key_length_ = 0;
  s_flags_ = 0;

  for (auto* iter : dd_elements_) {
    if (iter->hidden()) {
      continue;
    }
    s_user_defined_key_parts_++;
    s_key_length_ += iter->length();
  }
  switch (type()) {
    case Index::IT_MULTIPLE:
      s_flags_ = 0;
      break;
    case Index::IT_FULLTEXT:
      s_flags_ = HA_FULLTEXT;
      break;
    case Index::IT_SPATIAL:
      s_flags_ = HA_SPATIAL;
      break;
    case Index::IT_PRIMARY:
    case Index::IT_UNIQUE:
      s_flags_ = HA_NOSAME;
      break;
    default:
      assert(0);
      s_flags_ = 0;
  }
  FillSeIndex(ind);
  return true;
}

#define FTS_DOC_ID_COL_NAME "FTS_DOC_ID"

bool Index::IndexAddCol(Column* col, uint32_t prefix_len) {
  if (col->index_column() != nullptr) {
    ib_fields_.push_back(col->index_column());
  } else if (col->name() == FTS_DOC_ID_COL_NAME) {
    /*
     * The FTS_DOC_ID column is not defined in the SDI's PRIMARY index columns,
     * so we need to create it manually.
     */
    IndexColumn* index_column = IndexColumn::CreateIndexFTSDocIdColumn(col);
    ib_fields_.push_back(index_column);
  } else {
    assert(col->IsInstantDropped());
    IndexColumn* index_column = IndexColumn::CreateIndexDroppedColumn(col);
    ib_fields_.push_back(index_column);
  }
  ib_n_def_++;
  if (ib_type_ & HA_SPATIAL &&
      (col->ib_mtype() == DATA_POINT ||
       col->ib_mtype() == DATA_VAR_POINT) &&
      ib_n_def_ == 1) {
    col->index_column()->set_ib_fixed_len(DATA_MBR_LEN);
  } else {
    col->index_column()->set_ib_fixed_len(col->GetFixedSize());
  }

  if (prefix_len && col->index_column()->ib_fixed_len() > prefix_len) {
    col->index_column()->set_ib_fixed_len(prefix_len);
  }

  if (col->index_column()->ib_fixed_len() > DICT_MAX_FIXED_COL_LEN) {
    col->index_column()->set_ib_fixed_len(0);
  }

  if (col->is_nullable() &&
      !col->IsInstantDropped()) {
    ib_n_nullable_++;
  }
  return true;
}

uint32_t Index::GetNOriginalFields() {
  assert(table_->HasInstantCols());
  uint32_t n_inst_cols_v1 =  table_->GetNInstantAddedColV1();
  uint32_t n_drop_cols = table_->GetNInstantDropCols();
  uint32_t n_add_cols = table_->GetNInstantAddCols();
  uint32_t n_instant_fields =
    ib_n_fields_ + n_drop_cols - n_add_cols - n_inst_cols_v1;

  return n_instant_fields;
}

uint32_t Index::GetNNullableBefore(uint32_t nth) {
  uint32_t nullable = 0;
  for (uint32_t i = 0; i < nth; i++) {
    const IndexColumn* index_col = ib_fields_[i];
    assert(!index_col->column()->IsInstantDropped());
    if (index_col->column()->is_nullable()) {
      nullable++;
    }
  }
  return nullable;
}

uint32_t Index::CalculateNInstantNullable(uint32_t n_fields) {
  if (!table_->HasRowVersions()) {
    return GetNNullableBefore(n_fields);
  }

  uint32_t n_drop_nullable_cols = 0;
  uint32_t new_n_nullable = 0;
  for (uint32_t i = 0; i < ib_n_def_; i++) {
    const IndexColumn* index_col = ib_fields_[i];
    if (index_col->column()->IsInstantAdded()) {
      continue;
    }
    if (index_col->column()->IsInstantDropped()) {
      if (index_col->column()->ib_phy_pos() < n_fields &&
          index_col->column()->is_nullable()) {
        n_drop_nullable_cols++;
      }
      continue;
    }

    if (index_col->column()->ib_phy_pos() < n_fields) {
      if (index_col->column()->is_nullable()) {
        new_n_nullable++;
      }
    }
  }
  new_n_nullable += n_drop_nullable_cols;
  return new_n_nullable;
}

bool Index::HasInstantColsOrRowVersions() {
  if (!IsClustered()) {
    return false;
  }
  return (ib_row_versions_ || ib_instant_cols_);
}

uint32_t Index::GetNullableInVersion(uint8_t version) {
  return ib_nullables_[version];
}

uint16_t Index::GetNullableBeforeInstantAddDrop() {
  if (ib_instant_cols_) {
    return ib_n_instant_nullable_;
  }

  if (ib_row_versions_) {
    return GetNullableInVersion(0);
  }

  return ib_n_nullable_;
}

uint16_t Index::GetNUniqueInTree() {
  if (IsClustered()) {
    return static_cast<uint16_t>(ib_n_uniq_);
  } else {
    return static_cast<uint16_t>(GetNFields());
  }
}

uint16_t Index::GetNUniqueInTreeNonleaf() {
  if (ib_type_ & DICT_SPATIAL) {
    // TODO(zhao): support spatial Index
    assert(0);
    return DICT_INDEX_SPATIAL_NODEPTR_SIZE;
  } else {
    return static_cast<uint16_t>(GetNUniqueInTree());
  }
}

IndexColumn* Index::GetPhysicalField(size_t pos) {
  if (ib_row_versions_) {
    return ib_fields_[ib_fields_array_[pos]];
  }

  return ib_fields_[pos];
}

uint32_t Index::GetNFields() const {
  if (table_->HasRowVersions()) {
    return ib_n_total_fields_;
  }
  return ib_n_fields_;
}

#define UNSUPP_INDEX_MASK 0x7
#define UNSUPP_INDEX_MASK_VIRTUAL 0x1
#define UNSUPP_INDEX_MASK_FTS 0x2
#define UNSUPP_INDEX_MASK_SPATIAL 0x4

void Index::PreCheck() {
  if (dd_type_ == IT_FULLTEXT) {
    unsupported_reason_ |= UNSUPP_INDEX_MASK_FTS;
  }
  if (dd_type_ == IT_SPATIAL) {
    unsupported_reason_ |= UNSUPP_INDEX_MASK_SPATIAL;
  }
  for (auto* iter : dd_elements_) {
    if (iter->hidden()) {
      continue;
    }
    if (iter->column()->is_virtual()) {
      unsupported_reason_ |= UNSUPP_INDEX_MASK_VIRTUAL;
      break;
    }
  }
}

bool Index::IsIndexSupported() {
  return (unsupported_reason_ & UNSUPP_INDEX_MASK) == 0;
}

std::string Index::UnsupportedReason() {
  assert(!IsIndexSupported());
  std::string reason = "";
  if (unsupported_reason_ & UNSUPP_INDEX_MASK_VIRTUAL) {
    reason += "[Index using virtual columns as keys]";
  }
  if (unsupported_reason_ & UNSUPP_INDEX_MASK_FTS) {
    reason += "[Fulltext index]";
  }
  if (unsupported_reason_ & UNSUPP_INDEX_MASK_SPATIAL) {
    reason += "[Spatial index]";
  }
  return reason;
}

bool Index::IsIndexParsingRecSupported() {
  if (!table_->IsTableParsingRecSupported()) {
    return false;
  }
  return IsIndexSupported();
  // Additional checks may be needed here.
}

#define FTS_DOC_ID_INDEX_NAME "FTS_DOC_ID_INDEX"

bool Index::FillSeIndex(uint32_t ind) {
  PreCheck();
  if (!IsIndexSupported()) {
    return true;
  }
  ib_n_fields_ = s_user_defined_key_parts_;
  ib_n_uniq_ = ib_n_fields_;
  if (s_flags_ & HA_SPATIAL) {
    ib_type_ = DICT_SPATIAL;
    assert(ib_n_fields_ == 1);
  } else if (s_flags_ & HA_FULLTEXT) {
    ib_type_ = DICT_FTS;
    ib_n_uniq_ = 0;
  } else if (ind == 0) {
    assert(s_flags_ & HA_NOSAME);
    // dd_hidden_ == true means there no explicit primary index
    assert(ib_n_uniq_ > 0 || dd_hidden_ == true);
    if (dd_hidden_ == false) {
      ib_type_ = DICT_CLUSTERED | DICT_UNIQUE;
    } else {
      /*
       * The implicit primary index type is DICT_CLUSTERED.
       * This is made consistent with InnoDB.
       */
      ib_type_ = DICT_CLUSTERED;
    }
  } else {
    ib_type_ = (s_flags_ & HA_NOSAME) ? DICT_UNIQUE : 0;
  }

  ib_n_def_ = 0;
  ib_n_nullable_ = 0;
  ib_fields_.clear();

  for (auto* iter : dd_elements_) {
    if (iter->hidden()) {
      continue;
    }
    uint32_t prefix_len = 0;
    IndexAddCol(iter->column(), prefix_len);
  }

  /*
   * Special case: "FTS_DOC_ID_INDEX"
   * The columns of FTS_DOC_ID_INDEX defined in the SDI are correct,
   * but the information for the FTS_DOC_ID column is
   * missing (e.g., ib_mtype_).
   * Note that we have created a new FTS_DOC_ID in Table::ib_cols_,
   * so use that instead.
   */
  if (dd_name_ == FTS_DOC_ID_INDEX_NAME) {
    for (auto* iter : dd_elements_) {
      if (iter->column()->name() == FTS_DOC_ID_COL_NAME) {
        for (auto* col : *table()->ib_cols()) {
          if (col->name() == FTS_DOC_ID_COL_NAME) {
            iter->set_column(col);
          }
        }
      }
      /*
       * The user has explicitly defined the FTS_DOC_ID column,
       * so no need to add another.
       */
      if (iter->hidden()) {
        IndexAddCol(iter->column(), 0);
      }
    }
  }

  if (IsClustered()) {
    ib_n_user_defined_cols_ = s_user_defined_key_parts_;
    if (!IsIndexUnique()) {
      ib_n_uniq_ += 1;
    }
    uint32_t n_fields_processed = 0;
    while (n_fields_processed < ib_n_def_) {
      IndexColumn* col = ib_fields_[n_fields_processed];
      if (!table()->HasRowVersions()) {
        col->column()->set_ib_phy_pos(n_fields_processed);
      } else {
        assert(col->column()->ib_phy_pos() != UINT32_UNDEFINED);
      }
      n_fields_processed++;
    }
    bool found_db_row_id = false;
    bool found_db_trx_id = false;
    bool found_db_roll_ptr = false;
    for (auto* col : *(table()->ib_cols())) {
      if (!IsIndexUnique() && col->name() == "DB_ROW_ID") {
        found_db_row_id = true;
        if (!table()->HasRowVersions()) {
          col->set_ib_phy_pos(n_fields_processed);
        } else {
          assert(col->ib_phy_pos() != UINT32_UNDEFINED);
        }
        IndexAddCol(col, 0);
        n_fields_processed++;
        ib_n_fields_++;
      }
      if (col->name() == "DB_TRX_ID") {
        found_db_trx_id = true;
        if (!table()->HasRowVersions()) {
          col->set_ib_phy_pos(n_fields_processed);
        } else {
          assert(col->ib_phy_pos() != UINT32_UNDEFINED);
        }
        IndexAddCol(col, 0);
        n_fields_processed++;
        ib_n_fields_++;
      }
      if (col->name() == "DB_ROLL_PTR") {
        found_db_roll_ptr = true;
        if (!table()->HasRowVersions()) {
          col->set_ib_phy_pos(n_fields_processed);
        } else {
          assert(col->ib_phy_pos() != UINT32_UNDEFINED);
        }
        IndexAddCol(col, 0);
        n_fields_processed++;
        ib_n_fields_++;
      }
    }
    assert((IsIndexUnique() || found_db_row_id) &&
        found_db_trx_id && found_db_roll_ptr);

    std::vector<bool> indexed(table()->GetTotalCols(), false);
    for (auto* iter : ib_fields_) {
      indexed[iter->column()->ib_ind()] = true;
    }
    for (uint32_t i = 0; i < table()->ib_n_cols() - DATA_N_SYS_COLS; i++) {
      Column* col = table()->ib_cols()->at(i);
      assert(col->ib_mtype() != DATA_SYS);
      if (indexed[col->ib_ind()]) {
        continue;
      }
      if (!table()->HasRowVersions()) {
        col->set_ib_phy_pos(n_fields_processed);
      } else {
        assert(col->ib_phy_pos() != UINT32_UNDEFINED);
      }

      IndexAddCol(col, 0);
      n_fields_processed++;
      ib_n_fields_++;
    }
    for (uint32_t i = table()->ib_n_cols(); i < table()->GetTotalCols(); i++) {
      Column* col = table()->ib_cols()->at(i);
      assert(col->ib_mtype() != DATA_SYS);
      IndexAddCol(col, 0);
      n_fields_processed++;
      // Do not add ib_n_fields_ here, as we are adding dropped columns.
    }
    if (!table()->ib_is_system_table()) {
      ib_fields_array_.clear();
      memset(ib_nullables_, 0,
          (MAX_ROW_VERSION + 1) * sizeof(ib_nullables_[0]));
      if (table()->HasRowVersions()) {
        ib_fields_array_.resize(ib_n_def_);
        for (uint32_t i = 0; i < ib_n_def_; i++) {
          IndexColumn *index_col = ib_fields_[i];
          assert(index_col != nullptr && index_col->column() != nullptr);

          size_t pos = index_col->column()->ib_phy_pos();

          ib_fields_array_[pos] = i;
        }

        uint32_t current_row_version = table()->ib_current_row_version();
        auto update_nullable = [&](size_t start_version, bool is_increment) {
          for (size_t i = start_version; i <= current_row_version; i++) {
            assert(is_increment || ib_nullables_[i] > 0);

            if (is_increment) {
              ++ib_nullables_[i];
            } else {
              --ib_nullables_[i];
            }
          }
        };
        for (uint32_t i = 0; i < ib_n_def_; i++) {
          IndexColumn *index_col = ib_fields_[i];

          if (index_col->column()->name() == "DB_ROW_ID" ||
               index_col->column()->name() == "DB_TRX_ID" ||
               index_col->column()->name() == "DB_ROLL_PTR") {
            continue;
          }

          if (!index_col->column()->is_nullable()) {
            continue;
          }

          size_t start_from = 0;
          if (index_col->column()->IsInstantAdded()) {
            start_from = index_col->column()->GetVersionAdded();
          }
          update_nullable(start_from, true);

          if (index_col->column()->IsInstantDropped()) {
            update_nullable(index_col->column()->GetVersionDropped(), false);
          }
        }
      }
    }
    assert(table_->clust_index() == nullptr);
    table_->set_clust_index(this);
  } else {
    ib_n_user_defined_cols_ = s_user_defined_key_parts_;
    std::vector<bool> indexed(table()->GetTotalCols(), false);
    for (auto* iter : ib_fields_) {
      if (iter->column()->is_virtual()) {
        continue;
      }
      indexed[iter->column()->ib_ind()] = true;
    }
    assert(table_->clust_index() != nullptr);
    for (uint32_t i = 0; i < table_->clust_index()->ib_n_uniq(); i++) {
      IndexColumn* clust_index_col = table_->clust_index()->ib_fields()->at(i);
      if (!indexed[clust_index_col->column()->ib_ind()]) {
        uint32_t prefix_len = 0;
        IndexAddCol(clust_index_col->column(), prefix_len);
      } else {
        // TODO(Zhao): Support spatial index
      }
    }

    if (IsIndexUnique()) {
      ib_n_uniq_ = ib_n_fields_;
    } else {
      ib_n_uniq_ = ib_n_def_;
    }
    ib_n_fields_ = ib_n_def_;
  }
  dd_se_private_data_.Get("id", &ib_id_);
  dd_se_private_data_.Get("root", &ib_page_);

  ib_n_fields_ = ib_n_def_;

  if (IsClustered() && table_->HasRowVersions()) {
    ib_n_fields_ = ib_n_def_ - table_->GetNInstantDropCols();
  }

  ib_n_total_fields_ = ib_n_def_;
  ib_row_versions_ = false;
  ib_instant_cols_ = false;
  ib_n_instant_nullable_ = ib_n_nullable_;
  if (IsClustered()) {
    ib_row_versions_ = table_->HasRowVersions();
    if (table_->HasInstantCols()) {
      ib_instant_cols_ = true;
      uint32_t n_instant_fields = GetNOriginalFields();
      uint32_t new_n_nullable = CalculateNInstantNullable(n_instant_fields);
      ib_n_instant_nullable_ = new_n_nullable;
    }
  }
  return true;
}

}  // namespace ibd_ninja
