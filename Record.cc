/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#include "Record.h"
#include "Index.h"
#include "Table.h"
#include "Column.h"

#include <cassert>
#include <cinttypes>
#include <cstdio>

namespace ibd_ninja {

#define ninja_pt(p, fmt, ...) \
    do { if (p) printf(fmt, ##__VA_ARGS__); } while (0)

#define UT_BITS_IN_BYTES(b) (((b) + 7UL) / 8UL)

/* ------ Record ------ */
uint32_t Record::GetBitsFrom1B(uint32_t offs, uint32_t mask, uint32_t shift) {
  return ((ReadFrom1B(rec_ - offs) & mask) >> shift);
}
uint32_t Record::GetBitsFrom2B(uint32_t offs, uint32_t mask, uint32_t shift) {
  return ((ReadFrom2B(rec_ - offs) & mask) >> shift);
}

uint32_t Record::GetStatus() {
  uint32_t ret = 0;
  ret = GetBitsFrom1B(REC_NEW_STATUS, REC_NEW_STATUS_MASK,
                      REC_NEW_STATUS_SHIFT);
  assert((ret & ~REC_NEW_STATUS_MASK) == 0);
  return ret;
}

uint32_t* Record::GetColumnOffsets() {
  uint32_t n = 0;
  if (index_->table()->IsCompact()) {
    switch (GetStatus()) {
      case REC_STATUS_ORDINARY:
       n = index_->GetNFields();
       break;
      case REC_STATUS_NODE_PTR:
       n = index_->GetNUniqueInTreeNonleaf() + 1;
       break;
      case REC_STATUS_INFIMUM:
      case REC_STATUS_SUPREMUM:
        n = 1;
        break;
      default:
        assert(0);
    }
  } else {
    // TODO(Zhao): Support redundant row format
  }
  uint32_t size = n + (1 + REC_OFFS_HEADER_SIZE);
  assert(offsets_ == nullptr);
  offsets_ = new uint32_t[size];
  SetNAlloc(size);
  SetNFields(n);
  InitColumnOffsets();

  return offsets_;
}

static uint32_t* RecOffsBase(uint32_t* offsets) {
  return offsets + REC_OFFS_HEADER_SIZE;
}

static uint32_t RecOffsNFields(uint32_t* offsets) {
  uint32_t n_fields = offsets[1];
  assert(n_fields > 0);
  return n_fields;
}

void Record::InitColumnOffsetsCompact() {
  uint32_t n_node_ptr_field = ULINT_UNDEFINED;
  switch (GetStatus()) {
    case REC_STATUS_INFIMUM:
    case REC_STATUS_SUPREMUM:
      RecOffsBase(offsets_)[0] = REC_N_NEW_EXTRA_BYTES | REC_OFFS_COMPACT;
      RecOffsBase(offsets_)[1] = 8;
      return;
    case REC_STATUS_NODE_PTR:
      n_node_ptr_field = index_->GetNUniqueInTreeNonleaf();
      break;
    case REC_STATUS_ORDINARY:
      InitColumnOffsetsCompactLeaf();
      return;
  }

  // non-leaf page record
  assert(!IsVersionedCompact());
  const unsigned char* nulls = rec_ - (REC_N_NEW_EXTRA_BYTES + 1);
  const size_t nullable_cols = index_->GetNullableBeforeInstantAddDrop();

  const unsigned char* lens = nulls - UT_BITS_IN_BYTES(nullable_cols);
  uint32_t offs = 0;
  uint32_t null_mask = 1;

  uint32_t i = 0;
  IndexColumn* index_col = nullptr;
  Column* col = nullptr;
  do {
    uint32_t len;
    if (i == n_node_ptr_field) {
      len = offs += REC_NODE_PTR_SIZE;
      goto resolved;
    }

    index_col = index_->ib_fields()->at(i);
    col = index_col->column();
    if (col->is_nullable()) {
      if (!(unsigned char)null_mask) {
        nulls--;
        null_mask = 1;
      }

      if (*nulls & null_mask) {
        null_mask <<= 1;
        len = offs | REC_OFFS_SQL_NULL;
        goto resolved;
      }
      null_mask <<= 1;
    }

    if (!index_col->ib_fixed_len()) {
      /* DATA_POINT should always be a fixed
      length column. */
      assert(col->ib_mtype() != DATA_POINT);
      /* Variable-length field: read the length */
      len = *lens--;
      if (col->IsBigCol()) {
        if (len & 0x80) {
          len <<= 8;
          len |= *lens--;
          assert(!(len & 0x4000));
          offs += len & 0x3fff;
          len = offs;

          goto resolved;
        }
      }

      len = offs += len;
    } else {
      len = offs += index_col->ib_fixed_len();
    }
  resolved:
    RecOffsBase(offsets_)[i + 1] = len;
  } while (++i < RecOffsNFields(offsets_));

  *RecOffsBase(offsets_) = (rec_ - (lens + 1)) | REC_OFFS_COMPACT;
}

void Record::InitColumnOffsets() {
  if (index_->table()->IsCompact()) {
    InitColumnOffsetsCompact();
  } else {
    // TODO(Zhao): Support redundant row format
  }
}

void Record::InitColumnOffsetsCompactLeaf() {
  const unsigned char* nulls = nullptr;
  const unsigned char* lens = nullptr;
  uint16_t n_null = 0;
  enum REC_INSERT_STATE rec_insert_state = REC_INSERT_STATE::NONE;
  uint8_t row_version = UINT8_UNDEFINED;
  uint16_t non_default_fields = 0;
  rec_insert_state = InitNullAndLengthCompact(&nulls, &lens, &n_null,
                                 &non_default_fields, &row_version);

  uint32_t offs = 0;
  uint32_t any_ext = 0;
  uint32_t null_mask = 1;
  uint16_t i = 0;
  do {
    IndexColumn* index_col = index_->GetPhysicalField(i);
    Column* col = index_col->column();
    uint64_t len;
    switch (rec_insert_state) {
      case INSERTED_INTO_TABLE_WITH_NO_INSTANT_NO_VERSION:
        assert(!index_->HasInstantColsOrRowVersions());
        break;

      case INSERTED_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION: {
        assert(row_version == UINT8_UNDEFINED || row_version == 0);
        assert(index_->ib_row_versions());
        row_version = 0;
      }
      [[fallthrough]];
      case INSERTED_AFTER_UPGRADE_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION:
      case INSERTED_AFTER_INSTANT_ADD_NEW_IMPLEMENTATION: {
        assert(index_->ib_row_versions() ||
              (index_->table()->ib_m_upgraded_instant() && row_version == 0));

        if (col->IsDroppedInOrBefore(row_version)) {
          len = offs | REC_OFFS_DROP;
          goto resolved;
        } else if (col->IsAddedAfter(row_version)) {
          len = GetInstantOffset(i, offs);
          goto resolved;
        }
      } break;

      case INSERTED_BEFORE_INSTANT_ADD_OLD_IMPLEMENTATION:
      case INSERTED_AFTER_INSTANT_ADD_OLD_IMPLEMENTATION: {
        assert(non_default_fields > 0);
        assert(index_->ib_instant_cols());

        if (i >= non_default_fields) {
          len = GetInstantOffset(i, offs);
          goto resolved;
        }
      } break;

      default:
        assert(false);
    }

    if (col->is_nullable()) {
      assert(n_null--);

      if (!(unsigned char)null_mask) {
        nulls--;
        null_mask = 1;
      }

      if (*nulls & null_mask) {
        null_mask <<= 1;
        len = offs | REC_OFFS_SQL_NULL;
        goto resolved;
      }
      null_mask <<= 1;
    }

    if (!index_col->ib_fixed_len()) {
      /* Variable-length field: read the length */
      len = *lens--;
      if (col->IsBigCol()) {
        if (len & 0x80) {
          len <<= 8;
          len |= *lens--;

          offs += len & 0x3fff;
          if (len & 0x4000) {
            assert(index_->IsClustered());
            any_ext = REC_OFFS_EXTERNAL;
            len = offs | REC_OFFS_EXTERNAL;
          } else {
            len = offs;
          }
          goto resolved;
        }
      }

      len = offs += len;
    } else {
      len = offs += index_col->ib_fixed_len();
    }
  resolved:
    RecOffsBase(offsets_)[i + 1] = len;
  } while (++i < RecOffsNFields(offsets_));

  *RecOffsBase(offsets_) = (rec_ - (lens + 1)) | REC_OFFS_COMPACT | any_ext;
}

Record::REC_INSERT_STATE Record::InitNullAndLengthCompact(
            const unsigned char** nulls, const unsigned char** lens,
            uint16_t* n_null, uint16_t* non_default_fields,
            uint8_t* row_version) {
  *non_default_fields = static_cast<uint16_t>(index_->GetNFields());
  *row_version = UINT8_UNDEFINED;

  *nulls = rec_ - (REC_N_NEW_EXTRA_BYTES + 1);

  const enum REC_INSERT_STATE rec_insert_state =
            GetInsertState();
  switch (rec_insert_state) {
    case INSERTED_INTO_TABLE_WITH_NO_INSTANT_NO_VERSION: {
      assert(!GetInstantFlagCompact());
      assert(!IsVersionedCompact());

      *n_null = index_->ib_n_nullable();
    } break;

    case INSERTED_AFTER_INSTANT_ADD_NEW_IMPLEMENTATION:
    case INSERTED_AFTER_UPGRADE_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION: {
      *row_version = (uint8_t)(**nulls);

      *nulls -= 1;
      *n_null = index_->GetNullableInVersion(*row_version);
    } break;

    case INSERTED_AFTER_INSTANT_ADD_OLD_IMPLEMENTATION: {
      uint16_t length;
      *non_default_fields =
          GetNFieldsInstant(REC_N_NEW_EXTRA_BYTES, &length);
      assert(length == 1 || length == 2);

      *nulls -= length;
      *n_null = index_->CalculateNInstantNullable(*non_default_fields);
    } break;

    case INSERTED_BEFORE_INSTANT_ADD_OLD_IMPLEMENTATION: {
      *n_null = index_->GetNullableBeforeInstantAddDrop();
      *non_default_fields = index_->GetNOriginalFields();
    } break;

    case INSERTED_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION: {
      *n_null = index_->GetNullableBeforeInstantAddDrop();
    } break;

    default:
      assert(0);
  }
  *lens = *nulls - UT_BITS_IN_BYTES(*n_null);

  return (rec_insert_state);
}

Record::REC_INSERT_STATE Record::GetInsertState() {
  if (!index_->HasInstantColsOrRowVersions()) {
    return INSERTED_INTO_TABLE_WITH_NO_INSTANT_NO_VERSION;
  }
  const unsigned char* v_ptr =
      (const unsigned char*)rec_ - (REC_N_NEW_EXTRA_BYTES + 1);
  const bool is_versioned = IsVersionedCompact();
  const uint8_t version = (is_versioned) ? (uint8_t)(*v_ptr) : UINT8_UNDEFINED;
  const bool is_instant = GetInstantFlagCompact();
  assert(!is_versioned || !is_instant);
  enum REC_INSERT_STATE rec_insert_state = REC_INSERT_STATE::NONE;
  if (is_versioned) {
    if (version == 0) {
      assert(index_->ib_instant_cols());
      rec_insert_state =
          INSERTED_AFTER_UPGRADE_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION;
    } else {
      assert(index_->ib_row_versions());
      rec_insert_state = INSERTED_AFTER_INSTANT_ADD_NEW_IMPLEMENTATION;
    }
  } else if (is_instant) {
    assert(index_->table()->HasInstantCols());
    rec_insert_state = INSERTED_AFTER_INSTANT_ADD_OLD_IMPLEMENTATION;
  } else if (index_->table()->HasInstantCols()) {
    rec_insert_state = INSERTED_BEFORE_INSTANT_ADD_OLD_IMPLEMENTATION;
  } else {
    rec_insert_state = INSERTED_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION;
  }

  assert(rec_insert_state != REC_INSERT_STATE::NONE);
  return rec_insert_state;
}

bool Record::IsVersionedCompact() {
  uint32_t info = GetInfoBits(true);
  return ((info & REC_INFO_VERSION_FLAG) != 0);
}
uint32_t Record::GetInfoBits(bool comp) {
  const uint32_t val = GetBitsFrom1B(comp ?
                                     REC_NEW_INFO_BITS : REC_OLD_INFO_BITS,
                                     REC_INFO_BITS_MASK, REC_INFO_BITS_SHIFT);
  return val;
}
bool Record::GetInstantFlagCompact() {
  uint32_t info = GetInfoBits(true);
  return ((info & REC_INFO_INSTANT_FLAG) != 0);
}

uint32_t Record::GetNFieldsInstant(const uint32_t extra_bytes,
                                   uint16_t* length) {
  uint16_t n_fields;
  const unsigned char *ptr;

  ptr = rec_ - (extra_bytes + 1);

  if ((*ptr & REC_N_FIELDS_TWO_BYTES_FLAG) == 0) {
    *length = 1;
    return (*ptr);
  }

  *length = 2;
  n_fields = ((*ptr-- & REC_N_FIELDS_ONE_BYTE_MAX) << 8);
  n_fields |= *ptr;
  assert(n_fields < REC_MAX_N_FIELDS);
  assert(n_fields != 0);

  return (n_fields);
}

uint64_t Record::GetInstantOffset(uint32_t n, uint64_t offs) {
  assert(index_->HasInstantColsOrRowVersions());
  Column* col = index_->GetPhysicalField(n)->column();
  if (col->ib_instant_default()) {
    return (offs | REC_OFFS_DEFAULT);
  } else {
    return (offs | REC_OFFS_SQL_NULL);
  }
}

void Record::ParseRecord(bool leaf, uint32_t row_no,
                         PageAnalysisResult* result,
                         bool print) {
  uint32_t n_fields = leaf ? index_->GetNFields() :
                             index_->GetNUniqueInTreeNonleaf() + 1;
  uint32_t header_len = (RecOffsBase(offsets_)[0] & REC_OFFS_MASK);
  uint32_t rec_len = (RecOffsBase(offsets_)[n_fields] &
                      REC_OFFS_MASK);
  ninja_pt(print, "=========================================="
                  "=============================\n");
  ninja_pt(print, "[ROW %u] Length: %u (%d | %d), Number of fields: %u\n",
                   row_no,
                   header_len + rec_len,
                   header_len,
                   rec_len,
                   n_fields);
  bool deleted = false;
  if (RecGetDeletedFlag(rec_, true) != 0) {
    ninja_pt(print, "[DELETED MARK]\n");
    deleted = true;
  }
  if (!deleted) {
    if (leaf) {
      result->n_recs_leaf++;
      result->headers_len_leaf += header_len;
      result->recs_len_leaf += rec_len;
    } else {
      result->n_recs_non_leaf++;
      result->headers_len_non_leaf += header_len;
      result->recs_len_non_leaf += rec_len;
    }
  } else {
    if (leaf) {
      result->n_deleted_recs_leaf++;
      result->deleted_recs_len_leaf += (header_len + rec_len);
    } else {
      result->n_deleted_recs_non_leaf++;
      result->deleted_recs_len_non_leaf += (header_len + rec_len);
    }
  }
  uint32_t start_pos = 0;
  uint32_t len = 0;
  uint32_t end_pos = 0;
  ibd_ninja::IndexColumn* index_col = nullptr;
  ninja_pt(print, "------------------------------------------"
                  "-----------------------------\n");
  ninja_pt(print, "  [HEADER   ]         ");
  int count = 0;
  for (uint32_t i = 0; i < header_len; i++) {
    ninja_pt(print, "%02x ", (rec_ - header_len)[i]);
    count++;
    if (count == 8) {
      ninja_pt(print, " ");
    } else if (count == 16) {
      ninja_pt(print, "\n                      ");
      count = 0;
    }
  }
  ninja_pt(print, "\n");
  bool dropped_column_counted = false;
  for (uint32_t i = 0; i < n_fields; i++) {
    index_col = nullptr;
    if (!leaf && i == n_fields - 1) {
      ninja_pt(print, "  [FIELD %3u] Name  : *NODE_PTR(Child page no)\n",
                         i + 1);
    } else {
      index_col = index_->GetPhysicalField(i);
      ninja_pt(print, "  [FIELD %3u] Name  : %s\n",
                         i + 1,
                         index_col->column()->name().c_str());
    }

    len = RecOffsBase(offsets_)[i + 1];
    end_pos = (len & REC_OFFS_MASK);
    ninja_pt(print, "              "
                    "Length: %-5u\n",
                    end_pos - start_pos);
    // TODO(Zhao): handle external part
    if (index_col != nullptr &&
        index_col->column()->IsColumnDropped()) {
      // Only count valid records with non-zero size for dropped columns.
      if (!deleted && !(len & REC_OFFS_DROP)) {
        if (leaf) {
          result->dropped_cols_len_leaf += (end_pos - start_pos);
          if (!dropped_column_counted) {
            result->n_contain_dropped_cols_recs_leaf++;
          }
        } else {
          result->dropped_cols_len_non_leaf += (end_pos - start_pos);
          if (!dropped_column_counted) {
            result->n_contain_dropped_cols_recs_non_leaf++;
          }
        }
        dropped_column_counted = true;
      }
    }

    // index_col is nullptr for node_ptr
    if (index_col != nullptr) {
    ninja_pt(print, "              "
                    "Type  : %-15s | %-12s | %-20s\n",
                    index_col->column()->dd_column_type_utf8().c_str(),
                    index_col->column()->FieldTypeString().c_str(),
                    index_col->column()->SeTypeString().c_str());
    }

    ninja_pt(print, "              "
                    "Value : ");
    if (len & REC_OFFS_SQL_NULL) {
      ninja_pt(print, "*NULL*\n");
      start_pos = end_pos;
      continue;
    }
    if (len & REC_OFFS_DROP) {
      ninja_pt(print, "*NULL*\n"
                      "                      "
                      "(This row was inserted after this column "
                      "was instantly dropped)\n");
      start_pos = end_pos;
      continue;
    }
    if (len & REC_OFFS_DEFAULT) {
      ninja_pt(print, "*DEFAULT*\n"
                      "                      "
                      "(This row was inserted before this column "
                      "was instantly added)\n");
      start_pos = end_pos;
      continue;
    }
    count = 0;
    while (start_pos < end_pos) {
      ninja_pt(print, "%02x ", rec_[start_pos]);
      count++;
      if (count == 8) {
        ninja_pt(print, " ");
      } else if (count == 16) {
        ninja_pt(print, "\n                      ");
        count = 0;
      }
      start_pos++;
    }
    if (len & REC_OFFS_EXTERNAL) {
      const unsigned char* ext_ref = &rec_[end_pos - 20];
      uint32_t space_id = ReadFrom4B(ext_ref + BTR_EXTERN_SPACE_ID);
      uint32_t ext_page_no = ReadFrom4B(ext_ref + BTR_EXTERN_PAGE_NO);
      uint32_t ext_version = ReadFrom4B(ext_ref + BTR_EXTERN_VERSION);
      uint64_t ext_len = ReadFrom8B(ext_ref + BTR_EXTERN_LEN) & 0x1FFFFFFFFFULL;
      ninja_pt(print, "\n                      "
              "[EXTERNAL: space=%u, page=%u, version=%u, len=%" PRIu64 "]",
              space_id, ext_page_no, ext_version, ext_len);
      FetchAndDisplayExternalLob(space_id, ext_page_no, ext_version, ext_len,
                                 g_lob_output_format,
                                 g_lob_show_version_history, print);
    }
    ninja_pt(print, "\n");
  }
}

uint32_t Record::GetChildPageNo() {
  uint32_t n_fields = GetNFields();
  assert(n_fields >= 2);
  uint32_t last_2_len = (RecOffsBase(offsets_)[n_fields - 1]);
  uint32_t last_2_end_pos = (last_2_len & REC_OFFS_MASK);
  uint32_t last_len = (RecOffsBase(offsets_)[n_fields]);
  uint32_t last_end_pos = (last_len & REC_OFFS_MASK);
  assert(last_end_pos - last_2_end_pos == 4);
  return ReadFrom4B(&rec_[last_2_end_pos]);
}

}  // namespace ibd_ninja
