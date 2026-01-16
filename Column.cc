/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */

#include "Column.h"
#include "ibdCollations.h"
#include "JSONHelpers.h"

#include <cassert>

namespace ibd_ninja {

/* ------ Column ------ */
const std::set<std::string> Column::default_valid_option_keys = {
  "column_format",
  "geom_type",
  "interval_count",
  "not_secondary",
  "storage",
  "treat_bit_as_char",
  "is_array",
  "gipk" /* generated implicit primary key column */
};

bool Column::Init(const rapidjson::Value& dd_col_obj) {
  Read(&dd_name_, dd_col_obj, "name");
  ReadEnum(&dd_type_, dd_col_obj, "type");
  Read(&dd_is_nullable_, dd_col_obj, "is_nullable");
  Read(&dd_is_zerofill_, dd_col_obj, "is_zerofill");
  Read(&dd_is_unsigned_, dd_col_obj, "is_unsigned");
  Read(&dd_is_auto_increment_, dd_col_obj, "is_auto_increment");
  Read(&dd_is_virtual_, dd_col_obj, "is_virtual");
  ReadEnum(&dd_hidden_, dd_col_obj, "hidden");
  Read(&dd_ordinal_position_, dd_col_obj, "ordinal_position");
  Read(&dd_char_length_, dd_col_obj, "char_length");
  Read(&dd_numeric_precision_, dd_col_obj, "numeric_precision");
  Read(&dd_numeric_scale_, dd_col_obj, "numeric_scale");
  Read(&dd_numeric_scale_null_, dd_col_obj, "numeric_scale_null");
  Read(&dd_datetime_precision_, dd_col_obj, "datetime_precision");
  Read(&dd_datetime_precision_null_, dd_col_obj, "datetime_precision_null");
  Read(&dd_has_no_default_, dd_col_obj, "has_no_default");
  Read(&dd_default_value_null_, dd_col_obj, "default_value_null");
  Read(&dd_srs_id_null_, dd_col_obj, "srs_id_null");
  if (!dd_srs_id_null_) {
    uint32_t srs_id = 0;
    Read(&srs_id, dd_col_obj, "srs_id");
    dd_srs_id_ = srs_id;
  }
  Read(&dd_default_value_, dd_col_obj, "default_value");
  Read(&dd_default_value_utf8_null_, dd_col_obj, "default_value_utf8_null");
  Read(&dd_default_value_utf8_, dd_col_obj, "default_value_utf8");
  Read(&dd_default_option_, dd_col_obj, "default_option");
  Read(&dd_update_option_, dd_col_obj, "update_option");
  Read(&dd_comment_, dd_col_obj, "comment");
  Read(&dd_generation_expression_, dd_col_obj, "generation_expression");
  Read(&dd_generation_expression_utf8_, dd_col_obj,
      "generation_expression_utf8");
  ReadProperties(&dd_options_, dd_col_obj, "options");
  ReadProperties(&dd_se_private_data_, dd_col_obj, "se_private_data");
  Read(&dd_engine_attribute_, dd_col_obj, "engine_attribute");
  Read(&dd_secondary_engine_attribute_, dd_col_obj,
      "secondary_engine_attribute");
  ReadEnum(&dd_column_key_, dd_col_obj, "column_key");
  Read(&dd_column_type_utf8_, dd_col_obj, "column_type_utf8");
  // TODO(Zhao): dd_elements_
  if (dd_col_obj.HasMember("elements") && dd_col_obj["elements"].IsArray()) {
    dd_elements_size_tmp_ = dd_col_obj["elements"].GetArray().Size();
  }
  Read(&dd_collation_id_, dd_col_obj, "collation_id");
  Read(&dd_is_explicit_collation_, dd_col_obj, "is_explicit_collation");

  return true;
}

Column* Column::CreateColumn(const rapidjson::Value& dd_col_obj) {
  Column* column = new Column();
  bool init_ret = column->Init(dd_col_obj);
  if (!init_ret) {
    delete column;
    column = nullptr;
  }
  return column;
}

Column::enum_field_types Column::DDType2FieldType(
                  enum_column_types type) {
  switch (type) {
    // 31 in total
    case enum_column_types::DECIMAL:
      return enum_field_types::MYSQL_TYPE_DECIMAL;
    case enum_column_types::TINY:
      return enum_field_types::MYSQL_TYPE_TINY;
    case enum_column_types::SHORT:
      return enum_field_types::MYSQL_TYPE_SHORT;
    case enum_column_types::LONG:
      return enum_field_types::MYSQL_TYPE_LONG;
    case enum_column_types::FLOAT:
      return enum_field_types::MYSQL_TYPE_FLOAT;
    case enum_column_types::DOUBLE:
      return enum_field_types::MYSQL_TYPE_DOUBLE;
    case enum_column_types::TYPE_NULL:
      return enum_field_types::MYSQL_TYPE_NULL;
    case enum_column_types::TIMESTAMP:
      return enum_field_types::MYSQL_TYPE_TIMESTAMP;
    case enum_column_types::LONGLONG:
      return enum_field_types::MYSQL_TYPE_LONGLONG;
    case enum_column_types::INT24:
      return enum_field_types::MYSQL_TYPE_INT24;
    case enum_column_types::DATE:
      return enum_field_types::MYSQL_TYPE_DATE;
    case enum_column_types::TIME:
      return enum_field_types::MYSQL_TYPE_TIME;
    case enum_column_types::DATETIME:
      return enum_field_types::MYSQL_TYPE_DATETIME;
    case enum_column_types::YEAR:
      return enum_field_types::MYSQL_TYPE_YEAR;
    case enum_column_types::NEWDATE:
      return enum_field_types::MYSQL_TYPE_NEWDATE;
    case enum_column_types::VARCHAR:
      return enum_field_types::MYSQL_TYPE_VARCHAR;
    case enum_column_types::BIT:
      return enum_field_types::MYSQL_TYPE_BIT;
    case enum_column_types::TIMESTAMP2:
      return enum_field_types::MYSQL_TYPE_TIMESTAMP2;
    case enum_column_types::DATETIME2:
      return enum_field_types::MYSQL_TYPE_DATETIME2;
    case enum_column_types::TIME2:
      return enum_field_types::MYSQL_TYPE_TIME2;
    case enum_column_types::NEWDECIMAL:
      return enum_field_types::MYSQL_TYPE_NEWDECIMAL;
    case enum_column_types::ENUM:
      return enum_field_types::MYSQL_TYPE_ENUM;
    case enum_column_types::SET:
      return enum_field_types::MYSQL_TYPE_SET;
    case enum_column_types::TINY_BLOB:
      return enum_field_types::MYSQL_TYPE_TINY_BLOB;
    case enum_column_types::MEDIUM_BLOB:
      return enum_field_types::MYSQL_TYPE_MEDIUM_BLOB;
    case enum_column_types::LONG_BLOB:
      return enum_field_types::MYSQL_TYPE_LONG_BLOB;
    case enum_column_types::BLOB:
      return enum_field_types::MYSQL_TYPE_BLOB;
    case enum_column_types::VAR_STRING:
      return enum_field_types::MYSQL_TYPE_VAR_STRING;
    case enum_column_types::STRING:
      return enum_field_types::MYSQL_TYPE_STRING;
    case enum_column_types::GEOMETRY:
      return enum_field_types::MYSQL_TYPE_GEOMETRY;
    case enum_column_types::JSON:
      return enum_field_types::MYSQL_TYPE_JSON;
    // TODO(Zhao): support other types
    default:
      assert(false);
  }
}

std::string Column::FieldTypeString() {
  enum_field_types field_type = DDType2FieldType(dd_type_);
  switch (field_type) {
    case MYSQL_TYPE_DECIMAL:
      return "DECIMAL";
    case MYSQL_TYPE_TINY:
      return "TINY";
    case MYSQL_TYPE_SHORT:
      return "SHORT";
    case MYSQL_TYPE_LONG:
      return "LONG";
    case MYSQL_TYPE_FLOAT:
      return "FLOAT";
    case MYSQL_TYPE_DOUBLE:
      return "DOUBLE";
    case MYSQL_TYPE_NULL:
      return "NULL";
    case MYSQL_TYPE_TIMESTAMP:
      return "TIMESTAMP";
    case MYSQL_TYPE_LONGLONG:
      return "LONGLONG";
    case MYSQL_TYPE_INT24:
      return "INT24";
    case MYSQL_TYPE_DATE:
      return "DATE";
    case MYSQL_TYPE_TIME:
      return "TIME";
    case MYSQL_TYPE_DATETIME:
      return "DATETIME";
    case MYSQL_TYPE_YEAR:
      return "YEAR";
    case MYSQL_TYPE_NEWDATE:
      return "NEWDATE";
    case MYSQL_TYPE_VARCHAR:
      return "VARCHAR";
    case MYSQL_TYPE_BIT:
      return "BIT";
    case MYSQL_TYPE_TIMESTAMP2:
      return "TIMESTAMP2";
    case MYSQL_TYPE_DATETIME2:
      return "DATETIME2";
    case MYSQL_TYPE_TIME2:
      return "TIME2";
    case MYSQL_TYPE_TYPED_ARRAY:
      return "TYPED_ARRAY";
    case MYSQL_TYPE_INVALID:
      return "INVALID";
    case MYSQL_TYPE_BOOL:
      return "BOOL";
    case MYSQL_TYPE_JSON:
      return "JSON";
    case MYSQL_TYPE_NEWDECIMAL:
      return "NEWDECIMAL";
    case MYSQL_TYPE_ENUM:
      return "ENUM";
    case MYSQL_TYPE_SET:
      return "SET";
    case MYSQL_TYPE_TINY_BLOB:
      return "TINY_BLOB";
    case MYSQL_TYPE_MEDIUM_BLOB:
      return "MEDIUM_BLOB";
    case MYSQL_TYPE_LONG_BLOB:
      return "LONG_BLOB";
    case MYSQL_TYPE_BLOB:
      return "BLOB";
    case MYSQL_TYPE_VAR_STRING:
      return "VAR_STRING";
    case MYSQL_TYPE_STRING:
      return "STRING";
    case MYSQL_TYPE_GEOMETRY:
      return "GEOMETRY";
    default:
      return "UNKNOWN";
  }
}

std::string Column::SeTypeString() {
  switch (ib_mtype_) {
    case DATA_VARCHAR:
      return "DATA_VARCHAR";
    case DATA_CHAR:
      return "DATA_CHAR";
    case DATA_FIXBINARY:
      return "DATA_FIXBINARY";
    case DATA_BINARY:
      return "DATA_BINARY";
    case DATA_BLOB:
      return "DATA_BLOB";
    case DATA_INT:
      return "DATA_INT";
    case DATA_SYS:
      return "DATA_SYS";
    case DATA_FLOAT:
      return "DATA_FLOAT";
    case DATA_DOUBLE:
      return "DATA_DOUBLE";
    case DATA_DECIMAL:
      return "DATA_DECIMAL";
    case DATA_VARMYSQL:
      return "DATA_VARMYSQL";
    case DATA_MYSQL:
      return "DATA_MYSQL";
    case DATA_GEOMETRY:
      return "DATA_GEOMETRY";
    case DATA_POINT:
      return "DATA_POINT";
    case DATA_VAR_POINT:
      return "DATA_VAR_POINT";
    default:
      return "UNKNOWN";
  }
}

Column::enum_field_types Column::FieldType() const {
  switch (DDType2FieldType(dd_type_)) {
    /*
     * 30 in total
     * Missing MYSQL_TYPE_DATE ?
     */
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
      // Field_string -> Field_longstr -> Field_str
      return MYSQL_TYPE_STRING;
    case MYSQL_TYPE_VARCHAR:
      // Field_varstring -> Field_longstr -> Field_str
      return MYSQL_TYPE_VARCHAR;
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
      // Field_blob -> Field_longstr -> Field_str
      return MYSQL_TYPE_BLOB;
    case MYSQL_TYPE_GEOMETRY:
      // Field_geom -> Field_blob -> Field_longstr -> Field_str
      return MYSQL_TYPE_GEOMETRY;
    case MYSQL_TYPE_JSON:
      // Field_json -> Field_blob -> Field_longstr -> Field_str
      return MYSQL_TYPE_JSON;
    case MYSQL_TYPE_ENUM:
      // Field_enum -> Field_str
      return MYSQL_TYPE_STRING;
    case MYSQL_TYPE_SET:
      // Field_set -> Field_enum -> Field_str
      return MYSQL_TYPE_STRING;
    case MYSQL_TYPE_DECIMAL:
      // Field_decimal -> Field_real -> Field_num
      return MYSQL_TYPE_DECIMAL;
    case MYSQL_TYPE_NEWDECIMAL:
      // Field_new_decimal -> Field_num
      return MYSQL_TYPE_NEWDECIMAL;
    case MYSQL_TYPE_FLOAT:
      // Field_float -> Field_real -> Field_num
      return MYSQL_TYPE_FLOAT;
    case MYSQL_TYPE_DOUBLE:
      // Field_double -> Field_real -> Field_num
      return MYSQL_TYPE_DOUBLE;
    case MYSQL_TYPE_TINY:
      // Field_tiny -> Field_num
      return MYSQL_TYPE_TINY;
    case MYSQL_TYPE_SHORT:
      // Field_short -> Field_num
      return MYSQL_TYPE_SHORT;
    case MYSQL_TYPE_INT24:
      // Field_medium -> Field_num
      return MYSQL_TYPE_INT24;
    case MYSQL_TYPE_LONG:
      // Field_long -> Field_num
      return MYSQL_TYPE_LONG;
    case MYSQL_TYPE_LONGLONG:
      // Field_longlong -> Field_num
      return MYSQL_TYPE_LONGLONG;
    case MYSQL_TYPE_TIMESTAMP:
      // Field_timestamp -> Field_temporal_with_date_and_time
      //                 -> Field_temporal_with_date
      //                 -> Field_temporal
      return MYSQL_TYPE_TIMESTAMP;
    case MYSQL_TYPE_TIMESTAMP2:
      // Field_timestampf -> Field_temporal_with_date_and_timef
      //                  -> Field_temporal_with_date_and_time
      //                  -> Field_temporal_with_date
      //                  -> Field_temporal
      // !!!
      return MYSQL_TYPE_TIMESTAMP;
    case MYSQL_TYPE_YEAR:
      // Field_year -> Field_tiny -> Field_num
      return MYSQL_TYPE_YEAR;
    case MYSQL_TYPE_NEWDATE:
      // Field_newdate -> Field_temporal_with_date
      //               -> Field_temporal
      // !!!
      return MYSQL_TYPE_DATE;
    case MYSQL_TYPE_TIME:
      // Field_time -> Field_time_common
      //            -> Field_temporal
      return MYSQL_TYPE_TIME;
    case MYSQL_TYPE_TIME2:
      // Field_timef -> Field_time_common
      //             -> Field_temporal
      // !!!
      return MYSQL_TYPE_TIME;
    case MYSQL_TYPE_DATETIME:
      // Field_datetime -> Field_temporal_with_date_and_time
      //                -> Field_temporal_with_date
      //                -> Field_temporal
      return MYSQL_TYPE_DATETIME;
    case MYSQL_TYPE_DATETIME2:
      // Field_datetimef -> Field_temporal_with_date_and_timef
      //                 -> Field_temporal_with_date_and_time
      //                 -> Field_temporal_with_date
      //                 -> Field_temporal
      // !!!
      return MYSQL_TYPE_DATETIME;
    case MYSQL_TYPE_NULL:
      // Field_null -> Field_str
      return MYSQL_TYPE_NULL;
    case MYSQL_TYPE_BIT:
      // (Field_bit_as_char) -> Field_bit
      return MYSQL_TYPE_BIT;
    // TODO(Zhao): support other types

    // These 2 are not valid user defined in DD
    case MYSQL_TYPE_INVALID:
    case MYSQL_TYPE_BOOL:
    default:
      assert(false);
  }
}

uint32_t Column::FieldType2SeType() const {
  // Check real_type()
  if (dd_type_ == enum_column_types::ENUM ||
      dd_type_ == enum_column_types::SET) {
    return DATA_INT;
  }

  switch (FieldType()) {
    case MYSQL_TYPE_VAR_STRING: /* old <= 4.1 VARCHAR */
    case MYSQL_TYPE_VARCHAR:    /* new >= 5.0.3 true VARCHAR */
      if (IsBinary()) {
        return (DATA_BINARY);
      } else if (dd_collation_id_ == 8 /* my_charset_latin1 */) {
        return (DATA_VARCHAR);
      } else {
        return (DATA_VARMYSQL);
      }
    case MYSQL_TYPE_BIT:
    case MYSQL_TYPE_STRING:
      if (IsBinary()) {
        return (DATA_FIXBINARY);
      } else if (dd_collation_id_ == 8 /* my_charset_latin1 */) {
        return (DATA_CHAR);
      } else {
        return (DATA_MYSQL);
      }
    case MYSQL_TYPE_NEWDECIMAL:
      return (DATA_FIXBINARY);
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_BOOL:
      return (DATA_INT);
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIMESTAMP2:
      // Check real_type()
      switch (dd_type_) {
        case enum_column_types::TIME:
        case enum_column_types::DATETIME:
        case enum_column_types::TIMESTAMP:
          return (DATA_INT);
        default:
          [[fallthrough]];
        case enum_column_types::TIME2:
        case enum_column_types::DATETIME2:
        case enum_column_types::TIMESTAMP2:
          return (DATA_FIXBINARY);
      }
    case MYSQL_TYPE_FLOAT:
      return (DATA_FLOAT);
    case MYSQL_TYPE_DOUBLE:
      return (DATA_DOUBLE);
    case MYSQL_TYPE_DECIMAL:
      return (DATA_DECIMAL);
    case MYSQL_TYPE_GEOMETRY:
      return (DATA_GEOMETRY);
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_JSON:  // JSON fields are stored as BLOBs
      return (DATA_BLOB);
    case MYSQL_TYPE_NULL:
      break;
    default:
      assert(false);
  }
  return 0;
}

#define DIG_PER_DEC1 9
#define PORTABLE_SIZEOF_CHAR_PTR 8
static const int dig2bytes[DIG_PER_DEC1 + 1] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};

uint32_t Column::PackLength() const {
  switch (DDType2FieldType(dd_type_)) {
    /*
     * 30 in total
     */
    case MYSQL_TYPE_VAR_STRING:
      return dd_char_length_;
    case MYSQL_TYPE_STRING:
      return dd_char_length_;
    case MYSQL_TYPE_VARCHAR:
      return VarcharLenBytes() + dd_char_length_;
    case MYSQL_TYPE_BLOB:
      return 2 + PORTABLE_SIZEOF_CHAR_PTR;
    case MYSQL_TYPE_MEDIUM_BLOB:
      return 3 + PORTABLE_SIZEOF_CHAR_PTR;
    case MYSQL_TYPE_TINY_BLOB:
      return 1 + PORTABLE_SIZEOF_CHAR_PTR;
    case MYSQL_TYPE_LONG_BLOB:
      return 4 + PORTABLE_SIZEOF_CHAR_PTR;
    case MYSQL_TYPE_GEOMETRY:
      return 4 + PORTABLE_SIZEOF_CHAR_PTR;
    case MYSQL_TYPE_JSON:
      return 4 + PORTABLE_SIZEOF_CHAR_PTR;
    case MYSQL_TYPE_ENUM:
      return dd_elements_size_tmp_ < 256 ? 1 : 2;
    case MYSQL_TYPE_SET: {
      uint64_t len = (dd_elements_size_tmp_ + 7) / 8;
      return len > 4 ? 8 : len;
      }
      [[fallthrough]];
    case MYSQL_TYPE_DECIMAL:
      return dd_char_length_;
    case MYSQL_TYPE_NEWDECIMAL: {
      int precision = dd_numeric_precision_;
      int scale = dd_numeric_scale_;
      int intg = precision - scale;
      int intg0 = intg / DIG_PER_DEC1;
      int frac0 = scale / DIG_PER_DEC1;
      int intg0x = intg - intg0 * DIG_PER_DEC1;
      int frac0x = scale - frac0 * DIG_PER_DEC1;

      assert(scale >= 0 && precision > 0 &&
             scale <= precision);
      assert(intg0x >= 0);
      assert(intg0x <= DIG_PER_DEC1);
      assert(frac0x >= 0);
      assert(frac0x <= DIG_PER_DEC1);
      return intg0 * sizeof(int32_t) + dig2bytes[intg0x] +
             frac0 * sizeof(int32_t) + dig2bytes[frac0x];
      }
      [[fallthrough]];
    case MYSQL_TYPE_FLOAT:
      return sizeof(float);
    case MYSQL_TYPE_DOUBLE:
      return sizeof(double);
    case MYSQL_TYPE_TINY:
      return 1;
    case MYSQL_TYPE_SHORT:
      return 2;
    case MYSQL_TYPE_INT24:
      return 3;
    case MYSQL_TYPE_LONG:
      return 4;
    case MYSQL_TYPE_LONGLONG:
      return 8;
    case MYSQL_TYPE_TIMESTAMP:
      return dd_char_length_;
    case MYSQL_TYPE_TIMESTAMP2:
      return 4 + (dd_datetime_precision_ + 1) / 2;
    case MYSQL_TYPE_YEAR:
      return 1;
    case MYSQL_TYPE_NEWDATE:
      return 3;
    case MYSQL_TYPE_TIME:
      return 3;
    case MYSQL_TYPE_TIME2:
      return 3 + (dd_datetime_precision_ + 1) / 2;
    case MYSQL_TYPE_DATETIME:
      return 8;
    case MYSQL_TYPE_DATETIME2:
      return 5 + (dd_datetime_precision_ + 1) / 2;
    case MYSQL_TYPE_NULL:
      return 0;
    case MYSQL_TYPE_BIT:
      // treat_bit_as_char should be true
      return (dd_char_length_ + 7) / 8;
    // TODO(Zhao): support other types

    // These 2 are not valid user defined in DD
    case MYSQL_TYPE_INVALID:
    case MYSQL_TYPE_BOOL:
    default:
      assert(false);
  }
}

bool Column::IsBinary() const {
  switch (FieldType()) {
    // For Field_str
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_GEOMETRY:
    case MYSQL_TYPE_JSON:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_NULL:
      return (dd_collation_id_ == 63 /* my_charset_bin */);
    default:
      return true;
  }
}

bool Column::IsColumnAdded() const {
  if (dd_se_private_data_.Exists("version_added")) {
    return true;
  } else {
    return false;
  }
}

uint32_t Column::GetVersionAdded() const {
  uint32_t version = UINT8_UNDEFINED;
  if (!IsColumnAdded()) {
    return version;
  }
  dd_se_private_data_.Get("version_added", &version);
  return version;
}

bool Column::IsInstantAdded() const {
  if (ib_version_added_ != UINT8_UNDEFINED &&
      ib_version_added_ > 0) {
    return true;
  }
  return false;
}

bool Column::IsColumnDropped() const {
  if (dd_se_private_data_.Exists("version_dropped")) {
    return true;
  } else {
    return false;
  }
}

uint32_t Column::GetVersionDropped() const {
  uint32_t version = UINT8_UNDEFINED;
  if (!IsColumnDropped()) {
    return version;
  }
  dd_se_private_data_.Get("version_dropped", &version);
  return version;
}

bool Column::IsInstantDropped() const {
  if (ib_version_dropped_ != UINT8_UNDEFINED &&
      ib_version_dropped_ > 0) {
    return true;
  } else {
    return false;
  }
}

uint32_t Column::GetFixedSize() {
  switch (ib_mtype_) {
    case DATA_SYS:
    case DATA_CHAR:
    case DATA_FIXBINARY:
    case DATA_INT:
    case DATA_FLOAT:
    case DATA_DOUBLE:
    case DATA_POINT:
      return ib_col_len_;
    case DATA_MYSQL:
      if (IsBinary()) {
        return ib_col_len_;
      // TODO(Zhao): support redundant row format
      } else {
        auto iter = g_collation_map.find(dd_collation_id_);
        assert(iter != g_collation_map.end());
        if (iter->second.min == iter->second.max) {
          return ib_col_len_;
        }
      }
      [[fallthrough]];
    case DATA_VARCHAR:
    case DATA_BINARY:
    case DATA_DECIMAL:
    case DATA_VARMYSQL:
    case DATA_VAR_POINT:
    case DATA_GEOMETRY:
    case DATA_BLOB:
      return (0);
    default:
      assert(0);
  }
}

bool Column::IsDroppedInOrBefore(uint8_t version) const {
  if (!IsInstantDropped()) {
    return false;
  }

  return (GetVersionDropped() <= version);
}

bool Column::IsAddedAfter(uint8_t version) const {
  if (!IsInstantAdded()) {
    return false;
  }

  return (GetVersionAdded() > version);
}

bool Column::IsBigCol() const {
  return (ib_col_len_ > 255 ||
          (ib_mtype_ == DATA_BLOB || ib_mtype_ == DATA_VAR_POINT ||
           ib_mtype_ == DATA_GEOMETRY));
}

/* ------ IndexColumn ------ */
bool IndexColumn::Init(const rapidjson::Value& dd_index_col_obj,
                              const std::vector<Column*>& columns) {
  Read(&dd_ordinal_position_, dd_index_col_obj, "ordinal_position");
  Read(&dd_length_, dd_index_col_obj, "length");
  ReadEnum(&dd_order_, dd_index_col_obj, "order");
  Read(&dd_hidden_, dd_index_col_obj, "hidden");
  Read(&dd_column_opx_, dd_index_col_obj, "column_opx");

  /*
   * Points to the corresponding Column object.
   * NOTICE:
   * The ordinal_position of a Column starts from 1, while
   * the column_opx of an IndexColumn starts from 0.
   * However, this is not an issue, as the columns array starts from 0.
   */
  column_ = columns[dd_column_opx_];
  column_->set_index_column(this);
  return true;
}

IndexColumn* IndexColumn::CreateIndexColumn(
                        const rapidjson::Value& dd_index_col_obj,
                        const std::vector<Column*>& columns) {
  IndexColumn* element = new IndexColumn(false);
  bool init_ret = element->Init(dd_index_col_obj, columns);
  if (!init_ret) {
    delete element;
    element = nullptr;
  }
  return element;
}

// Used only when creating index columns for a dropped column.
IndexColumn* IndexColumn::CreateIndexDroppedColumn(Column* dropped_col) {
  IndexColumn* index_column = new IndexColumn(true);
  index_column->set_column(dropped_col);
  dropped_col->set_index_column(index_column);

  return index_column;
}

// Used only when creating a FTS_DOC_ID index column.
IndexColumn* IndexColumn::CreateIndexFTSDocIdColumn(Column* doc_id_col) {
  IndexColumn* index_column = new IndexColumn(true);
  index_column->set_column(doc_id_col);
  doc_id_col->set_index_column(index_column);

  return index_column;
}

}  // namespace ibd_ninja
