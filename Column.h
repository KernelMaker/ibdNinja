/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#ifndef COLUMN_H_
#define COLUMN_H_

#include "ibdUtils.h"
#include "Properties.h"

#include <rapidjson/document.h>

#include <iostream>
#include <optional>
#include <vector>
#include <string>
#include <set>

namespace ibd_ninja {

class IndexColumn;
class Column {
 public:
  static Column* CreateColumn(const rapidjson::Value& dd_col_obj);
  // Only used in creating SE system column
  Column(std::string name, uint32_t ind) :
         dd_name_(name), dd_is_nullable_(false),
         dd_is_virtual_(false), ib_ind_(ind),
         ib_mtype_(DATA_SYS), ib_is_visible_(false),
         ib_version_added_(0), ib_version_dropped_(0),
         ib_phy_pos_(UINT32_UNDEFINED),
         se_explicit_(true),
         index_column_(nullptr) {
  }
  // Only used in creating FTS_DOC_ID column
  Column(std::string name, uint32_t ind, bool fts) :
         dd_name_(name), dd_is_nullable_(false),
         dd_is_virtual_(false), ib_ind_(ind),
         ib_mtype_(DATA_INT), ib_is_visible_(false),
         ib_version_added_(UINT8_UNDEFINED),
         ib_version_dropped_(UINT8_UNDEFINED),
         ib_phy_pos_(UINT32_UNDEFINED),
         se_explicit_(true),
         index_column_(nullptr) {
           assert(fts);
  }
  void DebugDump(int space = 0) {
    std::string space_str(space, ' ');
    std::cout << space_str << "[" << std::endl;
    std::cout << space_str << "Dump Column:" << std::endl;
    std::cout << space_str << "  " << "name: "
              << dd_name_ << std::endl
              << space_str << "  " << "type: "
              << dd_type_ << std::endl
              << space_str << "  " << "is_nullable: "
              << dd_is_nullable_ << std::endl
              << space_str << "  " << "is_zerofill: "
              << dd_is_zerofill_ << std::endl
              << space_str << "  " << "is_unsigned: "
              << dd_is_unsigned_ << std::endl
              << space_str << "  " << "is_auto_increment: "
              << dd_is_auto_increment_ << std::endl
              << space_str << "  " << "is_virtual: "
              << dd_is_virtual_ << std::endl
              << space_str << "  " << "hidden: "
              << dd_hidden_ << std::endl
              << space_str << "  " << "ordinal_position: "
              << dd_ordinal_position_ << std::endl
              << space_str << "  " << "char_length: "
              << dd_char_length_ << std::endl
              << space_str << "  " << "numeric_precision: "
              << dd_numeric_precision_ << std::endl
              << space_str << "  " << "numeric_scale: "
              << dd_numeric_scale_ << std::endl
              << space_str << "  " << "numeric_scale_null: "
              << dd_numeric_scale_null_ << std::endl
              << space_str << "  " << "datetime_precision: "
              << dd_datetime_precision_ << std::endl
              << space_str << "  " << "datetime_precision_null: "
              << dd_datetime_precision_null_ << std::endl
              << space_str << "  " << "has_no_default: "
              << dd_has_no_default_ << std::endl
              << space_str << "  " << "default_value_null: "
              << dd_default_value_null_ << std::endl
              << space_str << "  " << "srs_id_null: "
              << dd_srs_id_null_ << std::endl
              << space_str << "  " << "srs_id: "
              << (dd_srs_id_.has_value() ? dd_srs_id_.value() : 0) << std::endl
              << space_str << "  " << "default_value: "
              << dd_default_value_ << std::endl
              << space_str << "  " << "default_value_utf8_null: "
              << dd_default_value_utf8_null_ << std::endl
              << space_str << "  " << "default_value_utf8"
              << dd_default_value_utf8_ << std::endl
              << space_str << "  " << "default_option: "
              << dd_default_option_ << std::endl
              << space_str << "  " << "update_option: "
              << dd_update_option_ << std::endl
              << space_str << "  " << "comment: "
              << dd_comment_ << std::endl
              << space_str << "  " << "generation_expression: "
              << dd_generation_expression_ << std::endl
              << space_str << "  " << "generation_expression_utf8: "
              << dd_generation_expression_utf8_ << std::endl;
    std::cout << space_str << "  " << "options: " << std::endl;
    dd_options_.DebugDump(space + 4);
    std::cout << space_str << "  " << "se_private_data: " << std::endl;
    dd_se_private_data_.DebugDump(space + 4);

    std::cout << space_str << "  " << "engine_attribute: "
              << dd_engine_attribute_ << std::endl
              << space_str << "  " << "secondary_engine_attribute: "
              << dd_secondary_engine_attribute_ << std::endl
              << space_str << "  " << "column_key: "
              << dd_column_key_ << std::endl
              << space_str << "  " << "column_type_utf8: "
              << dd_column_type_utf8_ << std::endl
              // TODO(Zhao):
              // dd_elements_
              << space_str << "  " << "collation_id: "
              << dd_collation_id_ << std::endl
              << space_str << "  " << "is_explicit_collation: "
              << dd_is_explicit_collation_ << std::endl;
    std::cout << space_str << "]" << std::endl;
  }
  enum enum_column_types {
    // 32 in total
    DECIMAL = 1,
    TINY,
    SHORT,
    LONG,
    FLOAT,
    DOUBLE,
    TYPE_NULL,
    TIMESTAMP,
    LONGLONG,
    INT24,
    DATE,
    TIME,
    DATETIME,
    YEAR,
    NEWDATE,
    VARCHAR,
    BIT,
    TIMESTAMP2,
    DATETIME2,
    TIME2,
    NEWDECIMAL,
    ENUM,
    SET,
    TINY_BLOB,
    MEDIUM_BLOB,
    LONG_BLOB,
    BLOB,
    VAR_STRING,
    STRING,
    GEOMETRY,
    JSON,
    VECTOR  // MySQL 9.0+, dd_type = 32
  };
  enum enum_column_key {
    CK_NONE = 1,
    CK_PRIMARY,
    CK_UNIQUE,
    CK_MULTIPLE
  };
  enum enum_hidden_type {
    HT_VISIBLE = 1,
    HT_HIDDEN_SE = 2,
    HT_HIDDEN_SQL = 3,
    HT_HIDDEN_USER = 4
  };
  enum enum_field_types {
    /*
     * 34 in total, the below 3 are different with enum_column_fields
     * MYSQL_TYPE_TYPED_ARRAY
     * MYSQL_TYPE_INVALID
     * MYSQL_TYPE_BOOL
     */
    MYSQL_TYPE_DECIMAL,
    MYSQL_TYPE_TINY,
    MYSQL_TYPE_SHORT,
    MYSQL_TYPE_LONG,
    MYSQL_TYPE_FLOAT,
    MYSQL_TYPE_DOUBLE,
    MYSQL_TYPE_NULL,
    MYSQL_TYPE_TIMESTAMP,
    MYSQL_TYPE_LONGLONG,
    MYSQL_TYPE_INT24,
    MYSQL_TYPE_DATE,
    MYSQL_TYPE_TIME,
    MYSQL_TYPE_DATETIME,
    MYSQL_TYPE_YEAR,
    MYSQL_TYPE_NEWDATE, /**< Internal to MySQL. Not used in protocol */
    MYSQL_TYPE_VARCHAR,
    MYSQL_TYPE_BIT,
    MYSQL_TYPE_TIMESTAMP2,
    MYSQL_TYPE_DATETIME2,   /**< Internal to MySQL. Not used in protocol */
    MYSQL_TYPE_TIME2,       /**< Internal to MySQL. Not used in protocol */
    MYSQL_TYPE_TYPED_ARRAY, /**< Used for replication only */
    MYSQL_TYPE_VECTOR = 242, /**< MySQL 9.0+ VECTOR type */
    MYSQL_TYPE_INVALID = 243,
    MYSQL_TYPE_BOOL = 244, /**< Currently just a placeholder */
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255
  };
  static const std::set<std::string> default_valid_option_keys;

  const std::string& name() const {
    return dd_name_;
  }
  enum_column_types type() const {
    return dd_type_;
  }
  // Only used in setting SE DB_TRX_ID column type
  void set_type(enum_column_types type) {
    dd_type_ = type;
  }
  bool is_nullable() const {
    return dd_is_nullable_;
  }
  bool is_virtual() const {
    return dd_is_virtual_;
  }
  enum_hidden_type hidden() const {
    return dd_hidden_;
  }
  bool IsSeHidden() const {
    return dd_hidden_ == enum_hidden_type::HT_HIDDEN_SE;
  }
  const Properties& options() const {
    return dd_options_;
  }
  const Properties& se_private_data() const {
    return dd_se_private_data_;
  }

  /*------TABLE SHARE------*/
  static enum_field_types DDType2FieldType(enum_column_types);
  enum_field_types FieldType() const;
  bool IsBinary() const;
  uint32_t PackLength() const;
  static uint32_t VarcharLenBytes(uint32_t char_length) {
    return ((char_length) < 256 ? 1 : 2);
  }
  uint32_t VarcharLenBytes() const {
    return ((dd_char_length_) < 256 ? 1 : 2);
  }

  std::string FieldTypeString();

  /*------SE------*/
  bool IsSystemColumn() const {
    return (dd_name_ == "DB_ROW_ID" || dd_name_ == "DB_TRX_ID" ||
            dd_name_ == "DB_ROLL_PTR");
  }
  bool IsColumnAdded() const;
  uint32_t GetVersionAdded() const;
  bool IsInstantAdded() const;
  bool IsColumnDropped() const;
  uint32_t GetVersionDropped() const;
  bool IsInstantDropped() const;
  uint32_t FieldType2SeType() const;
  std::string SeTypeString();

  uint32_t ib_ind() {
    return ib_ind_;
  }
  void set_ib_ind(uint32_t ind) {
    ib_ind_ = ind;
  }
  std::string dd_column_type_utf8() {
    return dd_column_type_utf8_;
  }
  uint32_t ib_mtype() {
    return ib_mtype_;
  }
  void set_ib_mtype(uint32_t m_type) {
    ib_mtype_ = m_type;
  }
  uint32_t ib_is_visible() {
    return ib_is_visible_;
  }
  void set_ib_is_visible(bool ib_is_visible) {
    ib_is_visible_ = ib_is_visible;
  }
  uint32_t ib_version_added() {
    return ib_version_added_;
  }
  void set_ib_version_added(uint32_t version_added) {
    ib_version_added_ = version_added;
  }
  uint32_t ib_version_dropped() {
    return ib_version_dropped_;
  }
  void set_ib_version_dropped(uint32_t version_dropped) {
    ib_version_dropped_ = version_dropped;
  }
  uint32_t ib_phy_pos() {
    return ib_phy_pos_;
  }
  void set_ib_phy_pos(uint32_t phy_pos) {
    ib_phy_pos_ = phy_pos;
  }
  uint32_t ib_col_len() {
    return ib_col_len_;
  }
  void set_ib_col_len(uint32_t col_len) {
    ib_col_len_ = col_len;
  }
  IndexColumn* index_column() {
    return index_column_;
  }
  void set_index_column(IndexColumn* index_column) {
    index_column_ = index_column;
  }
  void set_ib_instant_default(bool instant_default) {
    ib_instant_default_ = instant_default;
  }
  bool ib_instant_default() {
    return ib_instant_default_;
  }
  bool is_array() const {
    return is_array_;
  }
  bool se_explicit() {
    return se_explicit_;
  }

  uint32_t GetFixedSize();
  bool IsDroppedInOrBefore(uint8_t version) const;
  bool IsAddedAfter(uint8_t version) const;
  bool IsBigCol() const;

 private:
  Column() : dd_options_(default_valid_option_keys),
             dd_se_private_data_(),
             se_explicit_(false), index_column_(nullptr) {
  }
  bool Init(const rapidjson::Value& dd_col_obj);
  std::string dd_name_;
  enum_column_types dd_type_;
  bool dd_is_nullable_;
  bool dd_is_zerofill_;
  bool dd_is_unsigned_;
  bool dd_is_auto_increment_;
  bool dd_is_virtual_;
  enum_hidden_type dd_hidden_;
  uint32_t dd_ordinal_position_;
  uint32_t dd_char_length_;
  uint32_t dd_numeric_precision_;
  uint32_t dd_numeric_scale_;
  bool dd_numeric_scale_null_;
  uint32_t dd_datetime_precision_;
  uint32_t dd_datetime_precision_null_;
  bool dd_has_no_default_;
  bool dd_default_value_null_;
  bool dd_srs_id_null_;
  std::optional<std::uint32_t> dd_srs_id_;
  std::string dd_default_value_;
  bool dd_default_value_utf8_null_;
  std::string dd_default_value_utf8_;
  std::string dd_default_option_;
  std::string dd_update_option_;
  std::string dd_comment_;
  std::string dd_generation_expression_;
  std::string dd_generation_expression_utf8_;
  Properties dd_options_;
  Properties dd_se_private_data_;
  std::string dd_engine_attribute_;
  std::string dd_secondary_engine_attribute_;
  enum_column_key dd_column_key_;
  std::string dd_column_type_utf8_;
  // TODO(Zhao):
  // Column_type_element_collection dd_elements_;
  uint64_t dd_elements_size_tmp_;
  uint64_t dd_collation_id_;
  bool dd_is_explicit_collation_;

  /*------SE------*/
  uint32_t ib_ind_;
  uint32_t ib_mtype_;
  bool ib_is_visible_;
  uint32_t ib_version_added_;
  uint32_t ib_version_dropped_;
  uint32_t ib_phy_pos_;
  uint32_t ib_col_len_;
  bool ib_instant_default_;
  bool is_array_ = false;

  bool se_explicit_;
  IndexColumn* index_column_;
};

class IndexColumn {
 public:
  static IndexColumn* CreateIndexColumn(
                         const rapidjson::Value& dd_index_col_obj,
                         const std::vector<Column*>& columns);
  // Used only when creating index columns for a dropped column.
  static IndexColumn* CreateIndexDroppedColumn(Column* dropped_col);
  // Used only when creating a FTS_DOC_ID index column.
  static IndexColumn* CreateIndexFTSDocIdColumn(Column* doc_id_col);
  void DebugDump(int space = 0) {
    std::string space_str(space, ' ');
    std::cout << space_str << "[" << std::endl;

    std::cout << space_str << "Dump IndexColumn:" << std::endl;
    std::cout << space_str << "  " << "ordinal_position: "
              << dd_ordinal_position_ << std::endl
              << space_str << "  " << "length: "
              << dd_length_ << std::endl
              << space_str << "  " << "order: "
              << dd_order_ << std::endl
              << space_str << "  " << "hidden: "
              << dd_hidden_ << std::endl;

    std::cout << space_str << "]" << std::endl;
  }
  enum enum_index_element_order {
    ORDER_UNDEF = 1,
    ORDER_ASC,
    ORDER_DESC
  };

  Column* column() const {
    return column_;
  }
  uint32_t length() const {
    return dd_length_;
  }
  bool hidden() const {
    return dd_hidden_;
  }
  uint32_t ib_fixed_len() {
    return ib_fixed_len_;
  }
  void set_ib_fixed_len(uint32_t fixed_len) {
    ib_fixed_len_ = fixed_len;
  }
  void set_column(Column* column) {
    column_ = column;
  }
  bool se_explicit() {
    return se_explicit_;
  }

 private:
  explicit IndexColumn(bool se_explicit) : se_explicit_(se_explicit),
    column_(nullptr) {
  }
  bool Init(const rapidjson::Value& dd_index_col_obj,
            const std::vector<Column*>& columns);
  uint32_t dd_ordinal_position_;
  uint32_t dd_length_;
  enum_index_element_order dd_order_;
  bool dd_hidden_;
  uint32_t dd_column_opx_;

  /*------SE------*/
  uint32_t ib_fixed_len_;

  bool se_explicit_;

  Column* column_;
};

}  // namespace ibd_ninja

#endif  // COLUMN_H_
