/*
 * Copyright (c) [2025-2026] [Zhao Song]
 *
 * MySQL Binary JSON (JSONB) decoder implementation.
 *
 * Binary format (little-endian):
 * - Objects/Arrays: [type 1B][count 2/4B][size 2/4B][key_entries...][value_entries...][data...]
 * - Key entries: [offset 2/4B][length 2B]
 * - Value entries: [type 1B][offset_or_inlined_value 2/4B]
 * - Strings: [var_length][utf8_data]
 * - Inlineable (small): LITERAL, INT16, UINT16
 * - Inlineable (large): LITERAL, INT16, UINT16, INT32, UINT32
 */
#include "JsonBinary.h"

#include <cinttypes>
#include <cmath>
#include <cstring>
#include <sstream>

namespace ibd_ninja {

// Read little-endian values
static uint16_t ReadLE2(const unsigned char* p) {
  return static_cast<uint16_t>(p[0]) |
         (static_cast<uint16_t>(p[1]) << 8);
}

static uint32_t ReadLE4(const unsigned char* p) {
  return static_cast<uint32_t>(p[0]) |
         (static_cast<uint32_t>(p[1]) << 8) |
         (static_cast<uint32_t>(p[2]) << 16) |
         (static_cast<uint32_t>(p[3]) << 24);
}

static uint64_t ReadLE8(const unsigned char* p) {
  return static_cast<uint64_t>(ReadLE4(p)) |
         (static_cast<uint64_t>(ReadLE4(p + 4)) << 32);
}

// Read variable-length encoded size (7 bits per byte, high bit = continuation)
static bool ReadVariableLength(const unsigned char* data, size_t data_len,
                               size_t pos, uint32_t* out_len,
                               size_t* bytes_consumed) {
  uint32_t len = 0;
  size_t consumed = 0;
  for (size_t i = 0; i < 5 && pos + i < data_len; i++) {
    uint8_t b = data[pos + i];
    len |= static_cast<uint32_t>(b & 0x7F) << (7 * i);
    consumed++;
    if ((b & 0x80) == 0) {
      *out_len = len;
      *bytes_consumed = consumed;
      return true;
    }
  }
  return false;
}

// Escape a string for JSON output
static std::string EscapeJsonString(const unsigned char* s, size_t len) {
  std::string out;
  out.reserve(len + 2);
  out += '"';
  for (size_t i = 0; i < len; i++) {
    unsigned char c = s[i];
    switch (c) {
      case '"':  out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\b': out += "\\b"; break;
      case '\f': out += "\\f"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (c < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += static_cast<char>(c);
        }
        break;
    }
  }
  out += '"';
  return out;
}

// Read offset or size: 2 bytes for small format, 4 bytes for large format
static uint32_t ReadOffsetOrSize(const unsigned char* p, bool large) {
  return large ? ReadLE4(p) : ReadLE2(p);
}

// Check if a type can be inlined in a value entry
static bool IsInlineable(uint8_t type, bool large) {
  switch (type) {
    case JSONB_TYPE_LITERAL:
    case JSONB_TYPE_INT16:
    case JSONB_TYPE_UINT16:
      return true;
    case JSONB_TYPE_INT32:
    case JSONB_TYPE_UINT32:
      return large;
    default:
      return false;
  }
}

// Forward declaration
static bool DecodeValue(const unsigned char* data, size_t data_len,
                        uint8_t type, size_t value_offset,
                        std::string* out);

static bool DecodeObjectOrArray(const unsigned char* data, size_t data_len,
                                size_t offset, bool is_object, bool large,
                                std::string* out) {
  size_t offset_size = large ? 4 : 2;
  // Need at least count + size
  if (offset + 2 * offset_size > data_len) {
    *out = "\"<truncated>\"";
    return false;
  }

  uint32_t count = ReadOffsetOrSize(data + offset, large);
  uint32_t total_size = ReadOffsetOrSize(data + offset + offset_size, large);
  (void)total_size;

  size_t header_size = 2 * offset_size;

  // Key entries for objects: [offset][length_2B] per key
  size_t key_entry_size = offset_size + 2;
  // Value entries: [type_1B][offset_or_inline]
  size_t value_entry_size = 1 + offset_size;

  size_t key_entries_offset = offset + header_size;
  size_t value_entries_offset = key_entries_offset +
                                (is_object ? count * key_entry_size : 0);

  std::string result;
  result += is_object ? '{' : '[';

  for (uint32_t i = 0; i < count; i++) {
    if (i > 0) result += ',';

    if (is_object) {
      // Read key entry
      size_t ke_off = key_entries_offset + i * key_entry_size;
      if (ke_off + key_entry_size > data_len) {
        result += "\"<truncated>\"";
        break;
      }
      uint32_t key_offset = ReadOffsetOrSize(data + ke_off, large);
      uint16_t key_length = ReadLE2(data + ke_off + offset_size);

      // key_offset is relative to the start of the object/array data
      size_t abs_key_offset = offset + key_offset;
      if (abs_key_offset + key_length > data_len) {
        result += "\"<truncated>\"";
        break;
      }
      result += EscapeJsonString(data + abs_key_offset, key_length);
      result += ':';
    }

    // Read value entry
    size_t ve_off = value_entries_offset + i * value_entry_size;
    if (ve_off + value_entry_size > data_len) {
      result += "\"<truncated>\"";
      break;
    }
    uint8_t val_type = data[ve_off];
    uint32_t val_offset_or_inline = ReadOffsetOrSize(data + ve_off + 1, large);

    if (IsInlineable(val_type, large)) {
      // Value is inlined in the value entry
      std::string val_str;
      switch (val_type) {
        case JSONB_TYPE_LITERAL:
          if (val_offset_or_inline == JSONB_NULL) val_str = "null";
          else if (val_offset_or_inline == JSONB_TRUE) val_str = "true";
          else if (val_offset_or_inline == JSONB_FALSE) val_str = "false";
          else val_str = "null";
          break;
        case JSONB_TYPE_INT16:
          val_str = std::to_string(static_cast<int16_t>(val_offset_or_inline));
          break;
        case JSONB_TYPE_UINT16:
          val_str = std::to_string(static_cast<uint16_t>(val_offset_or_inline));
          break;
        case JSONB_TYPE_INT32:
          val_str = std::to_string(static_cast<int32_t>(val_offset_or_inline));
          break;
        case JSONB_TYPE_UINT32:
          val_str = std::to_string(val_offset_or_inline);
          break;
        default:
          val_str = "null";
          break;
      }
      result += val_str;
    } else {
      // Value is at an offset relative to the start of the object/array
      size_t abs_val_offset = offset + val_offset_or_inline;
      std::string val_str;
      if (!DecodeValue(data, data_len, val_type, abs_val_offset, &val_str)) {
        result += "\"<decode_error>\"";
      } else {
        result += val_str;
      }
    }
  }

  result += is_object ? '}' : ']';
  *out = result;
  return true;
}

static bool DecodeValue(const unsigned char* data, size_t data_len,
                        uint8_t type, size_t value_offset,
                        std::string* out) {
  switch (type) {
    case JSONB_TYPE_SMALL_OBJECT:
      return DecodeObjectOrArray(data, data_len, value_offset,
                                 true, false, out);
    case JSONB_TYPE_LARGE_OBJECT:
      return DecodeObjectOrArray(data, data_len, value_offset,
                                 true, true, out);
    case JSONB_TYPE_SMALL_ARRAY:
      return DecodeObjectOrArray(data, data_len, value_offset,
                                 false, false, out);
    case JSONB_TYPE_LARGE_ARRAY:
      return DecodeObjectOrArray(data, data_len, value_offset,
                                 false, true, out);

    case JSONB_TYPE_LITERAL: {
      if (value_offset >= data_len) {
        *out = "null";
        return true;
      }
      uint8_t lit = data[value_offset];
      if (lit == JSONB_NULL) *out = "null";
      else if (lit == JSONB_TRUE) *out = "true";
      else if (lit == JSONB_FALSE) *out = "false";
      else *out = "null";
      return true;
    }

    case JSONB_TYPE_INT16: {
      if (value_offset + 2 > data_len) {
        *out = "0";
        return false;
      }
      int16_t val = static_cast<int16_t>(ReadLE2(data + value_offset));
      *out = std::to_string(val);
      return true;
    }

    case JSONB_TYPE_UINT16: {
      if (value_offset + 2 > data_len) {
        *out = "0";
        return false;
      }
      uint16_t val = ReadLE2(data + value_offset);
      *out = std::to_string(val);
      return true;
    }

    case JSONB_TYPE_INT32: {
      if (value_offset + 4 > data_len) {
        *out = "0";
        return false;
      }
      int32_t val = static_cast<int32_t>(ReadLE4(data + value_offset));
      *out = std::to_string(val);
      return true;
    }

    case JSONB_TYPE_UINT32: {
      if (value_offset + 4 > data_len) {
        *out = "0";
        return false;
      }
      uint32_t val = ReadLE4(data + value_offset);
      *out = std::to_string(val);
      return true;
    }

    case JSONB_TYPE_INT64: {
      if (value_offset + 8 > data_len) {
        *out = "0";
        return false;
      }
      int64_t val = static_cast<int64_t>(ReadLE8(data + value_offset));
      *out = std::to_string(val);
      return true;
    }

    case JSONB_TYPE_UINT64: {
      if (value_offset + 8 > data_len) {
        *out = "0";
        return false;
      }
      uint64_t val = ReadLE8(data + value_offset);
      *out = std::to_string(val);
      return true;
    }

    case JSONB_TYPE_DOUBLE: {
      if (value_offset + 8 > data_len) {
        *out = "0.0";
        return false;
      }
      double val;
      memcpy(&val, data + value_offset, 8);
      if (std::isnan(val) || std::isinf(val)) {
        *out = "null";
      } else {
        char buf[64];
        snprintf(buf, sizeof(buf), "%.17g", val);
        *out = buf;
      }
      return true;
    }

    case JSONB_TYPE_STRING: {
      uint32_t str_len = 0;
      size_t bytes_consumed = 0;
      if (!ReadVariableLength(data, data_len, value_offset,
                              &str_len, &bytes_consumed)) {
        *out = "\"<truncated>\"";
        return false;
      }
      size_t str_start = value_offset + bytes_consumed;
      if (str_start + str_len > data_len) {
        // Truncate to what we have
        str_len = static_cast<uint32_t>(data_len - str_start);
      }
      *out = EscapeJsonString(data + str_start, str_len);
      return true;
    }

    case JSONB_TYPE_OPAQUE: {
      // Opaque types contain a MySQL type byte followed by variable-length data.
      // We print a placeholder rather than attempting full decode.
      if (value_offset >= data_len) {
        *out = "\"<opaque:unknown>\"";
        return true;
      }
      uint8_t mysql_type = data[value_offset];
      uint32_t opaque_len = 0;
      size_t bytes_consumed = 0;
      if (value_offset + 1 < data_len &&
          ReadVariableLength(data, data_len, value_offset + 1,
                             &opaque_len, &bytes_consumed)) {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "\"<opaque:type=%u, %u bytes>\"",
                 mysql_type, opaque_len);
        *out = buf;
      } else {
        char buf[64];
        snprintf(buf, sizeof(buf),
                 "\"<opaque:type=%u>\"", mysql_type);
        *out = buf;
      }
      return true;
    }

    default: {
      char buf[64];
      snprintf(buf, sizeof(buf),
               "\"<unknown_type:0x%02x>\"", type);
      *out = buf;
      return false;
    }
  }
}

std::string JsonBinaryToString(const unsigned char* data, size_t len) {
  if (data == nullptr || len == 0) {
    return "<empty>";
  }

  // First byte is the type of the top-level value
  uint8_t type = data[0];
  std::string result;

  if (!DecodeValue(data, len, type, 1, &result)) {
    // Partial decode is still useful, return what we have
    if (result.empty()) {
      return "<decode_error>";
    }
  }

  return result;
}

}  // namespace ibd_ninja
