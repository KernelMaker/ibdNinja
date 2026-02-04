/*
 * Copyright (c) [2025-2026] [Zhao Song]
 *
 * MySQL Binary JSON (JSONB) decoder.
 * Converts the internal binary JSON format stored in InnoDB
 * back to human-readable JSON text.
 *
 * Reference: MySQL sql-common/json_binary.h
 */
#ifndef JSONBINARY_H_
#define JSONBINARY_H_

#include <cstddef>
#include <cstdint>
#include <string>

namespace ibd_ninja {

// JSONB type constants (from MySQL json_binary.h)
constexpr uint8_t JSONB_TYPE_SMALL_OBJECT = 0x00;
constexpr uint8_t JSONB_TYPE_LARGE_OBJECT = 0x01;
constexpr uint8_t JSONB_TYPE_SMALL_ARRAY  = 0x02;
constexpr uint8_t JSONB_TYPE_LARGE_ARRAY  = 0x03;
constexpr uint8_t JSONB_TYPE_LITERAL      = 0x04;
constexpr uint8_t JSONB_TYPE_INT16        = 0x05;
constexpr uint8_t JSONB_TYPE_UINT16       = 0x06;
constexpr uint8_t JSONB_TYPE_INT32        = 0x07;
constexpr uint8_t JSONB_TYPE_UINT32       = 0x08;
constexpr uint8_t JSONB_TYPE_INT64        = 0x09;
constexpr uint8_t JSONB_TYPE_UINT64       = 0x0A;
constexpr uint8_t JSONB_TYPE_DOUBLE       = 0x0B;
constexpr uint8_t JSONB_TYPE_STRING       = 0x0C;
constexpr uint8_t JSONB_TYPE_OPAQUE       = 0x0F;

// JSONB literal values
constexpr uint8_t JSONB_NULL  = 0x00;
constexpr uint8_t JSONB_TRUE  = 0x01;
constexpr uint8_t JSONB_FALSE = 0x02;

// Main entry point: decode binary JSON to human-readable JSON string.
// Returns the JSON text, or an error message on failure.
std::string JsonBinaryToString(const unsigned char* data, size_t len);

}  // namespace ibd_ninja

#endif  // JSONBINARY_H_
