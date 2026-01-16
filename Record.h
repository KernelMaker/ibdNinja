/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#ifndef RECORD_H_
#define RECORD_H_

#include "ibdUtils.h"

#include <cassert>
#include <cstdint>

namespace ibd_ninja {

class Index;

struct PageAnalysisResult {
  uint32_t n_recs_non_leaf = 0;
  uint32_t n_recs_leaf = 0;
  uint32_t headers_len_non_leaf = 0;
  uint32_t headers_len_leaf = 0;
  uint32_t recs_len_non_leaf = 0;
  uint32_t recs_len_leaf = 0;
  uint32_t n_deleted_recs_non_leaf = 0;
  uint32_t n_deleted_recs_leaf = 0;
  uint32_t deleted_recs_len_non_leaf = 0;
  uint32_t deleted_recs_len_leaf = 0;
  uint32_t n_contain_dropped_cols_recs_non_leaf = 0;  // should always be 0
  uint32_t n_contain_dropped_cols_recs_leaf = 0;
  uint32_t dropped_cols_len_non_leaf = 0;  // should always be 0
  uint32_t dropped_cols_len_leaf = 0;
  uint32_t innodb_internal_used_non_leaf = 0;
  uint32_t innodb_internal_used_leaf = 0;
  uint32_t free_non_leaf = 0;
  uint32_t free_leaf = 0;
};

struct IndexAnalyzeResult {
  uint32_t n_level = 0;
  uint32_t n_pages_non_leaf = 0;
  uint32_t n_pages_leaf = 0;
  PageAnalysisResult recs_result;
};

class Record {
 public:
  Record(const unsigned char* rec, Index* index) :
    rec_(rec), index_(index), offsets_(nullptr) {
  }
  ~Record() {
    if (offsets_ != nullptr) {
      delete [] offsets_;
    }
  }
  uint32_t GetStatus();
  uint32_t* GetColumnOffsets();
  uint32_t GetChildPageNo();
  void ParseRecord(bool leaf, uint32_t row_no,
                   PageAnalysisResult* result,
                   bool print);

 private:
  uint32_t GetBitsFrom1B(uint32_t offs, uint32_t mask, uint32_t shift);
  uint32_t GetBitsFrom2B(uint32_t offs, uint32_t mask, uint32_t shift);
  void SetNAlloc(uint32_t n_alloc) {
    assert(offsets_);
    offsets_[0] = n_alloc;
  }
  void SetNFields(uint32_t n_fields) {
    assert(offsets_);
    offsets_[1] = n_fields;
  }
  uint32_t GetNAlloc() {
    assert(offsets_);
    return offsets_[0];
  }
  uint32_t GetNFields() {
    assert(offsets_);
    return offsets_[1];
  }
  enum REC_INSERT_STATE {
    INSERTED_BEFORE_INSTANT_ADD_OLD_IMPLEMENTATION,
    INSERTED_AFTER_INSTANT_ADD_OLD_IMPLEMENTATION,
    INSERTED_AFTER_UPGRADE_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION,
    INSERTED_BEFORE_INSTANT_ADD_NEW_IMPLEMENTATION,
    INSERTED_AFTER_INSTANT_ADD_NEW_IMPLEMENTATION,
    INSERTED_INTO_TABLE_WITH_NO_INSTANT_NO_VERSION,
    NONE
  };

  void InitColumnOffsets();
  void InitColumnOffsetsCompact();
  void InitColumnOffsetsCompactLeaf();
  bool IsVersionedCompact();
  bool GetInstantFlagCompact();
  REC_INSERT_STATE InitNullAndLengthCompact(const unsigned char** nulls,
                                            const unsigned char** lens,
                                            uint16_t* n_null,
                                            uint16_t* non_default_fields,
                                            uint8_t* row_version);

  REC_INSERT_STATE GetInsertState();
  uint32_t GetInfoBits(bool comp);
  uint32_t GetNFieldsInstant(const uint32_t extra_bytes,
                             uint16_t* length);
  uint64_t GetInstantOffset(uint32_t n, uint64_t offs);
  const unsigned char* rec_;
  Index* index_;
  uint32_t* offsets_;
};

}  // namespace ibd_ninja

#endif  // RECORD_H_
