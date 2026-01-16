/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#include "ibdUtils.h"
#include <unistd.h>
#include <cstring>
#include <cassert>

namespace ibd_ninja {
/*
 * BIG-ENDIAN
 */
uint8_t ReadFrom1B(const unsigned char* b) {
  return ((uint8_t)(b[0]));
}
uint16_t ReadFrom2B(const unsigned char* b) {
  return (((uint32_t)(b[0]) << 8) | (uint32_t)(b[1]));
}
uint32_t ReadFrom3B(const unsigned char* b) {
  return ((static_cast<uint32_t>(b[0]) << 16) |
          (static_cast<uint32_t>(b[1]) << 8) | static_cast<uint32_t>(b[2]));
}
uint32_t ReadFrom4B(const unsigned char* b) {
  return ((static_cast<uint32_t>(b[0]) << 24) |
          (static_cast<uint32_t>(b[1]) << 16) |
          (static_cast<uint32_t>(b[2]) << 8) | static_cast<uint32_t>(b[3]));
}
uint64_t ReadFrom8B(const unsigned char* b) {
  uint64_t u64;
  u64 = ReadFrom4B(b);
  u64 <<= 32;
  u64 |= ReadFrom4B(b + 4);

  return u64;
}

uint32_t PageSizeValidate(uint32_t page_size) {
  for (uint32_t n = UNIV_PAGE_SIZE_SHIFT_MIN;
       n <= UNIV_PAGE_SIZE_SHIFT_MAX; n++) {
    if (page_size == static_cast<uint32_t>(1 << n)) {
      return n;
    }
  }
  return 0;
}

uint32_t FSPHeaderGetField(const unsigned char* page, uint32_t field) {
  return ReadFrom4B(FSP_HEADER_OFFSET + field + page);
}
uint32_t FSPHeaderGetFlags(const unsigned char* page) {
  return FSPHeaderGetField(page, FSP_SPACE_FLAGS);
}

bool FSPFlagsIsValid(uint32_t flags) {
  bool post_antelope = FSP_FLAGS_GET_POST_ANTELOPE(flags);
  uint32_t zip_ssize = FSP_FLAGS_GET_ZIP_SSIZE(flags);
  bool atomic_blobs = FSP_FLAGS_HAS_ATOMIC_BLOBS(flags);
  uint32_t page_ssize = FSP_FLAGS_GET_PAGE_SSIZE(flags);
  bool has_data_dir = FSP_FLAGS_HAS_DATA_DIR(flags);
  bool is_shared = FSP_FLAGS_GET_SHARED(flags);
  bool is_temp = FSP_FLAGS_GET_TEMPORARY(flags);
  bool is_encryption = FSP_FLAGS_GET_ENCRYPTION(flags);

  uint32_t unused = FSP_FLAGS_GET_UNUSED(flags);

  if (flags == 0) {
    return true;
  }

  if (post_antelope != atomic_blobs) {
    return false;
  }

  if (unused != 0) {
    return false;
  }

  if (zip_ssize > PAGE_ZIP_SSIZE_MAX) {
    return false;
  }

  if (page_ssize != 0 &&
      (page_ssize < UNIV_PAGE_SSIZE_MIN || page_ssize > UNIV_PAGE_SSIZE_MAX)) {
    return false;
  }

  if (has_data_dir && (is_shared || is_temp)) {
    return false;
  }

  if (is_encryption && (is_temp)) {
    return false;
  }

  assert(FSP_FLAGS_POS_UNUSED == 15);

  return true;
}

std::string PageType2String(uint32_t type) {
  switch (type) {
    case FIL_PAGE_INDEX:
      return "INDEX";
    case FIL_PAGE_RTREE:
      return "RTREE";
    case FIL_PAGE_SDI:
      return "SDI";
    case FIL_PAGE_TYPE_UNUSED:
      return "UNUSED";
    case FIL_PAGE_UNDO_LOG:
      return "UNDO_LOG";
    case FIL_PAGE_INODE:
      return "INODE";
    case FIL_PAGE_IBUF_FREE_LIST:
      return "IBUF_FREE_LIST";
    case FIL_PAGE_TYPE_ALLOCATED:
      return "ALLOCATED";
    case FIL_PAGE_IBUF_BITMAP:
      return "IBUF_BITMAP";
    case FIL_PAGE_TYPE_SYS:
      return "SYS";
    case FIL_PAGE_TYPE_TRX_SYS:
      return "TRX_SYS";
    case FIL_PAGE_TYPE_FSP_HDR:
      return "FSP_HDR";
    case FIL_PAGE_TYPE_XDES:
      return "XDES";
    case FIL_PAGE_TYPE_BLOB:
      return "BLOB";
    case FIL_PAGE_TYPE_ZBLOB:
      return "ZBLOB";
    case FIL_PAGE_TYPE_ZBLOB2:
      return "ZBLOB2";
    case FIL_PAGE_TYPE_UNKNOWN:
      return "UNKNOWN";
    case FIL_PAGE_COMPRESSED:
      return "COMPRESSED";
    case FIL_PAGE_ENCRYPTED:
      return "ENCRYPTED";
    case FIL_PAGE_COMPRESSED_AND_ENCRYPTED:
      return "COMPRESSED_AND_ENCRYPTED";
    case FIL_PAGE_ENCRYPTED_RTREE:
      return "ENCRYPTED_RTREE";
    case FIL_PAGE_SDI_BLOB:
      return "SDI_BLOB";
    case FIL_PAGE_SDI_ZBLOB:
      return  "SDI_ZBLOB";
    case FIL_PAGE_TYPE_LEGACY_DBLWR:
      return "LEGACY_DBLWR";
    case FIL_PAGE_TYPE_RSEG_ARRAY:
      return "RSEG_ARRAY";
    case FIL_PAGE_TYPE_LOB_INDEX:
      return "LOB_INDEX";
    case FIL_PAGE_TYPE_LOB_DATA:
      return "LOB_DATA";
    case FIL_PAGE_TYPE_LOB_FIRST:
      return "LOB_FIRST";
    case FIL_PAGE_TYPE_ZLOB_FIRST:
      return "ZLOB_FIRST";
    case FIL_PAGE_TYPE_ZLOB_DATA:
      return "ZLOB_DATA";
    case FIL_PAGE_TYPE_ZLOB_INDEX:
      return "ZLOB_INDEX";
    case FIL_PAGE_TYPE_ZLOB_FRAG:
      return "ZLOB_FRAG";
    case FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY:
      return "ZLOG_FRAG_ENTRY";
    default:
      return "UNDEFINED";
  }
}

int g_fd = 0;
uint32_t g_page_size_shift = 0;
uint32_t g_page_logical_size = 0;
uint32_t g_page_physical_size = 0;
bool g_page_compressed = false;

uint32_t RecGetBitField1B(const unsigned char* rec, uint32_t offs,
                                        uint32_t mask, uint32_t shift) {
  assert(rec);
  return ((ReadFrom1B(rec - offs) & mask) >> shift);
}
bool RecGetDeletedFlag(const unsigned char* rec, bool comp) {
  if (comp) {
    return (RecGetBitField1B(rec, REC_NEW_INFO_BITS, REC_INFO_DELETED_FLAG,
                             REC_INFO_BITS_SHIFT));
  } else {
    assert(0);
    // TODO(Zhao): Support redundant row format
  }
}
uint8_t RecGetType(const unsigned char* rec) {
  unsigned char rec_type_byte = *(rec - REC_OFF_TYPE);
  return (rec_type_byte & 0x7);
}

void *ut_align(const void *ptr, unsigned long align_no) {
  assert(align_no > 0);
  assert(((align_no - 1) & align_no) == 0);
  assert(ptr);

  static_assert(sizeof(void *) == sizeof(unsigned long));

  return ((void *)((((unsigned long)ptr) + align_no - 1) & ~(align_no - 1)));
}

unsigned long ut_align_offset(const void *ptr, unsigned long align_no) {
  assert(align_no > 0);
  assert(((align_no - 1) & align_no) == 0);
  assert(ptr);

  static_assert(sizeof(void *) == sizeof(unsigned long));

  return (((unsigned long)ptr) & (align_no - 1));
}

uint32_t page_offset(
    const void* ptr) {
  return (ut_align_offset(ptr, UNIV_PAGE_SIZE));
}

void *ut_align_down(const void *ptr, unsigned long align_no) {
  assert(align_no > 0);
  assert(((align_no - 1) & align_no) == 0);
  assert(ptr);

  static_assert(sizeof(void *) == sizeof(unsigned long));

  return ((void *)((((unsigned long)ptr)) & ~(align_no - 1)));
}

unsigned char *page_align(
    const void *ptr) {
  return ((unsigned char *)ut_align_down(ptr, UNIV_PAGE_SIZE));
}

uint32_t RecGetNextOffs(const unsigned char* rec, bool comp) {
  uint32_t field_value;
  static_assert(REC_NEXT_MASK == 0xFFFFUL, "REC_NEXT_MASK != 0xFFFFUL");
  static_assert(REC_NEXT_SHIFT == 0, "REC_NEXT_SHIFT != 0");

  field_value = ReadFrom2B(rec - REC_NEXT);

  if (comp) {
    assert(static_cast<uint16_t>(field_value +
                                ut_align_offset(rec, UNIV_PAGE_SIZE)));

    if (field_value == 0) {
      return (0);
    }

    assert((field_value > REC_N_NEW_EXTRA_BYTES && field_value < 32768) ||
          field_value < (uint16_t) - REC_N_NEW_EXTRA_BYTES);

    return (ut_align_offset(rec + field_value, UNIV_PAGE_SIZE));
  } else {
    // TODO(Zhao): Support redundant row format
    assert(0);
    assert(field_value < UNIV_PAGE_SIZE);

    return (field_value);
  }
}

uint16_t PageHeaderGetField(const unsigned char* page, uint32_t field) {
  assert(page);
  assert(field <= PAGE_INDEX_ID);

  return (ReadFrom2B(page + PAGE_HEADER + field));
}
uint16_t PageDirGetNHeap(const unsigned char* page) {
  return (PageHeaderGetField(page, PAGE_N_HEAP) & 0x7fff);
}
bool PageIsCompact(const unsigned char* page) {
  return (PageHeaderGetField(page, PAGE_N_HEAP) & 0x8000) != 0;
}
uint16_t PageGetType(const unsigned char* page) {
  return (static_cast<uint16_t>(ReadFrom2B(page + FIL_PAGE_TYPE)));
}

bool page_rec_check(const unsigned char* rec) {
  const unsigned char* page = page_align(rec);

  assert(rec);

  assert(page_offset(rec) <= PageHeaderGetField(page, PAGE_HEAP_TOP));
  assert(page_offset(rec) >= PAGE_DATA);

  return true;
}

bool RecIsInfimum(const unsigned char* rec) {
  assert(page_rec_check(rec));
  uint32_t offset = page_offset(rec);
  assert(offset >= PAGE_NEW_INFIMUM);
  assert(offset <= UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START);
  return (offset == PAGE_NEW_INFIMUM || offset == PAGE_OLD_INFIMUM);
}

bool RecIsSupremum(const unsigned char* rec) {
  assert(page_rec_check(rec));
  uint32_t offset = page_offset(rec);
  assert(offset >= PAGE_NEW_INFIMUM);
  assert(offset <= UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START);
  return (offset == PAGE_NEW_SUPREMUM || offset == PAGE_OLD_SUPREMUM);
}
}  // namespace ibd_ninja
