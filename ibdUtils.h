/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#ifndef IBDUTILS_H_
#define IBDUTILS_H_
#include <sys/types.h>
#include <unistd.h>
#include <cstdint>
#include <cstddef>
#include <limits>
#include <string>

namespace ibd_ninja {

extern int g_fd;
extern uint32_t g_page_size_shift;
extern uint32_t g_page_logical_size;
extern uint32_t g_page_physical_size;
extern bool g_page_compressed;

uint8_t ReadFrom1B(const unsigned char* b);
uint16_t ReadFrom2B(const unsigned char* b);
uint32_t ReadFrom3B(const unsigned char* b);
uint32_t ReadFrom4B(const unsigned char* b);
uint64_t ReadFrom8B(const unsigned char* b);
// Page size related
uint32_t PageSizeValidate(uint32_t page_size);
constexpr uint32_t UNIV_ZIP_SIZE_SHIFT_MIN = 10;
constexpr uint32_t UNIV_ZIP_SIZE_SHIFT_MAX = 14;
constexpr uint32_t UNIV_PAGE_SIZE_SHIFT_MIN = 12;
constexpr uint32_t UNIV_PAGE_SIZE_SHIFT_MAX = 16;
constexpr uint32_t UNIV_PAGE_SIZE_SHIFT_DEF = 14;
constexpr uint32_t UNIV_PAGE_SIZE_SHIFT_ORIG = 14;
constexpr uint32_t UNIV_PAGE_SSIZE_ORIG = UNIV_PAGE_SIZE_SHIFT_ORIG - 9;
constexpr uint32_t UNIV_PAGE_SIZE_MIN = 1 << UNIV_PAGE_SIZE_SHIFT_MIN;
constexpr size_t UNIV_PAGE_SIZE_MAX = 1 << UNIV_PAGE_SIZE_SHIFT_MAX;
constexpr uint32_t UNIV_PAGE_SIZE_DEF = 1 << UNIV_PAGE_SIZE_SHIFT_DEF;
constexpr uint32_t UNIV_PAGE_SIZE_ORIG = 1 << UNIV_PAGE_SIZE_SHIFT_ORIG;
constexpr uint32_t UNIV_ZIP_SIZE_MIN = 1 << UNIV_ZIP_SIZE_SHIFT_MIN;
constexpr uint32_t UNIV_ZIP_SIZE_MAX = 1 << UNIV_ZIP_SIZE_SHIFT_MAX;
#define UNIV_PAGE_SIZE_SHIFT g_page_size_shift
#define UNIV_PAGE_SSIZE_MAX \
  static_cast<uint32_t>(UNIV_PAGE_SIZE_SHIFT - UNIV_ZIP_SIZE_SHIFT_MIN + 1)
#define UNIV_PAGE_SSIZE_MIN \
  static_cast<uint32_t>(UNIV_PAGE_SIZE_SHIFT_MIN - UNIV_ZIP_SIZE_SHIFT_MIN + 1)

constexpr size_t PAGE_SIZE_T_SIZE_BITS = 17;
constexpr uint32_t PAGE_ZIP_SSIZE_MAX =
    UNIV_ZIP_SIZE_SHIFT_MAX - UNIV_ZIP_SIZE_SHIFT_MIN + 1;

// FSP related
uint32_t FSPHeaderGetField(const unsigned char* page, uint32_t field);
uint32_t FSPHeaderGetFlags(const unsigned char* page);
bool FSPFlagsIsValid(uint32_t flags);
constexpr uint32_t FIL_PAGE_SPACE_OR_CHKSUM = 0;
constexpr uint32_t FIL_PAGE_OFFSET = 4;
constexpr uint32_t FIL_PAGE_PREV = 8;
constexpr uint32_t FIL_PAGE_SRV_VERSION = 8;
constexpr uint32_t FIL_PAGE_NEXT = 12;
constexpr uint32_t FIL_PAGE_SPACE_VERSION = 12;
constexpr uint32_t FIL_PAGE_LSN = 16;
constexpr uint32_t FIL_PAGE_TYPE = 24;
constexpr uint32_t FIL_PAGE_FILE_FLUSH_LSN = 26;
constexpr uint32_t FIL_PAGE_VERSION = FIL_PAGE_FILE_FLUSH_LSN;
constexpr uint32_t FIL_PAGE_ALGORITHM_V1 = FIL_PAGE_VERSION + 1;
constexpr uint32_t FIL_PAGE_ORIGINAL_TYPE_V1 = FIL_PAGE_ALGORITHM_V1 + 1;
constexpr uint32_t FIL_PAGE_ORIGINAL_SIZE_V1 = FIL_PAGE_ORIGINAL_TYPE_V1 + 2;
constexpr uint32_t FIL_PAGE_COMPRESS_SIZE_V1 = FIL_PAGE_ORIGINAL_SIZE_V1 + 2;
constexpr uint32_t FIL_RTREE_SPLIT_SEQ_NUM = FIL_PAGE_FILE_FLUSH_LSN;
constexpr uint32_t FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID = 34;
constexpr uint32_t FIL_PAGE_SPACE_ID = FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID;
constexpr uint32_t FIL_PAGE_DATA = 38;
constexpr uint32_t FIL_PAGE_END_LSN_OLD_CHKSUM = 8;
constexpr uint32_t FIL_PAGE_DATA_END = 8;
constexpr size_t FIL_ADDR_PAGE = 0;
constexpr size_t FIL_ADDR_BYTE = 4;
constexpr size_t FIL_ADDR_SIZE = 6;
constexpr char FIL_PATH_SEPARATOR = ';';
constexpr uint32_t FLST_BASE_NODE_SIZE = 4 + 2 * FIL_ADDR_SIZE;
constexpr uint32_t FLST_NODE_SIZE = 2 * FIL_ADDR_SIZE;
constexpr uint32_t FSP_HEADER_OFFSET = FIL_PAGE_DATA;
constexpr uint32_t FSP_SPACE_ID = 0;
constexpr uint32_t FSP_NOT_USED = 4;
constexpr uint32_t FSP_SIZE = 8;
constexpr uint32_t FSP_FREE_LIMIT = 12;
constexpr uint32_t FSP_SPACE_FLAGS = 16;
constexpr uint32_t FSP_FRAG_N_USED = 20;
constexpr uint32_t FSP_FREE = 24;
constexpr uint32_t FSP_FREE_FRAG = 24 + FLST_BASE_NODE_SIZE;
constexpr uint32_t FSP_FULL_FRAG = 24 + 2 * FLST_BASE_NODE_SIZE;
constexpr uint32_t FSP_SEG_ID = 24 + 3 * FLST_BASE_NODE_SIZE;
constexpr uint32_t FSP_SEG_INODES_FULL = 32 + 3 * FLST_BASE_NODE_SIZE;
constexpr uint32_t FSP_SEG_INODES_FREE = 32 + 4 * FLST_BASE_NODE_SIZE;
constexpr uint32_t FSP_HEADER_SIZE = 32 + 5 * FLST_BASE_NODE_SIZE;
constexpr uint32_t FSP_FREE_ADD = 4;
constexpr uint32_t FSP_FLAGS_WIDTH_POST_ANTELOPE = 1;
constexpr uint32_t FSP_FLAGS_WIDTH_ZIP_SSIZE = 4;
constexpr uint32_t FSP_FLAGS_WIDTH_ATOMIC_BLOBS = 1;
constexpr uint32_t FSP_FLAGS_WIDTH_PAGE_SSIZE = 4;
constexpr uint32_t FSP_FLAGS_WIDTH_DATA_DIR = 1;
constexpr uint32_t FSP_FLAGS_WIDTH_SHARED = 1;
constexpr uint32_t FSP_FLAGS_WIDTH_TEMPORARY = 1;
constexpr uint32_t FSP_FLAGS_WIDTH_ENCRYPTION = 1;
constexpr uint32_t FSP_FLAGS_WIDTH_SDI = 1;
constexpr uint32_t FSP_FLAGS_WIDTH =
    FSP_FLAGS_WIDTH_POST_ANTELOPE + FSP_FLAGS_WIDTH_ZIP_SSIZE +
    FSP_FLAGS_WIDTH_ATOMIC_BLOBS + FSP_FLAGS_WIDTH_PAGE_SSIZE +
    FSP_FLAGS_WIDTH_DATA_DIR + FSP_FLAGS_WIDTH_SHARED +
    FSP_FLAGS_WIDTH_TEMPORARY + FSP_FLAGS_WIDTH_ENCRYPTION +
    FSP_FLAGS_WIDTH_SDI;
constexpr uint32_t FSP_FLAGS_MASK = ~(~0U << FSP_FLAGS_WIDTH);
constexpr uint32_t FSP_FLAGS_POS_POST_ANTELOPE = 0;
constexpr uint32_t FSP_FLAGS_POS_ZIP_SSIZE =
    FSP_FLAGS_POS_POST_ANTELOPE + FSP_FLAGS_WIDTH_POST_ANTELOPE;
constexpr uint32_t FSP_FLAGS_POS_ATOMIC_BLOBS =
    FSP_FLAGS_POS_ZIP_SSIZE + FSP_FLAGS_WIDTH_ZIP_SSIZE;
constexpr uint32_t FSP_FLAGS_POS_PAGE_SSIZE =
    FSP_FLAGS_POS_ATOMIC_BLOBS + FSP_FLAGS_WIDTH_ATOMIC_BLOBS;
constexpr uint32_t FSP_FLAGS_POS_DATA_DIR =
    FSP_FLAGS_POS_PAGE_SSIZE + FSP_FLAGS_WIDTH_PAGE_SSIZE;
constexpr uint32_t FSP_FLAGS_POS_SHARED =
    FSP_FLAGS_POS_DATA_DIR + FSP_FLAGS_WIDTH_DATA_DIR;
constexpr uint32_t FSP_FLAGS_POS_TEMPORARY =
    FSP_FLAGS_POS_SHARED + FSP_FLAGS_WIDTH_SHARED;
constexpr uint32_t FSP_FLAGS_POS_ENCRYPTION =
    FSP_FLAGS_POS_TEMPORARY + FSP_FLAGS_WIDTH_TEMPORARY;
constexpr uint32_t FSP_FLAGS_POS_SDI =
    FSP_FLAGS_POS_ENCRYPTION + FSP_FLAGS_WIDTH_ENCRYPTION;
constexpr uint32_t FSP_FLAGS_POS_UNUSED =
    FSP_FLAGS_POS_SDI + FSP_FLAGS_WIDTH_SDI;
constexpr uint32_t FSP_FLAGS_MASK_POST_ANTELOPE =
    (~(~0U << FSP_FLAGS_WIDTH_POST_ANTELOPE)) << FSP_FLAGS_POS_POST_ANTELOPE;
constexpr uint32_t FSP_FLAGS_MASK_ZIP_SSIZE =
    (~(~0U << FSP_FLAGS_WIDTH_ZIP_SSIZE)) << FSP_FLAGS_POS_ZIP_SSIZE;
constexpr uint32_t FSP_FLAGS_MASK_ATOMIC_BLOBS =
    (~(~0U << FSP_FLAGS_WIDTH_ATOMIC_BLOBS)) << FSP_FLAGS_POS_ATOMIC_BLOBS;
constexpr uint32_t FSP_FLAGS_MASK_PAGE_SSIZE =
    (~(~0U << FSP_FLAGS_WIDTH_PAGE_SSIZE)) << FSP_FLAGS_POS_PAGE_SSIZE;
constexpr uint32_t FSP_FLAGS_MASK_DATA_DIR =
    (~(~0U << FSP_FLAGS_WIDTH_DATA_DIR)) << FSP_FLAGS_POS_DATA_DIR;
constexpr uint32_t FSP_FLAGS_MASK_SHARED = (~(~0U << FSP_FLAGS_WIDTH_SHARED))
                                           << FSP_FLAGS_POS_SHARED;
constexpr uint32_t FSP_FLAGS_MASK_TEMPORARY =
    (~(~0U << FSP_FLAGS_WIDTH_TEMPORARY)) << FSP_FLAGS_POS_TEMPORARY;
constexpr uint32_t FSP_FLAGS_MASK_ENCRYPTION =
    (~(~0U << FSP_FLAGS_WIDTH_ENCRYPTION)) << FSP_FLAGS_POS_ENCRYPTION;
constexpr uint32_t FSP_FLAGS_MASK_SDI = (~(~0U << FSP_FLAGS_WIDTH_SDI))
                                        << FSP_FLAGS_POS_SDI;
constexpr uint32_t FSP_FLAGS_GET_POST_ANTELOPE(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_POST_ANTELOPE) >> FSP_FLAGS_POS_POST_ANTELOPE;
}
constexpr uint32_t FSP_FLAGS_GET_ZIP_SSIZE(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_ZIP_SSIZE) >> FSP_FLAGS_POS_ZIP_SSIZE;
}
constexpr uint32_t FSP_FLAGS_HAS_ATOMIC_BLOBS(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_ATOMIC_BLOBS) >> FSP_FLAGS_POS_ATOMIC_BLOBS;
}
constexpr uint32_t FSP_FLAGS_GET_PAGE_SSIZE(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_PAGE_SSIZE) >> FSP_FLAGS_POS_PAGE_SSIZE;
}
constexpr uint32_t FSP_FLAGS_HAS_DATA_DIR(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_DATA_DIR) >> FSP_FLAGS_POS_DATA_DIR;
}
constexpr uint32_t FSP_FLAGS_GET_SHARED(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_SHARED) >> FSP_FLAGS_POS_SHARED;
}
constexpr uint32_t FSP_FLAGS_GET_TEMPORARY(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_TEMPORARY) >> FSP_FLAGS_POS_TEMPORARY;
}
constexpr uint32_t FSP_FLAGS_GET_ENCRYPTION(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_ENCRYPTION) >> FSP_FLAGS_POS_ENCRYPTION;
}
constexpr uint32_t FSP_FLAGS_HAS_SDI(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK_SDI) >> FSP_FLAGS_POS_SDI;
}
constexpr uint32_t FSP_FLAGS_GET_UNUSED(uint32_t flags) {
  return flags >> FSP_FLAGS_POS_UNUSED;
}
constexpr bool FSP_FLAGS_ARE_NOT_SET(uint32_t flags) {
  return (flags & FSP_FLAGS_MASK) == 0;
}
constexpr uint32_t XDES_ID = 0;
constexpr uint32_t XDES_FLST_NODE = 8;
constexpr uint32_t XDES_STATE = FLST_NODE_SIZE + 8;
constexpr uint32_t XDES_BITMAP = FLST_NODE_SIZE + 12;
constexpr uint32_t XDES_BITS_PER_PAGE = 2;
constexpr uint32_t XDES_FREE_BIT = 0;
constexpr uint32_t XDES_CLEAN_BIT = 1;
#define UT_BITS_IN_BYTES(b) (((b) + 7UL) / 8UL)
// TODO(Zhao): double check?
#define UNIV_PAGE_SIZE ((uint32_t)g_page_logical_size)
#define FSP_EXTENT_SIZE                                                 \
  static_cast<uint32_t>(                                               \
      ((UNIV_PAGE_SIZE <= (16384)                                       \
            ? (1048576 / UNIV_PAGE_SIZE)                                \
            : ((UNIV_PAGE_SIZE <= (32768)) ? (2097152 / UNIV_PAGE_SIZE) \
                                           : (4194304 / UNIV_PAGE_SIZE)))))
#define XDES_SIZE \
  (XDES_BITMAP + UT_BITS_IN_BYTES(FSP_EXTENT_SIZE * XDES_BITS_PER_PAGE))
#define XDES_SIZE_MAX \
  (XDES_BITMAP + UT_BITS_IN_BYTES(FSP_EXTENT_SIZE_MAX * XDES_BITS_PER_PAGE))
#define XDES_SIZE_MIN \
  (XDES_BITMAP + UT_BITS_IN_BYTES(FSP_EXTENT_SIZE_MIN * XDES_BITS_PER_PAGE))
constexpr uint32_t XDES_ARR_OFFSET = FSP_HEADER_OFFSET + FSP_HEADER_SIZE;
const uint32_t XDES_FRAG_N_USED = 2;
constexpr uint32_t FSEG_PAGE_DATA = FIL_PAGE_DATA;
constexpr uint32_t FSEG_HDR_SPACE = 0;
constexpr uint32_t FSEG_HDR_PAGE_NO = 4;
constexpr uint32_t FSEG_HDR_OFFSET = 8;
constexpr uint32_t FSEG_HEADER_SIZE = 10;


// Page dir related
constexpr uint32_t PAGE_DIR = FIL_PAGE_DATA_END;
constexpr uint32_t PAGE_DIR_SLOT_SIZE = 2;
constexpr uint32_t PAGE_EMPTY_DIR_START = PAGE_DIR + 2 * PAGE_DIR_SLOT_SIZE;
constexpr uint32_t PAGE_DIR_SLOT_MAX_N_OWNED = 8;
constexpr uint32_t PAGE_DIR_SLOT_MIN_N_OWNED = 4;

// Encrypt related
static constexpr size_t KEY_LEN = 32;
static constexpr size_t MAGIC_SIZE = 3;
static constexpr size_t SERVER_UUID_LEN = 36;
static constexpr size_t INFO_SIZE =
    (MAGIC_SIZE + sizeof(uint32_t) + (KEY_LEN * 2) + SERVER_UUID_LEN +
     sizeof(uint32_t));
static constexpr size_t INFO_MAX_SIZE = INFO_SIZE + sizeof(uint32_t);

// Column related
constexpr uint32_t DATA_MISSING = 0;
constexpr uint32_t DATA_VARCHAR = 1;
constexpr uint32_t DATA_CHAR = 2;
constexpr uint32_t DATA_FIXBINARY = 3;
constexpr uint32_t DATA_BINARY = 4;
constexpr uint32_t DATA_BLOB = 5;
constexpr uint32_t DATA_INT = 6;
constexpr uint32_t DATA_SYS = 8;
constexpr uint32_t DATA_FLOAT = 9;
constexpr uint32_t DATA_DOUBLE = 10;
constexpr uint32_t DATA_DECIMAL = 11;
constexpr uint32_t DATA_VARMYSQL = 12;
constexpr uint32_t DATA_MYSQL = 13;
constexpr uint32_t DATA_GEOMETRY = 14;
constexpr uint32_t DATA_POINT = 15;
constexpr uint32_t DATA_VAR_POINT = 16;
constexpr uint32_t DATA_MTYPE_MAX = 63;
constexpr uint32_t DATA_MTYPE_CURRENT_MIN = DATA_VARCHAR;
constexpr uint32_t DATA_MTYPE_CURRENT_MAX = DATA_VAR_POINT;
constexpr uint32_t DATA_ENGLISH = 4;
constexpr uint32_t DATA_ERROR = 111;
constexpr uint32_t DATA_MYSQL_TYPE_MASK = 255;
constexpr uint32_t DATA_MYSQL_TRUE_VARCHAR = 15;
constexpr uint32_t DATA_ROW_ID = 0;
constexpr uint32_t DATA_ROW_ID_LEN = 6;
constexpr size_t DATA_TRX_ID = 1;
constexpr size_t DATA_TRX_ID_LEN = 6;
constexpr size_t DATA_ROLL_PTR = 2;
constexpr size_t DATA_ROLL_PTR_LEN = 7;
constexpr uint32_t DATA_N_SYS_COLS = 3;
constexpr uint32_t DATA_ITT_N_SYS_COLS = 2;
constexpr uint32_t DATA_FTS_DOC_ID = 3;
constexpr uint32_t DATA_SYS_PRTYPE_MASK = 0xF;
constexpr uint32_t DATA_NOT_NULL = 256;
constexpr uint32_t DATA_UNSIGNED = 512;
constexpr uint32_t DATA_BINARY_TYPE = 1024;
constexpr uint32_t DATA_GIS_MBR = 2048;
constexpr uint32_t SPDIMS = 2;
constexpr uint32_t DATA_LONG_TRUE_VARCHAR = 4096;
constexpr uint32_t DATA_VIRTUAL = 8192;
constexpr uint32_t DATA_MULTI_VALUE = 16384;
constexpr uint32_t DATA_ORDER_NULL_TYPE_BUF_SIZE = 4;
constexpr uint32_t DATA_NEW_ORDER_NULL_TYPE_BUF_SIZE = 6;
constexpr uint32_t DATA_MBMAX = 5;
constexpr uint32_t DATA_POINT_LEN = 25;
constexpr uint32_t DATA_MBR_LEN = SPDIMS * 2 * sizeof(double);
constexpr uint32_t DICT_MAX_FIXED_COL_LEN = 768;

constexpr uint8_t UINT8_UNDEFINED = std::numeric_limits<uint8_t>::max();
constexpr uint32_t UINT32_UNDEFINED = std::numeric_limits<uint32_t>::max();

// Record related
uint32_t RecGetBitField1B(const unsigned char* rec, uint32_t offs,
                                        uint32_t mask, uint32_t shift);
bool RecGetDeletedFlag(const unsigned char* rec, bool comp);
static const uint32_t REC_OFF_TYPE = 3;
uint8_t RecGetType(const unsigned char* rec);
void *ut_align(const void *ptr, unsigned long align_no);
uint32_t page_offset(const void *ptr);
void *ut_align_down(const void *ptr, unsigned long align_no);
unsigned char *page_align(const void *ptr);
uint32_t RecGetNextOffs(const unsigned char* rec, bool comp);
uint16_t PageHeaderGetField(const unsigned char* page, uint32_t field);
uint16_t PageDirGetNHeap(const unsigned char* page);
bool PageIsCompact(const unsigned char* page);
uint16_t PageGetType(const unsigned char* page);
bool page_rec_check(const unsigned char* rec);
bool RecIsInfimum(const unsigned char* rec);
bool RecIsSupremum(const unsigned char* rec);
constexpr uint32_t REC_OFFS_COMPACT = 1U << 31;
constexpr uint32_t REC_OFFS_SQL_NULL = 1U << 31;
constexpr uint32_t REC_OFFS_EXTERNAL = 1 << 30;
constexpr uint32_t REC_OFFS_DEFAULT = 1 << 29;
constexpr uint32_t REC_OFFS_DROP = 1 << 28;
constexpr uint32_t REC_OFFS_MASK = REC_OFFS_DROP - 1;
constexpr uint32_t REC_NEW_HEAP_NO = 4;
constexpr uint32_t REC_HEAP_NO_SHIFT = 3;
constexpr uint32_t REC_NEXT = 2;
constexpr uint32_t REC_NEXT_MASK = 0xFFFFUL;
constexpr uint32_t REC_NEXT_SHIFT = 0;
constexpr uint32_t REC_OLD_SHORT = 3; /* This is single byte bit-field */
constexpr uint32_t REC_OLD_SHORT_MASK = 0x1UL;
constexpr uint32_t REC_OLD_SHORT_SHIFT = 0;
constexpr uint32_t REC_OLD_N_FIELDS = 4;
constexpr uint32_t REC_OLD_N_FIELDS_MASK = 0x7FEUL;
constexpr uint32_t REC_OLD_N_FIELDS_SHIFT = 1;
constexpr uint32_t REC_NEW_STATUS = 3; /* This is single byte bit-field */
constexpr uint32_t REC_NEW_STATUS_MASK = 0x7UL;
constexpr uint32_t REC_NEW_STATUS_SHIFT = 0;
constexpr uint32_t REC_OLD_HEAP_NO = 5;
constexpr uint32_t REC_HEAP_NO_MASK = 0xFFF8UL;
constexpr uint32_t REC_OLD_N_OWNED = 6; /* This is single byte bit-field */
constexpr uint32_t REC_NEW_N_OWNED = 5; /* This is single byte bit-field */
constexpr uint32_t REC_N_OWNED_MASK = 0xFUL;
constexpr uint32_t REC_N_OWNED_SHIFT = 0;
constexpr uint32_t REC_OLD_INFO_BITS = 6; /* This is single byte bit-field */
constexpr uint32_t REC_NEW_INFO_BITS = 5; /* This is single byte bit-field */
constexpr uint32_t REC_TMP_INFO_BITS = 1; /* This is single byte bit-field */
constexpr uint32_t REC_INFO_BITS_MASK = 0xF0UL;
constexpr uint32_t REC_INFO_BITS_SHIFT = 0;
static_assert((REC_OLD_SHORT_MASK << (8 * (REC_OLD_SHORT - 3)) ^
               REC_OLD_N_FIELDS_MASK << (8 * (REC_OLD_N_FIELDS - 4)) ^
               REC_HEAP_NO_MASK << (8 * (REC_OLD_HEAP_NO - 4)) ^
               REC_N_OWNED_MASK << (8 * (REC_OLD_N_OWNED - 3)) ^
               REC_INFO_BITS_MASK << (8 * (REC_OLD_INFO_BITS - 3)) ^
               0xFFFFFFFFUL) == 0,
              "sum of old-style masks != 0xFFFFFFFFUL");
static_assert((REC_NEW_STATUS_MASK << (8 * (REC_NEW_STATUS - 3)) ^
               REC_HEAP_NO_MASK << (8 * (REC_NEW_HEAP_NO - 4)) ^
               REC_N_OWNED_MASK << (8 * (REC_NEW_N_OWNED - 3)) ^
               REC_INFO_BITS_MASK << (8 * (REC_NEW_INFO_BITS - 3)) ^
               0xFFFFFFUL) == 0,
              "sum of new-style masks != 0xFFFFFFUL");
constexpr uint32_t REC_INFO_MIN_REC_FLAG = 0x10UL;
constexpr uint32_t REC_INFO_DELETED_FLAG = 0x20UL;
constexpr uint32_t REC_INFO_VERSION_FLAG = 0x40UL;
constexpr uint32_t REC_INFO_INSTANT_FLAG = 0x80UL;
constexpr uint32_t REC_N_OLD_EXTRA_BYTES = 6;
constexpr int32_t REC_N_NEW_EXTRA_BYTES = 5;
constexpr uint32_t REC_N_TMP_EXTRA_BYTES = 1;
constexpr auto MAX_REC_PER_PAGE = UNIV_PAGE_SIZE_MAX / REC_N_NEW_EXTRA_BYTES;
constexpr uint32_t REC_STATUS_ORDINARY = 0;
constexpr uint32_t REC_STATUS_NODE_PTR = 1;
constexpr uint32_t REC_STATUS_INFIMUM = 2;
constexpr uint32_t REC_STATUS_SUPREMUM = 3;
constexpr uint32_t REC_NODE_PTR_SIZE = 4;
constexpr uint32_t REC_1BYTE_SQL_NULL_MASK = 0x80UL;
constexpr uint32_t REC_2BYTE_SQL_NULL_MASK = 0x8000UL;
constexpr uint32_t REC_2BYTE_EXTERN_MASK = 0x4000UL;
constexpr uint32_t REC_OFFS_HEADER_SIZE = 2;
constexpr uint32_t REC_OFFS_NORMAL_SIZE = 100;
constexpr uint32_t REC_OFFS_SMALL_SIZE = 10;
constexpr uint32_t REC_MAX_N_FIELDS = 1024 - 1;
constexpr uint32_t REC_MAX_HEAP_NO = 2 * 8192 - 1;
constexpr uint32_t REC_MAX_N_OWNED = 16 - 1;
constexpr uint32_t REC_MAX_N_USER_FIELDS =
    REC_MAX_N_FIELDS - DATA_N_SYS_COLS * 2;
constexpr uint32_t REC_ANTELOPE_MAX_INDEX_COL_LEN = 768;
constexpr uint32_t REC_VERSION_56_MAX_INDEX_COL_LEN = 3072;
constexpr uint8_t REC_N_FIELDS_TWO_BYTES_FLAG = 0x80;
constexpr uint8_t REC_N_FIELDS_ONE_BYTE_MAX = 0x7F;

// SDI related
/** Length of ID field in record of SDI Index. */
static const uint32_t REC_DATA_ID_LEN = 8;
/** Length of TYPE field in record of SDI Index. */
static const uint32_t REC_DATA_TYPE_LEN = 4;
/** Length of UNCOMPRESSED_LEN field in record of SDI Index. */
static const uint32_t REC_DATA_UNCOMP_LEN = 4;
/** Length of COMPRESSED_LEN field in record of SDI Index. */
static const uint32_t REC_DATA_COMP_LEN = 4;
/** SDI Index record Origin. */
static const uint32_t REC_ORIGIN = 0;
/** Length of SDI Index record header. */
static const uint32_t REC_MIN_HEADER_SIZE = REC_N_NEW_EXTRA_BYTES;
/** Stored at rec origin minus 3rd byte. Only 3bits of 3rd byte are used for
rec type. */
// static const uint32_t REC_OFF_TYPE = 3;
/** Stored at rec_origin minus 2nd byte and length 2 bytes. */
static const uint32_t REC_OFF_NEXT = 2;
/** Offset of TYPE field in record (0). */
static const uint32_t REC_OFF_DATA_TYPE = REC_ORIGIN;
// ut_ad(REC_OFF_DATA_ID == 0);
/** Offset of ID field in record (4). */
static const uint32_t REC_OFF_DATA_ID = REC_OFF_DATA_TYPE + REC_DATA_TYPE_LEN;
// ut_ad(REC_OFF_DATA_TYPE == 8);
/** Offset of 6-byte trx id (12). */
static const uint32_t REC_OFF_DATA_TRX_ID = REC_OFF_DATA_ID + REC_DATA_ID_LEN;
// ut_ad(REC_OFF_DATA_TRX_ID == 12);
/** 7-byte roll-ptr (18). */
static const uint32_t REC_OFF_DATA_ROLL_PTR =
    REC_OFF_DATA_TRX_ID + DATA_TRX_ID_LEN;
// ut_ad(REC_OFF_DATA_ROLL_PTR == 18);
/** 4-byte un-compressed len (25) */
static const uint32_t REC_OFF_DATA_UNCOMP_LEN =
    REC_OFF_DATA_ROLL_PTR + DATA_ROLL_PTR_LEN;
/** 4-byte compressed len (29) */
static const uint32_t REC_OFF_DATA_COMP_LEN =
    REC_OFF_DATA_UNCOMP_LEN + REC_DATA_UNCOMP_LEN;
/** Variable length Data (33). */
static const uint32_t REC_OFF_DATA_VARCHAR =
    REC_OFF_DATA_COMP_LEN + REC_DATA_COMP_LEN;
/** Record size in page. This will be used determine the maximum number
of records on a page. */
static const uint32_t SDI_REC_SIZE = 1                     /* rec_len */
                                     + REC_MIN_HEADER_SIZE /* rec_header */
                                     + REC_DATA_TYPE_LEN   /* type field size */
                                     + REC_DATA_ID_LEN     /* id field len */
                                     + DATA_ROLL_PTR_LEN   /* roll ptr len */
                                     + DATA_TRX_ID_LEN /* TRX_ID len */;
/** SDI BLOB not expected before the following page number.
0 (tablespace header), 1 (tabespace bitmap), 2 (ibuf bitmap)
3 (SDI Index root page) */
static const uint64_t SDI_BLOB_ALLOWED = 4;

// Page header related
constexpr uint32_t PAGE_HEADER = FSEG_PAGE_DATA;
constexpr uint32_t PAGE_N_DIR_SLOTS = 0;
constexpr uint32_t PAGE_HEAP_TOP = 2;
constexpr uint32_t PAGE_N_HEAP = 4;
constexpr uint32_t PAGE_FREE = 6;
constexpr uint32_t PAGE_GARBAGE = 8;
constexpr uint32_t PAGE_LAST_INSERT = 10;
constexpr uint32_t PAGE_DIRECTION = 12;
constexpr uint32_t PAGE_N_DIRECTION = 14;
constexpr uint32_t PAGE_N_RECS = 16;
constexpr uint32_t PAGE_MAX_TRX_ID = 18;
constexpr uint32_t PAGE_HEADER_PRIV_END = 26;
constexpr uint32_t PAGE_LEVEL = 26;
constexpr uint32_t PAGE_INDEX_ID = 28;
constexpr uint32_t PAGE_BTR_SEG_LEAF = 36;
constexpr uint32_t PAGE_BTR_IBUF_FREE_LIST = PAGE_BTR_SEG_LEAF;
constexpr uint32_t PAGE_BTR_IBUF_FREE_LIST_NODE = PAGE_BTR_SEG_LEAF;
constexpr uint32_t PAGE_BTR_SEG_TOP = 36 + FSEG_HEADER_SIZE;
constexpr uint32_t PAGE_DATA = PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE;
#define PAGE_OLD_INFIMUM (PAGE_DATA + 1 + REC_N_OLD_EXTRA_BYTES)
#define PAGE_OLD_SUPREMUM (PAGE_DATA + 2 + 2 * REC_N_OLD_EXTRA_BYTES + 8)
#define PAGE_OLD_SUPREMUM_END (PAGE_OLD_SUPREMUM + 9)
#define PAGE_NEW_INFIMUM (PAGE_DATA + REC_N_NEW_EXTRA_BYTES)
#define PAGE_NEW_SUPREMUM (PAGE_DATA + 2 * REC_N_NEW_EXTRA_BYTES + 8)
#define PAGE_NEW_SUPREMUM_END (PAGE_NEW_SUPREMUM + 8)
constexpr uint32_t PAGE_HEAP_NO_INFIMUM = 0;
constexpr uint32_t PAGE_HEAP_NO_SUPREMUM = 1;
constexpr uint32_t PAGE_HEAP_NO_USER_LOW = 2;


// File releated
constexpr uint16_t FIL_PAGE_INDEX = 17855;
constexpr uint16_t FIL_PAGE_RTREE = 17854;
constexpr uint16_t FIL_PAGE_SDI = 17853;
constexpr uint16_t FIL_PAGE_TYPE_UNUSED = 1;
constexpr uint16_t FIL_PAGE_UNDO_LOG = 2;
constexpr uint16_t FIL_PAGE_INODE = 3;
constexpr uint16_t FIL_PAGE_IBUF_FREE_LIST = 4;
constexpr uint16_t FIL_PAGE_TYPE_ALLOCATED = 0;
constexpr uint16_t FIL_PAGE_IBUF_BITMAP = 5;
constexpr uint16_t FIL_PAGE_TYPE_SYS = 6;
constexpr uint16_t FIL_PAGE_TYPE_TRX_SYS = 7;
constexpr uint16_t FIL_PAGE_TYPE_FSP_HDR = 8;
constexpr uint16_t FIL_PAGE_TYPE_XDES = 9;
constexpr uint16_t FIL_PAGE_TYPE_BLOB = 10;
constexpr uint16_t FIL_PAGE_TYPE_ZBLOB = 11;
constexpr uint16_t FIL_PAGE_TYPE_ZBLOB2 = 12;
constexpr uint16_t FIL_PAGE_TYPE_UNKNOWN = 13;
constexpr uint16_t FIL_PAGE_COMPRESSED = 14;
constexpr uint16_t FIL_PAGE_ENCRYPTED = 15;
constexpr uint16_t FIL_PAGE_COMPRESSED_AND_ENCRYPTED = 16;
constexpr uint16_t FIL_PAGE_ENCRYPTED_RTREE = 17;
constexpr uint16_t FIL_PAGE_SDI_BLOB = 18;
constexpr uint16_t FIL_PAGE_SDI_ZBLOB = 19;
constexpr uint16_t FIL_PAGE_TYPE_LEGACY_DBLWR = 20;
constexpr uint16_t FIL_PAGE_TYPE_RSEG_ARRAY = 21;
constexpr uint16_t FIL_PAGE_TYPE_LOB_INDEX = 22;
constexpr uint16_t FIL_PAGE_TYPE_LOB_DATA = 23;
constexpr uint16_t FIL_PAGE_TYPE_LOB_FIRST = 24;
constexpr uint16_t FIL_PAGE_TYPE_ZLOB_FIRST = 25;
constexpr uint16_t FIL_PAGE_TYPE_ZLOB_DATA = 26;
constexpr uint16_t FIL_PAGE_TYPE_ZLOB_INDEX = 27;
constexpr uint16_t FIL_PAGE_TYPE_ZLOB_FRAG = 28;
constexpr uint16_t FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = 29;
constexpr uint16_t FIL_PAGE_TYPE_LAST = FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY;
std::string PageType2String(uint32_t type);
constexpr uint32_t FIL_NULL = std::numeric_limits<uint32_t>::max();

// Blob related
const uint32_t BTR_EXTERN_SPACE_ID = 0;
const uint32_t BTR_EXTERN_PAGE_NO = 4;
const uint32_t BTR_EXTERN_OFFSET = 8;
const uint32_t BTR_EXTERN_VERSION = BTR_EXTERN_OFFSET;
const uint32_t BTR_EXTERN_LEN = 12;
const uint32_t BTR_EXTERN_OWNER_FLAG = 128UL;
const uint32_t BTR_EXTERN_INHERITED_FLAG = 64UL;
const uint32_t BTR_EXTERN_BEING_MODIFIED_FLAG = 32UL;
const uint32_t LOB_HDR_PART_LEN = 0;
const uint32_t LOB_HDR_NEXT_PAGE_NO = 4;
const uint32_t LOB_HDR_SIZE = 8;
const uint32_t ZLOB_PAGE_DATA = FIL_PAGE_DATA;

// Index related
constexpr uint32_t DICT_INDEX_SPATIAL_NODEPTR_SIZE = 1;

// Others
#define ULINT_UNDEFINED ~uint32_t{0U}
}  // namespace ibd_ninja
#endif  // IBDUTILS_H_
