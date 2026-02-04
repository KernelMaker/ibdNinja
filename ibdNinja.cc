/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#include "ibdNinja.h"
#include "Table.h"
#include "Index.h"
#include "Column.h"
#include "Record.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <zlib.h>

#include <fcntl.h>
#include <sys/stat.h>

#include <cassert>
#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <sys/stat.h>
#include <fstream>

namespace ibd_ninja {

#define ninja_error(fmt, ...) \
    fprintf(stderr, "[ibdNinja][ERROR]: " fmt "\n", ##__VA_ARGS__)
#define ninja_warn(fmt, ...) \
    fprintf(stderr, "[ibdNinja][WARN]: " fmt "\n", ##__VA_ARGS__)
#define ninja_pt(p, fmt, ...) \
    do { if (p) printf(fmt, ##__VA_ARGS__); } while (0)

/* ------ LOB Helpers ------ */
static constexpr uint32_t LOB_MAX_FETCH_SIZE = 16 * 1024 * 1024;  // 16MB cap
static constexpr uint32_t LOB_MAX_PAGES_VISITED = 4096;

static FilAddr ReadFilAddr(const unsigned char* ptr) {
  FilAddr addr;
  addr.page_no = ReadFrom4B(ptr);
  addr.byte_offset = ReadFrom2B(ptr + 4);
  return addr;
}

static FlstBaseNode ReadFlstBaseNode(const unsigned char* ptr) {
  FlstBaseNode node;
  node.length = ReadFrom4B(ptr + FLST_LEN);
  node.first = ReadFilAddr(ptr + FLST_FIRST);
  node.last = ReadFilAddr(ptr + FLST_LAST);
  return node;
}

static uint64_t ReadTrxId(const unsigned char* ptr) {
  return (static_cast<uint64_t>(ReadFrom2B(ptr)) << 32) | ReadFrom4B(ptr + 2);
}

static LobIndexEntry ReadLobIndexEntry(const unsigned char* ptr) {
  LobIndexEntry entry;
  entry.prev = ReadFilAddr(ptr + LOB_ENTRY_PREV);
  entry.next = ReadFilAddr(ptr + LOB_ENTRY_NEXT);
  entry.versions = ReadFlstBaseNode(ptr + LOB_ENTRY_VERSIONS);
  entry.creator_trx_id = ReadTrxId(ptr + LOB_ENTRY_CREATOR_TRX_ID);
  entry.modifier_trx_id = ReadTrxId(ptr + LOB_ENTRY_MODIFIER_TRX_ID);
  entry.creator_undo_no = ReadFrom4B(ptr + LOB_ENTRY_CREATOR_UNDO_NO);
  entry.modifier_undo_no = ReadFrom4B(ptr + LOB_ENTRY_MODIFIER_UNDO_NO);
  entry.data_page_no = ReadFrom4B(ptr + LOB_ENTRY_PAGE_NO);
  // data_len is stored as a 2-byte big-endian value on disk
  entry.data_len = ReadFrom2B(ptr + LOB_ENTRY_DATA_LEN);
  entry.lob_version = ReadFrom4B(ptr + LOB_ENTRY_LOB_VERSION);
  return entry;
}

static LobFirstPageHeader ReadLobFirstPageHeader(const unsigned char* page_buf) {
  const unsigned char* p = page_buf + FIL_PAGE_DATA;
  LobFirstPageHeader hdr;
  hdr.version = ReadFrom1B(p + LOB_FIRST_PAGE_VERSION);
  hdr.flags = ReadFrom1B(p + LOB_FIRST_PAGE_FLAGS);
  hdr.lob_version = ReadFrom4B(p + LOB_FIRST_PAGE_LOB_VERSION);
  hdr.last_trx_id = ReadTrxId(p + LOB_FIRST_PAGE_LAST_TRX_ID);
  hdr.last_undo_no = ReadFrom4B(p + LOB_FIRST_PAGE_LAST_UNDO_NO);
  hdr.data_len = ReadFrom4B(p + LOB_FIRST_PAGE_DATA_LEN);
  hdr.creator_trx_id = ReadTrxId(p + LOB_FIRST_PAGE_TRX_ID);
  hdr.index_list = ReadFlstBaseNode(p + LOB_FIRST_PAGE_INDEX_LIST);
  hdr.free_list = ReadFlstBaseNode(p + LOB_FIRST_PAGE_FREE_LIST);
  return hdr;
}

static uint64_t FetchModernUncompLob(uint32_t first_page_no,
                                     uint64_t total_length,
                                     unsigned char* dest_buf,
                                     bool* error) {
  *error = false;
  unsigned char page_buf[UNIV_PAGE_SIZE_MAX];
  unsigned char data_buf[UNIV_PAGE_SIZE_MAX];

  uint64_t cap = total_length;
  if (cap > LOB_MAX_FETCH_SIZE) {
    cap = LOB_MAX_FETCH_SIZE;
  }

  ssize_t bytes = ibdNinja::ReadPage(first_page_no, page_buf);
  if (bytes != g_page_physical_size) {
    ninja_error("Failed to read LOB first page: %u", first_page_no);
    *error = true;
    return 0;
  }

  uint16_t page_type = PageGetType(page_buf);
  if (page_type != FIL_PAGE_TYPE_LOB_FIRST) {
    ninja_error("Expected LOB_FIRST page type (24), got %u", page_type);
    *error = true;
    return 0;
  }

  LobFirstPageHeader hdr = ReadLobFirstPageHeader(page_buf);

  // Data offset on the first page: after the index entry array
  uint32_t first_page_data_offset = FIL_PAGE_DATA + LOB_FIRST_PAGE_INDEX_BEGIN +
                                    LOB_FIRST_PAGE_N_ENTRIES * LOB_INDEX_ENTRY_SIZE;

  FilAddr cur_addr = hdr.index_list.first;
  uint64_t bytes_copied = 0;
  uint32_t pages_visited = 0;

  // Cache: track the page number of the currently loaded index page
  uint32_t cached_index_page_no = first_page_no;
  // page_buf already contains the first page

  while (!cur_addr.is_null() && bytes_copied < cap) {
    if (++pages_visited > LOB_MAX_PAGES_VISITED) {
      ninja_error("LOB traversal exceeded max pages limit (%u), "
                  "possible corruption", LOB_MAX_PAGES_VISITED);
      *error = true;
      break;
    }

    // Load the index page if different from cached
    if (cur_addr.page_no != cached_index_page_no) {
      bytes = ibdNinja::ReadPage(cur_addr.page_no, page_buf);
      if (bytes != g_page_physical_size) {
        ninja_error("Failed to read LOB index page: %u", cur_addr.page_no);
        *error = true;
        break;
      }
      cached_index_page_no = cur_addr.page_no;
    }

    LobIndexEntry entry = ReadLobIndexEntry(page_buf + cur_addr.byte_offset);

    const unsigned char* data_src = nullptr;
    uint32_t data_len = entry.data_len;

    if (bytes_copied + data_len > cap) {
      data_len = static_cast<uint32_t>(cap - bytes_copied);
    }

    if (entry.data_page_no == first_page_no) {
      // Data is on the first page itself
      // Need to re-read first page if we changed the cached page
      if (cached_index_page_no != first_page_no) {
        bytes = ibdNinja::ReadPage(first_page_no, data_buf);
        if (bytes != g_page_physical_size) {
          ninja_error("Failed to re-read LOB first page: %u", first_page_no);
          *error = true;
          break;
        }
        data_src = data_buf + first_page_data_offset;
      } else {
        data_src = page_buf + first_page_data_offset;
      }
    } else {
      bytes = ibdNinja::ReadPage(entry.data_page_no, data_buf);
      if (bytes != g_page_physical_size) {
        ninja_error("Failed to read LOB data page: %u", entry.data_page_no);
        *error = true;
        break;
      }
      uint16_t data_page_type = PageGetType(data_buf);
      if (data_page_type != FIL_PAGE_TYPE_LOB_DATA) {
        ninja_error("Expected LOB_DATA page type (23), got %u on page %u",
                    data_page_type, entry.data_page_no);
        *error = true;
        break;
      }
      data_src = data_buf + FIL_PAGE_DATA + LOB_DATA_PAGE_DATA_BEGIN;
    }

    if (dest_buf != nullptr) {
      memcpy(dest_buf + bytes_copied, data_src, data_len);
    }
    bytes_copied += data_len;

    cur_addr = entry.next;
  }

  return bytes_copied;
}

static void PrintLobVersionHistory(uint32_t first_page_no, bool print) {
  unsigned char page_buf[UNIV_PAGE_SIZE_MAX];
  unsigned char ver_buf[UNIV_PAGE_SIZE_MAX];

  ssize_t bytes = ibdNinja::ReadPage(first_page_no, page_buf);
  if (bytes != g_page_physical_size) {
    ninja_error("Failed to read LOB first page for version history: %u",
                first_page_no);
    return;
  }

  if (PageGetType(page_buf) != FIL_PAGE_TYPE_LOB_FIRST) {
    return;
  }

  LobFirstPageHeader hdr = ReadLobFirstPageHeader(page_buf);
  FilAddr cur_addr = hdr.index_list.first;
  uint32_t entry_no = 0;
  uint32_t cached_page_no = first_page_no;
  uint32_t pages_visited = 0;

  ninja_pt(print, "\n                      [LOB VERSION HISTORY]\n");

  while (!cur_addr.is_null()) {
    if (++pages_visited > LOB_MAX_PAGES_VISITED) {
      break;
    }

    if (cur_addr.page_no != cached_page_no) {
      bytes = ibdNinja::ReadPage(cur_addr.page_no, page_buf);
      if (bytes != g_page_physical_size) {
        break;
      }
      cached_page_no = cur_addr.page_no;
    }

    LobIndexEntry entry = ReadLobIndexEntry(page_buf + cur_addr.byte_offset);

    ninja_pt(print, "                      [LOB INDEX ENTRY %u]:\n", entry_no);
    ninja_pt(print, "                        Current (v%u): page=%u, len=%u, "
             "creator_trx=%" PRIu64 ", modifier_trx=%" PRIu64 "\n",
             entry.lob_version, entry.data_page_no, entry.data_len,
             entry.creator_trx_id, entry.modifier_trx_id);

    // Traverse old versions if present
    if (entry.versions.length > 0) {
      FilAddr ver_addr = entry.versions.first;
      uint32_t ver_cached_page_no = 0;
      while (!ver_addr.is_null()) {
        if (++pages_visited > LOB_MAX_PAGES_VISITED) {
          break;
        }
        if (ver_addr.page_no != ver_cached_page_no) {
          bytes = ibdNinja::ReadPage(ver_addr.page_no, ver_buf);
          if (bytes != g_page_physical_size) {
            break;
          }
          ver_cached_page_no = ver_addr.page_no;
        }
        LobIndexEntry old_entry = ReadLobIndexEntry(
            ver_buf + ver_addr.byte_offset);
        ninja_pt(print, "                        Old    (v%u): page=%u, "
                 "len=%u, creator_trx=%" PRIu64 ", modifier_trx=%" PRIu64 "\n",
                 old_entry.lob_version, old_entry.data_page_no,
                 old_entry.data_len, old_entry.creator_trx_id,
                 old_entry.modifier_trx_id);
        ver_addr = old_entry.next;
      }
    }

    entry_no++;
    cur_addr = entry.next;
  }
}

void FetchAndDisplayExternalLob(uint32_t space_id, uint32_t page_no,
                                uint32_t version, uint64_t ext_len,
                                LobOutputFormat format,
                                bool show_versions, bool print) {
  (void)space_id;
  (void)version;

  unsigned char tmp_buf[UNIV_PAGE_SIZE_MAX];
  ssize_t bytes = ibdNinja::ReadPage(page_no, tmp_buf);
  if (bytes != g_page_physical_size) {
    ninja_pt(print, "\n                      "
             "[LOB: Failed to read page %u]", page_no);
    return;
  }

  uint16_t page_type = PageGetType(tmp_buf);

  if (page_type == FIL_PAGE_TYPE_LOB_FIRST) {
    uint64_t fetch_len = ext_len;
    if (fetch_len > LOB_MAX_FETCH_SIZE) {
      fetch_len = LOB_MAX_FETCH_SIZE;
    }

    unsigned char* lob_data = nullptr;
    if (format != LobOutputFormat::SUMMARY_ONLY) {
      lob_data = new unsigned char[fetch_len + 1]();
    }

    bool error = false;
    uint64_t fetched = FetchModernUncompLob(page_no, ext_len, lob_data, &error);

    if (error) {
      ninja_pt(print, "\n                      "
               "[LOB: Error fetching data from page %u]", page_no);
      delete[] lob_data;
      return;
    }

    switch (format) {
      case LobOutputFormat::SUMMARY_ONLY: {
        LobFirstPageHeader hdr = ReadLobFirstPageHeader(tmp_buf);
        ninja_pt(print, "\n                      "
                 "[LOB SUMMARY: type=LOB_FIRST, lob_version=%u, "
                 "data_len=%u, index_entries=%u, total_ext_len=%" PRIu64 "]",
                 hdr.lob_version, hdr.data_len,
                 hdr.index_list.length, ext_len);
      } break;

      case LobOutputFormat::HEX: {
        uint32_t show_len = static_cast<uint32_t>(fetched);
        if (show_len > g_lob_text_truncate_len) {
          show_len = g_lob_text_truncate_len;
        }
        ninja_pt(print, "\n                      [LOB DATA (hex, %" PRIu64
                 " bytes total)]: ", fetched);
        for (uint32_t i = 0; i < show_len; i++) {
          ninja_pt(print, "%02x ", lob_data[i]);
          if ((i + 1) % 16 == 0 && i + 1 < show_len) {
            ninja_pt(print, "\n                      ");
          }
        }
        if (fetched > show_len) {
          ninja_pt(print, "\n                      "
                   "[... %" PRIu64 " more bytes]",
                   fetched - show_len);
        }
      } break;

      case LobOutputFormat::TEXT_TRUNC: {
        uint32_t show_len = static_cast<uint32_t>(fetched);
        if (show_len > g_lob_text_truncate_len) {
          show_len = g_lob_text_truncate_len;
        }
        lob_data[show_len] = '\0';
        ninja_pt(print, "\n                      [LOB DATA (text, %" PRIu64
                 " bytes total)]: ", fetched);
        ninja_pt(print, "%.*s", show_len, lob_data);
        if (fetched > show_len) {
          ninja_pt(print, "\n                      "
                   "[... %" PRIu64 " more bytes]",
                   fetched - show_len);
        }
      } break;

      case LobOutputFormat::RAW_FILE: {
        struct stat st;
        if (stat(g_lob_output_dir.c_str(), &st) != 0) {
          mkdir(g_lob_output_dir.c_str(), 0755);
        }
        char filename[256];
        snprintf(filename, sizeof(filename), "%s/page%u.bin",
                 g_lob_output_dir.c_str(), page_no);
        std::ofstream ofs(filename, std::ios::binary);
        if (ofs.is_open()) {
          ofs.write(reinterpret_cast<const char*>(lob_data), fetched);
          ofs.close();
          ninja_pt(print, "\n                      "
                   "[LOB DATA written to %s (%" PRIu64 " bytes)]",
                   filename, fetched);
        } else {
          ninja_pt(print, "\n                      "
                   "[LOB: Failed to write to %s]", filename);
        }
      } break;
    }

    delete[] lob_data;

    if (show_versions) {
      PrintLobVersionHistory(page_no, print);
    }
  } else if (page_type == FIL_PAGE_TYPE_BLOB || page_type == FIL_PAGE_SDI_BLOB) {
    ninja_pt(print, "\n                      "
             "[LOB: Legacy BLOB format (page type %u), "
             "display not yet supported]", page_type);
  } else {
    ninja_pt(print, "\n                      "
             "[LOB: Unsupported page type %u (%s)]",
             page_type, PageType2String(page_type).c_str());
  }
}

/* ------ Ninja ------ */
static bool ValidateSDI(const rapidjson::Document& doc) {
  bool ret = true;
  if (!doc.HasMember("dd_object_type") ||
      !doc["dd_object_type"].IsString() ||
      (std::string(doc["dd_object_type"].GetString()) != "Table" &&
      (std::string(doc["dd_object_type"].GetString()) != "Tablespace")) ||
      !doc.HasMember("dd_object") ||
      !doc["dd_object"].IsObject()) {
    ret = false;
  }

  if (!doc.HasMember("mysqld_version_id") ||
      !doc["mysqld_version_id"].IsUint() ||
      !doc.HasMember("dd_version") ||
      !doc["dd_version"].IsUint() ||
      !doc.HasMember("sdi_version") ||
      !doc["sdi_version"].IsUint()) {
    ret = false;
  }
  return ret;
}

const char* ibdNinja::g_version_ = "1.0.0";

void ibdNinja::PrintName() {
  fprintf(stdout,
"|--------------------------------------------------------------------------------------------------------------|\n"
"|    _      _                         _   _           _      _                              _                  |\n"
"|   (_)    (_)                       (_) (_) _       (_)    (_)                            (_)                 |\n"
"| _  _     (_) _  _  _       _  _  _ (_) (_)(_)_     (_)  _  _      _  _  _  _           _  _     _  _  _      |\n"
"|(_)(_)    (_)(_)(_)(_)_   _(_)(_)(_)(_) (_)  (_)_   (_) (_)(_)    (_)(_)(_)(_)_        (_)(_)   (_)(_)(_) _   |\n"
"|   (_)    (_)        (_) (_)        (_) (_)    (_)_ (_)    (_)    (_)        (_)          (_)    _  _  _ (_)  |\n"
"|   (_)    (_)        (_) (_)        (_) (_)      (_)(_)    (_)    (_)        (_)          (_)  _(_)(_)(_)(_)  |\n"
"| _ (_) _  (_) _  _  _(_) (_)_  _  _ (_) (_)         (_)  _ (_) _  (_)        (_)          (_) (_)_  _  _ (_)_ |\n"
"|(_)(_)(_) (_)(_)(_)(_)     (_)(_)(_)(_) (_)         (_) (_)(_)(_) (_)        (_)  _      _(_)   (_)(_)(_)  (_)|\n"
"|                                                                                 (_)_  _(_)                   |\n"
"|                                                                                   (_)(_)                     |\n"
"|--------------------------------------------------------------------------------------------------------------|\n");
}

ibdNinja* ibdNinja::CreateNinja(const char* ibd_filename) {
  unsigned char buf[UNIV_PAGE_SIZE_MAX];
  memset(buf, 0, UNIV_PAGE_SIZE_MAX);
  struct stat stat_info;
  if (stat(ibd_filename, &stat_info) != 0) {
    ninja_error("Failed to get file stats: %s, error: %d(%s)",
            ibd_filename, errno, strerror(errno));
    return nullptr;
  }
  uint64_t size = stat_info.st_size;
  g_fd = open(ibd_filename, O_RDONLY);
  if (g_fd == -1) {
    ninja_error("Failed to open file: %s, error: %d(%s)",
            ibd_filename, errno, strerror(errno));
    close(g_fd);
    return nullptr;
  }
  if (size < UNIV_ZIP_SIZE_MIN) {
    ninja_error("The file is too small to be a valid ibd file");
    close(g_fd);
    return nullptr;
  }
  ssize_t bytes = read(g_fd, buf, UNIV_ZIP_SIZE_MIN);
  if (bytes != UNIV_ZIP_SIZE_MIN) {
    ninja_error("Failed to read file header: %s, error: %d(%s)",
            ibd_filename, errno, strerror(errno));
    close(g_fd);
    return nullptr;
  }
  uint32_t space_id = ReadFrom4B(buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
  uint32_t first_page_no = ReadFrom4B(buf + FIL_PAGE_OFFSET);
  uint32_t flags = FSPHeaderGetFlags(buf);
  bool is_valid_flags = FSPFlagsIsValid(flags);
  uint32_t page_size = 0;
  if (is_valid_flags) {
    uint32_t ssize = FSP_FLAGS_GET_PAGE_SSIZE(flags);
    if (ssize == 0) {
      page_size = UNIV_PAGE_SIZE_ORIG;
    } else {
      page_size = ((UNIV_ZIP_SIZE_MIN >> 1) << ssize);
    }
    g_page_size_shift = PageSizeValidate(page_size);
  }
  if (!is_valid_flags || g_page_size_shift == 0) {
    ninja_error("Found corruption on page 0 of file %s",
            ibd_filename);
    close(g_fd);
    return nullptr;
  }

  g_page_logical_size = page_size;

  assert(g_page_logical_size <= UNIV_PAGE_SIZE_MAX);
  assert(g_page_logical_size <= (1 << PAGE_SIZE_T_SIZE_BITS));

  uint32_t ssize = FSP_FLAGS_GET_ZIP_SSIZE(flags);

  if (ssize == 0) {
    g_page_compressed = false;
    g_page_physical_size = g_page_logical_size;
  } else {
    g_page_compressed = true;

    g_page_physical_size = ((UNIV_ZIP_SIZE_MIN >> 1) << ssize);

    assert(g_page_physical_size <= UNIV_ZIP_SIZE_MAX);
    assert(g_page_physical_size <= (1 << PAGE_SIZE_T_SIZE_BITS));
  }
  uint32_t n_pages = size / g_page_physical_size;

  uint32_t post_antelope = FSP_FLAGS_GET_POST_ANTELOPE(flags);
  uint32_t atomic_blobs = FSP_FLAGS_HAS_ATOMIC_BLOBS(flags);
  uint32_t has_data_dir = FSP_FLAGS_HAS_DATA_DIR(flags);
  uint32_t shared = FSP_FLAGS_GET_SHARED(flags);
  uint32_t temporary = FSP_FLAGS_GET_TEMPORARY(flags);
  uint32_t encryption = FSP_FLAGS_GET_ENCRYPTION(flags);
  uint32_t has_sdi = FSP_FLAGS_HAS_SDI(flags);

  bytes = ReadPage(0, buf);
  if (bytes == -1) {
    ninja_error("Failed to read file header: %s, error: %d(%s)",
            ibd_filename, errno, strerror(errno));
    close(g_fd);
    return nullptr;
  }
  uint32_t sdi_offset = XDES_ARR_OFFSET +
                    XDES_SIZE * (g_page_physical_size / FSP_EXTENT_SIZE) +
                    INFO_MAX_SIZE;
  assert(sdi_offset + 4 < bytes);
  uint32_t sdi_root = ReadFrom4B(buf + sdi_offset + 4);
  if (!has_sdi) {
    ninja_warn("FSP doesn't have SDI flags... "
               "Attempting to parse the SDI root page %u directly anyway.",
               sdi_root);
  }
  fprintf(stdout, "=========================================="
                  "==========================================\n");
  fprintf(stdout, "|  FILE INFORMATION                       "
                  "                                         |\n");
  fprintf(stdout, "------------------------------------------"
                  "------------------------------------------\n");
  fprintf(stdout, "    File name:             %s\n", ibd_filename);
  fprintf(stdout, "    File size:             %" PRIu64 " B\n", size);
  fprintf(stdout, "    Space id:              %u\n", space_id);
  fprintf(stdout, "    Page logical size:     %u B\n", g_page_logical_size);
  fprintf(stdout, "    Page physical size:    %u B\n", g_page_physical_size);
  fprintf(stdout, "    Total number of pages: %u\n", n_pages);
  fprintf(stdout, "    Is compressed page?    %u\n", g_page_compressed);
  fprintf(stdout, "    First page number:     %u\n", first_page_no);
  fprintf(stdout, "    SDI root page number:  %u\n", sdi_root);
  fprintf(stdout, "    Post antelop:          %u\n", post_antelope);
  fprintf(stdout, "    Atomic blobs:          %u\n", atomic_blobs);
  fprintf(stdout, "    Has data dir:          %u\n", has_data_dir);
  fprintf(stdout, "    Shared:                %u\n", shared);
  fprintf(stdout, "    Temporary:             %u\n", temporary);
  fprintf(stdout, "    Encryption:            %u\n", encryption);
  fprintf(stdout, "------------------------------------------"
                  "------------------------------------------\n");

  if (g_page_compressed) {
    ninja_error("Parsing of compressed table/tablespaces is "
                "not yet supported.");
    return nullptr;
  }
  if (encryption) {
    ninja_error("Parsing of encrpted space is not yet supported");
    return nullptr;
  }
  if (temporary) {
    ninja_error("Parsing of temporary space is not yet supported");
    return nullptr;
  }

  /* DEBUG
  fprintf(stdout, "[ibdNinja]: Loading SDI...\n"
                  "            1. Traversaling down to the "
                  "leafmost leaf page\n");
  */
  unsigned char buf_unalign[2 * UNIV_PAGE_SIZE_MAX];
  memset(buf_unalign, 0, 2 * UNIV_PAGE_SIZE_MAX);
  unsigned char* buf_align = static_cast<unsigned char*>(
                    ut_align(buf_unalign, g_page_physical_size));
  uint32_t leaf_page_no = 0;
  bool res = SDIToLeftmostLeaf(buf_align, sdi_root, &leaf_page_no);
  if (!res) {
    return nullptr;
  }

  /* DEBUG
  fprintf(stdout, "            2. Parsing SDI records and loading tables:\n");
  */
  unsigned char* current_rec = SDIGetFirstUserRec(buf_align,
                                                  g_page_physical_size);
  if (current_rec == nullptr) {
    return nullptr;
  }
  ibdNinja* ninja = new ibdNinja(n_pages);
  bool corrupt = false;
  uint64_t sdi_id = 0;
  uint64_t sdi_type = 0;
  unsigned char* sdi_data = nullptr;
  uint64_t sdi_data_len = 0;
  while (current_rec != nullptr && !corrupt) {
    bool ret = SDIParseRec(current_rec, &sdi_type, &sdi_id, &sdi_data, &sdi_data_len);
    if (ret == false) {
      corrupt = true;
      break;
    }
    rapidjson::Document doc;
    rapidjson::ParseResult ok = doc.Parse(
                                reinterpret_cast<const char*>(sdi_data));
    if (!ok) {
      std::cerr << "JSON parse error: "
                << rapidjson::GetParseError_En(ok.Code()) << " (offset "
                << ok.Offset() << ")"
                << " sdi: " << sdi_data << std::endl;
      delete[] sdi_data;
      corrupt = true;
      delete ninja;
      return nullptr;
    }
    if (!ValidateSDI(doc)) {
      std::cerr << "Invalid SDI: " << sdi_data << std::endl;
      delete[] sdi_data;
      corrupt = true;
      delete ninja;
      return nullptr;
    }

    [[maybe_unused]] uint32_t mysqld_version_id =
                                doc["mysqld_version_id"].GetUint();
    [[maybe_unused]] uint32_t dd_version =
                                doc["dd_version"].GetUint();
    [[maybe_unused]] uint32_t sdi_version =
                                doc["sdi_version"].GetUint();

    if (std::string(doc["dd_object_type"].GetString()) == "Table") {
      const rapidjson::Value& dd_object = doc["dd_object"];
      Table* table = Table::CreateTable(dd_object, sdi_data);
      if (table != nullptr) {
        // table->DebugDump();
        ninja->AddTable(table);
        /* DEBUG
           fprintf(stdout, "              %s.%s\n",
           table->schema_ref().c_str(),
           table->name().c_str());
         */
      } else {
        ninja_warn("Failed to recover table %s from SDI, "
                    "the SDI may be corrupt, skipping it",
                    dd_object["name"].GetString());
      }
    } else {
      // Tablespace
      delete[] sdi_data;
    }

    current_rec = SDIGetNextRec(current_rec, buf_align,
                            g_page_physical_size, &corrupt);
  }
  if (corrupt) {
    delete ninja;
    return nullptr;
  }
  fprintf(stdout, "[ibdNinja]: Successfully loaded %5lu tables "
                  "with %5lu indexes.\n",
          ninja->tables()->size(), ninja->indexes()->size());
  fprintf(stdout, "=========================================="
                  "==========================================\n\n");
  return ninja;
}

void ibdNinja::AddTable(Table* table) {
  all_tables_.push_back(table);
  if (!table->IsTableSupported()) {
    ninja_warn("Skipping loading table '%s.%s', Reason: '%s'",
               table->schema_ref().c_str(), table->name().c_str(),
               table->UnsupportedReason().c_str());
    return;
  }
  tables_.insert({table->se_private_id(), table});
  for (auto iter : table->indexes()) {
    if (!iter->IsIndexSupported()) {
      ninja_warn("Skipping loading index '%s' of table '%s.%s', "
                 "Reason: '%s'\n",
                 iter->name().c_str(),
                 table->schema_ref().c_str(), table->name().c_str(),
                 iter->UnsupportedReason().c_str());
      continue;
    }
    uint64_t index_id = 0;
    assert(iter->se_private_data().Exists("id"));
    iter->se_private_data().Get("id", &index_id);
    indexes_.insert({index_id, iter});
  }
}

bool ibdNinja::SDIToLeftmostLeaf(unsigned char* buf, uint32_t sdi_root,
                                 uint32_t* leaf_page_no) {
  uint32_t bytes = ReadPage(sdi_root, buf);
  if (bytes != g_page_physical_size) {
    ninja_error("Failed to read page: %u, error: %d(%s)",
            sdi_root, errno, strerror(errno));
    return false;
  }
  uint32_t page_level = ReadFrom2B(buf + FIL_PAGE_DATA + PAGE_LEVEL);
  uint32_t n_of_recs = ReadFrom2B(buf + FIL_PAGE_DATA + PAGE_N_RECS);
  if (n_of_recs == 0) {
    ninja_warn("No SDI is found in this file, "
               "it might be from an older MySQL version.");
    ninja_warn("ibdNinja currently supports MySQL 8.0.16 to 8.0.40, 8.4.0 to 8.4.8, and 9.0.0 to 9.6.0.");
    return false;
  }

  uint32_t curr_page_no = sdi_root;
  unsigned char rec_type_byte;
  unsigned char rec_type;
  /* DEBUG
  fprintf(stdout, "              Level %u: page %u\n",
                  page_level, curr_page_no);
  */
  while (page_level != 0) {
    rec_type_byte = *(buf + PAGE_NEW_INFIMUM - REC_OFF_TYPE);

    rec_type = rec_type_byte & 0x7;

    if (rec_type != REC_STATUS_INFIMUM) {
      ninja_error("Failed to get INFIMUM from page: %u", curr_page_no);
      break;
    }

    uint32_t next_rec_off_t =
        ReadFrom2B(buf + PAGE_NEW_INFIMUM - REC_OFF_NEXT);

    uint32_t child_page_no =
        ReadFrom4B(buf + PAGE_NEW_INFIMUM + next_rec_off_t +
                         REC_DATA_TYPE_LEN + REC_DATA_ID_LEN);

    if (child_page_no < SDI_BLOB_ALLOWED) {
      ninja_error("Failed to get INFIMUM from page: %u", child_page_no);
      return false;
    }

    uint64_t curr_page_level = page_level;

    bytes = ReadPage(child_page_no, buf);
    if (bytes != g_page_physical_size) {
      ninja_error("Failed to read page: %u, error: %d(%s)",
              child_page_no, errno, strerror(errno));
      return false;
    }
    page_level = ReadFrom2B(buf + FIL_PAGE_DATA + PAGE_LEVEL);
    n_of_recs = ReadFrom2B(buf + FIL_PAGE_DATA + PAGE_N_RECS);

    if (page_level != curr_page_level - 1) {
      break;
    }
    curr_page_no = child_page_no;
    /* DEBUG
    fprintf(stdout, "              Level %u: page %u\n",
                    page_level, curr_page_no);
    */
  }

  if (page_level != 0) {
    ninja_error("Failed to find the leftmost page. "
                "The page may be compressed or corrupted\n");

    return false;
  }
  *leaf_page_no = curr_page_no;
  return true;
}

unsigned char* ibdNinja::SDIGetFirstUserRec(unsigned char* buf,
                                            uint32_t buf_len) {
  uint32_t next_rec_off_t =
            ReadFrom2B(buf + PAGE_NEW_INFIMUM - REC_OFF_NEXT);

  assert(PAGE_NEW_INFIMUM + next_rec_off_t != PAGE_NEW_SUPREMUM);

  if (next_rec_off_t > buf_len) {
    assert(0);
    return (nullptr);
  }

  if (memcmp(buf + PAGE_NEW_INFIMUM, "infimum", strlen("infimum")) != 0) {
    ninja_error("Found corrupt INFIMUM");
    return nullptr;
  }

  unsigned char* current_rec = buf + PAGE_NEW_INFIMUM + next_rec_off_t;

  assert(static_cast<uint32_t>(current_rec - buf) <= buf_len);

  bool is_comp = PageIsCompact(buf);

  // TODO(Zhao): Support redundant row format
  assert(is_comp);
  /* record is delete marked, get next record */
  if (RecGetDeletedFlag(current_rec, is_comp) != 0) {
    bool corrupt;
    current_rec =
        SDIGetNextRec(current_rec, buf, buf_len, &corrupt);
    if (corrupt) {
      return nullptr;
    }
  }

  return current_rec;
}

unsigned char* ibdNinja::SDIGetNextRec(unsigned char* current_rec,
                                       unsigned char* buf,
                                       uint32_t buf_len,
                                       bool* corrupt) {
  *corrupt = false;
  uint32_t page_no = ReadFrom4B(buf + FIL_PAGE_OFFSET);
  bool is_comp = PageIsCompact(buf);
  uint32_t next_rec_offset = RecGetNextOffs(current_rec, is_comp);

  if (next_rec_offset == 0) {
    ninja_error("Record is corrupt");
    *corrupt = true;
    return nullptr;
  }

  unsigned char* next_rec = buf + next_rec_offset;

  assert(static_cast<uint32_t>(next_rec - buf) <= buf_len);

  if (RecGetDeletedFlag(next_rec, is_comp) != 0) {
    unsigned char* curr_rec = next_rec;
    return SDIGetNextRec(curr_rec, buf, buf_len, corrupt);
  }

  if (RecGetType(next_rec) == REC_STATUS_SUPREMUM) {
    if (memcmp(next_rec, "supremum", strlen("supremum")) != 0) {
      ninja_error("Found corrupt SUPREMUM on page %u", page_no);
      *corrupt = false;
      return nullptr;
    }

    uint32_t supremum_next_rec_off = ReadFrom2B(next_rec - REC_OFF_NEXT);

    if (supremum_next_rec_off != 0) {
      ninja_error("Found corrupt next of SUPREMUM on page %u", page_no);
      *corrupt = false;
      return nullptr;
    }

    uint32_t next_page_no = ReadFrom4B(buf + FIL_PAGE_NEXT);

    if (next_page_no == FIL_NULL) {
      *corrupt = false;
      return nullptr;
    }

    uint32_t bytes = ReadPage(next_page_no, buf);
    if (bytes != g_page_physical_size) {
      ninja_error("Failed to read page: %u, error: %d(%s)",
              next_page_no, errno, strerror(errno));
      *corrupt = true;
      return nullptr;
    }

    uint16_t page_type = PageGetType(buf);

    if (page_type != FIL_PAGE_SDI) {
      ninja_error("Unexpected page type: %u (%u)",
              page_type, FIL_PAGE_SDI);
      *corrupt = true;
      return nullptr;
    }

    next_rec = SDIGetFirstUserRec(buf, buf_len);
  }

  *corrupt = false;

  return next_rec;
}

bool ibdNinja::SDIParseRec(unsigned char* rec,
                        uint64_t* sdi_type, uint64_t* sdi_id,
                        unsigned char** sdi_data, uint64_t* sdi_data_len) {
  if (RecIsInfimum(rec) || RecIsSupremum(rec)) {
    return false;
  }

  *sdi_type = ReadFrom4B(rec + REC_OFF_DATA_TYPE);
  *sdi_id = ReadFrom8B(rec + REC_OFF_DATA_ID);
  uint32_t sdi_uncomp_len = ReadFrom4B(rec + REC_OFF_DATA_UNCOMP_LEN);
  uint32_t sdi_comp_len = ReadFrom4B(rec + REC_OFF_DATA_COMP_LEN);

  unsigned rec_data_len_partial = *(rec - REC_MIN_HEADER_SIZE - 1);

  uint64_t rec_data_length;
  bool is_rec_data_external = false;
  uint32_t rec_data_in_page_len = 0;

  if (rec_data_len_partial & 0x80) {
    rec_data_in_page_len = (rec_data_len_partial & 0x3f) << 8;
    if (rec_data_len_partial & 0x40) {
      is_rec_data_external = true;
      rec_data_length =
          ReadFrom8B(rec + REC_OFF_DATA_VARCHAR + rec_data_in_page_len +
                           BTR_EXTERN_LEN);

      rec_data_length += rec_data_in_page_len;
    } else {
      rec_data_length = *(rec - REC_MIN_HEADER_SIZE - 2);
      rec_data_length += rec_data_in_page_len;
    }
  } else {
    rec_data_length = rec_data_len_partial;
  }

  unsigned char* str = new unsigned char[rec_data_length + 1]();

  unsigned char* rec_data_origin = rec + REC_OFF_DATA_VARCHAR;

  if (is_rec_data_external) {
    assert(rec_data_in_page_len == 0 ||
          rec_data_in_page_len == REC_ANTELOPE_MAX_INDEX_COL_LEN);

    if (rec_data_in_page_len != 0) {
      memcpy(str, rec_data_origin, rec_data_in_page_len);
    }

    /* Copy from off-page blob-pages */
    uint32_t first_blob_page_no =
        ReadFrom4B(rec + REC_OFF_DATA_VARCHAR + rec_data_in_page_len +
                         BTR_EXTERN_PAGE_NO);

    uint64_t blob_len_retrieved = 0;
    if (g_page_compressed) {
      // TODO(Zhao): Support compressed page
    } else {
      uint32_t n_ext_pages = 0;
      bool error = false;
      blob_len_retrieved = SDIFetchUncompBlob(
          first_blob_page_no, rec_data_length - rec_data_in_page_len,
          str + rec_data_in_page_len, &n_ext_pages, &error);
    }
    *sdi_data_len = rec_data_in_page_len + blob_len_retrieved;
  } else {
    memcpy(str, rec_data_origin, static_cast<size_t>(rec_data_length));
    *sdi_data_len = rec_data_length;
  }

  *sdi_data_len = rec_data_length;
  *sdi_data = str;

  assert(rec_data_length == sdi_comp_len);

  if (rec_data_length != sdi_comp_len) {
    /* Record Corruption */
    ninja_error("SDI record corruption");
    delete[] str;
    return false;
  }

  unsigned char* uncompressed_sdi = new unsigned char[sdi_uncomp_len + 1]();
  int ret;
  uLongf dest_len = sdi_uncomp_len;
  ret = uncompress(uncompressed_sdi, &dest_len,
                   str, sdi_comp_len);

  if (ret != Z_OK) {
    ninja_error("Failed to uncompress SDI record, error: %d", ret);
    delete[] str;
    return false;
  }

  *sdi_data_len = sdi_uncomp_len + 1;
  *sdi_data = uncompressed_sdi;
  delete[] str;

  return true;
}

uint64_t ibdNinja::SDIFetchUncompBlob(uint32_t first_blob_page_no,
                                      uint64_t total_off_page_length,
                                      unsigned char* dest_buf,
                                      uint32_t* n_ext_pages,
                                      bool* error) {
  unsigned char page_buf[UNIV_PAGE_SIZE_MAX];
  uint64_t calc_length = 0;
  uint64_t part_len;
  uint32_t next_page_no = first_blob_page_no;
  *error = false;
  *n_ext_pages = 0;

  do {
    uint32_t bytes = ReadPage(next_page_no, page_buf);
    *n_ext_pages += 1;
    if (bytes != g_page_physical_size) {
      ninja_error("Failed to read BLOB page: %u, error: %d(%s)",
              next_page_no, errno, strerror(errno));
      *error = true;
      break;
    }

    if (PageGetType(page_buf) != FIL_PAGE_SDI_BLOB) {
      ninja_error("Unexpected BLOB page type: %u (%u)",
                      PageGetType(page_buf), FIL_PAGE_SDI_BLOB);
      *error = true;
      break;
    }

    part_len =
        ReadFrom4B(page_buf + FIL_PAGE_DATA + LOB_HDR_PART_LEN);

    if (dest_buf) {
      memcpy(dest_buf + calc_length, page_buf + FIL_PAGE_DATA + LOB_HDR_SIZE,
          static_cast<size_t>(part_len));
    }

    calc_length += part_len;

    next_page_no =
        ReadFrom4B(page_buf + FIL_PAGE_DATA + LOB_HDR_NEXT_PAGE_NO);

    if (next_page_no <= SDI_BLOB_ALLOWED) {
      ninja_error("Failed to get next BLOB page: %u", next_page_no);
      *error = true;
      break;
    }
  } while (next_page_no != FIL_NULL);

  if (!*error) {
    assert(calc_length == total_off_page_length);
  }
  return calc_length;
}

unsigned char* ibdNinja::GetFirstUserRec(unsigned char* buf) {
  uint32_t next_rec_off_t =
            ReadFrom2B(buf + PAGE_NEW_INFIMUM - REC_OFF_NEXT);

  assert(PAGE_NEW_INFIMUM + next_rec_off_t != PAGE_NEW_SUPREMUM);

  if (next_rec_off_t > g_page_physical_size) {
    assert(0);
    return (nullptr);
  }

  if (memcmp(buf + PAGE_NEW_INFIMUM, "infimum", strlen("infimum")) != 0) {
    ninja_error("Found corrupt INFIMUM");
    return nullptr;
  }

  unsigned char* current_rec = buf + PAGE_NEW_INFIMUM + next_rec_off_t;

  assert(static_cast<uint32_t>(current_rec - buf) <= g_page_physical_size);

  bool is_comp = PageIsCompact(buf);

  // TODO(Zhao): Support redundant row format
  assert(is_comp);

  return current_rec;
}

unsigned char* ibdNinja::GetNextRecInPage(unsigned char* current_rec,
                                          unsigned char* buf,
                                          bool* corrupt) {
  *corrupt = false;
  uint32_t page_no = ReadFrom4B(buf + FIL_PAGE_OFFSET);
  bool is_comp = PageIsCompact(buf);
  uint32_t next_rec_offset = RecGetNextOffs(current_rec, is_comp);

  if (next_rec_offset == 0) {
    ninja_error("Record is corrupt");
    *corrupt = true;
    assert(0);
    return nullptr;
  }

  unsigned char* next_rec = buf + next_rec_offset;

  assert(static_cast<uint32_t>(next_rec - buf) <= g_page_physical_size);

  if (RecGetType(next_rec) == REC_STATUS_SUPREMUM) {
    if (memcmp(next_rec, "supremum", strlen("supremum")) != 0) {
      ninja_error("Found corrupt SUPREMUM on page %u", page_no);
      *corrupt = false;
    }

    uint32_t supremum_next_rec_off = ReadFrom2B(next_rec - REC_OFF_NEXT);

    if (supremum_next_rec_off != 0) {
      ninja_error("Found corrupt next rec of SUPREMUM on page %u", page_no);
      *corrupt = false;
    }
    return nullptr;
  }

  *corrupt = false;

  return next_rec;
}

ssize_t ibdNinja::ReadPage(uint32_t page_no, unsigned char* buf) {
  assert(buf != nullptr);
  memset(buf, 0, g_page_physical_size);
  off_t offset = page_no * g_page_physical_size;
  ssize_t n_bytes_read = pread(g_fd, buf, g_page_physical_size, offset);

  // TODO(Zhao): Support compressed page
  return n_bytes_read;
}

bool ibdNinja::ParsePage(uint32_t page_no,
                         PageAnalysisResult* result_aggr,
                         bool print,
                         bool print_record) {
  if (page_no >= n_pages_) {
    ninja_error("Page number %u is too large", page_no);
    return false;
  }

  unsigned char buf_unalign[2 * UNIV_PAGE_SIZE_MAX];
  memset(buf_unalign, 0, 2 * UNIV_PAGE_SIZE_MAX);
  unsigned char* buf = static_cast<unsigned char*>(
                    ut_align(buf_unalign, g_page_physical_size));
  ssize_t bytes = ReadPage(page_no, buf);
  if (bytes != g_page_physical_size) {
    ninja_error("Failed to read page: %u, error: %d(%s)",
            page_no, errno, strerror(errno));
    return false;
  }
  if (memcmp(
          buf + FIL_PAGE_LSN + 4,
          buf + g_page_logical_size - FIL_PAGE_END_LSN_OLD_CHKSUM + 4,
          4)) {
    ninja_error("The LSN on page %u is inconsistent", page_no);
    return false;
  }

  uint32_t type = ReadFrom2B(buf + FIL_PAGE_TYPE);

  // Handle LOB page types
  if (type == FIL_PAGE_TYPE_LOB_FIRST ||
      type == FIL_PAGE_TYPE_LOB_DATA ||
      type == FIL_PAGE_TYPE_LOB_INDEX) {
    uint32_t page_no_in_fil = ReadFrom4B(buf + FIL_PAGE_OFFSET);
    uint32_t space_id = ReadFrom4B(buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    ninja_pt(print, "=========================================="
                    "==========================================\n");
    ninja_pt(print, "|  PAGE INFORMATION                       "
                    "                                         |\n");
    ninja_pt(print, "------------------------------------------"
                    "------------------------------------------\n");
    ninja_pt(print, "    Page no:           %u\n", page_no_in_fil);
    ninja_pt(print, "    Space id:          %u\n", space_id);
    ninja_pt(print, "    Page type:         %s\n",
             PageType2String(type).c_str());

    if (type == FIL_PAGE_TYPE_LOB_FIRST) {
      LobFirstPageHeader hdr = ReadLobFirstPageHeader(buf);
      ninja_pt(print, "\n");
      ninja_pt(print, "    [LOB FIRST PAGE HEADER]\n");
      ninja_pt(print, "    Version:           %u\n", hdr.version);
      ninja_pt(print, "    Flags:             %u\n", hdr.flags);
      ninja_pt(print, "    LOB version:       %u\n", hdr.lob_version);
      ninja_pt(print, "    Last trx id:       %" PRIu64 "\n", hdr.last_trx_id);
      ninja_pt(print, "    Last undo no:      %u\n", hdr.last_undo_no);
      ninja_pt(print, "    Data len:          %u\n", hdr.data_len);
      ninja_pt(print, "    Creator trx id:    %" PRIu64 "\n",
               hdr.creator_trx_id);
      ninja_pt(print, "    Index list len:    %u\n", hdr.index_list.length);
      ninja_pt(print, "    Free list len:     %u\n", hdr.free_list.length);

      // Print index entries on this page
      ninja_pt(print, "\n    [INDEX ENTRIES ON FIRST PAGE]\n");
      const unsigned char* entry_ptr = buf + FIL_PAGE_DATA +
                                       LOB_FIRST_PAGE_INDEX_BEGIN;
      uint32_t n_entries = hdr.index_list.length;
      if (n_entries > LOB_FIRST_PAGE_N_ENTRIES) {
        n_entries = LOB_FIRST_PAGE_N_ENTRIES;
      }
      for (uint32_t i = 0; i < n_entries; i++) {
        LobIndexEntry entry = ReadLobIndexEntry(entry_ptr);
        ninja_pt(print, "    Entry %u: data_page=%u, data_len=%u, "
                 "lob_version=%u, creator_trx=%" PRIu64
                 ", modifier_trx=%" PRIu64 "\n",
                 i, entry.data_page_no, entry.data_len,
                 entry.lob_version, entry.creator_trx_id,
                 entry.modifier_trx_id);
        if (entry.versions.length > 0) {
          ninja_pt(print, "             old_versions=%u\n",
                   entry.versions.length);
        }
        entry_ptr += LOB_INDEX_ENTRY_SIZE;
      }

      // If there are more entries on other index pages, traverse them
      if (hdr.index_list.length > LOB_FIRST_PAGE_N_ENTRIES) {
        // The last entry on this page has a 'next' pointer to continue
        FilAddr cur_addr;
        // Read the next pointer from the last entry on the first page
        LobIndexEntry last_entry = ReadLobIndexEntry(
            buf + FIL_PAGE_DATA + LOB_FIRST_PAGE_INDEX_BEGIN +
            (LOB_FIRST_PAGE_N_ENTRIES - 1) * LOB_INDEX_ENTRY_SIZE);
        cur_addr = last_entry.next;
        uint32_t entry_idx = LOB_FIRST_PAGE_N_ENTRIES;
        unsigned char idx_buf[UNIV_PAGE_SIZE_MAX];
        uint32_t cached_page = 0;
        uint32_t pages_visited = 0;

        while (!cur_addr.is_null() &&
               entry_idx < hdr.index_list.length) {
          if (++pages_visited > LOB_MAX_PAGES_VISITED) break;
          if (cur_addr.page_no != cached_page) {
            ssize_t b = ReadPage(cur_addr.page_no, idx_buf);
            if (b != g_page_physical_size) break;
            cached_page = cur_addr.page_no;
          }
          LobIndexEntry entry = ReadLobIndexEntry(
              idx_buf + cur_addr.byte_offset);
          ninja_pt(print, "    Entry %u: data_page=%u, data_len=%u, "
                   "lob_version=%u, creator_trx=%" PRIu64
                   ", modifier_trx=%" PRIu64 "\n",
                   entry_idx, entry.data_page_no, entry.data_len,
                   entry.lob_version, entry.creator_trx_id,
                   entry.modifier_trx_id);
          if (entry.versions.length > 0) {
            ninja_pt(print, "             old_versions=%u\n",
                     entry.versions.length);
          }
          entry_idx++;
          cur_addr = entry.next;
        }
      }

      // Print data summary from first page
      uint32_t first_page_data_offset = FIL_PAGE_DATA +
          LOB_FIRST_PAGE_INDEX_BEGIN +
          LOB_FIRST_PAGE_N_ENTRIES * LOB_INDEX_ENTRY_SIZE;
      uint32_t avail_data = g_page_logical_size - FIL_PAGE_DATA_END -
                            first_page_data_offset;
      ninja_pt(print, "\n    First page data capacity: %u bytes\n",
               avail_data);
      ninja_pt(print, "    First page data stored:   %u bytes\n",
               hdr.data_len);

    } else if (type == FIL_PAGE_TYPE_LOB_DATA) {
      const unsigned char* p = buf + FIL_PAGE_DATA;
      uint8_t ver = ReadFrom1B(p + LOB_DATA_PAGE_VERSION);
      uint32_t data_len = ReadFrom4B(p + LOB_DATA_PAGE_DATA_LEN);
      uint64_t trx_id = ReadTrxId(p + LOB_DATA_PAGE_TRX_ID);
      ninja_pt(print, "\n");
      ninja_pt(print, "    [LOB DATA PAGE HEADER]\n");
      ninja_pt(print, "    Version:           %u\n", ver);
      ninja_pt(print, "    Data len:          %u\n", data_len);
      ninja_pt(print, "    Trx id:            %" PRIu64 "\n", trx_id);

    } else if (type == FIL_PAGE_TYPE_LOB_INDEX) {
      ninja_pt(print, "\n");
      ninja_pt(print, "    [LOB INDEX PAGE]\n");
      // LOB index pages contain index entries starting at some offset
      // after the page header. The entries are linked via FLST.
      // We can scan for entries by following the linked list from the
      // first page, but when viewing this page standalone we show
      // what's available.
      ninja_pt(print, "    (Use --parse-page on the LOB_FIRST page "
               "to see full index entry details)\n");
    }

    return true;
  }

  if (type != FIL_PAGE_INDEX) {
    fprintf(stderr, "[ibdNinja] Currently, only INDEX and LOB pages are "
                "supported. Support for other types (e.g., '%s') "
                "will be added soon\n",
                PageType2String(type).c_str());
    return false;
  }

  uint32_t page_no_in_fil_header = ReadFrom4B(buf + FIL_PAGE_OFFSET);
  assert(page_no_in_fil_header == page_no);
  uint32_t prev_page_no = ReadFrom4B(buf + FIL_PAGE_PREV);
  uint32_t next_page_no = ReadFrom4B(buf + FIL_PAGE_NEXT);
  uint32_t space_id = ReadFrom4B(buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
  uint32_t lsn = ReadFrom8B(buf + FIL_PAGE_LSN);
  uint32_t flush_lsn = ReadFrom8B(buf + FIL_PAGE_FILE_FLUSH_LSN);

  uint32_t n_dir_slots = ReadFrom2B(buf + PAGE_HEADER + PAGE_N_DIR_SLOTS);
  uint32_t heap_top = ReadFrom2B(buf + PAGE_HEADER + PAGE_HEAP_TOP);
  uint32_t n_heap = ReadFrom2B(buf + PAGE_HEADER + PAGE_N_HEAP);
  n_heap = (n_heap & 0x7FFF);
  uint32_t free = ReadFrom2B(buf + PAGE_HEADER + PAGE_FREE);
  uint32_t garbage = ReadFrom2B(buf + PAGE_HEADER + PAGE_GARBAGE);
  uint32_t last_insert = ReadFrom2B(buf + PAGE_HEADER + PAGE_LAST_INSERT);
  uint32_t direction = ReadFrom2B(buf + PAGE_HEADER + PAGE_DIRECTION);
  uint32_t n_direction = ReadFrom2B(buf + PAGE_HEADER + PAGE_N_DIRECTION);
  uint32_t n_recs = ReadFrom2B(buf + PAGE_HEADER + PAGE_N_RECS);
  uint32_t max_trx_id = ReadFrom2B(buf + PAGE_HEADER + PAGE_MAX_TRX_ID);
  uint32_t page_level = ReadFrom2B(buf + PAGE_HEADER + PAGE_LEVEL);
  uint64_t index_id = ReadFrom8B(buf + PAGE_HEADER + PAGE_INDEX_ID);

  bool index_not_found = false;
  Index* index = GetIndex(index_id);
  if (index == nullptr) {
    ninja_error("Unable find index %" PRIu64 " in the loaded indexes",
            index_id);
    index_not_found = true;
  }

  ninja_pt(print, "=========================================="
                  "==========================================\n");
  ninja_pt(print, "|  PAGE INFORMATION                       "
                  "                                         |\n");
  ninja_pt(print, "------------------------------------------"
                  "------------------------------------------\n");
  ninja_pt(print, "    Page no:           %u\n", page_no);
  if (prev_page_no != FIL_NULL) {
    ninja_pt(print, "    Slibling pages no: %u ", prev_page_no);
  } else {
    ninja_pt(print, "    Slibling pages no: NULL ");
  }
  ninja_pt(print, "[%u] ", page_no_in_fil_header);
  if (next_page_no != FIL_NULL) {
    ninja_pt(print, "%u\n", next_page_no);
  } else {
    ninja_pt(print, "NULL\n");
  }
  ninja_pt(print, "    Space id:          %u\n", space_id);
  ninja_pt(print, "    Page type:         %s\n", PageType2String(type).c_str());
  ninja_pt(print, "    Lsn:               %u\n", lsn);
  ninja_pt(print, "    FLush lsn:         %u\n", flush_lsn);
  ninja_pt(print, "    -------------------\n");
  ninja_pt(print, "    Page level:        %u\n", page_level);
  ninja_pt(print, "    Page size:         [logical: %u B], [physical: %u B]\n",
                       g_page_logical_size,
                       g_page_physical_size);
  ninja_pt(print, "    Number of records: %u\n", n_recs);
  ninja_pt(print, "    Index id:          %" PRIu64 "\n", index_id);
  if (!index_not_found) {
  ninja_pt(print, "    Belongs to:        [table: %s.%s], [index: %s]\n",
                       index->table()->schema_ref().c_str(),
                       index->table()->name().c_str(),
                       index->name().c_str());
  ninja_pt(print, "    Row format:        %s\n",
                       index->table()->RowFormatString().c_str());
  }
  ninja_pt(print, "    Number dir slots:  %u\n", n_dir_slots);
  ninja_pt(print, "    Heap top:          %u\n", heap_top);
  ninja_pt(print, "    Number of heap:    %u\n", n_heap);
  ninja_pt(print, "    First free rec:    %u\n", free);
  ninja_pt(print, "    Garbage:           %u B\n", garbage);
  ninja_pt(print, "    Last insert:       %u\n", last_insert);
  ninja_pt(print, "    Direction:         %u\n", direction);
  ninja_pt(print, "    Number direction:  %u\n", n_direction);
  ninja_pt(print, "    Max trx id:        %u\n", max_trx_id);

  ninja_pt(print, "\n");

  if (index_not_found) {
    ninja_warn("Skipping record parsing");
    return false;
  }

  if (!index->IsIndexParsingRecSupported()) {
    ninja_warn("Skipping record parsing");
    return false;
  }

  bool print_rec = print & print_record;
  ninja_pt(print_rec, "=========================================="
                  "==========================================\n");
  ninja_pt(print_rec, "|  RECORDS INFORMATION                    "
                  "                                         |\n");
  ninja_pt(print_rec, "------------------------------------------"
                  "------------------------------------------\n");
  uint32_t i = 0;
  unsigned char* current_rec = nullptr;
  PageAnalysisResult result;
  if (n_recs > 0) {
    current_rec = GetFirstUserRec(buf);
    bool corrupt = false;
    while (current_rec != nullptr && corrupt != true) {
      i++;
      Record rec(current_rec, index);
      rec.GetColumnOffsets();
      rec.ParseRecord(page_level == 0, i, &result,
                      print_rec);
      current_rec = GetNextRecInPage(current_rec, buf, &corrupt);
    }
    if (corrupt != true) {
      assert(i == n_recs);
    }
  } else {
    ninja_pt(print_rec, "No record\n");
  }


  ninja_pt(print, "=========================================="
      "==========================================\n");
  ninja_pt(print, "|  PAGE ANALYSIS RESULT                    "
      "                                         |\n");
  ninja_pt(print, "------------------------------------------"
      "------------------------------------------\n");
  if (page_level == 0) {
    ninja_pt(print, "Total valid records count:                %u\n",
        result.n_recs_leaf);
    ninja_pt(print, "Total valid records size:                 %u B\n"
        "                                            "
        "[Headers: %u B]\n"
        "                                            "
        "[Bodies:  %u B]\n",
        result.headers_len_leaf + result.recs_len_leaf,
        result.headers_len_leaf, result.recs_len_leaf);
    ninja_pt(print, "Valid records to page space ratio:        "
        "%02.05lf %%\n",
        static_cast<double>(
          (result.headers_len_leaf +
           result.recs_len_leaf)) /
        g_page_physical_size * 100);

    ninja_pt(print, "\n");
    ninja_pt(print, "Total records with dropped columns count: %u\n",
        result.n_contain_dropped_cols_recs_leaf);
    ninja_pt(print, "Total instant dropped columns size:       %u B\n",
        result.dropped_cols_len_leaf);
    ninja_pt(print, "Dropped columns to page space ratio:      "
        "%02.05lf %%\n",
        static_cast<double>(
          result.dropped_cols_len_leaf) /
        g_page_physical_size * 100);

    ninja_pt(print, "\n");
    ninja_pt(print, "Total delete-marked records count:        %u\n",
        result.n_deleted_recs_leaf);
    ninja_pt(print, "Total delete-marked records size:         %u B\n",
        result.deleted_recs_len_leaf);
    ninja_pt(print, "Delete-marked recs to page space ratio:   "
        "%02.05lf %%\n",
        static_cast<double>(
          result.deleted_recs_len_leaf) /
        g_page_physical_size * 100);

    result.innodb_internal_used_leaf =
      PAGE_NEW_SUPREMUM_END + result.headers_len_leaf +
      n_dir_slots * PAGE_DIR_SLOT_SIZE  + FIL_PAGE_DATA_END;
    ninja_pt(print, "\n");
    ninja_pt(print, "Total InnoDB internal space used:         %u B\n"
        "                                            "
        "[FIL HEADER     38 B]\n"
        "                                            "
        "[PAGE HEADER    36 B]\n"
        "                                            "
        "[FSEG HEADER    20 B]\n"
        "                                            "
        "[INFI + SUPRE   26 B]\n"
        "                                            "
        "[RECORD HEADERS %u B]*\n"
        "                                            "
        "[PAGE DIRECTORY %u B]\n"
        "                                            "
        "[FIL TRAILER    8 B]\n",
        result.innodb_internal_used_leaf,
        result.headers_len_leaf,
        n_dir_slots * PAGE_DIR_SLOT_SIZE);
    ninja_pt(print, "InnoDB internals to page space ratio:     "
        "%02.05lf %%\n",
        static_cast<double>(
          result.innodb_internal_used_leaf) /
        g_page_physical_size * 100);

    ninja_pt(print, "\n");
    result.free_leaf = garbage + UNIV_PAGE_SIZE - PAGE_DIR -
      n_dir_slots * PAGE_DIR_SLOT_SIZE - heap_top;
    ninja_pt(print, "Total free space:                         %u B\n",
        result.free_leaf);
    ninja_pt(print, "Free space ratio:                         "
        "%02.05lf %%\n",
        static_cast<double>(
          result.free_leaf) /
        g_page_physical_size * 100);
  } else {
    ninja_pt(print, "Total valid records count:               %u\n",
        result.n_recs_non_leaf);
    ninja_pt(print, "Total valid records size:                %u B\n"
        "                                           "
        "[Headers: %u B]\n"
        "                                           "
        "[Bodies : %u B)\n",
        result.headers_len_non_leaf + result.recs_len_non_leaf,
        result.headers_len_non_leaf, result.recs_len_non_leaf);
    ninja_pt(print, "Valid records to page space ratio:       "
        "%02.05lf %%\n",
        static_cast<double>(
          (result.headers_len_non_leaf +
           result.recs_len_non_leaf)) /
        g_page_physical_size * 100);

    ninja_pt(print, "\n");
    ninja_pt(print, "Total delete-marked records count:       %u\n",
        result.n_deleted_recs_non_leaf);
    ninja_pt(print, "Total delete-marked records size:        %u B\n",
        result.deleted_recs_len_non_leaf);
    ninja_pt(print, "Delete-marked recs to page space ratio:  "
        "%02.05lf %%\n",
        static_cast<double>(
          result.deleted_recs_len_non_leaf) /
        g_page_physical_size * 100);

    assert(result.n_contain_dropped_cols_recs_non_leaf == 0);
    assert(result.dropped_cols_len_non_leaf == 0);

    result.innodb_internal_used_non_leaf =
      PAGE_NEW_SUPREMUM_END + result.headers_len_non_leaf +
      n_dir_slots * PAGE_DIR_SLOT_SIZE  + FIL_PAGE_DATA_END;
    ninja_pt(print, "\n");
    ninja_pt(print, "Total innoDB internal space used:        %u B\n"
        "                                           "
        "[FIL HEADER     38 B]\n"
        "                                           "
        "[PAGE HEADER    36 B]\n"
        "                                           "
        "[FSEG HEADER    20 B]\n"
        "                                           "
        "[INFI + SUPRE   26 B]\n"
        "                                           "
        "[RECORD HEADERS %u B]*\n"
        "                                           "
        "[PAGE DIRECTORY %u B]\n"
        "                                           "
        "[FIL TRAILER    8 B]\n",
        result.innodb_internal_used_non_leaf,
        result.headers_len_non_leaf,
        n_dir_slots * PAGE_DIR_SLOT_SIZE);
    ninja_pt(print, "InnoDB internals to page space ratio:    "
        "%02.05lf %%\n",
        static_cast<double>(
          result.innodb_internal_used_non_leaf) /
        g_page_physical_size * 100);

    ninja_pt(print, "\n");
    result.free_non_leaf = garbage + UNIV_PAGE_SIZE - PAGE_DIR -
      n_dir_slots * PAGE_DIR_SLOT_SIZE - heap_top;
    ninja_pt(print, "Total free space:                        %u B\n",
        result.free_non_leaf);
    ninja_pt(print, "Free space ratio:                        "
        "%02.05lf %%\n",
        static_cast<double>(
          result.free_non_leaf) /
        g_page_physical_size * 100);
  }
  // aggregate the page result to the index result
  if (result_aggr != nullptr) {
    result_aggr->n_recs_non_leaf +=
      result.n_recs_non_leaf;
    result_aggr->n_recs_leaf +=
      result.n_recs_leaf;
    result_aggr->headers_len_non_leaf +=
      result.headers_len_non_leaf;
    result_aggr->headers_len_leaf +=
      result.headers_len_leaf;
    result_aggr->recs_len_non_leaf +=
      result.recs_len_non_leaf;
    result_aggr->recs_len_leaf +=
      result.recs_len_leaf;
    result_aggr->n_deleted_recs_non_leaf +=
      result.n_deleted_recs_non_leaf;
    result_aggr->n_deleted_recs_leaf +=
      result.n_deleted_recs_leaf;
    result_aggr->deleted_recs_len_non_leaf +=
      result.deleted_recs_len_non_leaf;
    result_aggr->deleted_recs_len_leaf +=
      result.deleted_recs_len_leaf;
    result_aggr->n_contain_dropped_cols_recs_non_leaf +=
      result.n_contain_dropped_cols_recs_non_leaf;
    result_aggr->n_contain_dropped_cols_recs_leaf +=
      result.n_contain_dropped_cols_recs_leaf;
    result_aggr->dropped_cols_len_non_leaf +=
      result.dropped_cols_len_non_leaf;
    result_aggr->dropped_cols_len_leaf +=
      result.dropped_cols_len_leaf;
    result_aggr->innodb_internal_used_non_leaf +=
      result.innodb_internal_used_non_leaf;
    result_aggr->innodb_internal_used_leaf +=
      result.innodb_internal_used_leaf;
    result_aggr->free_non_leaf +=
      result.free_non_leaf;
    result_aggr->free_leaf +=
      result.free_leaf;
  }

  return true;
}

bool ibdNinja::ToLeftmostLeaf(Index* index,
                              unsigned char* buf, uint32_t root,
                              std::vector<uint32_t>* leaf_pages_no) {
  if (!index->IsIndexParsingRecSupported()) {
    // ninja_warn("Skip getting leftmost pages");
    return false;
  }
  uint32_t bytes = ReadPage(root, buf);
  if (bytes != g_page_physical_size) {
    ninja_error("Failed to read page: %u, error: %d(%s)",
            root, errno, strerror(errno));
    return false;
  }
  uint32_t curr_page_no = root;
  leaf_pages_no->push_back(curr_page_no);

  uint32_t page_level = ReadFrom2B(buf + FIL_PAGE_DATA + PAGE_LEVEL);
  // uint32_t n_of_recs = ReadFrom2B(buf + FIL_PAGE_DATA + PAGE_N_RECS);

  unsigned char* current_rec = nullptr;
  while (page_level != 0) {
    current_rec = GetFirstUserRec(buf);
    if (current_rec == nullptr) {
      break;
    }
    Record record(current_rec, index);
    record.GetColumnOffsets();
    uint32_t child_page_no = record.GetChildPageNo();

    uint64_t curr_page_level = page_level;

    bytes = ReadPage(child_page_no, buf);
    if (bytes != g_page_physical_size) {
      ninja_error("Failed to read page: %u, error: %d(%s)",
              child_page_no, errno, strerror(errno));
      return false;
    }
    page_level = ReadFrom2B(buf + FIL_PAGE_DATA + PAGE_LEVEL);

    if (page_level != curr_page_level - 1) {
      break;
    }
    curr_page_no = child_page_no;
    leaf_pages_no->push_back(curr_page_no);
  }

  if (page_level != 0) {
    ninja_error("Failed to find leatmost page");
    return false;
  }
  return true;
}

bool ibdNinja::ParseIndex(uint32_t index_id) {
  auto iter = indexes_.find(index_id);
  if (iter == indexes_.end()) {
    ninja_error("Failed to parse the index. "
                "No index with ID %u was found", index_id);
    return false;
  }
  return ParseIndex(iter->second);
}

bool ibdNinja::ParseIndex(Index* index) {
  unsigned char buf_unalign[2 * UNIV_PAGE_SIZE_MAX];
  memset(buf_unalign, 0, 2 * UNIV_PAGE_SIZE_MAX);
  unsigned char* buf = static_cast<unsigned char*>(
                    ut_align(buf_unalign, g_page_physical_size));

  uint32_t page_no = index->ib_page();
  std::vector<uint32_t> left_pages_no;
  bool ret = ToLeftmostLeaf(index, buf, page_no, &left_pages_no);
  if (!ret) {
    return false;
  }
  uint32_t n_levels = left_pages_no.size();
  IndexAnalyzeResult index_result;
  fprintf(stdout, "\n");
  for (auto iter : left_pages_no) {
    fprintf(stdout, "Analyzing index %s at level %u...\n",
                    index->name().c_str(), --n_levels);
    index_result.n_level++;
    uint32_t current_page_no = iter;
    uint32_t next_page_no = FIL_NULL;
    do {
      ssize_t bytes = ReadPage(current_page_no, buf);
      if (bytes != g_page_physical_size) {
        ninja_error("Failed to read page: %u, error: %d(%s)",
            current_page_no, errno, strerror(errno));
        return false;
      }
      uint32_t page_level = ReadFrom2B(buf + PAGE_HEADER + PAGE_LEVEL);
      if (page_level > 0) {
        index_result.n_pages_non_leaf++;
      } else {
        index_result.n_pages_leaf++;
      }
      bool ret = ParsePage(current_page_no, &(index_result.recs_result),
                           false, true);
      if (!ret) {
        ninja_error("Error occurred while parsing page %u at level %u, "
                    "Skipping analysis for this level.",
                    current_page_no, n_levels);
        break;
      }
      next_page_no = ReadFrom4B(buf + FIL_PAGE_NEXT);
      current_page_no = next_page_no;
    } while (current_page_no != FIL_NULL);
  }
  fprintf(stdout, "=========================================="
                  "==========================================\n");
  fprintf(stdout, "|  INDEX ANALYSIS RESULT                   "
                  "                                         |\n");
  fprintf(stdout, "------------------------------------------"
                  "------------------------------------------\n");
  fprintf(stdout, "Index name:                                       %s\n",
                   index->name().c_str());
  fprintf(stdout, "Index id:                                         %u\n",
                   index->ib_id());
  fprintf(stdout, "Belongs to:                                       %s.%s\n",
                   index->table()->schema_ref().c_str(),
                   index->table()->name().c_str());
  fprintf(stdout, "Root page no:                                     %u\n",
                   index->ib_page());
  fprintf(stdout, "Num of fields(ALL):                               %u\n",
                   index->GetNFields());
  assert(left_pages_no.size() == index_result.n_level);
  fprintf(stdout, "Num of levels:                                    %u\n",
                   index_result.n_level);
  fprintf(stdout, "Num of pages:                                     %u\n"
                  "                                                  "
                  "  [Non leaf pages: %u]\n"
                  "                                                  "
                  "  [Leaf pages:     %u]\n",
                   index_result.n_pages_non_leaf + index_result.n_pages_leaf,
                   index_result.n_pages_non_leaf, index_result.n_pages_leaf);
  if (index_result.n_level > 1) {
    uint32_t total_pages_size = index_result.n_pages_non_leaf *
                                g_page_physical_size;
    // Print non-leaf pages statistic
    fprintf(stdout, "\n--------NON-LEAF-LEVELS--------\n");
    fprintf(stdout, "Total pages count:                                "
                    "%u\n",
                     index_result.n_pages_non_leaf);
    fprintf(stdout, "Total pages size:                                 "
                    "%u B\n",
                     total_pages_size);

    fprintf(stdout, "\n");
    fprintf(stdout, "Total valid records count:                        "
                    "%u\n",
                     index_result.recs_result.n_recs_non_leaf);
    fprintf(stdout, "Total valid records size:                         "
                    "%u B\n"
                    "                                                  "
                    "  [Headers: %u B]\n"
                    "                                                  "
                    "  [Bodies:  %u B]\n",
                     index_result.recs_result.headers_len_non_leaf +
                     index_result.recs_result.recs_len_non_leaf,
                     index_result.recs_result.headers_len_non_leaf,
                     index_result.recs_result.recs_len_non_leaf);
    fprintf(stdout, "Valid records to non-leaf pages space ratio:      "
        "%02.05lf %%\n",
        static_cast<double>(
          (index_result.recs_result.headers_len_non_leaf +
           index_result.recs_result.recs_len_non_leaf)) /
        total_pages_size * 100);

    fprintf(stdout, "\n");
    fprintf(stdout, "Total delete-marked records count:                "
                    "%u\n",
                     index_result.recs_result.n_deleted_recs_non_leaf);
    fprintf(stdout, "Total delete-marked records size:                 "
                     "%u B\n",
                     index_result.recs_result.deleted_recs_len_non_leaf);
    fprintf(stdout, "Delete-marked recs to non-leaf pages space ratio: "
        "%02.05lf %%\n",
        static_cast<double>(
          index_result.recs_result.deleted_recs_len_non_leaf) /
        total_pages_size * 100);

    assert(index_result.recs_result.n_contain_dropped_cols_recs_non_leaf == 0);
    assert(index_result.recs_result.dropped_cols_len_non_leaf == 0);

    fprintf(stdout, "\n");
    fprintf(stdout, "Total Innodb internal space used:                 "
                    "%u B\n",
                    index_result.recs_result.innodb_internal_used_non_leaf);
    fprintf(stdout, "InnoDB internals to non-leaf pages space ratio:   "
        "%02.05lf %%\n",
        static_cast<double>(
          index_result.recs_result.innodb_internal_used_non_leaf) /
        total_pages_size * 100);

    fprintf(stdout, "\n");
    fprintf(stdout, "Total free space:                                 "
                    "%u B\n",
                    index_result.recs_result.free_non_leaf);
    fprintf(stdout, "Free space ratio:                                 "
                    "%02.05lf %%\n",
                     static_cast<double>(
                      index_result.recs_result.free_non_leaf) /
                      total_pages_size * 100);
  }
  uint32_t total_pages_size = index_result.n_pages_leaf * g_page_physical_size;
  fprintf(stdout, "\n--------LEAF-LEVEL---------------\n");
  fprintf(stdout, "Total pages count:                                "
                  "%u\n",
                   index_result.n_pages_leaf);
  fprintf(stdout, "Total pages size:                                 "
                  "%u B\n",
                   total_pages_size);
  fprintf(stdout, "\n");
  fprintf(stdout, "Total valid records count:                        "
                  "%u\n",
                   index_result.recs_result.n_recs_leaf);
  fprintf(stdout, "Total valid records size:                         "
                  "%u B\n"
                  "                                                  "
                  "  [Headers: %u B]\n"
                  "                                                  "
                  "  [Bodies:  %u B]\n",
                   index_result.recs_result.headers_len_leaf +
                   index_result.recs_result.recs_len_leaf,
                   index_result.recs_result.headers_len_leaf,
                   index_result.recs_result.recs_len_leaf);
  fprintf(stdout, "Valid records to leaf pages space ratio:          "
                  "%02.05lf %%\n",
                   static_cast<double>(
                   (index_result.recs_result.headers_len_leaf +
                    index_result.recs_result.recs_len_leaf)) /
                    total_pages_size * 100);

  fprintf(stdout, "\n");
  fprintf(stdout, "Total records with instant dropped columns count: "
                  "%u\n",
                   index_result.recs_result.n_contain_dropped_cols_recs_leaf);
  fprintf(stdout, "Total instant dropped columns size:               "
                  "%u B\n",
                   index_result.recs_result.dropped_cols_len_leaf);
  fprintf(stdout, "Dropped columns to leaf pages space ratio:        "
                  "%02.05lf %%\n",
                   static_cast<double>(
                    index_result.recs_result.dropped_cols_len_leaf) /
                    total_pages_size * 100);

  fprintf(stdout, "\n");
  fprintf(stdout, "Total delete-marked records count:                "
                  "%u\n",
                   index_result.recs_result.n_deleted_recs_leaf);
  fprintf(stdout, "Total delete-marked records size:                 "
                   "%u B\n",
                   index_result.recs_result.deleted_recs_len_leaf);
  fprintf(stdout, "Delete-marked records to leaf pages space ratio:  "
                  "%02.05lf %%\n",
                   static_cast<double>(
                    index_result.recs_result.deleted_recs_len_leaf) /
                    total_pages_size * 100);

  fprintf(stdout, "\n");
  fprintf(stdout, "Total Innodb internal space used:                 "
                  "%u B\n",
                   index_result.recs_result.innodb_internal_used_leaf);
  fprintf(stdout, "InnoDB internal space to leaf pages space ratio:  "
                  "%02.05lf %%\n",
                   static_cast<double>(
                    index_result.recs_result.innodb_internal_used_leaf) /
                    total_pages_size * 100);

  fprintf(stdout, "\n");
  fprintf(stdout, "Total free space:                                 "
                  "%u B\n",
                   index_result.recs_result.free_leaf);
  fprintf(stdout, "Free space ratio:                                 "
                  "%02.05lf %%\n",
                   static_cast<double>(
                    index_result.recs_result.free_leaf) /
                    total_pages_size * 100);

  return ret;
}

void ibdNinja::ShowTables(bool only_supported) {
  if (!only_supported) {
    fprintf(stdout, "Listing all tables and indexes "
                    "in the specified ibd file:\n");
    for (auto table : all_tables_) {
      fprintf(stdout, "---------------------------------------\n");
      fprintf(stdout, "[Table] name: %s.%s\n",
              table->schema_ref().c_str(),
              table->name().c_str());
      for (auto index : table->indexes()) {
        fprintf(stdout, "        [Index] name: %s\n",
                index->name().c_str());
      }
    }
  } else {
    fprintf(stdout, "Listing all *supported* tables and indexes "
                    "in the specified ibd file:\n");
    for (auto table : tables_) {
      fprintf(stdout, "---------------------------------------\n");
      fprintf(stdout, "[Table] id: %-7" PRIu64 " name: %s.%s\n", table.first,
          table.second->schema_ref().c_str(),
          table.second->name().c_str());
      for (auto index : table.second->indexes()) {
        if (index->IsIndexSupported() &&
            indexes_.find(index->ib_id()) != indexes_.end()) {
          fprintf(stdout, "        [Index] id: %-7u, "
              "root page no: %-7u, name: %s\n",
              index->ib_id(), index->ib_page(),
              index->name().c_str());
        }
      }
    }
  }
}

void ibdNinja::ShowLeftmostPages(uint32_t index_id) {
  auto iter = indexes_.find(index_id);
  if (iter == indexes_.end()) {
    ninja_error("Failed to parse the index. "
                "No index with ID %u was found", index_id);
    return;
  }
  Index* index = iter->second;
  unsigned char buf_unalign[2 * UNIV_PAGE_SIZE_MAX];
  memset(buf_unalign, 0, 2 * UNIV_PAGE_SIZE_MAX);
  unsigned char* buf = static_cast<unsigned char*>(
                    ut_align(buf_unalign, g_page_physical_size));

  uint32_t page_no = index->ib_page();
  std::vector<uint32_t> left_pages_no;
  bool ret = ToLeftmostLeaf(index, buf, page_no, &left_pages_no);
  if (!ret) {
    return;
  }

  uint32_t n_levels = left_pages_no.size();
  uint32_t curr_level = n_levels - 1;
  fprintf(stdout, "---------------------------------------\n");
  fprintf(stdout, "Table name: %s.%s\n",
                   index->table()->schema_ref().c_str(),
                   index->table()->name().c_str());
  fprintf(stdout, "Index name: %s\n",
                   index->name().c_str());
  for (auto iter : left_pages_no) {
    fprintf(stdout, "  Level %u: page %u\n",
        curr_level, iter);
    curr_level--;
  }
}

bool ibdNinja::ParseTable(uint32_t table_id) {
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()) {
    ninja_error("Failed to parse the table. "
                "No table with ID %u was found", table_id);
    return false;
  }
  assert(iter->second != nullptr);
  fprintf(stdout, "=========================================="
                  "==========================================\n");
  fprintf(stdout, "|  TABLE ANALYSIS RESULT                   "
                  "                                         |\n");
  fprintf(stdout, "------------------------------------------"
                  "------------------------------------------\n");
  fprintf(stdout, "Table name:        %s.%s\n",
                   iter->second->schema_ref().c_str(),
                   iter->second->name().c_str());
  fprintf(stdout, "Table id:          %u\n",
                   iter->second->ib_id());
  fprintf(stdout, "Number of indexes: %lu\n",
                   iter->second->indexes().size());
  fprintf(stdout, "Analyze each index:\n");
  for (auto index : iter->second->indexes()) {
    if (index->IsIndexSupported()) {
      ParseIndex(index);
    }
  }
  return true;
}

}  // namespace ibd_ninja
