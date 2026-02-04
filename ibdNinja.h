/*
 * Copyright (c) [2025-2026] [Zhao Song]
 */
#ifndef IBDNINJA_H_
#define IBDNINJA_H_

#include "ibdUtils.h"
#include "Table.h"
#include "Index.h"
#include "Record.h"

#include <map>
#include <string>
#include <vector>

namespace ibd_ninja {

class ibdNinja {
 public:
  static ibdNinja* CreateNinja(const char* idb_filename);
  ~ibdNinja() {
    for (auto iter : all_tables_) {
      delete iter;
    }
  }

  const std::map<uint64_t, Table*>* tables() const {
    return &tables_;
  }
  const std::map<uint64_t, Index*>* indexes() const {
    return &indexes_;
  }

  void AddTable(Table* table);
  Table* GetTable(const std::string& db_name, const std::string& table_name) {
    Table* tab = nullptr;
    for (auto iter : tables_) {
      if (iter.second->schema_ref() == db_name &&
          iter.second->name() == table_name) {
        tab = iter.second;
        break;
      }
    }
    return tab;
  }
  Table* GetTable(uint64_t table_id) {
    Table* tab = nullptr;
    for (auto iter : tables_) {
      if (iter.first == table_id) {
        tab = iter.second;
        break;
      }
    }
    return tab;
  }
  Index* GetIndex(uint64_t index_id) {
    Index* idx = nullptr;
    for (auto iter : indexes_) {
      if (iter.first == index_id) {
        idx = iter.second;
        break;
      }
    }
    return idx;
  }

  static ssize_t ReadPage(uint32_t page_no, unsigned char* buf);
  bool ParsePage(uint32_t page_no,
                 PageAnalysisResult* result_aggr,
                 bool print,
                 bool print_record);
  bool ParseIndex(uint32_t index_id);
  void InspectBlob(uint32_t page_no, uint32_t rec_no);

  bool ParseTable(uint32_t table_id);

  void ShowTables(bool only_supported);
  void ShowLeftmostPages(uint32_t index_id);
  static const char* g_version_;
  static void PrintName();

 private:
  explicit ibdNinja(uint32_t n_pages) : n_pages_(n_pages) {
    all_tables_.clear();
    tables_.clear();
    indexes_.clear();
  }
  static bool SDIToLeftmostLeaf(unsigned char* buf, uint32_t sdi_root,
                                uint32_t* leaf_page_no);
  static uint64_t SDIFetchUncompBlob(uint32_t first_blob_page_no,
                                     uint64_t total_off_page_length,
                                     unsigned char* dest_buf,
                                     uint32_t* n_ext_pages,
                                     bool* error);
  static unsigned char* SDIGetFirstUserRec(unsigned char* buf,
                                           uint32_t buf_len);
  static unsigned char* SDIGetNextRec(unsigned char* current_rec,
                                      unsigned char* buf,
                                      uint32_t buf_len,
                                      bool* corrupt);
  static bool SDIParseRec(unsigned char* rec,
                          uint64_t* sdi_type, uint64_t* sdi_id,
                          unsigned char** sdi_data, uint64_t* sdi_data_len);

  static unsigned char* GetFirstUserRec(unsigned char* buf);
  static unsigned char* GetNextRecInPage(unsigned char* current_rec,
                                         unsigned char* buf,
                                         bool* corrupt);
  static bool ToLeftmostLeaf(Index* index,
                             unsigned char* buf, uint32_t root,
                             std::vector<uint32_t>* leaf_pages_no);
  bool ParseIndex(Index* index);

  uint32_t n_pages_;
  std::vector<Table*> all_tables_;
  std::map<uint64_t, Table*> tables_;
  std::map<uint64_t, Index*> indexes_;
};

}  // namespace ibd_ninja

#endif  // IBDNINJA_H_
