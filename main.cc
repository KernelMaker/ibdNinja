/*
 * Copyright (c) [2025] [Zhao Song]
 */
#include <getopt.h>
#include <algorithm>
#include "ibdNinja.h"

void Usage() {
  fprintf(stdout, "Usage: ibdNinja [OPTIONS]\n");
  fprintf(stdout, "Options:\n");
  fprintf(stdout, "  --help, -h                                Display this "
                  "help message\n");
  fprintf(stdout, "  --file, -f                                Specify the "
                  "path to the ibd file\n");
  fprintf(stdout, "  --list-tables, -l                         List all "
                  "*supported* tables and their supported indexes in the "
                  "specified ibd file\n");
  fprintf(stdout, "  --list-all-tables, -a                     List all "
                  "tables and their indexes in the specified ibd file\n");
  fprintf(stdout, "  --list-leafmost-pages, -e INDEX_ID        Show the "
                  "leftmost page number at each level of the specified "
                  "index\n");
  fprintf(stdout, "  --analyze-table, -t TABLE_ID              Analyze the "
                  "specified table\n");
  fprintf(stdout, "  --analyze-index, -i INDEX_ID              Analyze the "
                  "specified index\n");
  fprintf(stdout, "  --parse-page, -p PAGE_ID                  Parse the "
                  "specified page\n");
  fprintf(stdout, "    --no-print-record, -n                   Skip printing "
                  "record details when parsing a page\n");
  fprintf(stdout, "  --version, -v                             Display version "
                  "information\n");
}
int main(int argc, char* argv[]) {
  if (argc < 2) {
    Usage();
    return 1;
  }
  struct option options[] = {
    {"help", no_argument, 0, 'h'},
    {"file", required_argument, 0, 'f'},
    {"list-all-tables", no_argument, 0, 'a'},
    {"list-tables", no_argument, 0, 'l'},
    {"list-leftmost-pages", required_argument, 0, 'e'},
    {"analyze-table", required_argument, 0, 't'},
    {"analyze-index", required_argument, 0, 'i'},
    {"parse-page", required_argument, 0, 'p'},
    {"no-print-record", no_argument, 0, 'n'},
    {"version", no_argument, 0, 'v'},
    {0, 0, 0, 0}  // End of options
  };

  int opt;
  int option_index = 0;

  std::string ibd_file = "";
  bool list_all_tables = false;
  bool list_tables = false;
  bool list_leftmost_pages = false;
  uint32_t table_id = ibd_ninja::FIL_NULL;
  uint32_t index_id = ibd_ninja::FIL_NULL;
  uint32_t page_no = ibd_ninja::FIL_NULL;
  bool print_record = true;

  while ((opt = getopt_long(argc,
                argv, "halvf:e:t:i:p:n", options, &option_index)) != -1) {
    switch (opt) {
      case 'h':
        ibd_ninja::ibdNinja::PrintName();
        Usage();
        return 0;
      case 'v':
        ibd_ninja::ibdNinja::PrintName();
        printf("Version: %s\n", ibd_ninja::ibdNinja::g_version_);
        return 0;
      case 'f':
        ibd_file = optarg;
        break;
      case 'a':
        list_all_tables = true;
        break;
      case 'l':
        list_tables = true;
        break;
      case 'e': {
          list_leftmost_pages = true;
          std::string str(optarg);
          if (std::all_of(str.begin(), str.end(), ::isdigit)) {
            index_id = std::stoul(optarg);
          } else {
            Usage();
            return 1;
          }
        }
        break;
      case 't': {
          std::string str(optarg);
          if (std::all_of(str.begin(), str.end(), ::isdigit)) {
            table_id = std::stoul(optarg);
          } else {
            Usage();
            return 1;
          }
        }
        break;
      case 'i': {
          std::string str(optarg);
          if (std::all_of(str.begin(), str.end(), ::isdigit)) {
            index_id = std::stoul(optarg);
          } else {
            Usage();
            return 1;
          }
        }
        break;
      case 'p': {
          std::string str(optarg);
          if (std::all_of(str.begin(), str.end(), ::isdigit)) {
            page_no = std::stoul(optarg);
          } else {
            Usage();
            return 1;
          }
        }
        break;
      case 'n':
        print_record = false;
        break;
      case '?':
        return 1;
      default:
        printf("Unknown option: %c\n", opt);
        return 1;
    }
  }

  if (ibd_file.empty()) {
    fprintf(stderr, "You must specify the ibd file using the "
                    "--file (-f) option.\n");
    return 1;
  }

  ibd_ninja::ibdNinja* ninja =
    ibd_ninja::ibdNinja::CreateNinja(ibd_file.c_str());

  if (ninja != nullptr) {
    if (list_tables) {
      ninja->ShowTables(true);
    } else if (list_all_tables) {
      ninja->ShowTables(false);
    } else if (list_leftmost_pages) {
      ninja->ShowLeftmostPages(index_id);
    } else if (table_id != ibd_ninja::FIL_NULL) {
      ninja->ParseTable(table_id);
    } else if (index_id != ibd_ninja::FIL_NULL) {
      ninja->ParseIndex(index_id);
    } else if (page_no != ibd_ninja::FIL_NULL) {
      ninja->ParsePage(page_no, nullptr, true, print_record);
    } else {
      ninja->ShowTables(true);
    }
    delete ninja;
  }
  return 0;
}
