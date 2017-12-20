// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

#include <iostream>
#include "json/json.hpp"
using json = nlohmann::json;

namespace leveldb {

class VersionSet;

struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

void to_json(json& j, const FileMetaData& f);

void from_json(const json& j, FileMetaData& f);

struct CloudFile {
  int refs;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;
  InternalKey largest;
  uint64_t obj_num;
  // TODO Add Bloom Filter

  CloudFile() : refs(0), file_size(0) { }

  CloudFile(const CloudFile& cf) : refs(0) { 
    file_size = cf.file_size;
    smallest = cf.smallest;
    largest = cf.largest;
    obj_num = cf.obj_num;
  }
};

void to_json(json& j, const CloudFile& cf);

void from_json(const json& j, CloudFile& cf);

class VersionEdit {
 public:
  VersionEdit() { 
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit()" << std::endl;
#endif
  
    Clear(); 
  }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::SetLogNumber()" << std::endl;
#endif
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::SetPrevLogNumber()" << std::endl;
#endif
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::SetNextFile()" << std::endl;
#endif
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetNextCloudFile(uint64_t num) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::SetNextCloudFile()" << std::endl;
#endif
    has_next_cloud_file_number_ = true;
    next_cloud_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::SetLastSequence()" << std::endl;
#endif
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::SetCompactPointer()" << std::endl;
#endif
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  void SetCloudCompactPointer(const InternalKey& key) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::SetCloudCompactPointer()" << std::endl;
#endif
    cloud_compact_pointers_.push_back(key);
  }
  
  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::AddFile()" << std::endl;
#endif
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  void AddCloudFile(CloudFile& f) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::AddCloudFile()" << std::endl;
#endif
    new_cloud_files_.push_back(f);
  }


  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::DeleteFile()" << std::endl;
#endif
    deleted_files_.insert(std::make_pair(level, file));
  }

  void DeleteCloudFile(uint64_t name) {
#ifdef DEBUG_LOG
    std::cerr << "VersionEdit::DeleteCloudFile()" << std::endl;
#endif
    deleted_cloud_files_.push_back(name);
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

  std::vector< std::pair<int, FileMetaData> > new_files_; // TODO find a better way to log this
 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;
  bool has_next_cloud_file_number_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  std::vector<InternalKey> cloud_compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector<CloudFile> new_cloud_files_;
  std::vector<uint64_t> deleted_cloud_files_;
  uint64_t next_cloud_file_number_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
