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

#define LOG 1

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

struct CloudFile {
  int refs;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;
  InternalKey largest;
  uint64_t obj_num;
  // TODO Add Bloom Filter

  CloudFile() : refs(0), file_size(0) { }
};

class VersionEdit {
 public:
  VersionEdit() { 
    if (LOG)
      std::cout << "VersionEdit()" << std::endl;
  
    Clear(); 
  }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    if (LOG)
      std::cout << "VersionEdit::SetLogNumber()" << std::endl;
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    if (LOG)
      std::cout << "VersionEdit::SetPrevLogNumber()" << std::endl;
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    if (LOG)
      std::cout << "VersionEdit::SetNextFile()" << std::endl;
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    if (LOG)
      std::cout << "VersionEdit::SetLastSequence()" << std::endl;
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    if (LOG)
      std::cout << "VersionEdit::SetCompactPointer()" << std::endl;
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  void SetCloudCompactPointer(const InternalKey& key) {
    if (LOG)
      std::cout << "VersionEdit::SetCloudCompactPointer()" << std::endl;
    cloud_compact_pointers_.push_back(key);
  }
  
  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    if (LOG)
      std::cout << "VersionEdit::AddFile()" << std::endl;
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  void AddCloudFile(uint64_t name,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    if (LOG)
      std::cout << "VersionEdit::AddCloudFile()" << std::endl;
    CloudFile f;
    f.obj_num = name;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_cloud_files_.push_back(f);
  }


  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    if (LOG)
      std::cout << "VersionEdit::DeleteFile()" << std::endl;
    deleted_files_.insert(std::make_pair(level, file));
  }

  void DeleteCloudFile(uint64_t name) {
    if (LOG)
      std::cout << "VersionEdit::DeleteCloudFile()" << std::endl;
    deleted_cloud_files_.push_back(name);
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

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

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  std::vector<InternalKey> cloud_compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, FileMetaData> > new_files_;
  std::vector<CloudFile> new_cloud_files_;
  std::vector<uint64_t> deleted_cloud_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
