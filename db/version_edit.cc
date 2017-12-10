// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"
#include "base64/base64.h"
#include <iostream>

#define LOG 1

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator           = 1,
  kLogNumber            = 2,
  kNextFileNumber       = 3,
  kLastSequence         = 4,
  kCompactPointer       = 5,
  kDeletedFile          = 6,
  kNewFile              = 7,
  // 8 was used for large value refs
  kPrevLogNumber        = 9,
  kNewCloudFile         = 10,
  kDeletedCloudFile     = 11,
  kCloudCompactPointer  = 12,
  kNextCloudFileNumber  = 13,
};

void VersionEdit::Clear() {
  if (LOG)
    std::cout << "VersionEdit::Clear()" << std::endl;
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
  deleted_cloud_files_.clear();
  new_cloud_files_.clear();
  has_next_cloud_file_number_ = false;
}

void to_json(json& j, const CloudFile& cf) {
  j["file_size"] =  cf.file_size;
  std::string smallest =  cf.smallest.Encode().ToString();
  j["smallest"] = base64_encode((const unsigned char*)smallest.c_str(), smallest.size());
  std::string largest = cf.largest.Encode().ToString();
  j["largest"]  = base64_encode((const unsigned char*)largest.c_str(), largest.size());
  j["number"] = cf.obj_num;
}

void from_json(const json& j, CloudFile& cf) {
  cf.file_size = j.at("file_size").get<uint64_t>();
  cf.largest.DecodeFrom(Slice(base64_decode(j.at("largest").get<std::string>().c_str())));
  cf.smallest.DecodeFrom(Slice(base64_decode(j.at("smallest").get<std::string>().c_str())));
  cf.obj_num = j.at("number").get<uint64_t>();
}

void to_json(json& j, const FileMetaData& f) {
  j["file_size"] =  f.file_size;
  std::string smallest =  f.smallest.Encode().ToString();
  j["smallest"] = base64_encode((const unsigned char*)smallest.c_str(), smallest.size());
  std::string largest = f.largest.Encode().ToString();
  j["largest"]  = base64_encode((const unsigned char*)largest.c_str(), largest.size());
  j["number"] = f.number;
}

void from_json(const json& j, FileMetaData f) {
  f.file_size = j.at("file_size").get<uint64_t>();
  f.largest.DecodeFrom(Slice(base64_decode(j.at("largest").get<std::string>().c_str())));
  f.smallest.DecodeFrom(Slice(base64_decode(j.at("smallest").get<std::string>().c_str())));
  f.number = j.at("number").get<uint64_t>();
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (LOG)
    std::cout << "VersionEdit::EncodeTo()" << std::endl;
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }
  if (has_next_cloud_file_number_) {
    PutVarint32(dst, kNextCloudFileNumber);
    PutVarint64(dst, next_cloud_file_number_);
  } 

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());
  }

  for (size_t i = 0; i < cloud_compact_pointers_.size(); i++) {
    PutVarint32(dst, kCloudCompactPointer);
    PutLengthPrefixedSlice(dst, cloud_compact_pointers_[i].Encode());
  }

  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    PutVarint32(dst, kDeletedFile);
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // file number
  }

  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    PutVarint32(dst, kNewFile);
    PutVarint32(dst, new_files_[i].first);  // level
    PutVarint64(dst, f.number);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }
  
  for (size_t i = 0; i < new_cloud_files_.size(); i++) {
    const CloudFile& f = new_cloud_files_[i];
    PutVarint32(dst, kNewCloudFile);
    PutVarint64(dst, f.obj_num);
    PutVarint64(dst, f.file_size);
    PutLengthPrefixedSlice(dst, f.smallest.Encode());
    PutLengthPrefixedSlice(dst, f.largest.Encode());
  }

  for (size_t i = 0; i < deleted_cloud_files_.size(); i++) {
    PutVarint32(dst, kDeletedCloudFile);
    PutVarint64(dst, deleted_cloud_files_[i]); 
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  if (LOG)
    std::cout << "VersionEdit::GetInternalKey()" << std::endl;
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  if (LOG)
    std::cout << "VersionEdit::GetLevel()" << std::endl;
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  if (LOG)
    std::cout << "VersionEdit::DecodeFrom()" << std::endl;
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  FileMetaData f;
  CloudFile cf;
  Slice str;
  InternalKey key;

  while (msg == NULL && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;
      
      case kNextCloudFileNumber:
        if (GetVarint64(&input, &next_cloud_file_number_)) {
          has_next_cloud_file_number_ = true;
        } else {
          msg = "next cloud file number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      case kCloudCompactPointer:
        if (GetInternalKey(&input, &key)) {
          cloud_compact_pointers_.push_back(key);
        } else {
          msg = "cloud compaction pointer";
        }
        break;

      case kDeletedFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &number)) {
          deleted_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFile:
        if (GetLevel(&input, &level) &&
            GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(std::make_pair(level, f));
        } else {
          msg = "new-file entry";
        }
        break;
    
      case kNewCloudFile:
        if (GetVarint64(&input, &cf.obj_num) &&
            GetVarint64(&input, &cf.file_size) &&
            GetInternalKey(&input, &cf.smallest) &&
            GetInternalKey(&input, &cf.largest)) {
          new_cloud_files_.push_back(cf);
        } else {
          msg = "new-cloud-file entry"; // TODO is this just an error message
        }

      case kDeletedCloudFile:
        if (GetVarint64(&input, &number)) {
          deleted_cloud_files_.push_back(number);
        } else {
          msg = "deleted cloud file";
        }

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  if (LOG)
    std::cout << "VersionEdit::DebugString()" << std::endl;
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       iter != deleted_files_.end();
       ++iter) {
    r.append("\n  DeleteFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMetaData& f = new_files_[i].second;
    r.append("\n  AddFile: ");
    AppendNumberTo(&r, new_files_[i].first);
    r.append(" ");
    AppendNumberTo(&r, f.number);
    r.append(" ");
    AppendNumberTo(&r, f.file_size);
    r.append(" ");
    r.append(f.smallest.DebugString());
    r.append(" .. ");
    r.append(f.largest.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
