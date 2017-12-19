#include <stdlib.h>
#include "db/dbformat.h"
#include "leveldb/options.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "base64/base64.h"

namespace leveldb {
namespace {
enum SaverState {
  kFound,
  kNotFound,
  kDeleted,
  kCorrupt,
};

struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}
}

int main(int argc, char *argv[]) {
  // ./table_reader <filename> <key>
  leveldb::Status s;
  leveldb::Options options;
  leveldb::ReadOptions readOptions;
  leveldb::RandomAccessFile *file = NULL;
  uint64_t file_size = -1;
  leveldb::Table *table = NULL;
  std::string key = base64_decode(std::string(argv[2]));
  leveldb::Slice user_key(key);
  std::string value;

  s = options.env->NewRandomAccessFile(argv[1], &file);
  s = options.env->GetFileSize(argv[1], &file_size);
  s = leveldb::Table::Open(options, file, file_size, &table);

  leveldb::Saver saver;
  saver.state = leveldb::kNotFound;
  saver.ucmp = options.comparator;
  saver.user_key = user_key;
  saver.value = &value;
  s = table->InternalGet(readOptions, user_key, &saver, leveldb::SaveValue);
  std::cout << base64_encode((const unsigned char *) saver.value->c_str(), saver.value->size());
  return saver.state;
}
