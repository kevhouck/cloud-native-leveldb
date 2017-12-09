#include <stdio.h>
#include <stdlib.h>
#include "db/cloud_manager.h"
#include "db/db_impl.h"
#include "db/version_edit.h"
#include "leveldb/options.h"

namespace leveldb {

class Env;

void ParseInputs(Env *env, CloudManager *cm, std::vector<FileMetaData*> inputs[2], int *next_file_number) {
  Status s;
  std::string base = "/tmp/leveldb/";
  std::string content;
  s = ReadFileToString(env, "/tmp/leveldb_merge.input", &content);
  json res_json = json::parse(content);
  std::vector<FileMetaData> local_files = res_json["local_files"];
  std::vector<FileMetaData> cloud_files = res_json["cloud_files"];
  *next_file_number = res_json["next_cloud_file_num"];

  for (int i = 0; i < local_files.size(); i++) {
    cm->FetchFile(local_files[i].number, base);
    inputs[0].push_back(new FileMetaData(local_files[i]));
  }
  for (int i = 0; i < local_files.size(); i++) {
    cm->FetchFile(cloud_files[i].number, base);
    inputs[1].push_back(new FileMetaData(cloud_files[i]));
  }
}

}

int main(int argc, char *argv[]) {
  // usage: standalone_merger region bucket
  leveldb::Options options;

  leveldb::CloudManager cm(argv[1], argv[2]);

  std::vector<leveldb::FileMetaData*> inputs[2], output;
  std::vector<leveldb::FileMetaData> files_deref;
  int next_fn = -1;
  leveldb::ParseInputs(options.env, &cm, inputs, &next_fn);

  leveldb::DBImpl db(options, "/tmp/leveldbtest-1000/dbbench");
  db.DoCloudCompactionWork(inputs, output, next_fn);

  for (int i = 0; i < output.size(); i++) {
    cm.SendFile(output[i]->number, "/tmp/leveldb/");
    files_deref.push_back(*output[i]);
  }

  json j = files_deref;
  std::cout << j.dump();

  return 0;
}
