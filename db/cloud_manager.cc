#include <iostream>
#include <string>
#include <fstream>
#include <aws/s3/S3Client.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/lambda/model/InvokeRequest.h>
#include "leveldb/status.h"
#include "db/version_set.h"
#include "db/version_edit.h"
#include "db/filename.h"
#include "base64/base64.h"

namespace leveldb {

extern std::ofstream bench_file;

CloudManager::CloudManager(Aws::String region, Aws::String bucket) {
  Aws::InitAPI(aws_options);
  
  Aws::Client::ClientConfiguration client_config;
  client_config.requestTimeoutMs = 600000;
  client_config.region = region;
  s3_client_ = new Aws::S3::S3Client(client_config);
  s3_bucket_ = bucket;

  Aws::Client::ClientConfiguration client_config_2;
  client_config_2.region = region;
  lambda_client_ = new Aws::Lambda::LambdaClient(client_config_2);
}

CloudManager::~CloudManager() {
  Aws::ShutdownAPI(aws_options);
}

Status CloudManager::SendFile(uint64_t file_number, std::string base) {
#ifdef DEBUG_LOG
    std::cerr << "SendLocalFile()" << std::endl;
#endif
  std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution_clock::now();

  std::string obj_name;
  std::string file_name; 
  if (file_number >= 1000000) {
    char buf[12];
    sprintf(buf, "%07lu.ldb", file_number);
    obj_name = std::string(buf, 12); 
    file_name = base + "/" + obj_name;
  } else {
    char buf[11];
    sprintf(buf, "%06lu.ldb", file_number);
    obj_name = std::string(buf, 11); 
    file_name = base + "/" + obj_name;
  }

  Aws::S3::Model::PutObjectRequest obj_req;
  obj_req.WithBucket(s3_bucket_).WithKey(Aws::String(obj_name.c_str()));
  auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
    file_name.c_str(), std::ios_base::in | std::ios_base::binary);
  obj_req.SetBody(input_data);
  auto put_object_outcome = s3_client_->PutObject(obj_req);

  if (put_object_outcome.IsSuccess()) {
    std::cerr << "Put successful" << std::endl;
    std::chrono::high_resolution_clock::time_point end_time = std::chrono::high_resolution_clock::now();
    bench_file << "send_file: " << (end_time - start_time).count() << std::endl;
    return Status::OK();
  } else {
    std::cerr << "Put failed" << std::endl;
    return Status::IOError(Slice("S3 Object Put Request failed"));
  }
}

Status CloudManager::FetchFile(uint64_t file_number, std::string base) {
#ifdef DEBUG_LOG
    std::cerr << "FetchFile()" << std::endl;
#endif
  std::string file_name; 
  if (file_number >= 1000000) {
    char buf[12];
    sprintf(buf, "%07lu.ldb", file_number);
    file_name = std::string(buf, 12); 
  } else {
    char buf[11];
    sprintf(buf, "%06lu.ldb", file_number);
    file_name = std::string(buf, 11); 
  }

  Aws::S3::Model::GetObjectRequest obj_req;
  obj_req.WithBucket(s3_bucket_).WithKey(Aws::String(file_name.c_str()));
  auto get_outcome = s3_client_->GetObject(obj_req);

  if (!get_outcome.IsSuccess()) {
    std::cerr << "File Fetch Failed" << std::endl;
    std::cerr << get_outcome.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("File Fetch Failed"));
  } else {
    std::cerr << "File Fetch Successful" << std::endl;
  }

  Aws::OFStream local_file;
  local_file.open((base + "/" + file_name).c_str(), std::ios::out | std::ios::binary);
  local_file << get_outcome.GetResult().GetBody().rdbuf();
  return Status::OK(); 
}

Status CloudManager::InvokeLambdaCompaction(CloudCompaction* cc, VersionSet* versions) {
#ifdef DEBUG_LOG
    std::cerr << "InvokeLambdaCompaction()" << std::endl;
#endif
  std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution_clock::now();
  json j;
  j["local_files"] = cc->local_inputs_;
  j["cloud_files"] = cc->cloud_inputs_;
  j["next_cloud_file_num"] = versions->NextCloudFileNumber(); 
  Aws::String js = Aws::String(j.dump().c_str());
  std::cerr << j.dump(4) << std::endl;

  Aws::Lambda::Model::InvokeRequest invoke_req;
  invoke_req.SetFunctionName("leveldb_compact");
  invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
  Aws::Utils::Json::JsonValue json_payload;
  json_payload.WithString("data", js);
  std::shared_ptr<Aws::IOStream> payload = Aws::MakeShared<Aws::StringStream>("");
  *payload << json_payload.WriteReadable();
  invoke_req.SetBody(payload);
  invoke_req.SetContentType("application/json");
  auto invoke_res = lambda_client_->Invoke(invoke_req);
  if (invoke_res.IsSuccess()) {
    std::cerr << "Compact Lambda Invoke Successful" << std::endl;
  } else {
    std::cerr << "Compact Lambda Invoke Failed" << std::endl;
    std::cerr << invoke_res.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("Compact Lambda Invoke Request failed"));
  }
  
  auto &result = invoke_res.GetResult();
  auto &result_payload = result.GetPayload();
  Aws::Utils::Json::JsonValue json_result(result_payload);
  Aws::String json_as_aws_string = json_result.GetString("data");
  std::string json_as_string = std::string(json_as_aws_string.c_str(), json_as_aws_string.size());
  std::cerr << json_as_string << std::endl;
  json res_json = json::parse(json_as_string);
  std::cerr << res_json.dump(4) << std::endl;
  std::vector<CloudFile> new_cloud_files = res_json;
  cc->new_cloud_files_ = new_cloud_files;
  std::cerr << "cc->new_cloud_files_.size() " << cc->new_cloud_files_.size() << std::endl;
  std::chrono::high_resolution_clock::time_point end_time = std::chrono::high_resolution_clock::now();
  bench_file << "lambda_compaction: " << (end_time - start_time).count() << std::endl;
  return Status::OK();
}

Status CloudManager::InvokeLambdaRandomGet(Slice user_key, CloudFile* cf, Slice** value) {
#ifdef DEBUG_LOG
    std::cerr << "InvokeLambdaRandomGet()" << std::endl;
#endif
  std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution_clock::now();

  std::string user_key_str = user_key.ToString();
  std::string encoded_user_key = base64_encode((const unsigned char*)user_key_str.c_str(), user_key_str.size());
  Aws::Lambda::Model::InvokeRequest invoke_req;
  invoke_req.SetFunctionName("leveldb_get");
  invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
  Aws::Utils::Json::JsonValue json_payload;
  json_payload.WithString("user_key", Aws::String(encoded_user_key.c_str(), encoded_user_key.size()));
  json_payload.WithInt64("number", cf->obj_num);
  std::shared_ptr<Aws::IOStream> payload = Aws::MakeShared<Aws::StringStream>("");
  std::cerr << "get payload " << json_payload.WriteReadable() << std::endl;
  *payload << json_payload.WriteReadable();
  invoke_req.SetBody(payload);
  invoke_req.SetContentType("application/json");
  auto invoke_res = lambda_client_->Invoke(invoke_req);
  if (invoke_res.IsSuccess()) {
    std::cerr << "Get Lambda Invoke Successful" << std::endl;
  } else {
    std::cerr << "Get Lambda Invoke Failed" << std::endl;
    std::cerr << invoke_res.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("Get Lambda Invoke Request failed"));
  }
  auto &result = invoke_res.GetResult();
  auto &result_payload = result.GetPayload();
  Aws::Utils::Json::JsonValue json_result(result_payload);

  uint32_t status_num = json_result.GetInteger("status");
  std::cerr << "status_num " << status_num << std::endl;
  
  std::chrono::high_resolution_clock::time_point end_time = std::chrono::high_resolution_clock::now();
  bench_file << "lambda_get: " << (end_time - start_time).count() << std::endl;
  if (status_num != 0) {
    // The key was not actually in the cloud file
    return Status::NotFound(Slice());
  }
  Aws::String value_as_aws_string = json_result.GetString("value");
  std::string value_str = std::string(value_as_aws_string.c_str(), value_as_aws_string.size());
  std::cerr << "value " << value_str << std::endl;
  *value = new Slice(base64_decode(value_str));
  return Status::OK();
}

Status CloudManager::FetchBloomFilter(uint64_t cloud_file_num, Slice* s) {
#ifdef DEBUG_LOG
    std::cerr << "FetchBloomFilter()" << std::endl;
#endif
  char buf[12] = { 0 };
  sprintf(buf, "%07lu.ldb", cloud_file_num);
  std::string bloom_file_obj_name = "bloom" + std::string(buf, 12); 

  Aws::S3::Model::GetObjectRequest obj_req;
  obj_req.WithBucket(s3_bucket_).WithKey(Aws::String(bloom_file_obj_name.c_str()));
  
  auto get_outcome = s3_client_->GetObject(obj_req);
  
  if (!get_outcome.IsSuccess()) {
    std::cerr << "Bloom Filter Fetch Failed" << std::endl;
    std::cerr << get_outcome.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("Bloom Filter Fetch Failed"));
  } else {
    std::cerr << "Bloom Filter Fetch Successful" << std::endl;
  }

  std::ostringstream binbuf;
  binbuf << get_outcome.GetResult().GetBody().rdbuf();
  s = new Slice(binbuf.str());  
  return Status::OK(); 
}

}
