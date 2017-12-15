#include <iostream>
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

CloudManager::CloudManager(Aws::String region, Aws::String bucket, std::string dbname) {
  dbname_ = dbname;

  Aws::InitAPI(aws_options);
  
  Aws::Client::ClientConfiguration client_config;
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

Status CloudManager::SendFile(uint64_t file_number, bool is_cloud, std::string base) {
  if (LOG)
    std::cout << "SendLocalFile()" << std::endl;
  std::string obj_name;
  std::string file_name; 
  if (base == dbname_) {
    file_name = TableFileName(dbname_, file_number);
    char buf[11] = { 0 };
    sprintf(buf, "%06lu.ldb", file_number);
    obj_name = std::string(buf, 11); 
  } else if (base != dbname_ && is_cloud) {
    char buf[12];
    sprintf(buf, "%07lu.ldb", file_number);
    obj_name = std::string(buf, 12); 
    file_name = base + "/" + obj_name;
  } else if (base != dbname_ && !is_cloud) {
    char buf[11];
    sprintf(buf, "%06lu.ldb", file_number);
    obj_name = std::string(buf, 11); 
    file_name = base + "/" + obj_name;
  } else {
    // invalid
    return Status::InvalidArgument(Slice("db_base and cloud file is invalid argument"));
  }

  Aws::S3::Model::PutObjectRequest obj_req;
  obj_req.WithBucket(s3_bucket_).WithKey(Aws::String(obj_name.c_str()));
  auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
    file_name.c_str(), std::ios_base::in | std::ios_base::binary);
  obj_req.SetBody(input_data);
  auto put_object_outcome = s3_client_->PutObject(obj_req);

  if (put_object_outcome.IsSuccess()) {
    std::cout << "Put successful" << std::endl;
    return Status::OK();
  } else {
    std::cout << "Put failed" << std::endl;
    return Status::IOError(Slice("S3 Object Put Request failed"));
  }
}

Status CloudManager::FetchFile(uint64_t file_number, bool is_cloud, std::string base) {
  if (LOG)
    std::cout << "FetchFile()" << std::endl;
 
  std::string file_name; 
  if (is_cloud) {
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
    std::cout << "File Fetch Failed" << std::endl;
    std::cout << get_outcome.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("File Fetch Failed"));
  } else {
    std::cout << "File Fetch Successful" << std::endl;
  }

  Aws::OFStream local_file;
  local_file.open((base + file_name).c_str(), std::ios::out | std::ios::binary);
  local_file << get_outcome.GetResult().GetBody().rdbuf();
  return Status::OK(); 
}

Status CloudManager::InvokeLambdaCompaction(CloudCompaction* cc, VersionSet* versions) {
  if (LOG)
    std::cout << "InvokeLambdaCompaction()" << std::endl;
  Aws::String local_file_names[cc->local_inputs_.size()];
  for (size_t i = 0; i < cc->local_inputs_.size(); i++) {
    local_file_names[i] = Aws::String(std::to_string(cc->local_inputs_[i].number).c_str());
  }
  Aws::String cloud_file_names[cc->cloud_inputs_.size()];
  for (size_t i = 0; i < cc->cloud_inputs_.size(); i++) {
    cloud_file_names[i] = Aws::String(std::to_string(cc->cloud_inputs_[i].obj_num).c_str());
  }
  json j;
  j["local_files"] = cc->local_inputs_;
  j["cloud_files"] = cc->cloud_inputs_;
  j["next_cloud_file_num"] = versions->NextCloudFileNumber(); 
  Aws::String js = Aws::String(j.dump().c_str());
  std::cout << j.dump(4) << std::endl;

  Aws::Lambda::Model::InvokeRequest invoke_req;
  invoke_req.SetFunctionName("leveldb_compact");
  invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
  Aws::Utils::Json::JsonValue json_payload;
  json_payload.WithString("data", js);
  json_payload.WithArray("local_files", Aws::Utils::Array<Aws::String>(local_file_names, cc->local_inputs_.size()));
  json_payload.WithArray("cloud_files", Aws::Utils::Array<Aws::String>(cloud_file_names, cc->cloud_inputs_.size()));
  std::shared_ptr<Aws::IOStream> payload = Aws::MakeShared<Aws::StringStream>("");
  *payload << json_payload.WriteReadable();
  invoke_req.SetBody(payload);
  invoke_req.SetContentType("application/json");
  auto invoke_res = lambda_client_->Invoke(invoke_req);
  if (invoke_res.IsSuccess()) {
    std::cout << "Compact Lambda Invoke Successful" << std::endl;
  } else {
    std::cout << "Compact Lambda Invoke Failed" << std::endl;
    std::cout << invoke_res.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("Compact Lambda Invoke Request failed"));
  }
  
  auto &result = invoke_res.GetResult();
  auto &result_payload = result.GetPayload();
  Aws::Utils::Json::JsonValue json_result(result_payload);
  Aws::String json_as_aws_string = json_result.GetString("data");
  std::string json_as_string = std::string(json_as_aws_string.c_str(), json_as_aws_string.size());
  json res_json = json::parse(json_as_string);
  std::cout << res_json.dump(4) << std::endl;
  std::vector<CloudFile> new_cloud_files = res_json;
  cc->new_cloud_files_ = new_cloud_files;

  return Status::OK();
}

Status CloudManager::InvokeLambdaRandomGet(Slice ikey, Slice** value) {
  if (LOG)
    std::cout << "InvokeLambdaRandomGet()" << std::endl;
  std::string ikey_str = ikey.ToString();
  json j;
  j["ikey"] = base64_encode((const unsigned char*)ikey_str.c_str(), ikey_str.size());
  Aws::String js = Aws::String(j.dump().c_str());
  Aws::Lambda::Model::InvokeRequest invoke_req;
  invoke_req.SetFunctionName("leveldb_get");
  invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
  Aws::Utils::Json::JsonValue json_payload;
  json_payload.WithString("data", js);
  std::shared_ptr<Aws::IOStream> payload = Aws::MakeShared<Aws::StringStream>("");
  *payload << json_payload.WriteReadable();
  invoke_req.SetBody(payload);
  invoke_req.SetContentType("application/json");
  auto invoke_res = lambda_client_->Invoke(invoke_req);
  if (invoke_res.IsSuccess()) {
    std::cout << "Get Lambda Invoke Successful" << std::endl;
  } else {
    std::cout << "Get Lambda Invoke Failed" << std::endl;
    std::cout << invoke_res.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("Get Lambda Invoke Request failed"));
  }
  auto &result = invoke_res.GetResult();
  auto &result_payload = result.GetPayload();
  Aws::Utils::Json::JsonValue json_result(result_payload);
  Aws::String json_as_aws_string = json_result.GetString("data");
  std::string json_as_string = std::string(json_as_aws_string.c_str(), json_as_aws_string.size());
  json res_json = json::parse(json_as_string);

  std::string value_str = res_json.at("value").get<std::string>().c_str();
  if (value_str == "") {
    // The key was not actually in the cloud file
    Status::NotFound(Slice());
  }
  *value = new Slice(base64_decode(value_str));
  return Status::OK();
}

Status CloudManager::FetchBloomFilter(uint64_t cloud_file_num, Slice* s) {
  if (LOG)
    std::cout << "FetchBloomFilter()" << std::endl;
  char buf[12] = { 0 };
  sprintf(buf, "%07lu.ldb", cloud_file_num);
  std::string bloom_file_obj_name = "bloom" + std::string(buf, 12); 

  Aws::S3::Model::GetObjectRequest obj_req;
  obj_req.WithBucket(s3_bucket_).WithKey(Aws::String(bloom_file_obj_name.c_str()));
  
  auto get_outcome = s3_client_->GetObject(obj_req);
  
  if (!get_outcome.IsSuccess()) {
    std::cout << "Bloom Filter Fetch Failed" << std::endl;
    std::cout << get_outcome.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("Bloom Filter Fetch Failed"));
  } else {
    std::cout << "Bloom Filter Fetch Successful" << std::endl;
  }

  std::ostringstream binbuf;
  binbuf << get_outcome.GetResult().GetBody().rdbuf();
  s = new Slice(binbuf.str());  
  return Status::OK(); 
}

}
