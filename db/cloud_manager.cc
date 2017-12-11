#include "db/cloud_manager.h"
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

namespace leveldb {

CloudManager::CloudManager(Aws::String region, Aws::String bucket, std::string dbname, VersionSet* versions) {
  dbname_ = dbname;
  versions_ = versions;

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

Status CloudManager::SendLocalFile(FileMetaData& f) {
  std::string file_name = TableFileName(dbname_, f.number);
  char buf[11] = { 0 };
  sprintf(buf, "%06lu.ldb", f.number);

  Aws::S3::Model::PutObjectRequest obj_req;
  obj_req.WithBucket(s3_bucket_).WithKey(buf);
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

Status CloudManager::InvokeLambdaCompaction(CloudCompaction* cc) {
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
  j["next_cloud_file_num"] = versions_->NextCloudFileNumber(); 
  Aws::String js = Aws::String(j.dump().c_str());
  std::cout << j.dump(4) << std::endl;

  Aws::Lambda::Model::InvokeRequest invoke_req;
  invoke_req.SetFunctionName("leveldb_compact");
  invoke_req.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
  Aws::Utils::Json::JsonValue json_payload;
  json_payload.WithString("data", js);
  json_payload.WithArray("local_files", Aws::Utils::Array<Aws::String>(local_file_names, cc->local_inputs_.size()));
  json_payload.WithArray("cloud_files", Aws::Utils::Array<Aws::String>(cloud_file_names, cc->cloud_inputs_.size()));
  std::shared_ptr<Aws::IOStream> payload = Aws::MakeShared<Aws::StringStream>("FunctionTest");
  *payload << json_payload.WriteReadable();
  invoke_req.SetBody(payload);
  invoke_req.SetContentType("application/json");
  auto invoke_res = lambda_client_->Invoke(invoke_req);
  if (invoke_res.IsSuccess()) {
    std::cout << "Invoke Successful" << std::endl;
  } else {
    std::cout << "Invoke Failed" << std::endl;
    std::cout << invoke_res.GetError().GetMessage() << std::endl;
    return Status::IOError(Slice("Lambda Invoke Request failed"));
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

Status CloudManager::InvokeLambdaRandomGet() {

}

Status CloudManager::FetchBloomFilter(uint64_t cloud_file_num, Slice* s) {
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
