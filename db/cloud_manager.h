#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/lambda/LambdaClient.h>
#include "leveldb/status.h"
#include "db/version_edit.h"

namespace leveldb {

class VersionSet;
class CloudCompaction; 

class CloudManager {
  public:
    CloudManager(Aws::String region, Aws::String bucket);
    ~CloudManager();
    Status SendFile(uint64_t file_number, std::string base);
    Status FetchFile(uint64_t file_number, std::string base);
    Status InvokeLambdaCompaction(CloudCompaction* cc, VersionSet* versions);
    Status InvokeLambdaRandomGet(Slice ikey, Slice **value);
    Status FetchBloomFilter(uint64_t cloud_file_num, Slice* s);
  
  private:
    VersionSet* versions_;
    Aws::S3::S3Client* s3_client_;
    Aws::String s3_bucket_;
    Aws::Lambda::LambdaClient* lambda_client_;
    Aws::SDKOptions aws_options;
};

}
