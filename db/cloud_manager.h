#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/lambda/LambdaClient.h>
#include "leveldb/status.h"
#include "db/version_set.h"

namespace leveldb {

class CloudManager {
  public:
    CloudManager(Aws::String region, Aws::String bucket, std::string dbname, VersionSet* versions);
    ~CloudManager();
    Status SendLocalFile(FileMetaData& f);
    Status InvokeLambdaCompaction(CloudCompaction* cc);
    Status InvokeLambdaRandomGet();
    Status FetchBloomFilter(uint64_t cloud_file_num, Slice* s);
  
  private:
    std::string dbname_;
    VersionSet* versions_;
    Aws::S3::S3Client* s3_client_;
    Aws::String s3_bucket_;
    Aws::Lambda::LambdaClient* lambda_client_;
    Aws::SDKOptions aws_options;
};

}
