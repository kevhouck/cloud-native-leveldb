#include "leveldb/status.h"
#include "db/version_edit.h"

namespace Aws {
namespace S3 {
class S3Client;
}
namespace Lambda {
class LambdaClient;
}
struct SDKOptions;
}

namespace leveldb {

class VersionSet;
class CloudCompaction; 

class CloudManager {
  public:
    CloudManager(std::string region, std::string bucket);
    ~CloudManager();
    Status SendFile(uint64_t file_number, std::string base);
    Status FetchFile(uint64_t file_number, std::string base);
    Status InvokeLambdaCompaction(CloudCompaction* cc, VersionSet* versions);
    Status InvokeLambdaRandomGet(Slice user_key, CloudFile* cf, Slice **value);
    Status FetchBloomFilter(uint64_t cloud_file_num, Slice* s);
  
  private:
    VersionSet* versions_;
    Aws::S3::S3Client* s3_client_;
    std::string s3_bucket_;
    Aws::Lambda::LambdaClient* lambda_client_;
    Aws::SDKOptions* aws_options;
};

}
