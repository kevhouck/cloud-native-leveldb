import logging
import tempfile
import os
import subprocess as sp
import boto3
import json
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info('got event{}'.format(json.dumps(event)))
    tmpdir = tempfile.mkdtemp()

    local_file = '%06d.ldb' % event['number']
    file_path = '%s/%s' % (tmpdir, local_file)

    s3 = boto3.resource('s3')
    s3.meta.client.download_file(os.environ['LEVELDB_BUCKET'],
            local_file, file_path)

    res_json = sp.check_output(['./table_reader', file_path, event['user_key']])
    logger.info('got result {}'.format(res_json))
    return json.loads(res_json)
