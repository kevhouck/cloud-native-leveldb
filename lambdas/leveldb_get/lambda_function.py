import logging
import tempfile
import os
import subprocess as sp
import boto3
import json
import time
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info('got event: {}'.format(json.dumps(event)))
    tmpdir = tempfile.mkdtemp()

    local_file = '%06d.ldb' % event['number']
    file_path = '%s/%s' % (tmpdir, local_file)

    result = {}

    start = time.time()
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(os.environ['LEVELDB_BUCKET'],
            local_file, file_path)
    end = time.time()
    result['download_time'] = end - start

    start = time.time()
    res_json = sp.check_output(['./table_reader', file_path, event['user_key']])
    end = time.time()
    result['command_time'] = end - start
    logger.info('got result: {}'.format(res_json))
    j = json.loads(res_json)

    for k,v in j.items():
        result[k] = v

    return result
