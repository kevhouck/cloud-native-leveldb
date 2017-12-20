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
    logger.info('got event: {}'.format(event['data']))
    tmpdir = tempfile.mkdtemp()
    f = open(tmpdir + '/leveldb_merge.input', 'w')
    f.write(event['data'])
    f.close()

    result = {}

    data = json.loads(event['data'])
    cloud_files = [f['number'] for f in data['cloud_files']]
    local_files= [f['number'] for f in data['local_files']]

    start = time.time()
    s3 = boto3.resource('s3')
    for fnum in cloud_files + local_files:
        s3.meta.client.download_file(os.environ['LEVELDB_BUCKET'],
                '%06d.ldb' % fnum,
                '%s/%06d.ldb' % (tmpdir, fnum))
    end = time.time()
    result['download_time'] = end - start

    start = time.time()
    res_json = sp.check_output(['./standalone_merger', os.environ['LEVELDB_REGION'], os.environ['LEVELDB_BUCKET'], tmpdir])
    end = time.end()
    result['merge_time'] = end - start
    logger.info('got result: {}'.format(res_json))
    result['data'] = res_json

    start = time.time()
    for f in json.loads(res_json):
        fnum = f['number']
        s3.meta.client.upload_file('%s/%06d.ldb' % (tmpdir, fnum),
                os.environ['LEVELDB_BUCKET'],
                '%06d.ldb' % fnum)
    end = time.time()
    result['upload_time'] = end - start

    return result
