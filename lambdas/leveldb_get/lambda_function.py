import logging
import tempfile
import os
import subprocess as sp
import boto3
import json
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info('got event{}'.format(event['data']))
    tmpdir = tempfile.mkdtemp()

    fnum = json.loads(event['number'])
    user_key = json.loads(event['user_key'])
    local_file = '%06d.ldb' % fnum
    file_path = '%s/%s' % (tmpdir, local_file)

    s3 = boto3.resource('s3')
    s3.meta.client.download_file(os.environ['LEVELDB_BUCKET'],
            local_file, file_path)

    proc = sp.run(['./standalone_merger', os.environ['LEVELDB_REGION'], os.           environ['LEVELDB_BUCKET'], tmpdir], encoding='utf-8')
     if proc.returncode == 0:
         logger.info('got result {}'.format(proc.stdout))
     elif proc.returncode > 0:
         logger.info('record is not found, corrupted, or deleted (rc=%d): stdout=%s,   stderr=%s' % (proc.returncode, proc.stdout, proc.stderr))  
     else:
         logger.info('process kill by signal %d: stdout=%s, stderr=%s' % (proc.        returncode, proc.stdout, proc.stderr))

    return {'statue': proc.returncode, 'value': proc.stdout}
