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

    try:
        value = sp.check_output(['./table_reader', file_path, event['user_key']])
        logger.info('got result {}'.format(value))
        return {'status': 0, 'value': value}
    except sp.CalledProcessError as e:
        if e.returncode > 0:
            logger.info('record is not found, corrupted, or deleted (rc=%d): %s' % (e.returncode, e.output))
        else:
            logger.info('process kill by signal %d: %s' % (e.returncode, e.output))
        return {'status': e.returncode, 'value': e.output}
