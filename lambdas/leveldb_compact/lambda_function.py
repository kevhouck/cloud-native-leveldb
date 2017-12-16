import logging
import tempfile
import sys
import subprocess as sp
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    logger.info('got event{}'.format(event))
    tmpdir = tempfile.mkdtemp()
    f = open(tmpdir + '/leveldb_merge.input', 'w')
    f.write(str(event))
    f.close()
    res_json = sp.check_output(['./standalone_merger', os.environ['LEVELDB_REGION'], os.environ['LEVELDB_BUCKET'], tmpdir])
    return {"data": res_json}

