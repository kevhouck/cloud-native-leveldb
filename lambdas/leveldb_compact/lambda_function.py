import logging
import tempfile
import subprocess as sp
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def lambda_handler(event, context):
    tmpdir = tempfile.mkdtemp()
    f = open(tmpdir + '/leveldb_merge.input', 'w')
    f.write(str(event)) 
    f.close()
    res_json = sp.check_output(['./standalone_merger', 'us-west-2', '739bench', tmpdir])
    logger.info('got event{}'.format(event))
    return {"data": res_json}

