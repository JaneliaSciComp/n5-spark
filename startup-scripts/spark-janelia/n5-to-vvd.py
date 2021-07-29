#!/usr/bin/env python

import os
import sys
import subprocess

sys.dont_write_bytecode = True
curr_script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(curr_script_dir))
from jar_path_util import get_provided_jar_path
bin_path = get_provided_jar_path()

flintstone_relpath = os.path.join('flintstone', 'flintstone-lsd.sh')
flintstone_path = '/groups/flyTEM/flyTEM/render/spark/spark-janelia/flintstone.sh' #os.path.join(curr_script_dir, flintstone_relpath)

os.environ['N_CORES_DRIVER'] = '2'
#os.environ['MEMORY_PER_NODE'] = '115'
os.environ['RUNTIME'] = '24:00'

nodes = int(sys.argv[1])

subprocess.call([flintstone_path, str(nodes), bin_path, 'org.janelia.saalfeldlab.n5.spark.N5ToVVDSpark'] + sys.argv[2:])