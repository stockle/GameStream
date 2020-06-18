activate_this = '/home/ubuntu/GameStream/app/flask-env/bin/activate_this.py'
with open(activate_this) as f:
	exec(f.read(), dict(__file__=activate_this))

import sys
import logging

logging.basicConfig(stream=sys.stderr)
sys.path.insert(0,"/var/www/html/app/")
sys.path.insert(1, '/home/ubuntu/server/spark-2.4.5-bin-hadoop2.7/python')
sys.path.insert(2, '/home/ubuntu/server/spark-2.4.5-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip')

from app import app as application
