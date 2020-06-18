activate_this = '/home/ubuntu/GameStream/app/flask-env/bin/activate_this.py'
with open(activate_this) as f:
	exec(f.read(), dict(__file__=activate_this))

import sys
import logging

sys.stdout = sys.stderr
logging.basicConfig(stream=sys.stderr)
sys.path.insert(0,"/var/www/html/app/")

from app import app as application
