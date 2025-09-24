import sys
import logging
import os

# Path to your project
sys.path.insert(0, '/var/www/purchase-dashboard')

# Activate virtual environment
activate_this = '/var/www/purchase-dashboard/venv/bin/activate_this.py'
with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))

# Import Flask app
from test import application as application

# Logging
logging.basicConfig(stream=sys.stderr)
