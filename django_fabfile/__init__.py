import logging
import os
import sys

from fabric.api import output

from django_fabfile.utils import Config
config = Config()


debug = config.getboolean('DEFAULT', 'DEBUG')
logging_folder = config.get('DEFAULT', 'LOGGING_FOLDER')

# Set up a specific logger with desired output level
LOG_FORMAT = '%(asctime)-15s %(levelname)s:%(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S %Z'

logger = logging.getLogger()

if debug:
    logger.setLevel(logging.DEBUG)
    output['debug'] = True
else:
    logger.setLevel(logging.INFO)

if logging_folder:
    LOG_FILENAME = os.path.join(logging_folder, __name__ + '.log')
    handler = logging.handlers.TimedRotatingFileHandler(
        LOG_FILENAME, 'midnight', backupCount=30)

    class StreamLogger():

        def __init__(self, level=logging.INFO):
            self.logger = logging.getLogger(__name__)
            self.level = level

        def write(self, row):
            row = row.strip()
            if row:
                self.logger.log(self.level, row)

        def flush(self):
            pass
    # Redirect Fabric output to log file.
    sys.stdout = StreamLogger()
    sys.stderr = StreamLogger(level=logging.ERROR)
else:
    handler = logging.StreamHandler(sys.stdout)

fmt = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFORMAT)
handler.setFormatter(fmt)
logger.addHandler(handler)
