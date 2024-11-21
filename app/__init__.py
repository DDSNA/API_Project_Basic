import logging
import os
import sys

logger_init = logging.getLogger(__name__)
print(os.getcwd())
logger_init.warning(f"Current working directory: {os.getcwd()}")