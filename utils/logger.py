import logging
import sys
from datetime import datetime

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f'logs/ingestion_{datetime.now().strftime("%Y%m%d")}.log')
        ]
    )

def info(message):
    logging.info(message)

def error(message):
    logging.error(message)

def warn(message):
    logging.warning(message)