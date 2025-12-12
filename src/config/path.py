import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent

DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')

os.makedirs(DATA_DIR, exist_ok = True)
os.makedirs(RAW_DATA_DIR, exist_ok = True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok = True)