import os
import dotenv
import logging
import yaml

from pipeline.ingest import Ingest
from pipeline.raw_to_base import RawToBase

dotenv.load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BUDGET_ID = os.getenv('BUDGET_ID')
logging.basicConfig(level=logging.DEBUG)

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

config['API_TOKEN'] = API_TOKEN
config['BUDGET_ID'] = BUDGET_ID

Ingest(config)
RawToBase(config)