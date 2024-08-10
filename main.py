import os
import dotenv
import logging
import yaml

from pipeline.ingest import Ingest
from pipeline.raw_to_base import RawToBase
from pipeline.dimensions import DimAccounts, DimCategories, DimPayees, DimDate
from pipeline.facts import FactTransactions, FactScheduledTransactions

dotenv.load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BUDGET_ID = os.getenv('BUDGET_ID')
logging.basicConfig(level=logging.DEBUG)
if not API_TOKEN or not BUDGET_ID:
    logging.error('API_TOKEN or BUDGET_ID is not set in .env file')
    exit(1)

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

config['API_TOKEN'] = API_TOKEN
config['BUDGET_ID'] = BUDGET_ID

if __name__ == '__main__':
    Ingest(config)
    RawToBase(config)
    DimAccounts(config)
    DimCategories(config)
    DimPayees(config)
    DimDate(config)
    FactTransactions(config)
    FactScheduledTransactions(config)
