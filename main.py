import os
import dotenv
import logging
from ingest import Ingest
from raw_to_base import RawToBase

dotenv.load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BUDGET_ID = os.getenv('BUDGET_ID')
logging.basicConfig(level=logging.DEBUG)

entities = ['accounts', 'categories', 'months', 'payees', 'transactions', 'scheduled_transactions']
ingest_info = {}

ingest_info['entities'] = entities
ingest_info['base_url'] = 'https://api.ynab.com/v1/budgets'
ingest_info['knowledge_file'] = 'server_knowledge_cache.json'
ingest_info['API_TOKEN'] = API_TOKEN
ingest_info['BUDGET_ID'] = BUDGET_ID


Ingest(ingest_info)
RawToBase(entities)