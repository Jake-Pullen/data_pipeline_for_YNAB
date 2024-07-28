import requests
import os
import json
import dotenv
import logging
import time
from injest import injest

dotenv.load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BUDGET_ID = os.getenv('BUDGET_ID')
headers = {'Authorization': f'Bearer {API_TOKEN}'}
logging.basicConfig(level=logging.DEBUG)

injest_info = {}
#entities = ['accounts', 'categories', 'months', 'payees', 'transactions', 'scheduled_transactions']
#injest_info['entities'] = entities
injest_info['base_url'] = 'https://api.ynab.com/v1/budgets'
injest_info['knowledge_file'] = 'server_knowledge_cache.json'
injest_info['API_TOKEN'] = API_TOKEN
injest_info['BUDGET_ID'] = BUDGET_ID


injest(injest_info)#.fetch_and_cache_entity_data()
