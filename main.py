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

