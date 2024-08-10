import os
import dotenv
import logging
import yaml
import sys
import atexit
import logging.config
import logging.handlers

import config.exit_codes as ec
from pipeline.ingest import Ingest
from pipeline.raw_to_base import RawToBase
from pipeline.dimensions import DimAccounts, DimCategories, DimPayees, DimDate
from pipeline.facts import FactTransactions, FactScheduledTransactions

def set_up_logging():
    with open('config/logging_config.yaml', 'r') as f:
        try:
            log_config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(e)
    logging.config.dictConfig(log_config)
    queue_handler = logging.getHandlerByName('queue_handler')
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)

logger = logging.getLogger("data_pipeline_for_ynab")
os.makedirs('logs', exist_ok=True)
set_up_logging()
# Load environment variables
dotenv.load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
BUDGET_ID = os.getenv('BUDGET_ID')

def main():
    if not API_TOKEN or not BUDGET_ID:
        logging.error('API_TOKEN or BUDGET_ID is not set in .env file')
        sys.exit(ec.MISSING_ENV_VARS)

    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    config['API_TOKEN'] = API_TOKEN
    config['BUDGET_ID'] = BUDGET_ID

    Ingest(config)
    RawToBase(config)
    DimAccounts(config)
    DimCategories(config)
    DimPayees(config)
    DimDate(config)
    FactTransactions(config)
    FactScheduledTransactions(config)

if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        exit_code = e.code
        logging.error(f'Program exited with code {exit_code}')
        raise