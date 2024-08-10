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
    try:
        with open('config/logging_config.yaml', 'r') as f:
            log_config = yaml.safe_load(f)
        logging.config.dictConfig(log_config)
    except yaml.YAMLError as e:
        print(f"Error parsing logging configuration file: {e}")
        log_config = {}  # Initialize log_config to an empty dictionary
        logging.basicConfig(level=logging.INFO)  # Fallback to a basic configuration
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

    try:
        with open('config/config.yaml', 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logging.error('config.yaml file not found')
        sys.exit(ec.MISSING_CONFIG_FILE)
    except yaml.YAMLError as e:
        logging.error(f'Error loading config.yaml: {e}')
        sys.exit(ec.CORRUPTED_CONFIG_FILE)

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