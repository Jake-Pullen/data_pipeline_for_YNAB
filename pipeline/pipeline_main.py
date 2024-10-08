'''Module to run the data pipeline'''

import logging

from pipeline.ingest import Ingest
from pipeline.raw_to_base import RawToBase
from pipeline.dimensions import DimAccounts, DimCategories, DimPayees, DimDate
from pipeline.facts import FactTransactions, FactScheduledTransactions


def pipeline_main(config):
    '''Run the data pipeline'''
    logging.info('Starting data pipeline')

    ingest = Ingest(config)
    ingest.start_ingestion()
    RawToBase(config)
    DimAccounts(config)
    DimCategories(config)
    DimPayees(config)
    DimDate(config)
    FactTransactions(config)
    FactScheduledTransactions(config)

    logging.info('Data pipeline completed successfully')
