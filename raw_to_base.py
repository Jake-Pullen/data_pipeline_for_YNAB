import pandas
import json
import os
import logging
from datetime import datetime
from typing import List

class RawToBase:
    def __init__(self, entities: List[str], raw_data_path: str, base_data_path: str):
        self.entities = entities
        self.raw_data_path = raw_data_path
        self.base_data_path = base_data_path
        self.data = {}
        self.base_data = {}
        logging.basicConfig(level=logging.DEBUG)
        self._load_raw_data()
        self._load_existing_base_data()
        self._combine_data()
        self._resolve_duplicates()
        self._save_base_data()
    
    def _load_raw_data(self):
        for entity in self.entities:
            entity_path = os.path.join(self.raw_data_path, entity)
            self.data[entity] = []
            logging.debug(f"Loading data for entity: {entity} from path: {entity_path}")
            for file_name in os.listdir(entity_path):
                if file_name.endswith('.json'):
                    file_path = os.path.join(entity_path, file_name)
                    logging.debug(f"Reading file: {file_path}")
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                            for record in data:
                                record['ingestion_date'] = datetime.strptime(file_name.split('.')[0], '%Y%m%d').date()
                            self.data[entity].append(data)
                            logging.debug(f"Successfully loaded data from file: {file_path}")
                    except Exception as e:
                        logging.error(f"Failed to load data from file: {file_path}, error: {e}")
    
    def _load_existing_base_data(self):
        for entity in self.entities:
            base_path = os.path.join(self.base_data_path, 'base', entity, f'{entity}.parquet')
            if os.path.exists(base_path):
                logging.debug(f"Loading existing base data for entity: {entity} from path: {base_path}")
                self.base_data[entity] = pandas.read_parquet(base_path)
                logging.debug(f"Successfully loaded existing base data for entity: {entity}")
            else:
                self.base_data[entity] = pandas.DataFrame()
                logging.debug(f"No existing base data found for entity: {entity}, starting with an empty DataFrame")
    
    def _combine_data(self):
        for entity in self.entities:
            logging.debug(f"Combining data for entity: {entity}")
            combined_data = []
            for data in self.data[entity]:
                combined_data.extend(data)
            new_data_df = pandas.DataFrame(combined_data)
            self.base_data[entity] = pandas.concat([self.base_data[entity], new_data_df], ignore_index=True)
            logging.debug(f"Successfully combined data for entity: {entity}")
    
    def _resolve_duplicates(self):
        for entity in self.entities:
            logging.debug(f"Resolving duplicates for entity: {entity}")
            self.base_data[entity] = self.base_data[entity].sort_values('ingestion_date', ascending=False).drop_duplicates('id', keep='first')
            logging.debug(f"Successfully resolved duplicates for entity: {entity}")

    def _save_base_data(self):
        for entity in self.entities:
            base_path = os.path.join(self.base_data_path, 'base', entity)
            os.makedirs(base_path, exist_ok=True)
            file_path = os.path.join(base_path, f'{entity}.parquet')
            self.base_data[entity].to_parquet(file_path)
            logging.debug(f"Saved base data for entity: {entity} to path: {file_path}")