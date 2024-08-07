import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any
import polars as pl

class RawToBase:
    def __init__(self, entities: List[str]):
        self.entities = entities
        self.config = {
            'accounts': {'unique_id': 'id'},
            'categories': {'unique_id': 'id'},
            'months': {'unique_id': 'month'},
            'payees': {'unique_id': 'id'},
            'transactions': {'unique_id': 'id'},
            'scheduled_transactions': {'unique_id': 'id'}
        }
        self.raw_data_path = 'data/raw'
        self.base_data_path = 'data/base'
        self.processed_data_path = 'data/processed'
        self.data = {}
        self.base_data = {}
        logging.basicConfig(level=logging.DEBUG)
        self.process_entities()
    
    def process_entities(self):
        for entity in self.entities:
            # check the file is in the raw data path, if not skip the entity
            folder_path = os.path.join(self.raw_data_path, entity)
            folder_contents = os.listdir(folder_path)
            # Check if the folder is empty
            if not folder_contents:
                logging.warning(f"The folder {folder_path} is empty skipping {entity}.")
                continue
            if not self._load_raw_data(entity):
                logging.warning(f"Skipping processing for entity: {entity} due to empty data.")
                continue
            self._load_existing_base_data(entity)
            self._combine_data(entity)
            self._resolve_duplicates(entity)
            self._save_base_data(entity)
            self._move_raw_to_processed(entity)
    
    def _load_raw_data(self, entity):
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
                        # Check if the data is empty
                        if not data.get(entity, []):
                            logging.warning(f"Received empty data for entity: {entity} in file: {file_path}, deleting file.")
                            # delete the file as it is empty
                            os.remove(file_path)
                            return  False
                        modified_data = []
                        if entity == 'categories':
                            for group in data.get('category_groups', []):
                                for category in group.get('categories', []):
                                    category['ingestion_date'] = datetime.strptime(file_name.split('.')[0], '%Y%m%d%H%M%S').date()
                                    modified_data.append(category)
                        else:
                            for record in data.get(f'{entity}', []):
                                if isinstance(record, dict):
                                    record['ingestion_date'] = datetime.strptime(file_name.split('.')[0], '%Y%m%d%H%M%S').date()
                                    modified_data.append(record)
                                else:
                                    modified_data.append({'record': record, 'ingestion_date': datetime.strptime(file_name.split('.')[0], '%Y%m%d%H%M%S').date()})
                        self.data[entity].append(modified_data)
                        logging.debug(f"Successfully loaded data from file: {file_path}")
                except Exception as e:
                    logging.error(f"Failed to load data from file: {file_path}, error: {e}")
                    exit(1)
        return True

    def _load_existing_base_data(self, entity):
        base_path = os.path.join(self.base_data_path, f'{entity}.parquet')
        if os.path.exists(base_path):
            logging.debug(f"Loading existing base data for entity: {entity} from path: {base_path}")
            self.base_data[entity] = pl.read_parquet(base_path)
            logging.debug(f"Successfully loaded existing base data for entity: {entity}")
        else:
            self.base_data[entity] = pl.DataFrame()
            logging.debug(f"No existing base data found for entity: {entity}, starting with an empty DataFrame")
    
    def _combine_data(self, entity):
        logging.debug(f"Combining data for entity: {entity}")
        combined_data = []
        if entity == 'categories':
            for data in self.data[entity]:
                for category in data:
                    combined_data.append(category)
        else:
            for data in self.data[entity]:
                combined_data.extend(data)
        
        new_data_df = pl.DataFrame(combined_data)
        #print(new_data_df)
        
        # Ensure the unique id column is preserved
        unique_id = self.config[entity]['unique_id']
        if unique_id not in new_data_df.columns:
            logging.error(f"Unique ID column '{unique_id}' not found in the combined data for entity: {entity}")
            exit(1)
        
        self.base_data[entity] = new_data_df
        logging.debug(f"Successfully combined data for entity: {entity}")

    def _resolve_duplicates(self, entity):
        logging.debug(f"Resolving duplicates for entity: {entity}")
        unique_id = self.config[entity]['unique_id']
        self.base_data[entity] = self.base_data[entity].sort(by='ingestion_date').unique(subset=unique_id, keep='first')
        logging.debug(f"Successfully resolved duplicates for entity: {entity}")

    def _save_base_data(self, entity):
        os.makedirs(self.base_data_path, exist_ok=True)
        file_path = os.path.join(self.base_data_path, f'{entity}.parquet')
        self.base_data[entity].write_parquet(file_path)
        logging.debug(f"Saved base data for entity: {entity} to path: {file_path}")

    def _move_raw_to_processed(self, entity):
        raw_entity_path = os.path.join(self.raw_data_path, entity)
        processed_path = os.path.join(self.processed_data_path, entity)
        
        # logging.debug(f"Raw entity path: {raw_entity_path}")
        # logging.debug(f"Processed path: {processed_path}")
        
        os.makedirs(processed_path, exist_ok=True)
        
        for file_name in os.listdir(raw_entity_path):
            if file_name.endswith('.json'):
                raw_file_path = os.path.join(raw_entity_path, file_name)
                processed_file_path = os.path.join(processed_path, file_name)
                
                logging.debug(f"Moving file: {raw_file_path} to {processed_file_path}")
                
                if os.path.exists(raw_file_path):
                    os.rename(raw_file_path, processed_file_path)
                    logging.debug(f"Moved file: {file_name}")
                else:
                    logging.error(f"File not found: {raw_file_path}")
        
        logging.debug(f"Moved processed files for entity: {entity} to path: {processed_path}")