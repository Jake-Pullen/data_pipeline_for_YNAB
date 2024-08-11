import os
import json
import logging
import sys
from datetime import datetime
from typing import Dict, Any
import config.exit_codes as ec
import polars as pl

class RawToBase:
    def __init__(self, config: Dict[str, Any]):
        self.entities = config['entities']
        self.primary_keys = config['primary_keys']
        self.raw_data_path = config['raw_data_path']
        self.processed_data_path = config['processed_data_path']
        self.base_data_path = config['base_data_path']
        self.data = {}
        self.base_data = {}
        self.process_entities()

    def process_entities(self):
        for entity in self.entities:
            logging.info(f"Processing entity: {entity}")
            # check the file is in the raw data path, if not skip the entity
            folder_path = os.path.join(self.raw_data_path, entity)
            folder_contents = os.listdir(folder_path)
            if not folder_contents:
                logging.warning(f"The folder {folder_path} is empty skipping {entity}.")
                continue
            if not self._load_raw_data(entity):
                logging.warning(f"Skipping processing for entity: {entity} due to empty data.")
                continue
            self._load_existing_base_data(entity)
            self._combine_data(entity)
            if not self._resolve_duplicates(entity):
                logging.error(f"entity: {entity} failed duplicate resolution.")
                sys.exit(ec.DUPLICATE_RESOLUTION_ERROR)
            if not self._save_base_data(entity):
                logging.error(f"Skipping processing for entity: {entity} due to failed saving base data.")
                continue
            if not self._move_raw_to_processed(entity):
                logging.error(f"entity: {entity} has been processed, but we could not move the file out of the raw folder, please clear the raw folder for {entity}.")
                sys.exit(ec.MOVE_FILE_ERROR)
            logging.info(f"Successfully processed entity: {entity}")
    
    def _load_raw_data(self, entity):
        entity_path = os.path.join(self.raw_data_path, entity)
        self.data[entity] = []
        logging.debug(f"Loading data for entity: {entity} from path: {entity_path}")
        
        files = [f for f in os.listdir(entity_path) if f.endswith('.json')]
        
        if len(files) > 1:
            logging.error(f"""More than one file found in path: {entity_path}. Skipping processing for entity: {entity}.
recommended actions is to move the newest file(s) out, re-run main.py.
Then move the files back in one at a time oldest to newest and run again for each file""")
            return False
        
        if len(files) == 1:
            file_name = files[0]
            file_path = os.path.join(entity_path, file_name)
            logging.debug(f"Reading file: {file_path}")
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                logging.error(f"Failed to load data from file: {file_path}, error: {e}")
                return False
            
            if self._is_data_empty(entity, data, file_path):
                return False
            
            modified_data = self._add_ingestion_date(entity, data, file_name)

            self.data[entity].append(modified_data)
            logging.debug(f"Successfully loaded data from file: {file_path}")
        return True

    def _is_data_empty(self, entity, data, file_path):
        logging.debug(f"Checking if data is empty for entity: {entity}")
        if entity == "categories":
            has_categories = any(group.get("categories") for group in data.get("category_groups", []))
            if not has_categories:
                logging.warning(f"Received empty data for entity: {entity} in file: {file_path}, deleting file.")
                os.remove(file_path)
                return True
        else:
            if not data.get(entity, []):
                logging.warning(f"Received empty data for entity: {entity} in file: {file_path}, deleting file.")
                os.remove(file_path)
                return True
        logging.debug(f"Data is not empty for entity: {entity}")
        return False
    
    def _add_ingestion_date(self, entity, data, file_name):
        modified_data = []
        ingestion_date = datetime.strptime(file_name.split('.')[0], '%Y%m%d%H%M%S').date()
        
        logging.debug(f"Adding ingestion date to data for entity: {entity}")
        if entity == 'categories':
            for group in data.get('category_groups', []):
                for category in group.get('categories', []):
                    category['ingestion_date'] = ingestion_date
                    modified_data.append(category)
        else:
            for record in data.get(f'{entity}', []):
                if isinstance(record, dict):
                    record['ingestion_date'] = ingestion_date
                    modified_data.append(record)
                else:
                    modified_data.append({'record': record, 'ingestion_date': ingestion_date})
        logging.debug(f"Successfully added ingestion date to data for entity: {entity}")
        return modified_data

    def _load_existing_base_data(self, entity):
        base_path = os.path.join(self.base_data_path, f'{entity}.parquet')
        if os.path.exists(base_path):
            logging.debug(f"Loading existing base data for entity: {entity} from path: {base_path}")
            try:
                self.base_data[entity] = pl.read_parquet(base_path)
            except Exception as e:
                logging.error(f"Failed to load existing base data for entity: {entity}, error: {e}, Creating an empty DataFrame")
                self.base_data[entity] = pl.DataFrame()
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
        
        # Ensure the unique id column is preserved
        unique_id = self.primary_keys[entity]['unique_id']
        if unique_id not in new_data_df.columns:
            logging.error(f"Unique ID column '{unique_id}' not found in the combined data for entity: {entity}")
            exit(1)
        
        self.base_data[entity] = new_data_df
        logging.debug(f"Successfully combined data for entity: {entity}")

    def _resolve_duplicates(self, entity):
        logging.debug(f"Resolving duplicates for entity: {entity}")
        unique_id = self.primary_keys[entity]['unique_id']
        try:
            self.base_data[entity] = self.base_data[entity].sort(by='ingestion_date').unique(subset=unique_id, keep='first')
        except Exception as e:
            logging.error(f"Failed to resolve duplicates for entity: {entity}, error: {e}")
            return False
        logging.debug(f"Successfully resolved duplicates for entity: {entity}")
        return True

    def _save_base_data(self, entity):
        os.makedirs(self.base_data_path, exist_ok=True)
        file_path = os.path.join(self.base_data_path, f'{entity}.parquet')
        try:
            self.base_data[entity].write_parquet(file_path)
        except Exception as e:
            logging.error(f"Failed to save base data for entity: {entity}, error: {e}")
            return False
        logging.debug(f"Saved base data for entity: {entity} to path: {file_path}")
        return True
    
    def _move_raw_to_processed(self, entity):
        raw_entity_path = os.path.join(self.raw_data_path, entity)
        processed_path = os.path.join(self.processed_data_path, entity)
        
        os.makedirs(processed_path, exist_ok=True)
        
        try:
            files = [f for f in os.listdir(raw_entity_path) if f.endswith('.json')]
            if len(files) != 1:
                logging.error(f"Expected exactly one file in path: {raw_entity_path}, but found {len(files)}")
                return False
            
            file_name = files[0]
            raw_file_path = os.path.join(raw_entity_path, file_name)
            processed_file_path = os.path.join(processed_path, file_name)
            
            logging.debug(f"Moving file: {raw_file_path} to {processed_file_path}")
            
            os.rename(raw_file_path, processed_file_path)
            logging.debug(f"Moved file: {file_name} to processed")
        
        except FileNotFoundError as e:
            logging.error(f"File not found: {e}")
            return False
        except Exception as e:
            logging.error(f"Failed to move file for entity: {entity}, error: {e}")
            return False
        
        logging.debug(f"Moved processed file for entity: {entity} to path: {processed_path}")
        return True