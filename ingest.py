import os
import time
import json
import logging
import requests
from typing import Dict, Any

class Ingest:
    def __init__(self, ingest_info: Dict[str, Any]):
        """
        Initialize the Ingest class with the provided configuration.
        """
        self.api_token = ingest_info['API_TOKEN']
        self.budget_id = ingest_info['BUDGET_ID']
        self.base_url = ingest_info['base_url']
        self.knowledge_file = ingest_info['knowledge_file']
        self.entities = ingest_info['entities']
        self.headers = {'Authorization': f'Bearer {self.api_token}'}
        self.knowledge_cache = self.load_knowledge_cache()
        self.fetch_and_cache_entity_data()

    def load_knowledge_cache(self) -> Dict[str, Any]:
        """
        Load the knowledge cache from the file if it exists.
        """
        if os.path.exists(self.knowledge_file):
            with open(self.knowledge_file, 'r') as f:
                return json.load(f)
        return {}

    def save_entity_data_to_raw(self, entity: str, data: Dict[str, Any]):
        """
        Save the data for a specific entity to a new cache file.
        """
        current_time = time.strftime('%Y%m%d%H%M%S')
        directory = f'data/raw/{entity}'
        if not os.path.exists(directory):
            os.makedirs(directory)
        entity_file = f'{directory}/{current_time}.json'
        with open(entity_file, 'w') as f:
            json.dump(data, f, indent=4)

    def update_server_knowledge_cache(self, entity: str, server_knowledge: Any):
        """
        Update the server knowledge cache for a specific entity.
        """
        try:
            with open(self.knowledge_file, 'r') as f:
                knowledge_cache = json.load(f)
        except FileNotFoundError:
            knowledge_cache = {}
        
        knowledge_cache[entity] = server_knowledge
        
        with open(self.knowledge_file, 'w') as f:
            json.dump(knowledge_cache, f, indent=4)

    def check_rate_limit(self, response: requests.Response):
        """
        Check and handle the rate limit based on the response headers.
        """
        rate_limit_header = response.headers.get('X-Rate-Limit')
        if rate_limit_header:
            requests_made, limit = map(int, rate_limit_header.split('/'))
            remaining_requests = limit - requests_made
            logging.info(f"Rate Limit: {remaining_requests}/{limit} requests remaining.")
            if remaining_requests < 20:
                logging.warning("Approaching rate limit. Consider pausing further requests.")
                # Implement pause or delay logic here if necessary
        else:
            logging.warning("X-Rate-Limit header is missing.")

    def fetch_and_cache_entity_data(self):
        """
        Fetch and cache data for all entities.
        """
        for entity in self.entities:
            last_knowledge = self.knowledge_cache.get(entity, 0)
            logging.debug(f'Last Knowledge of {entity.capitalize()}: {last_knowledge}')
            url = f'{self.base_url}/{self.budget_id}/{entity}'
            if last_knowledge:
                logging.info(f'Fetching {entity} data since last knowledge: {last_knowledge}')
                url = url + f'?last_knowledge_of_server={last_knowledge}'
            
            response = requests.get(url, headers=self.headers)
            self.check_rate_limit(response)
            
            if response.status_code == 429:
                logging.error("Rate limit exceeded. Pausing until the limit is reset.")
                # Implement pause until the limit reset logic here
                break
            
            data = response.json()
            server_knowledge = data['data'].get('server_knowledge')
            logging.debug(f'{entity.capitalize()} Server Knowledge: {server_knowledge}')
            
            if server_knowledge is not None and server_knowledge != last_knowledge:
                self.update_server_knowledge_cache(entity, server_knowledge)
                entity_data = data['data']
                entity_data.pop('server_knowledge', None)
                self.save_entity_data_to_raw(entity, entity_data)
            else:
                logging.info(f"No new data for {entity}. Skipping cache update.")