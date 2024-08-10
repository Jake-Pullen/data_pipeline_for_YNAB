import os
import time
import json
import logging
import requests
import sys
import yaml
from typing import Dict, Any
import config.exit_codes as ec

class Ingest:


    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Ingest class with the provided configuration.
        """
        self.api_token = config['API_TOKEN']
        self.budget_id = config['BUDGET_ID']
        self.base_url = config['base_url']
        self.knowledge_file = config['knowledge_file']
        self.entities = config['entities']
        self.raw_data_path = config['raw_data_path']
        self.headers = {'Authorization': f'Bearer {self.api_token}'}
        self.knowledge_cache = self.load_knowledge_cache()
        self.MAX_RETRIES = config['REQUESTS_MAX_RETRIES']
        self.RETRY_DELAY = config['REQUESTS_RETRY_DELAY']
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
        directory = os.path.join(self.raw_data_path, entity)
        if not os.path.exists(directory):
            os.makedirs(directory)
        entity_file = f'{directory}/{current_time}.json'
        logging.info(f"Saving {entity} data to {entity_file}")
        try:
            with open(entity_file, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logging.error(f"Error saving {entity} data: {e}")


    def update_server_knowledge_cache(self, entity: str, server_knowledge: Any):
        """
        Update the server knowledge cache for a specific entity.
        """
        try:
            with open(self.knowledge_file, 'r') as f:
                knowledge_cache = json.load(f)
        except FileNotFoundError:
            logging.info(f"Knowledge file not found. Creating a new one at {self.knowledge_file}. This is normal for the first run.")
            os.makedirs(os.path.dirname(self.knowledge_file), exist_ok=True)
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
            if remaining_requests == 1:
                logging.error("Rate limit exceeded. ending requests here and moving on with what we have.")
                return True #returning True here to break out of any more ingestions
                
        else:
            logging.warning("X-Rate-Limit header is missing.")
    
    def handle_response(self, response) -> bool:
        if response.status_code == 400:
            logging.error("Bad request. The request could not be understood by the API due to malformed syntax or validation errors.")
            sys.exit(ec.BAD_REQUEST)
        elif response.status_code == 401:
            logging.error("Unauthorized. Please check your API token.")
            sys.exit(ec.UNAUTHORIZED_API_TOKEN)
        elif response.status_code == 403:
            logging.error("Forbidden. Access is denied.")
            sys.exit(ec.FORBIDDEN)
        elif response.status_code == 404:
            logging.error("Not found. The specified URI does not exist.")
            sys.exit(ec.NOT_FOUND)
        elif response.status_code == 409:
            logging.error("Conflict. The resource cannot be saved due to a conflict.")
            sys.exit(ec.CONFLICT)
        elif response.status_code == 429:
            logging.error("Too many requests. You have made too many requests in a short amount of time.")
            return True 
        elif response.status_code == 500:
            logging.error("Internal server error. The API experienced an unexpected error.")
            return True
        elif response.status_code == 503:
            logging.error("Service unavailable. The API is temporarily disabled or a request timeout occurred.")
            return True
        else:
            response.raise_for_status()
            return False

    def fetch_and_cache_entity_data(self):
        """
        Fetch and cache data for all entities.
        """
        for entity in self.entities:
            file_path = f'data/raw/{entity}'
            if os.path.exists(file_path) and os.listdir(file_path):
                logging.warning(f"Raw data exists for {entity} processing any raw data we already have.")
                break # break here instead of continue as we dont want to update our server knowledge cache and potentially miss data.

            last_knowledge = self.knowledge_cache.get(entity, 0)
            #logging.debug(f'Last Knowledge of {entity}: {last_knowledge}')
            logging.info(f'Fetching {entity} data since last knowledge: {last_knowledge}')
            url = f'{self.base_url}/{self.budget_id}/{entity}?last_knowledge_of_server={last_knowledge}'

            for attempt in range(self.MAX_RETRIES):
                try:
                    response = requests.get(url, headers=self.headers)
                    should_retry = self.handle_response(response)
                    if not should_retry:
                        break  # Exit the loop if the request is successful
                except requests.exceptions.RequestException as e:
                    logging.error(f"Error fetching {entity} data (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(self.RETRY_DELAY)  # Wait before retrying
                    else:
                        logging.error("Max retries reached. Exiting.")
                        sys.exit(ec.REQUESTS_ERROR)
            
            data = response.json()
            server_knowledge = data['data'].get('server_knowledge')
            logging.debug(f'{entity} new server knowledge: {server_knowledge}')
            
            if server_knowledge is not None and server_knowledge != last_knowledge:
                self.update_server_knowledge_cache(entity, server_knowledge)
                entity_data = data['data']
                entity_data.pop('server_knowledge', None)
                self.save_entity_data_to_raw(entity, entity_data)
            else:
                logging.info(f"No new data for {entity}. Skipping cache update.")
            
            if self.check_rate_limit(response):
                break # break out here and continue processing the data we have.
