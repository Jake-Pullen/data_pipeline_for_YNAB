class injest:
    def __init__(self):

    
    def update_entity_data_cache(entity, data):
        current_time = time.strftime('%Y%m%d%H%M%S')
        directory = f'data/{entity}' # Directory name is the entity's name
        if not os.path.exists(directory):
            os.makedirs(directory)  # Create the directory if it does not exist
        entity_file = f'{directory}/{current_time}.json'  # Separate file for each entity's data
        with open(entity_file, 'w') as f:
            json.dump(data, f, indent=4)

    def update_server_knowledge_cache(entity, server_knowledge):
        knowledge_file = 'server_knowledge_cache.json'
        try:
            with open(knowledge_file, 'r') as f:
                knowledge_cache = json.load(f)
        except FileNotFoundError:
            knowledge_cache = {}
        
        knowledge_cache[entity] = server_knowledge
        
        with open(knowledge_file, 'w') as f:
            json.dump(knowledge_cache, f, indent=4)

    def check_rate_limit(response):
        rate_limit_header = response.headers.get('X-Rate-Limit')
        if rate_limit_header:
            requests_made, limit = map(int, rate_limit_header.split('/'))
            remaining_requests = limit - requests_made
            logging.info(f"Rate Limit: {remaining_requests}/{limit} requests remaining.")
            if remaining_requests < 20:  # Arbitrary low number to start handling the limit
                logging.warning("Approaching rate limit. Consider pausing further requests.")
                # Implement pause or delay logic here if necessary
        else:
            logging.warning("X-Rate-Limit header is missing.")

    def fetch_and_cache_entity_data(budget_id):
        entities = ['accounts', 'categories', 'months', 'payees', 'transactions', 'scheduled_transactions']
        base_url = 'https://api.ynab.com/v1/budgets'
        knowledge_file = 'server_knowledge_cache.json'
        
        # Load existing server knowledge cache
        try:
            with open(knowledge_file, 'r') as f:
                knowledge_cache = json.load(f)
        except FileNotFoundError:
            knowledge_cache = {}
        
        for entity in entities:
            last_knowledge = knowledge_cache.get(entity, 0)
            logging.debug(f'Last Knowledge of {entity.capitalize()}: {last_knowledge}')
            url = f'{base_url}/{budget_id}/{entity}'
            if last_knowledge:
                logging.info(f'Fetching {entity} data since last knowledge: {last_knowledge}')
                url = url + f'?last_knowledge_of_server={last_knowledge}'
            
            response = requests.get(url, headers=headers)
            check_rate_limit(response)  # Check and handle rate limit
            
            if response.status_code == 429:  # HTTP 429 Too Many Requests
                logging.error("Rate limit exceeded. Pausing until the limit is reset.")
                # Implement pause until the limit reset logic here
                break
            
            data = response.json()
            
            server_knowledge = data['data'].get('server_knowledge')
            logging.debug(f'{entity.capitalize()} Server Knowledge: {server_knowledge}')
            
            # Check if there is new server knowledge
            if server_knowledge is not None and server_knowledge != last_knowledge:
                # Update server knowledge cache
                update_server_knowledge_cache(entity, server_knowledge)
                
                # Update entity data cache without server knowledge
                entity_data = data['data']
                entity_data.pop('server_knowledge', None)  # Remove server knowledge if exists
                update_entity_data_cache(entity, entity_data)
            else:
                logging.info(f"No new data for {entity}. Skipping cache update.")
