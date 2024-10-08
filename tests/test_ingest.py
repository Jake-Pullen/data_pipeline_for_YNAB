import pytest
from unittest.mock import patch, mock_open, MagicMock
import json
import os
from typing import Dict, Any
import logging

from pipeline.ingest import Ingest  # Assuming the class is named Ingest

# Mock configuration for initializing the Ingest class
mock_config = {
    'API_TOKEN': 'test_token',
    'BUDGET_ID': 'test_budget_id',
    'base_url': 'http://test_base_url',
    'knowledge_file': 'test_knowledge_file.json',
    'entities': ['entity1', 'entity2'],
    'raw_data_path': 'test_raw_data_path',
    'REQUESTS_MAX_RETRIES': 3,
    'REQUESTS_RETRY_DELAY': 1
}

# Test for load_knowledge_cache method
def test_load_knowledge_cache_file_exists():
    mock_data = {"key": "value"}
    with patch('os.path.exists', return_value=True), \
         patch('builtins.open', mock_open(read_data=json.dumps(mock_data))) as mock_file:
        
        ingest_instance = Ingest(mock_config)
        result = ingest_instance.load_knowledge_cache()
        
        mock_file.assert_called_once_with('test_knowledge_file.json', 'r')
        assert result == mock_data

def test_load_knowledge_cache_file_not_exists():
    with patch('os.path.exists', return_value=False):
        
        ingest_instance = Ingest(mock_config)
        result = ingest_instance.load_knowledge_cache()
        
        assert result == {}

# # Test for save_entity_data_to_raw method
# def test_save_entity_data_to_raw_success():
#     entity = 'entity1'
#     data = {"key": "value"}
#     current_time = '20230101123000'
#     directory = os.path.join(mock_config['raw_data_path'], entity)
#     entity_file = f'{directory}/{current_time}.json'

#     with patch('os.path.exists', return_value=False), \
#          patch('os.makedirs') as mock_makedirs, \
#          patch('builtins.open', mock_open()) as mock_file, \
#          patch('time.strftime', return_value=current_time), \
#          patch('logging.info') as mock_logging_info:
        
#         ingest_instance = Ingest(mock_config)
#         ingest_instance.save_entity_data_to_raw(entity, data)
        
#         mock_makedirs.assert_called_once_with(directory)
#         mock_file.assert_called_once_with(entity_file, 'w')
#         mock_file().write.assert_called_once_with(json.dumps(data, indent=4))
#         mock_logging_info.assert_called_once_with(f"Saving {entity} data to {entity_file}")

# def test_save_entity_data_to_raw_existing_directory():
#     entity = 'entity1'
#     data = {"key": "value"}
#     current_time = '20230101123000'
#     directory = os.path.join(mock_config['raw_data_path'], entity)
#     entity_file = f'{directory}/{current_time}.json'

#     with patch('os.path.exists', return_value=True), \
#          patch('os.makedirs') as mock_makedirs, \
#          patch('builtins.open', mock_open()) as mock_file, \
#          patch('time.strftime', return_value=current_time), \
#          patch('logging.info') as mock_logging_info:
        
#         ingest_instance = Ingest(mock_config)
#         ingest_instance.save_entity_data_to_raw(entity, data)
        
#         mock_makedirs.assert_not_called()
#         mock_file.assert_called_once_with(entity_file, 'w')
#         mock_file().write.assert_called_once_with(json.dumps(data, indent=4))
#         mock_logging_info.assert_called_once_with(f"Saving {entity} data to {entity_file}")

# def test_save_entity_data_to_raw_error():
#     entity = 'entity1'
#     data = {"key": "value"}
#     current_time = '20230101123000'
#     directory = os.path.join(mock_config['raw_data_path'], entity)
#     entity_file = f'{directory}/{current_time}.json'

#     with patch('os.path.exists', return_value=True), \
#          patch('builtins.open', mock_open()) as mock_file, \
#          patch('time.strftime', return_value=current_time), \
#          patch('logging.info') as mock_logging_info, \
#          patch('logging.error') as mock_logging_error:
        
#         mock_file.side_effect = Exception("Test error")
        
#         ingest_instance = Ingest(mock_config)
#         ingest_instance.save_entity_data_to_raw(entity, data)
        
#         mock_file.assert_called_once_with(entity_file, 'w')
#         mock_logging_info.assert_called_once_with(f"Saving {entity} data to {entity_file}")
#         mock_logging_error.assert_called_once_with(f"Error saving {entity} data: Test error")


if __name__ == "__main__":
    pytest.main()