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
    'knowledge_file': 'data/test_knowledge_file.json',
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
        
        mock_file.assert_called_once_with(mock_config['knowledge_file'], 'r')
        assert result == mock_data

def test_load_knowledge_cache_file_not_exists():
    with patch('os.path.exists', return_value=False):
        
        ingest_instance = Ingest(mock_config)
        result = ingest_instance.load_knowledge_cache()
        
        assert result == {}

# Test for save_entity_data_to_raw method
def test_save_entity_data_to_raw_success():
    entity = 'entity1'
    data = {"key": "value"}
    current_time = '20230101123000'
    directory = os.path.join(mock_config['raw_data_path'], entity)
    entity_file = f'{directory}/{current_time}.json'

    with patch('os.path.exists', return_value=False), \
         patch('os.makedirs') as mock_makedirs, \
         patch('builtins.open', mock_open()) as mock_file, \
         patch('time.strftime', return_value=current_time), \
         patch('logging.info') as mock_logging_info:
        
        ingest_instance = Ingest(mock_config)
        ingest_instance.save_entity_data_to_raw(entity, data)
        
        mock_makedirs.assert_called_once_with(directory)
        mock_file.assert_called_once_with(entity_file, 'w')
        
        # Get the file handle and check the written content
        handle = mock_file()
        handle.write.assert_called()
        written_content = ''.join(call.args[0] for call in handle.write.call_args_list)
        assert written_content == json.dumps(data, indent=4)
        
        mock_logging_info.assert_called_once_with(f"Saving {entity} data to {entity_file}")

def test_save_entity_data_to_raw_existing_directory():
    entity = 'entity1'
    data = {"key": "value"}
    current_time = '20230101123000'
    directory = os.path.join(mock_config['raw_data_path'], entity)
    entity_file = f'{directory}/{current_time}.json'

    with patch('os.path.exists', return_value=True), \
         patch('os.makedirs') as mock_makedirs, \
         patch('builtins.open', mock_open()) as mock_file, \
         patch('time.strftime', return_value=current_time), \
         patch('logging.info') as mock_logging_info:
        
        ingest_instance = Ingest(mock_config)
        ingest_instance.save_entity_data_to_raw(entity, data)
        
        mock_makedirs.assert_not_called()
        mock_file.assert_called_once_with(entity_file, 'w')
        
        # Get the file handle and check the written content
        handle = mock_file()
        handle.write.assert_called()
        written_content = ''.join(call.args[0] for call in handle.write.call_args_list)
        assert written_content == json.dumps(data, indent=4)
        
        mock_logging_info.assert_called_once_with(f"Saving {entity} data to {entity_file}")

def test_save_entity_data_to_raw_error():
    entity = 'entity1'
    data = {"key": "value"}
    current_time = '20230101123000'
    directory = os.path.join(mock_config['raw_data_path'], entity)
    entity_file = f'{directory}/{current_time}.json'

    with patch('os.path.exists', return_value=True), \
         patch('builtins.open', mock_open()) as mock_file, \
         patch('time.strftime', return_value=current_time), \
         patch('logging.info') as mock_logging_info, \
         patch('logging.error') as mock_logging_error:

        mock_file.side_effect = Exception("Test error")

        ingest_instance = Ingest(mock_config)

        with pytest.raises(Exception, match="Test error"):
            ingest_instance.save_entity_data_to_raw(entity, data)

        mock_logging_error.assert_called_once_with(f"Failed to save data for {entity} to {entity_file}")

def test_update_server_knowledge_cache_file_exists():
    entity = 'entity1'
    server_knowledge = {"key": "value"}
    existing_cache = {"entity2": {"key": "old_value"}}
    updated_cache = {"entity2": {"key": "old_value"}, "entity1": {"key": "value"}}

    with patch('builtins.open', mock_open(read_data=json.dumps(existing_cache))) as mock_file, \
         patch('os.path.exists', return_value=True), \
         patch('logging.error') as mock_logging_error:

        ingest_instance = Ingest(mock_config)
        ingest_instance.update_server_knowledge_cache(entity, server_knowledge)

        mock_file.assert_called_with(mock_config['knowledge_file'], 'w')
        handle = mock_file()
        handle.write.assert_called()
        written_content = ''.join(call.args[0] for call in handle.write.call_args_list)
        assert json.loads(written_content) == updated_cache
        mock_logging_error.assert_not_called()

def test_update_server_knowledge_cache_file_not_exists():
    entity = 'entity1'
    server_knowledge = {"key": "value"}
    updated_cache = {"entity1": {"key": "value"}}

    with patch('builtins.open', mock_open()) as mock_file, \
         patch('os.path.exists', return_value=False), \
         patch('os.makedirs') as mock_makedirs, \
         patch('logging.info') as mock_logging_info, \
         patch('logging.error') as mock_logging_error:

        # Ensure the side_effect list has enough elements to cover all calls to open
        mock_file.side_effect = [FileNotFoundError(), mock_open().return_value]

        ingest_instance = Ingest(mock_config)
        
        with pytest.raises(FileNotFoundError):
            ingest_instance.update_server_knowledge_cache(entity, server_knowledge)

        mock_makedirs.assert_called_once_with(os.path.dirname(mock_config['knowledge_file']), exist_ok=True)
        mock_file.assert_called_with(mock_config['knowledge_file'], 'w')
        mock_logging_error.assert_called_once_with(f"Failed to update knowledge cache for {entity} in {mock_config['knowledge_file']}")

def test_update_server_knowledge_cache_write_error():
    entity = 'entity1'
    server_knowledge = {"key": "value"}

    with patch('builtins.open', mock_open()) as mock_file, \
         patch('logging.error') as mock_logging_error:

        mock_file.side_effect = Exception("Test error")

        ingest_instance = Ingest(mock_config)

        with pytest.raises(Exception, match="Test error"):
            ingest_instance.update_server_knowledge_cache(entity, server_knowledge)

        mock_logging_error.assert_called_once_with(f"Failed to update knowledge cache for {entity} in {mock_config['knowledge_file']}")

if __name__ == "__main__":
    pytest.main()