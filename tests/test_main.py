import pytest
from unittest.mock import patch, mock_open, MagicMock
import yaml
import logging
import atexit
import sys

from main import set_up_logging, load_config
import config.exit_codes as ec

# Test for set_up_logging function
def test_set_up_logging_success():
    with patch('builtins.open', mock_open(read_data="handlers:\n  queue_handler:\n    class: logging.handlers.QueueHandler")), \
         patch('yaml.safe_load', return_value={"handlers": {"queue_handler": {"class": "logging.handlers.QueueHandler"}}}), \
         patch('logging.config.dictConfig') as mock_dict_config, \
         patch('logging.getHandlerByName', return_value=MagicMock(listener=MagicMock(start=MagicMock(), stop=MagicMock()))), \
         patch('atexit.register') as mock_atexit_register:
        
        set_up_logging()
        
        mock_dict_config.assert_called_once_with({"handlers": {"queue_handler": {"class": "logging.handlers.QueueHandler"}}})
        mock_atexit_register.assert_called_once()

def test_set_up_logging_yaml_error():
    with patch('builtins.open', mock_open(read_data="invalid_yaml")), \
         patch('yaml.safe_load', side_effect=yaml.YAMLError("Error")), \
         patch('logging.basicConfig') as mock_basic_config:
        
        set_up_logging()
        
        mock_basic_config.assert_called_once_with(level=logging.INFO)

def test_set_up_logging_no_queue_handler():
    with patch('builtins.open', mock_open(read_data="handlers:\n  queue_handler:\n    class: logging.handlers.QueueHandler")), \
         patch('yaml.safe_load', return_value={"handlers": {"queue_handler": {"class": "logging.handlers.QueueHandler"}}}), \
         patch('logging.config.dictConfig') as mock_dict_config, \
         patch('logging.getHandlerByName', return_value=None):
        
        set_up_logging()
        
        mock_dict_config.assert_called_once_with({"handlers": {"queue_handler": {"class": "logging.handlers.QueueHandler"}}})

# Test for load_config function
def test_load_config_success():
    with patch('builtins.open', mock_open(read_data="key: value")), \
         patch('yaml.safe_load', return_value={"key": "value"}):
        
        config = load_config()
        
        assert config == {"key": "value"}

def test_load_config_file_not_found():
    with patch('builtins.open', side_effect=FileNotFoundError), \
         patch('logging.error') as mock_logging_error, \
         patch('sys.exit') as mock_sys_exit:
        
        load_config()
        
        mock_logging_error.assert_called_once_with('config.yaml file not found')
        mock_sys_exit.assert_called_once_with(ec.MISSING_CONFIG_FILE)

def test_load_config_yaml_error():
    with patch('builtins.open', mock_open(read_data="invalid_yaml")), \
         patch('yaml.safe_load', side_effect=yaml.YAMLError("Error")), \
         patch('logging.error') as mock_logging_error, \
         patch('sys.exit') as mock_sys_exit:
        
        load_config()
        
        mock_logging_error.assert_called_once()
        mock_sys_exit.assert_called_once_with(ec.CORRUPTED_CONFIG_FILE)