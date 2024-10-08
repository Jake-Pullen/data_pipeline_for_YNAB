import unittest
from unittest.mock import patch, mock_open
import yaml

import config.exit_codes as ec

from main import load_config

class TestLoadConfig(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data="key: value")
    @patch('yaml.safe_load', return_value={"key": "value"})
    def test_load_config_success(self, mock_safe_load, mock_open):
        config = load_config()
        self.assertEqual(config, {"key": "value"})
        mock_open.assert_called_once_with('config/config.yaml', 'r')
        mock_safe_load.assert_called_once()

    @patch('builtins.open', side_effect=FileNotFoundError)
    def test_load_config_file_not_found(self, mock_open):
        with self.assertLogs(level='ERROR') as log:
            with self.assertRaises(SystemExit) as cm:
                load_config()
            self.assertEqual(cm.exception.code, ec.MISSING_CONFIG_FILE)
            self.assertIn('config.yaml file not found', log.output[0])

    @patch('builtins.open', new_callable=mock_open, read_data="invalid_yaml")
    @patch('yaml.safe_load', side_effect=yaml.YAMLError("Error"))
    def test_load_config_yaml_error(self, mock_safe_load, mock_open):
        with self.assertLogs(level='ERROR') as log:
            with self.assertRaises(SystemExit) as cm:
                load_config()
            self.assertEqual(cm.exception.code, ec.CORRUPTED_CONFIG_FILE)
            self.assertIn('Error loading config.yaml: Error', log.output[0])

if __name__ == "__main__":
    unittest.main()