
import unittest
import sys
import os
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from redstalk.main import valid_date
from redstalk.config_utils import load_config
from redstalk.llm_wrapper import get_llm_provider

class TestRedstalk(unittest.TestCase):
    
    def test_valid_date(self):
        # Valid date
        dt = valid_date("2023-01-01")
        self.assertEqual(dt.year, 2023)
        self.assertEqual(dt.month, 1)
        self.assertEqual(dt.day, 1)
        
        # Invalid date
        import argparse
        with self.assertRaises(argparse.ArgumentTypeError):
            valid_date("not-a-date")

    def test_load_config_defaults(self):
        # Test that config loads (assuming config.json or defaults)
        config = load_config()
        self.assertIn('default_output_dir', config)
        self.assertIn('default_model_name', config)

    def test_llm_wrapper_provider(self):
        # Test that factory returns correct classes
        gemini = get_llm_provider("gemini", "gemini-pro")
        self.assertEqual(gemini.name, "gemini-pro")
        
        openrouter = get_llm_provider("openrouter", "openai/gpt-4")
        self.assertTrue(openrouter.name.startswith("openrouter/"))

if __name__ == '__main__':
    unittest.main()
