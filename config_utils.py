# config_utils.py
import os
import json
import logging

CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "default_output_dir": "data",
    "default_prompt_file": "prompt.txt",
    "default_prompt_dir": "prompts",
    "default_chunk_size": 500000,
    "api_key": None,
    "default_model_name": "gemini-1.5-flash", # Updated default
    "monitor_interval_seconds": 180,
    "user_agent": "Python:RedditProfilerScript:v1.5 (by /u/YourRedditUsername)", # Version bump
}

def load_config():
    """Loads configuration from JSON file, using defaults as fallback."""
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                user_config = json.load(f)
                config.update(user_config)
                # Validation for specific types
                if not isinstance(config.get("default_chunk_size"), int):
                    logging.warning(f"Invalid 'default_chunk_size' type in {CONFIG_FILE}, using default: {DEFAULT_CONFIG['default_chunk_size']}")
                    config["default_chunk_size"] = DEFAULT_CONFIG["default_chunk_size"]
                if not isinstance(config.get("monitor_interval_seconds"), int) or config.get("monitor_interval_seconds") < 30:
                    logging.warning(f"Invalid or too low 'monitor_interval_seconds' in {CONFIG_FILE}, using default: {DEFAULT_CONFIG['monitor_interval_seconds']}")
                    config["monitor_interval_seconds"] = DEFAULT_CONFIG["monitor_interval_seconds"]
                if not isinstance(config.get("default_model_name"), str):
                    logging.warning(f"Invalid 'default_model_name' type in {CONFIG_FILE}, using default: {DEFAULT_CONFIG['default_model_name']}")
                    config["default_model_name"] = DEFAULT_CONFIG["default_model_name"]
                if not isinstance(config.get("default_prompt_dir"), str):
                     logging.warning(f"Invalid 'default_prompt_dir' type in {CONFIG_FILE}, using default: {DEFAULT_CONFIG['default_prompt_dir']}")
                     config["default_prompt_dir"] = DEFAULT_CONFIG['default_prompt_dir']
                if not isinstance(config.get("user_agent"), str) or not config.get("user_agent"):
                     logging.warning(f"Invalid 'user_agent' in {CONFIG_FILE}, using default.")
                     config["user_agent"] = DEFAULT_CONFIG['user_agent']
                # Ensure api_key exists, even if None
                if "api_key" not in config:
                    config["api_key"] = None
                return config
        except json.JSONDecodeError:
            logging.error(f"Error decoding {CONFIG_FILE}. Using default configuration values.")
            return DEFAULT_CONFIG.copy()
        except Exception as e:
            logging.error(f"Error loading {CONFIG_FILE}: {e}. Using default configuration values.")
            return DEFAULT_CONFIG.copy()
    else:
        logging.info(f"{CONFIG_FILE} not found. Using default configuration.")
        return config

def save_config(config_data):
    """Saves the configuration data to the JSON file."""
    # Ensure all default keys exist before saving
    full_config = DEFAULT_CONFIG.copy()
    full_config.update(config_data)
    try:
        os.makedirs(os.path.dirname(CONFIG_FILE) or '.', exist_ok=True) # Ensure dir exists
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(full_config, f, indent=4)
        logging.info(f"Configuration saved to {CONFIG_FILE}")
        return True
    except IOError as e:
        logging.error(f"Error saving configuration to {CONFIG_FILE}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred saving config: {e}")
        return False