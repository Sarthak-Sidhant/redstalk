import os
import json
import logging

# Import ANSI codes (optional, or define locally if needed)
# from redstalk import CYAN, RESET, BOLD # Or copy definitions
CONFIG_FILE = "config.json"
# Define colors locally if not importing
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"

DEFAULT_CONFIG = {
    "default_output_dir": "data",
    "default_prompt_file": "prompt.txt", # Keep original default
    "default_prompt_dir": "prompts",
    "default_chunk_size": 1000000, # Increased default chunk size
    "api_key": None, # Store API key here or use ENV/flag
    "default_model_name": "gemini-1.5-flash", # Updated default model
    "monitor_interval_seconds": 180,
    "user_agent": "Python:RedditProfilerScript:v1.7 (by /u/YourRedditUsername)", # Version bump
}

def load_config():
    """Loads configuration from JSON file, using defaults as fallback."""
    config = DEFAULT_CONFIG.copy()
    config_path = CONFIG_FILE # Use local var for path clarity
    if os.path.exists(config_path):
        logging.debug(f"Attempting to load config from: {CYAN}{config_path}{RESET}")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                user_config = json.load(f)
                # Validate user_config is a dict before updating
                if isinstance(user_config, dict):
                    config.update(user_config)
                    logging.info(f"✅ Configuration loaded successfully from {CYAN}{config_path}{RESET}")
                else:
                    logging.warning(f"   ⚠️ Config file {CYAN}{config_path}{RESET} does not contain a valid JSON object. Using defaults.")
                    return DEFAULT_CONFIG.copy()

                # Validation for specific types (add logging for validation changes)
                if not isinstance(config.get("default_chunk_size"), int) or config["default_chunk_size"] <= 0:
                    logging.warning(f"   ⚠️ Invalid 'default_chunk_size' value in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['default_chunk_size']}")
                    config["default_chunk_size"] = DEFAULT_CONFIG["default_chunk_size"]

                if not isinstance(config.get("monitor_interval_seconds"), int) or config.get("monitor_interval_seconds") < 30:
                    logging.warning(f"   ⚠️ Invalid or too low 'monitor_interval_seconds' in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['monitor_interval_seconds']}")
                    config["monitor_interval_seconds"] = DEFAULT_CONFIG["monitor_interval_seconds"]

                if not isinstance(config.get("default_model_name"), str) or not config["default_model_name"]:
                     logging.warning(f"   ⚠️ Invalid 'default_model_name' type in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['default_model_name']}")
                     config["default_model_name"] = DEFAULT_CONFIG['default_model_name']

                if not isinstance(config.get("default_prompt_dir"), str) or not config["default_prompt_dir"]:
                      logging.warning(f"   ⚠️ Invalid 'default_prompt_dir' type in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['default_prompt_dir']}")
                      config["default_prompt_dir"] = DEFAULT_CONFIG['default_prompt_dir']

                if not isinstance(config.get("user_agent"), str) or not config.get("user_agent"):
                      logging.warning(f"   ⚠️ Invalid 'user_agent' in {CYAN}{config_path}{RESET}, using default.")
                      config["user_agent"] = DEFAULT_CONFIG['user_agent']

                # Ensure api_key exists, even if None
                if "api_key" not in config:
                    config["api_key"] = None
                elif config["api_key"] == "use_ur_own_keys_babe":
                     logging.warning(f"   ⚠️ API key in {CYAN}{config_path}{RESET} is set to the placeholder value.")
                     # Optionally set to None here if placeholder should be ignored: config["api_key"] = None

                return config
        except json.JSONDecodeError:
            logging.error(f"❌ Error decoding {CYAN}{config_path}{RESET}. Using default configuration values.")
            return DEFAULT_CONFIG.copy()
        except Exception as e:
            logging.error(f"❌ Error loading {CYAN}{config_path}{RESET}: {e}. Using default configuration values.")
            return DEFAULT_CONFIG.copy()
    else:
        logging.info(f"ℹ️ Config file {CYAN}{config_path}{RESET} not found. Using default configuration.")
        # Optionally: Create a default config file on first run?
        # save_config(DEFAULT_CONFIG.copy())
        return DEFAULT_CONFIG.copy()

def save_config(config_data):
    """Saves the configuration data to the JSON file."""
    # Ensure all default keys exist before saving
    full_config = DEFAULT_CONFIG.copy()
    full_config.update(config_data)
    config_path = CONFIG_FILE
    try:
        # Ensure parent directory exists before trying to save
        config_dir = os.path.dirname(config_path)
        if config_dir and not os.path.exists(config_dir):
             os.makedirs(config_dir) # Create dir if it doesn't exist
        elif not config_dir: # Handle saving in current directory
             config_dir = '.'

        # Check if directory is writable (basic check)
        if not os.access(config_dir, os.W_OK):
             raise IOError(f"Directory '{config_dir}' is not writable.")

        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(full_config, f, indent=4)
        logging.info(f"💾 Configuration saved to {CYAN}{config_path}{RESET}")
        return True
    except IOError as e:
        logging.error(f"❌ Error saving configuration to {CYAN}{config_path}{RESET}: {e}")
        return False
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred saving config to {CYAN}{config_path}{RESET}: {e}")
        return False