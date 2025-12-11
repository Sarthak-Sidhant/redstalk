# config_utils.py
"""
This module handles loading and saving the application's configuration.
It reads settings from a JSON file (defaulting to config.json) and provides
default values if the file is missing or if specific settings are not found
or are invalid. It also includes a function to save the current configuration
to the file.
"""

# Import necessary libraries
import os # For checking file existence and manipulating paths
import json # For reading and writing configuration data in JSON format
import logging # For logging information, warnings, and errors related to config loading/saving

# Define the name of the configuration file
CONFIG_FILE = "config.json"

# Define ANSI codes locally for coloring log/print output related to config
# These make messages about config file paths stand out in the terminal.
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"

# Define the default configuration settings.
# These values are used if no config file is found or if specific keys
# are missing or invalid in the file.
DEFAULT_CONFIG = {
    "default_output_dir": "data", # Default directory for saving scraped/analyzed data
    "default_prompt_file": "prompt.txt", # Default filename for the system prompt if not specified
    "default_prompt_dir": "prompts", # Default directory to store system prompt files
    "default_chunk_size": 1000000, # Default maximum token size for data chunks sent to the AI model
                                  # Increased to a large value as the AI models handle much larger contexts now.
                                  # This should prevent chunking for most typical analysis tasks unless data is huge.
    "api_key": None, # Placeholder for the Google Generative AI API key.
                     # It's recommended to use environment variables or other secure methods,
                     # but this provides a file-based option. Set to None initially.
    "openrouter_api_key": None, # Placeholder for OpenRouter API Key.
    "default_model_name": "gemini-1.5-flash", # Default name of the AI model to use (using a faster, cheaper model)
    "monitor_interval_seconds": 180, # Default interval (in seconds) between checks when monitoring is enabled
    "user_agent": "Python:RedditProfilerScript:v1.9 (by /u/YourRedditUsername)", # Default User-Agent string for Reddit API requests.
                                                                                # It's good practice to identify your script and provide contact info. Version bumped.
}

def load_config():
    """
    Loads configuration settings from the specified JSON file (CONFIG_FILE).
    It first loads the default settings and then updates them with values
    found in the config file. Includes basic validation for key settings.

    If the config file does not exist or is invalid, it logs a warning/error
    and returns the default configuration.

    Returns:
        A dictionary containing the merged configuration settings.
    """
    # Start with a copy of the default configuration so we have fallback values.
    config = DEFAULT_CONFIG.copy()
    config_path = CONFIG_FILE # Get the path to the config file

    # Check if the config file exists.
    if os.path.exists(config_path):
        logging.debug(f"Attempting to load config from: {CYAN}{config_path}{RESET}")
        try:
            # Open and read the JSON config file.
            with open(config_path, "r", encoding="utf-8") as f:
                user_config = json.load(f) # Load the JSON data

                # Validate that the loaded data is a dictionary before attempting to update.
                if isinstance(user_config, dict):
                    config.update(user_config) # Update defaults with values from the file
                    logging.info(f"✅ Configuration loaded successfully from {CYAN}{config_path}{RESET}")
                else:
                    # If the file contents are not a dictionary, log a warning and return only defaults.
                    logging.warning(f"   ⚠️ Config file {CYAN}{config_path}{RESET} does not contain a valid JSON object. Using defaults.")
                    return DEFAULT_CONFIG.copy() # Return a fresh copy of defaults

                # --- Basic Validation of Loaded Settings ---
                # Validate default_chunk_size: Must be an integer and positive.
                if not isinstance(config.get("default_chunk_size"), int) or config["default_chunk_size"] <= 0:
                    logging.warning(f"   ⚠️ Invalid 'default_chunk_size' value in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['default_chunk_size']}")
                    config["default_chunk_size"] = DEFAULT_CONFIG["default_chunk_size"] # Reset to default if invalid

                # Validate monitor_interval_seconds: Must be an integer and at least 30 seconds.
                if not isinstance(config.get("monitor_interval_seconds"), int) or config.get("monitor_interval_seconds") < 30:
                    logging.warning(f"   ⚠️ Invalid or too low 'monitor_interval_seconds' in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['monitor_interval_seconds']}")
                    config["monitor_interval_seconds"] = DEFAULT_CONFIG["monitor_interval_seconds"] # Reset to default if invalid

                # Validate default_model_name: Must be a non-empty string.
                if not isinstance(config.get("default_model_name"), str) or not config["default_model_name"]:
                     logging.warning(f"   ⚠️ Invalid 'default_model_name' type in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['default_model_name']}")
                     config["default_model_name"] = DEFAULT_CONFIG['default_model_name'] # Reset to default if invalid

                # Validate default_prompt_dir: Must be a non-empty string.
                if not isinstance(config.get("default_prompt_dir"), str) or not config["default_prompt_dir"]:
                      logging.warning(f"   ⚠️ Invalid 'default_prompt_dir' type in {CYAN}{config_path}{RESET}, using default: {DEFAULT_CONFIG['default_prompt_dir']}")
                      config["default_prompt_dir"] = DEFAULT_CONFIG['default_prompt_dir'] # Reset to default if invalid

                # Validate user_agent: Must be a non-empty string.
                if not isinstance(config.get("user_agent"), str) or not config.get("user_agent"):
                      logging.warning(f"   ⚠️ Invalid 'user_agent' in {CYAN}{config_path}{RESET}, using default.")
                      config["user_agent"] = DEFAULT_CONFIG['user_agent'] # Reset to default if invalid

                # Ensure 'api_key' key exists in the final config, even if its value is None.
                # This prevents potential KeyError later if code checks for config["api_key"].
                if "api_key" not in config:
                    config["api_key"] = None
                # Check for the placeholder value and warn the user.
                elif config["api_key"] == "use_ur_own_keys_babe":
                     logging.warning(f"   ⚠️ API key in {CYAN}{config_path}{RESET} is set to the placeholder value. Please replace it with your actual API key or use an environment variable.")
                     # Optionally, you could set config["api_key"] = None here if the placeholder should explicitly mean "no key".

                return config # Return the loaded and validated configuration

        except json.JSONDecodeError:
            # Handle cases where the file is not valid JSON.
            logging.error(f"❌ Error decoding {CYAN}{config_path}{RESET}. Using default configuration values.")
            return DEFAULT_CONFIG.copy() # Return a fresh copy of defaults on JSON error
        except Exception as e:
            # Handle any other unexpected errors during file loading.
            logging.error(f"❌ Error loading {CYAN}{config_path}{RESET}: {e}. Using default configuration values.", exc_info=True) # Include traceback
            return DEFAULT_CONFIG.copy() # Return a fresh copy of defaults on other errors
    else:
        # If the config file does not exist, log this and return defaults.
        logging.info(f"ℹ️ Config file {CYAN}{config_path}{RESET} not found. Using default configuration.")
        # Optional: You could add a call here to save the default config to the file
        # on the first run, prompting the user to edit it. E.g., save_config(DEFAULT_CONFIG.copy()).
        return DEFAULT_CONFIG.copy() # Return a fresh copy of defaults when file is missing

def save_config(config_data):
    """
    Saves the provided configuration data dictionary to the JSON file (CONFIG_FILE).

    Args:
        config_data: A dictionary containing the configuration settings to save.

    Returns:
        True if the save was successful, False otherwise.
    """
    # Create a complete dictionary for saving by starting with defaults
    # and updating with the provided data. This ensures all keys (even if
    # not changed) are present in the saved file in a consistent structure.
    full_config = DEFAULT_CONFIG.copy()
    full_config.update(config_data)
    config_path = CONFIG_FILE # Get the path to the config file

    try:
        # Get the directory part of the config path.
        config_dir = os.path.dirname(config_path)
        # If a directory is specified (not just a filename in the current dir),
        # ensure that directory exists.
        if config_dir and not os.path.exists(config_dir):
             os.makedirs(config_dir) # Create the directory if it doesn't exist
             logging.debug(f"Created config directory: {config_dir}") # Log directory creation
        elif not config_dir:
             # If config_dir is empty, it means the file is expected in the current directory.
             config_dir = '.' # Set dir to '.' for the writability check

        # Basic check if the directory is writable before attempting to open/write the file.
        if not os.access(config_dir, os.W_OK):
             raise IOError(f"Directory '{config_dir}' is not writable. Cannot save config.")

        # Open the file in write mode and save the JSON data.
        # indent=4 makes the JSON output human-readable.
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(full_config, f, indent=4)
        logging.info(f"💾 Configuration saved to {CYAN}{config_path}{RESET}")
        return True # Indicate success
    except IOError as e:
        # Handle specific IO errors (e.g., permission denied, disk full).
        logging.error(f"❌ Error saving configuration to {CYAN}{config_path}{RESET}: {e}")
        return False # Indicate failure
    except Exception as e:
        # Handle any other unexpected errors during the save process.
        logging.error(f"❌ An unexpected error occurred saving config to {CYAN}{config_path}{RESET}: {e}", exc_info=True) # Include traceback
        return False # Indicate failure