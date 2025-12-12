#!/usr/bin/env python3
# redstalk.py
"""
Redstalk is a command-line tool for scraping Reddit user activity
(posts and comments), generating statistics reports, and performing
AI-driven character analysis based on the scraped data.

It supports filtering data by date range and subreddit, saving/loading
data incrementally, comparing statistics between two users, generating
AI system prompts interactively, and monitoring a user for new activity.
"""

# Import standard libraries
import os # For interacting with the operating system (paths, directories)
import argparse # For parsing command-line arguments
import logging # For handling logging throughout the application
import sys # For system-specific parameters and functions (like exiting)
from datetime import datetime, timezone # For handling dates, times, and timezones (specifically UTC)
import re # For using regular expressions (used here for date parsing in argparse)
import time # For time-related functions (used for timing AI summary)
import json # For saving statistics data in JSON format
from .llm_wrapper import get_llm_provider # Import the generic LLM provider factory


# --- Import Utilities ---
# Configuration loading and saving utilities
from .config_utils import load_config, save_config, DEFAULT_CONFIG # Configuration tools
from .ai_utils import count_tokens # AI token counting utility
from .reddit_utils import save_reddit_data, _fetch_user_about_data
from .data_utils import extract_csvs_from_json # Note: The import signature might look different from its definition, but the usage later reflects the actual signature.
from .monitoring import monitor_user

# Import statistics generation functions. Wrapped in a try/except
# because these might depend on external libraries (like pandas, vaderSentiment)
# that might not be installed if the user only wants scraping/AI features.
try:
    # Import functions for single user stats report and user comparison report.
    from .stats.single_report import generate_stats_report # Usage later reflects its signature
    from .stats.comparison import generate_comparison_report
    stats_available = True # Flag to indicate if stats module was imported successfully
except ImportError as e:
    # If importing stats fails, log an error and disable stats features.
    logging.error(f"❌ Failed to import from the 'stats' package. Statistics generation disabled. Error: {e}")
    stats_available = False
    # Define dummy functions for stats commands to prevent NameErrors if they are called
    # despite the stats_available flag check. These functions simply log an error.
    def generate_stats_report(*args, **kwargs):
        logging.error("generate_stats_report called, but stats package failed to import.")
        return False, None # Return signature matches the real function
    def generate_comparison_report(*args, **kwargs):
        logging.error("generate_comparison_report called, but stats package failed to import.")
        return False # Return signature matches the real function

# --- ANSI Color Codes & Logging Setup ---
# Define ANSI escape codes for coloring console output.
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; RED = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; BLUE = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"
# Map logging levels to ANSI colors for the colored formatter.
LOG_LEVEL_COLORS = { logging.DEBUG: DIM + BLUE, logging.INFO: BLUE, logging.WARNING: YELLOW, logging.ERROR: RED, logging.CRITICAL: BOLD + RED, }

class ColoredFormatter(logging.Formatter):
    """Custom logging formatter to add ANSI color codes to log output."""
    def __init__(self, fmt=None, datefmt=None, style='%'):
        # Initialize the base Formatter.
        super().__init__(fmt, datefmt, style)
    def format(self, record):
        # Format the log record using the base formatter first.
        log_message = super().format(record)
        # Get the color for the log level and format the level name with color.
        level_color = LOG_LEVEL_COLORS.get(record.levelno, WHITE);
        levelname_colored = f"{level_color}{record.levelname:<8}{RESET}"

        # Reconstruct the log message to insert colors.
        # The default format string usually starts with timestamp, levelname, filename/lineno.
        # Extract the prefix part to format it with DIM color.
        log_prefix = f"{record.asctime} - {record.levelname:<8} - {record.filename}:{record.lineno:<4d} - "
        # The message body is the part after the standard prefix.
        message_body = log_message[len(log_prefix):]
        # Combine the parts with the desired colors.
        formatted_message = f"{DIM}{record.asctime}{RESET} - {levelname_colored} - {DIM}{record.filename}:{record.lineno:<4d}{RESET} - {message_body}"

        # Add regex to highlight file paths within the message body.
        # This is a simple pattern; could be more complex if needed.
        formatted_message = re.sub(r'([\s\'"])(/[^/\s]+)+/([^/\s]+\.\w+)([\s\'"]|$)', rf'\1{CYAN}\2/\3{RESET}\4', formatted_message)
        return formatted_message # Return the colorized message

def setup_logging(log_level_str):
    """Sets up the root logger with a console handler and custom formatter."""
    # Convert the input string log level to a logging module constant. Default to INFO.
    log_level = getattr(logging, log_level_str.upper(), logging.INFO);
    root_logger = logging.getLogger(); # Get the root logger
    root_logger.setLevel(log_level) # Set the overall minimum level for the logger

    # Remove any existing handlers to prevent duplicate messages.
    for handler in root_logger.handlers[:]: root_logger.removeHandler(handler)

    # Create a console handler that outputs to standard output.
    ch = logging.StreamHandler(sys.stdout);
    ch.setLevel(log_level) # Set the minimum level for this handler

    # Create an instance of the custom colored formatter.
    formatter = ColoredFormatter(fmt='%(asctime)s - %(levelname)-8s - %(filename)s:%(lineno)-4d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    # Set the formatter for the handler.
    ch.setFormatter(formatter);
    # Add the handler to the root logger.
    root_logger.addHandler(ch)

    # For less verbose output at lower debug levels, silence chatty libraries like requests and urllib3.
    if log_level > logging.DEBUG:
        logging.getLogger("requests").setLevel(logging.WARNING);
        logging.getLogger("urllib3").setLevel(logging.WARNING);
        logging.getLogger("httpx").setLevel(logging.WARNING) # For OpenAI client
        # logging.getLogger("google.generativeai").setLevel(logging.WARNING) # Handled largely by wrapper/logging logic now
        logging.getLogger("vaderSentiment").setLevel(logging.WARNING) # Add vaderSentiment if used in stats

    logging.debug(f"Logging initialized at level: {log_level_str}")


def valid_date(s):
    """
    Custom argparse type validator for date arguments.
    Parses a date string in YYYY-MM-DD format and returns a timezone-aware datetime object (UTC).
    """
    try:
        # Parse the input string into a naive datetime object.
        dt_naive = datetime.strptime(s, "%Y-%m-%d")
        # Make the datetime object timezone-aware by assigning UTC timezone.
        return dt_naive.replace(tzinfo=timezone.utc)
    except ValueError:
        # If parsing fails, raise an argparse.ArgumentTypeError with a helpful message.
        msg = f"Not a valid date: '{s}'. Expected format YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg)

# --- RESTORED Helper Function for Processing Single User for Comparison ---
def process_single_user_for_stats(username, config, args):
    """
    Handles the full data processing pipeline (scrape, CSV conversion, about fetch,
    stats calculation) for a single user *without* applying date/subreddit filters
    during CSV conversion or stats calculation.

    This is specifically designed as a helper for the --compare-user command
    where the base stats for comparison should cover the entire available
    unfiltered dataset.

    Args:
        username (str): The Reddit username to process.
        config (dict): The application configuration.
        args (argparse.Namespace): The parsed command-line arguments.

    Returns:
        A dictionary containing the calculated statistics data for the user,
        or None if any step in the processing pipeline fails critically.
    """
    logging.info(f"--- {BOLD}Processing User for Comparison: /u/{username}{RESET} ---")
    # Construct the user-specific data directory path and ensure it exists.
    user_data_dir = os.path.join(args.output_dir, username);
    os.makedirs(user_data_dir, exist_ok=True)

    # Determine sorting preference for scraping from arguments.
    sort_descending = (args.sort_order == "descending")

    # Initialize variables to store results.
    stats_results_data = None # Will hold the dictionary of stats results
    user_about_data = None    # Will hold the user's 'about' data
    success = False           # Flag to track overall success of the steps

    try:
        # 1. Scrape/Update User Data (JSON)
        logging.info(f"   ⚙️ Running data fetch/update for /u/{BOLD}{username}{RESET}...")
        # Call save_reddit_data to fetch new data and merge/update the JSON file.
        # Pass relevant arguments from the parsed args.
        json_path_actual = save_reddit_data(user_data_dir, username, config, sort_descending, args.scrape_comments_only, args.force_scrape)

        # Verify the JSON file exists after the scrape attempt.
        if not json_path_actual or not os.path.exists(json_path_actual):
            # If save_reddit_data failed or didn't return a path, check if a file exists at the expected path.
            json_path_expected = os.path.join(user_data_dir, f"{username}.json")
            if not os.path.exists(json_path_expected):
                # If the file doesn't exist at all, report failure.
                logging.error(f"   ❌ Scrape/Update failed for {username} and no JSON file found at {CYAN}{json_path_expected}{RESET}. Cannot proceed with comparison for this user.");
                return None # Indicate failure
            # If the file exists but save_reddit_data didn't return the path, use the expected path.
            json_path_actual = json_path_expected
            logging.warning(f"   ⚠️ Scrape function did not return path, using existing JSON: {CYAN}{json_path_actual}{RESET}")

        logging.info(f"   ✅ Scrape/Update successful for {username}.")

        # 2. Convert JSON to CSV
        logging.info(f"   ⚙️ Converting JSON to CSV for {username} (unfiltered)...")
        # Call extract_csvs_from_json to convert the JSON data to CSV files.
        # *** Crucially, pass None for date and subreddit filters *** to ensure
        # that the CSVs produced for comparison are based on the full available data.
        posts_csv, comments_csv = extract_csvs_from_json(
            json_path_actual, # Input JSON path
            os.path.join(user_data_dir, username), # Output CSV prefix
            date_filter=(0, float('inf')), # Ensure no date filter applied
            focus_subreddits=None,         # Ensure no focus subreddit filter applied
            ignore_subreddits=None         # Ensure no ignore subreddit filter applied
        )

        # Determine the actual paths to the created CSV files. If extraction returned None,
        # assume the file was not created and the expected path won't exist.
        posts_csv_path = posts_csv # posts_csv is already the full path or None
        comments_csv_path = comments_csv # comments_csv is already the full path or None

        # Check if at least one CSV file was successfully created.
        posts_csv_exists = os.path.exists(posts_csv_path) if posts_csv_path else False
        comments_csv_exists = os.path.exists(comments_csv_path) if comments_csv_path else False

        if not posts_csv_exists and not comments_csv_exists:
            logging.error(f"   ❌ CSV conversion failed or produced no files for {username}. Check JSON structure or permissions. Cannot generate stats.")
            return None # Indicate failure
        logging.info(f"   ✅ CSV Conversion successful for {username}.")

        # 3. Fetch User About Data
        logging.info(f"   ⚙️ Fetching 'about' data for {username}...")
        # Call the helper function to fetch the user's 'about' data from Reddit.
        user_about_data = _fetch_user_about_data(username, config)
        # Log a warning if fetching about data failed, but continue if possible.
        if user_about_data is None:
            logging.warning(f"   ⚠️ Failed to fetch 'about' data for {username}. Some stats might be incomplete.")
        else:
            logging.info(f"   ✅ 'About' data fetched successfully for {username}.")


        # 4. Generate Statistics (as a dictionary)
        logging.info(f"   ⚙️ Calculating stats for {username} (unfiltered)...")
        # Call generate_stats_report from the stats package.
        # *** Again, pass None for date and subreddit filters *** to ensure the stats
        # reflect the full dataset scraped, regardless of filters applied via CLI
        # for the main user processing.
        stats_success, stats_results_data = generate_stats_report(
             json_path=json_path_actual, # Input JSON path
             about_data=user_about_data, # User 'about' data
             posts_csv_path=posts_csv_path if posts_csv_exists else None, # Pass CSV paths (or None)
             comments_csv_path=comments_csv_path if comments_csv_exists else None,
             username=username, # Username for context
             output_path=None, # Do NOT write a standalone MD report for comparison sources
             stats_json_path=None, # Do NOT write a standalone JSON report for comparison sources
             date_filter=(0, float('inf')), # Explicitly pass unfiltered date range
             focus_subreddits=None,         # Explicitly pass None for focus subreddits
             ignore_subreddits=None,        # Explicitly pass None for ignore subreddits
             top_n_words=args.top_words, # Pass top words/items counts from args
             top_n_items=args.top_items,
             write_md_report=False, # Explicitly control flags to prevent writing files
             write_json_report=False
        )

        # Check if statistics calculation was successful.
        if not stats_success:
            logging.error(f"   ❌ Statistics generation failed for {username}. Check logs for details.")
            return None # Indicate failure

        logging.info(f"--- ✅ {BOLD}Finished Processing for Comparison: /u/{username}{RESET} ---")
        return stats_results_data # Return the calculated stats dictionary

    except Exception as e:
        # Catch any unexpected errors during the user processing helper function.
        logging.error(f"   ❌ Unexpected error processing user {username} for comparison: {e}", exc_info=True) # Log with traceback
        return None # Indicate failure


# --- Main Function ---
def main():
    """
    The main entry point of the Redstalk application.
    Parses command-line arguments, loads configuration, determines the requested
    action, and orchestrates the necessary steps (scraping, filtering,
    CSV conversion, stats generation, AI analysis, monitoring, comparison).
    """
    # Load the application configuration (defaults + config.json).
    current_config = load_config()

    # --- Argument Parsing ---
    # Set up the command-line argument parser.
    parser = argparse.ArgumentParser(
        # Description includes the version and format help.
        description=f"{BOLD}Redstalk v1.9.0{RESET} (Multiple Subreddit Filters)",
        # Use ArgumentDefaultsHelpFormatter to show default values in help text.
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # --- Positional Argument ---
    # The main 'username' argument, optional but required for single-user processing.
    parser.add_argument("username", nargs='?', default=None, help="Target Reddit username (for single user analysis/stats).")

    # --- Mutually Exclusive Actions ---
    # Define a group where only one argument can be used at a time.
    action_group = parser.add_mutually_exclusive_group()
    # --compare-user: Triggers the user comparison workflow. Requires two usernames.
    action_group.add_argument("--compare-user", nargs=2, metavar=('USER1', 'USER2'), default=None,
                              help="Compare stats between USER1 and USER2. Disables single-user processing and filters.")
    # --generate-prompt: Triggers the interactive prompt generation assistant.
    action_group.add_argument("--generate-prompt", action="store_true", help="Run interactive prompt generation assistant.")
    # --monitor: Triggers the user monitoring workflow. Takes a username.
    action_group.add_argument("--monitor", metavar='USERNAME', default=None, help="Monitor a user for new activity.")
    # --export-json-only: Scrapes/updates JSON only and exits. Requires positional username.
    action_group.add_argument("--export-json-only", action="store_true", help="Only scrape/update the specified 'username' JSON and exit.")
    # --reset-config: Resets the config.json file to defaults and exits.
    action_group.add_argument("--reset-config", action="store_true", help="Reset config.json to defaults and exit.")

    # --- Processing Actions (Combine with positional 'username') ---
    # Define a group for actions that are performed on the single target username.
    processing_group = parser.add_argument_group('Processing Actions (Combine with positional \'username\')')
    # --run-analysis: Triggers AI analysis.
    processing_group.add_argument("--run-analysis", action="store_true", help="Perform AI analysis on the specified username.")
    # --generate-stats: Triggers statistics report generation.
    processing_group.add_argument("--generate-stats", action="store_true", help="Generate statistics report for the specified username.")

    # --- General Options ---
    general_group = parser.add_argument_group('General Options')
    # --output-dir: Specifies the base directory for output files. Defaults from config.
    general_group.add_argument("--output-dir", default=current_config['default_output_dir'], help="Base directory for output data.")
    # --log-level: Sets the logging verbosity.
    general_group.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set logging level.")
    # --user-agent: Allows overriding the User-Agent string from the command line.
    general_group.add_argument("--user-agent", default=None, help="Custom User-Agent for Reddit API. Overrides config.")


    # --- Data Filtering Options ---
    filter_group = parser.add_argument_group('Data Filtering Options (Applies ONLY to single \'username\' processing)')
    # --start-date: Filters data to include items on or after this date. Uses custom valid_date type.
    filter_group.add_argument("--start-date", type=valid_date, default=None, help="Only analyze activity ON or AFTER this date (YYYY-MM-DD, UTC).")
    # --end-date: Filters data to include items on or before this date. Uses custom valid_date type.
    filter_group.add_argument("--end-date", type=valid_date, default=None, help="Only analyze activity ON or BEFORE this date (YYYY-MM-DD, UTC).")
    # --focus-subreddit: Lists subreddits to include. Can be specified multiple times.
    # --- MODIFIED: nargs='+' to allow multiple subreddits ---
    filter_group.add_argument("--focus-subreddit", metavar='SUBREDDIT', default=None, nargs='+',
                              help="Only include activity within these specific subreddits (case-insensitive). Provide multiple names separated by spaces.")
    # --ignore-subreddit: Lists subreddits to exclude. Can be specified multiple times.
    # --- NEW: Added this argument ---
    filter_argument = filter_group.add_argument("--ignore-subreddit", metavar='SUBREDDIT', default=None, nargs='+',
                              help="Exclude activity from these specific subreddits (case-insensitive). Provide multiple names separated by spaces.")


    # --- Scraping Options ---
    scraping_group = parser.add_argument_group('Scraping Options')
    # --force-scrape: Forces a full data re-scrape.
    scraping_group.add_argument("--force-scrape", action="store_true", help="Force re-scraping all data, updating existing JSON.")
    # --scrape-comments-only: Only scrapes comments.
    scraping_group.add_argument("--scrape-comments-only", action="store_true", help="Only scrape comments, skip submitted posts.")
    # --sort-order: Specifies the sort order for data saved in the JSON file.
    scraping_group.add_argument("--sort-order", default="descending", choices=["ascending", "descending"], help="Sort order for scraped data in JSON ('descending'=newest first).")


    # --- AI Analysis Options ---
    analysis_group = parser.add_argument_group('AI Analysis Options (used with --run-analysis)')
    # --prompt-file: Path to the system prompt file for AI analysis. Defaults from config.
    analysis_group.add_argument("--prompt-file", default=current_config['default_prompt_file'], help="Path to system prompt text file.")
    # --api-key: Allows specifying the API key directly (lowest priority).
    analysis_group.add_argument("--api-key", default=None, help="Google Gemini API Key (Priority: ENV > config.json > flag).")
    # --model-name: Allows overriding the default AI model name.
    analysis_group.add_argument("--model-name", default=None, help=f"Gemini model name. Overrides config (Default: {current_config['default_model_name']}).")
    # --chunk-size: Sets the maximum token size for AI analysis chunks. Defaults from config.
    analysis_group.add_argument("--chunk-size", type=int, default=current_config['default_chunk_size'], help="Target max tokens per chunk for large data.")
    # --analysis-mode: Chooses between 'mapped' and 'raw' analysis output formats.
    analysis_group.add_argument("--analysis-mode", default="mapped", choices=["mapped", "raw", "subreddit_persona"], help="Analysis format: 'mapped', 'raw', or 'subreddit_persona'.")
    # --provider: Selects the AI provider.
    analysis_group.add_argument("--provider", default="gemini", choices=["gemini", "openrouter", "nvidia"], help="AI Provider: 'gemini' (default), 'openrouter', or 'nvidia'.")
    # --no-cache-titles: Debug option to disable post title caching in mapped mode.
    analysis_group.add_argument("--no-cache-titles", action="store_true", help="Disable caching fetched post titles (mapped analysis debug).")
    # --fetch-external-context: Enables fetching titles for comments on external posts in mapped mode.
    analysis_group.add_argument("--fetch-external-context", action="store_true", help="[Mapped Mode] Fetch titles for external posts (slower).")
    # --summarize-stats: Uses AI to summarize the stats report (requires --generate-stats).
    analysis_group.add_argument("--summarize-stats", action="store_true", help="[Requires --generate-stats] Use AI to generate a summary of the stats report.")


    # --- Statistics Options ---
    stats_group = parser.add_argument_group('Statistics Options (used with --generate-stats or --compare-user)')
    # --top-words: Number of top words to include in stats.
    stats_group.add_argument("--top-words", type=int, default=50, help="Number of top words for frequency list.")
    # --top-items: Number of top/bottom scored items to include in stats.
    stats_group.add_argument("--top-items", type=int, default=5, help="Number of top/bottom scored posts/comments.")
    # --stats-output-json: Saves the calculated stats data to a JSON file in addition to MD report.
    stats_group.add_argument("--stats-output-json", metavar='FILEPATH', default=None, help="[Single User] Also save calculated stats data to a JSON file.")


    # --- Prompt Generation Options ---
    prompt_gen_group = parser.add_argument_group('Prompt Generation Options (used with --generate-prompt)')
    # --prompt-dir: Directory to store generated prompts. Defaults from config.
    prompt_gen_group.add_argument("--prompt-dir", default=current_config['default_prompt_dir'], help="Directory to store generated prompts.")


    # --- Monitoring Options ---
    monitor_group = parser.add_argument_group('Monitoring Options (used with --monitor)')
    # --monitor-interval: Sets the check interval for monitoring. Overrides config.
    monitor_group.add_argument("--monitor-interval", type=int, default=None, help=f"Check interval in seconds. Overrides config (Default: {current_config['monitor_interval_seconds']}).")


    # Parse the command-line arguments.
    args = parser.parse_args()

    # --- Logging Setup ---
    # Configure the logging level based on the parsed arguments.
    setup_logging(args.log_level)

    # --- Argument Validation and Action Determination ---
    action = None # Variable to store the determined primary action
    username_target = args.username # The username specified as a positional argument
    user1_comp, user2_comp = (args.compare_user[0], args.compare_user[1]) if args.compare_user else (None, None) # Usernames for comparison
    monitor_target = args.monitor # Username for monitoring

    # Determine the primary action based on the mutually exclusive flags first.
    if args.reset_config: action = "reset_config"
    elif args.generate_prompt: action = "generate_prompt"
    elif monitor_target:
        action = "monitor"
        # If --monitor is used, the target username for monitoring comes from that flag.
        username_target = monitor_target
        if args.username and args.username != monitor_target:
             logging.warning(f"⚠️ Positional username '{args.username}' ignored; monitoring target is '{monitor_target}'.")

    elif user1_comp and user2_comp: action = "compare_users"
    elif args.export_json_only:
        action = "export_json_only"
        # The --export-json-only action requires a username, which must be provided as the positional argument.
        if not username_target: parser.error("--export-json-only requires the positional 'username' argument.")
    elif username_target and (args.run_analysis or args.generate_stats):
        # If a positional username is given AND either --run-analysis or --generate-stats is used,
        # the action is single-user data processing.
        action = "process_user_data"
    # Handle cases where no valid action is selected or combinations are invalid.
    else:
        # If a username was provided but no processing action was specified.
        if username_target:
             parser.error(f"Username '{username_target}' provided, but no processing action specified (--run-analysis or --generate-stats).")
        # If neither an exclusive action nor a positional username with processing actions was given.
        else:
             parser.error("No action specified. Provide a username and processing actions, or use an exclusive action like --compare-user, --monitor, --generate-prompt, --reset-config, or --export-json-only.")


    # Further validation specific to the 'process_user_data' action.
    if action == "process_user_data" and not (args.run_analysis or args.generate_stats):
         # This check might be redundant due to the logic above, but adds robustness.
         parser.error("When providing a username, you must specify at least one processing action: --run-analysis or --generate-stats.")

    # --- NEW: Validate focus/ignore subreddit overlap ---
    # Ensure that no subreddit is present in BOTH the --focus-subreddit and --ignore-subreddit lists.
    if args.focus_subreddit and args.ignore_subreddit:
        focus_set = {sub.lower() for sub in args.focus_subreddit} # Convert lists to lowercase sets
        ignore_set = {sub.lower() for sub in args.ignore_subreddit}
        overlap = focus_set.intersection(ignore_set) # Find common elements
        if overlap:
            # If overlap is found, report the conflicting subreddits and exit.
            parser.error(f"Error: The following subreddits cannot be in both --focus-subreddit and --ignore-subreddit: {', '.join(sorted(overlap))}")

    # Validate incompatible flags based on the determined primary action.
    if action == "compare_users":
        # Warn about flags that are ignored when using --compare-user.
        if args.run_analysis or args.summarize_stats: logging.warning("⚠️ AI analysis (--run-analysis, --summarize-stats) is ignored when using --compare-user (comparison is stats only).")
        # Warn about filter flags which do not apply to the base data used for comparison.
        if args.focus_subreddit or args.ignore_subreddit or args.start_date or args.end_date:
            logging.warning("⚠️ Filters (--focus-subreddit, --ignore-subreddit, --start-date, --end-date) are ignored for --compare-user base stats (unfiltered data is used).")
        # Report an error if --export-json-only is used with --compare-user.
        if args.export_json_only: parser.error("--export-json-only cannot be used with --compare-user.") # Should be caught by group definition
        # Warn if --stats-output-json is used, as it's ignored for comparison reports.
        if args.stats_output_json: logging.warning("⚠️ --stats-output-json is ignored when using --compare-user (comparison report is MD only).")
        args.generate_stats = True # The comparison action implicitly requires stats generation for both users.
    elif action == "export_json_only":
         # Warn about processing/filter flags ignored with --export-json-only.
         if args.run_analysis or args.generate_stats: logging.warning("⚠️ --run-analysis and --generate-stats are ignored when using --export-json-only.")
         if args.focus_subreddit or args.ignore_subreddit or args.start_date or args.end_date:
             logging.warning("⚠️ Filters are ignored when using --export-json-only (JSON export includes all scraped data).")
    elif action == "process_user_data":
         # Report error if --compare-user is used with single username processing (mutually exclusive group should catch this).
         if args.compare_user: parser.error("Cannot use --compare-user with single username processing.") # Should be caught by group
         # Report error if --summarize-stats is requested without --generate-stats.
         if args.summarize_stats and not args.generate_stats: parser.error("--summarize-stats requires --generate-stats to be enabled.")
    elif action in ["generate_prompt", "monitor", "reset_config"]:
         # Warn about flags that are not applicable to these specific actions.
         if args.run_analysis or args.generate_stats: logging.warning(f"⚠️ --run-analysis/--generate-stats ignored with action '{action}'.")
         if args.focus_subreddit or args.ignore_subreddit or args.start_date or args.end_date:
             logging.warning(f"⚠️ Filters ignored with action '{action}'.")
         if args.stats_output_json: logging.warning(f"⚠️ --stats-output-json ignored with action '{action}'.")


    # Log the final determined action and the target user(s) for clarity.
    if action == "compare_users": logging.info(f"🎬 Selected action: {BOLD}Compare Users{RESET} (/u/{user1_comp} vs /u/{user2_comp})")
    elif action == "process_user_data":
        action_details = []; filter_details = []
        if args.run_analysis: action_details.append(f"{BOLD}AI Analysis{RESET}")
        if args.generate_stats: action_details.append(f"{BOLD}Statistics{RESET}")
        logging.info(f"🎬 Selected action: {BOLD}Process User Data{RESET} ({', '.join(action_details)})")
        logging.info(f"👤 Target username: {BOLD}{username_target}{RESET}")
        # Log which filters are being applied for single-user processing.
        # --- MODIFIED: Log both focus and ignore filters ---
        if args.focus_subreddit: filter_details.append(f"Focus Subreddits = {', '.join(args.focus_subreddit)}")
        if args.ignore_subreddit: filter_details.append(f"Ignore Subreddits = {', '.join(args.ignore_subreddit)}")
        if args.start_date or args.end_date:
            # Format dates for logging.
            start_str = args.start_date.strftime('%Y-%m-%d') if args.start_date else 'Beginning'
            end_str = args.end_date.strftime('%Y-%m-%d') if args.end_date else 'End'
            filter_details.append(f"Date = {start_str} to {end_str}")
        if filter_details: logging.info(f"    🔎 Filters Applied: {'; '.join(filter_details)}")
    elif action == "monitor": logging.info(f"🎬 Selected action: {BOLD}Monitor User{RESET}"); logging.info(f"👤 Target username: {BOLD}{username_target}{RESET}")
    elif action == "export_json_only": logging.info(f"🎬 Selected action: {BOLD}Export JSON Only{RESET}"); logging.info(f"👤 Target username: {BOLD}{username_target}{RESET}")
    elif action: logging.info(f"🎬 Selected action: {BOLD}{action.replace('_', ' ').title()}{RESET}") # Format other actions nicely


    # --- Load AI Model (Conditionally) ---
    model = None # Initialize model variable to None
    genai = None # Initialize genai library variable to None
    # Determine if any action requires the AI model.
    ai_needed = action in ["generate_prompt", "monitor"] or \
                (action == "process_user_data" and args.run_analysis) or \
                (action == "process_user_data" and args.summarize_stats)

    if ai_needed:
        logging.info(f"🤖 {BOLD}AI features required. Initializing AI Model...{RESET}")
        try:
            # Attempt to import AI-related modules.
            # These imports are placed here to avoid errors if the required packages are not installed,
            # unless an AI action is actually requested.
            from .ai_utils import generate_prompt_interactive, perform_ai_analysis # Import functions from ai_utils
            from .analysis import generate_mapped_analysis, generate_raw_analysis, generate_subreddit_persona_analysis # Import analysis functions
            logging.debug("AI modules imported successfully.")
        except ImportError as e:
            # If import fails, log a critical error, inform the user to install packages, and disable AI features.
            logging.critical(f"❌ Required Python packages for AI functionality not found: {e}")
            logging.critical("   Please install them: pip install google-generativeai openai langchain-nvidia-ai-endpoints python-dotenv")
            # If the requested action *strictly* requires AI (prompt generation, analysis, summary), exit.
            if action == "generate_prompt" or (action == "process_user_data" and (args.run_analysis or args.summarize_stats)): return # Exit main if AI essential for action
            logging.warning("   AI features will be unavailable.") # Otherwise, warn and continue without AI

        # Configure the Model via Wrapper
        # Override config values with command-line arguments if provided.
        if args.user_agent: current_config['user_agent'] = args.user_agent # User-agent can affect API calls
        if args.model_name: current_config['default_model_name'] = args.model_name # Override default model name
        if args.monitor_interval: current_config['monitor_interval_seconds'] = args.monitor_interval # Override monitor interval

        provider_name = args.provider
        model_name_to_use = current_config['default_model_name']
        
        # Determine API Key based on provider
        api_key_to_use = None
        source_indicator = "None"

        if provider_name == "gemini":
             api_key_to_use = os.environ.get("GOOGLE_API_KEY") or current_config.get("api_key") or args.api_key
             source_indicator = "ENV (GOOGLE_API_KEY)" if os.environ.get("GOOGLE_API_KEY") else ("config" if current_config.get("api_key") else ("flag" if args.api_key else "None"))
        elif provider_name == "openrouter":
             api_key_to_use = os.environ.get("OPENROUTER_API_KEY") or current_config.get("openrouter_api_key") or args.api_key
             source_indicator = "ENV (OPENROUTER_API_KEY)" if os.environ.get("OPENROUTER_API_KEY") else ("config" if current_config.get("openrouter_api_key") else ("flag" if args.api_key else "None"))
        elif provider_name == "nvidia":
             api_key_to_use = os.environ.get("NVIDIA_API_KEY") or current_config.get("nvidia_api_key") or args.api_key
             source_indicator = "ENV (NVIDIA_API_KEY)" if os.environ.get("NVIDIA_API_KEY") else ("config" if current_config.get("nvidia_api_key") else ("flag" if args.api_key else "None"))

        # Check if a valid API key was found.
        if not api_key_to_use or "use_ur" in str(api_key_to_use):
            # If key is missing or is the placeholder, log a critical error.
            logging.critical(f"❌ API Key missing or placeholder used for provider '{BOLD}{provider_name}{RESET}' (Source: {source_indicator}).")
            # If the requested action *strictly* requires AI, exit.
            if action == "generate_prompt" or (action == "process_user_data" and (args.run_analysis or args.summarize_stats)): return # Exit main
        else:
            # If a key is found, attempt to configure the Gemini model.
            logging.info(f"🔑 Using API key from {BOLD}{source_indicator}{RESET}.")
            logging.info(f"🧠 Attempting to configure {provider_name} model: {BOLD}{model_name_to_use}{RESET}")
            try:
                model = get_llm_provider(provider_name, model_name_to_use)
                model.configure(api_key=api_key_to_use)
                # Quick test
                model.count_tokens("test")
                logging.info(f"✅ Successfully configured {provider_name} model: {BOLD}{model_name_to_use}{RESET}")
            except Exception as e:
                # If model configuration or the test call fails, log a critical error.
                logging.critical(f"❌ Failed to configure {provider_name} model '{BOLD}{model_name_to_use}{RESET}': {e}", exc_info=args.log_level == "DEBUG") # Log traceback if debug level
                model = None # Set model to None to indicate failure
                # If the requested action *strictly* requires AI, exit.
                if action == "generate_prompt" or (action == "process_user_data" and (args.run_analysis or args.summarize_stats)): return # Exit main
                logging.warning("   AI features will be unavailable.") # Otherwise, warn and continue

    else:
         # If AI is not needed for the selected action, skip AI setup entirely.
         logging.info(f"🤖 AI model not required for the selected action(s). Skipping AI setup.")

    logging.info("-" * 70) # Print a separator line in the log


    # --- Execute Determined Action ---
    try:
        if action == "reset_config":
            # Handle the reset config action.
            logging.info(f"🔄 {BOLD}Resetting Configuration...{RESET}")
            try:
                save_config(DEFAULT_CONFIG) # Save the default configuration
                logging.info(f"✅ Configuration reset to defaults in {CYAN}config.json{RESET}")
            except Exception as e:
                logging.error(f"❌ Failed to reset configuration: {e}", exc_info=True) # Log errors during saving

        elif action == "generate_prompt":
            # Handle the interactive prompt generation action.
            logging.info(f"🖊️ {BOLD}Starting Interactive Prompt Generation...{RESET}")
            if not model:
                # This check is redundant with the AI setup logic above, but defensive.
                logging.critical("❌ AI Model needed but failed to initialize for prompt generation.");
                return # Exit if model is not available

            # Import the function here to ensure it's only attempted if AI setup passed.
            from ai_utils import generate_prompt_interactive
            # Get the directory for saving prompts from args or config.
            prompt_dir = args.prompt_dir or current_config['default_prompt_dir']
            # Call the interactive prompt generation function.
            generate_prompt_interactive(model, prompt_dir)
            logging.info(f"✅ {BOLD}Interactive Prompt Generation Finished.{RESET}")

        elif action == "monitor":
            # Handle the user monitoring action.
            logging.info(f"👀 {BOLD}Starting User Monitoring...{RESET}")
            if not username_target:
                # Username is required for monitoring.
                logging.critical("❌ Username required for monitoring.");
                return

            # Log a warning if the AI model isn't available for monitoring (auto-analysis might be disabled).
            if not model:
                logging.warning("   ⚠️ AI Model not available for monitoring. Auto-analysis on update will be skipped.")

            # Determine the user-specific data directory and ensure it exists.
            user_data_dir = os.path.join(args.output_dir, username_target);
            os.makedirs(user_data_dir, exist_ok=True)

            # Determine the monitor interval, prioritizing command-line arg over config.
            monitor_interval = args.monitor_interval or current_config.get('monitor_interval_seconds', 180)
            # Enforce a minimum interval for monitoring to avoid excessive API calls.
            if monitor_interval < 60:
                logging.warning(f"Monitor interval ({monitor_interval}s) is very low. Minimum recommended is 60s. Setting to 60s.")
                monitor_interval = 60

            # Load the system prompt for analysis (if auto-analysis is enabled in monitoring).
            prompt_file_path = args.prompt_file;
            system_prompt = "[Monitor Mode - Prompt Error]" # Default error message
            try:
                 logging.info(f"   Loading monitor base prompt from: {CYAN}{prompt_file_path}{RESET}")
                 # Open and read the system prompt file.
                 with open(prompt_file_path, "r", encoding="utf-8") as f:
                     system_prompt = f.read().strip()
                 if not system_prompt:
                     logging.warning(f"   ⚠️ Monitor prompt file {CYAN}{prompt_file_path}{RESET} is empty.")
                 else:
                    logging.info(f"   ✅ Monitor base prompt loaded ({len(system_prompt)} chars).")
            except Exception as e:
                logging.error(f"   ❌ Error reading monitor prompt file {CYAN}{prompt_file_path}{RESET}: {e}")
                system_prompt = "[Monitor Mode - Prompt Load Failed]" # Error message if prompt loading fails

            # Update config with command-line user-agent if provided.
            if args.user_agent:
                 current_config['user_agent'] = args.user_agent

            # Call the monitor_user function with all relevant arguments.
            # Pass filter arguments (focus_subreddits, ignore_subreddits) even if
            # monitor_user doesn't currently use them, in case that functionality is added later.
            monitor_user(
                username=username_target,
                user_data_dir=user_data_dir,
                config=current_config,
                interval_seconds=monitor_interval,
                model=model, # Pass the initialized model (can be None)
                system_prompt=system_prompt,
                chunk_size=args.chunk_size,
                sort_descending=(args.sort_order == "descending"),
                analysis_mode=args.analysis_mode,
                no_cache_titles=args.no_cache_titles,
                fetch_external_context=args.fetch_external_context,
                # Pass filter args:
                focus_subreddits=args.focus_subreddit,
                ignore_subreddits=args.ignore_subreddit
            )
            logging.info(f"🛑 {BOLD}Monitoring Stopped.{RESET}")

        elif action == "export_json_only":
             # Handle the export JSON only action.
             logging.info(f"--- {BOLD}Starting JSON Data Export Only{RESET} ---")
             if not username_target:
                 # Username is required for this action.
                 logging.critical("❌ Username required for export.");
                 return

             # Determine user data directory and ensure it exists.
             user_data_dir = os.path.join(args.output_dir, username_target);
             os.makedirs(user_data_dir, exist_ok=True)

             # Determine sorting order.
             sort_descending = (args.sort_order == "descending")

             # Update config with command-line user-agent if provided.
             if args.user_agent:
                 current_config['user_agent'] = args.user_agent

             logging.info(f"⚙️ Running data fetch/update for /u/{BOLD}{username_target}{RESET}...")
             # Call save_reddit_data to scrape/update and save the JSON file.
             # Filters are NOT applied during JSON export; the JSON contains all scraped data.
             json_path_actual = save_reddit_data(
                 user_data_dir, username_target, current_config,
                 sort_descending, args.scrape_comments_only, args.force_scrape
             )

             # Log the result of the save operation.
             if json_path_actual and os.path.exists(json_path_actual):
                 logging.info(f"✅ JSON data saved/updated successfully at: {CYAN}{json_path_actual}{RESET}")
                 logging.info(f"--- {BOLD}JSON Data Export Complete{RESET} ---")
             else:
                 logging.error(f"❌ Failed to save/update JSON data for {username_target}. Check permissions or logs.")
                 logging.info(f"--- {BOLD}JSON Data Export Failed{RESET} ---")


        elif action == "compare_users":
            # --- Comparison Workflow ---
            logging.info(f"--- 👥 {BOLD}Starting User Comparison{RESET} ---")
            # Ensure stats functionality is available before proceeding with comparison.
            if not stats_available:
                logging.critical("❌ Comparison requires statistics generation functionality, which is not available. Exiting.")
                return # Exit if stats module failed to import

            # Process the first user to get their unfiltered statistics data.
            stats1 = process_single_user_for_stats(user1_comp, current_config, args)
            # Process the second user to get their unfiltered statistics data.
            stats2 = process_single_user_for_stats(user2_comp, current_config, args)

            # If stats data was successfully generated for both users:
            if stats1 and stats2:
                 logging.info(f"   ✅ Data processed for both users. Generating comparison report...")
                 # Generate a timestamp for the comparison report filename.
                 comp_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                 # Construct the output path for the comparison report MD file.
                 comp_filename = f"comparison_{user1_comp}_vs_{user2_comp}_{comp_timestamp}.md"
                 comp_output_path = os.path.join(args.output_dir, comp_filename)
                 os.makedirs(args.output_dir, exist_ok=True) # Ensure the main output directory exists

                 logging.info(f"   💾 Comparison report will be saved to: {CYAN}{comp_output_path}{RESET}")

                 # Call the generate_comparison_report function from the stats package.
                 comp_success = generate_comparison_report(stats1, stats2, user1_comp, user2_comp, comp_output_path)

                 # Log the outcome of the comparison report generation.
                 if comp_success:
                     logging.info(f"--- ✅ {BOLD}User Comparison Complete{RESET} ---")
                 else:
                     logging.error(f"--- ❌ {BOLD}User Comparison Report Generation Failed{RESET} ---")
            else:
                 # If stats data generation failed for one or both users, comparison cannot proceed.
                 logging.error(f"--- ❌ {BOLD}User Comparison Failed{RESET}. Could not process statistics for one or both users.")
            # --- End Comparison Workflow ---

        elif action == "process_user_data":
            # --- Single User Processing Workflow (Scrape -> CSV -> Stats -> Analysis) ---
            if not username_target:
                # Username is required for this action.
                logging.critical("❌ Username required for processing.");
                return

            # Determine the user-specific data directory and ensure it exists.
            user_data_dir = os.path.join(args.output_dir, username_target);
            os.makedirs(user_data_dir, exist_ok=True)
            logging.info(f"📂 Using output directory: {CYAN}{user_data_dir}{RESET}")

            # Determine sorting order for saved JSON.
            sort_descending = (args.sort_order == "descending")

            # Prepare date filter timestamps (UTC). Add 1 day's seconds to end_date
            # to make it inclusive of the end date itself up to midnight UTC.
            start_ts = args.start_date.timestamp() if args.start_date else 0
            end_ts = (args.end_date.timestamp() + 86400) if args.end_date else float('inf')
            date_filter = (start_ts, end_ts)
            # focus_subreddits and ignore_subreddits are taken directly from args and passed down.

            # --- Data Preparation Phase (Scrape and Convert to CSV) ---
            logging.info(f"--- {BOLD}Starting Data Preparation Phase{RESET} ---")

            # Update config with command-line user-agent if provided.
            if args.user_agent:
                 current_config['user_agent'] = args.user_agent

            logging.info(f"⚙️ Running data fetch/update for /u/{BOLD}{username_target}{RESET}...")
            # Call save_reddit_data to scrape/update the JSON file.
            json_path_actual = save_reddit_data(
                user_data_dir, username_target, current_config,
                sort_descending, args.scrape_comments_only, args.force_scrape
            )

            # Verify the JSON file exists after the scrape attempt. If save failed, check for existing file.
            if not json_path_actual or not os.path.exists(json_path_actual):
                 json_path_expected = os.path.join(user_data_dir, f"{username_target}.json")
                 if os.path.exists(json_path_expected):
                     json_path_actual = json_path_expected
                     logging.warning(f"   ⚠️ Scrape failed, using existing JSON: {CYAN}{json_path_actual}{RESET}")
                 else:
                     logging.error(f"❌ Scraping failed and no JSON file found at {CYAN}{json_path_expected}{RESET}. Cannot proceed with CSV conversion.");
                     return # Exit if no JSON data is available
            else:
                logging.info(f"   ✅ Scrape/Update successful: {CYAN}{json_path_actual}{RESET}")


            logging.info(f"⚙️ Converting JSON data to CSV (applying filters if specified)...")
            # Call extract_csvs_from_json to convert the JSON to CSV, applying date and subreddit filters.
            # --- MODIFIED: Pass focus_subreddits and ignore_subreddits lists to the function ---
            posts_csv, comments_csv = extract_csvs_from_json(
                json_path_actual, # Input JSON path
                os.path.join(user_data_dir, username_target), # Output CSV prefix (includes username)
                date_filter=date_filter, # Pass the calculated date filter tuple
                focus_subreddits=args.focus_subreddit, # Pass the list of focus subreddits (or None)
                ignore_subreddits=args.ignore_subreddit # Pass the list of ignore subreddits (or None)
            )

            # Determine the actual paths to the created CSV files (or None if not created).
            posts_csv_path = posts_csv # extract_csvs_from_json returns the path or None
            comments_csv_path = comments_csv # extract_csvs_from_json returns the path or None

            # Check if at least one CSV file was created after filtering.
            posts_csv_exists = os.path.exists(posts_csv_path) if posts_csv_path else False
            comments_csv_exists = os.path.exists(comments_csv_path) if comments_csv_path else False

            # Log the status of the created CSV files.
            if not posts_csv_exists and not comments_csv_exists:
                logging.error("❌ CSV conversion produced no files after applying filters. Check filter criteria or input data. Cannot proceed with stats or analysis.")
                return # Exit if no data remains after filtering

            elif not posts_csv_exists:
                logging.info(f"   📄 Comments CSV: {CYAN}{comments_csv_path}{RESET} (Posts filtered out or not available)")
            elif not comments_csv_exists:
                logging.info(f"   📄 Posts CSV: {CYAN}{posts_csv_path}{RESET} (Comments filtered out or not available)")
            else:
                logging.info(f"   📄 Posts CSV: {CYAN}{posts_csv_path}{RESET}")
                logging.info(f"   📄 Comments CSV: {CYAN}{comments_csv_path}{RESET}")

            logging.info(f"--- {BOLD}Data Preparation Complete{RESET} ---"); logging.info("-" * 70) # Separator line


            # --- Fetch About Data (if needed for stats) ---
            user_about_data = None # Initialize about data variable
            # Fetch 'about' data only if statistics generation is requested.
            if args.generate_stats:
                 logging.info(f"--- {BOLD}Fetching User About Data{RESET} ---")
                 user_about_data = _fetch_user_about_data(username_target, current_config)
                 if user_about_data is None:
                     logging.warning("   ⚠️ Failed to fetch user 'about' data. Some statistics might be incomplete.")
                 else:
                     logging.info("   ✅ User 'about' data fetched successfully.")
                 logging.info("-" * 70) # Separator line


            # --- Statistics Generation Phase ---
            stats_report_path = None # Initialize stats report path variable
            stats_results_data = None # Initialize stats results data variable
            # Run statistics generation if requested.
            if args.generate_stats:
                 logging.info(f"--- 📊 {BOLD}Starting Statistics Generation Phase{RESET} ---")
                 # Ensure stats functionality is available.
                 if not stats_available:
                     logging.critical("❌ Statistics generation requested but not available. Check installation.")
                     # Do not return here, allow analysis to run if requested and possible.
                 else:
                     # Generate a timestamp for stats output files.
                     stats_timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                     # Construct the output path for the Markdown stats report.
                     stats_output_filename = f"{username_target}_stats_{stats_timestamp_str}.md"
                     stats_output_path = os.path.join(user_data_dir, stats_output_filename)

                     # Determine the output path for the JSON stats file if requested.
                     stats_json_output_path = args.stats_output_json # This is the user-provided path or None

                     logging.info(f"   ⚙️ Calculating statistics (applying filters)...")
                     # Call generate_stats_report function. Pass filter arguments.
                     # --- MODIFIED: Pass focus_subreddits and ignore_subreddits lists ---
                     stats_success, stats_results_data = generate_stats_report(
                         json_path=json_path_actual, # Pass the path to the main JSON file (used for counts, not filtering)
                         about_data=user_about_data, # Pass fetched about data
                         posts_csv_path=posts_csv_path if posts_csv_exists else None, # Pass filtered CSV paths
                         comments_csv_path=comments_csv_path if comments_csv_exists else None,
                         username=username_target, # Username
                         output_path=stats_output_path, # MD report output path
                         stats_json_path=stats_json_output_path, # JSON report output path (or None)
                         date_filter=date_filter, # Pass the date filter
                         focus_subreddits=args.focus_subreddit, # Pass focus subreddit list
                         ignore_subreddits=args.ignore_subreddit, # Pass ignore subreddit list
                         top_n_words=args.top_words, # Pass top N arguments
                         top_n_items=args.top_items,
                         write_md_report=True, # Explicitly request writing the MD report
                         write_json_report=True # Explicitly request writing the JSON report (if path provided)
                     )

                     # Log the outcome of stats generation.
                     if stats_success:
                         logging.info(f"--- ✅ {BOLD}Statistics Generation Complete{RESET} ---")
                         stats_report_path = stats_output_path # Store the path if successful
                     else:
                         logging.error(f"--- ❌ {BOLD}Statistics Generation Failed{RESET}. Check logs.")

                 logging.info("-" * 70) # Separator line


            # --- AI Summary of Stats (if requested and possible) ---
            # Run AI summary if requested (--summarize-stats) AND stats were generated AND AI model is available.
            if args.summarize_stats:
                 if not model:
                     logging.critical("❌ AI Stats Summary requested but AI model failed to initialize.");
                     # Do not return here, allow other actions if possible.
                 elif not stats_report_path or not os.path.exists(stats_report_path):
                     # Cannot summarize if the stats report file doesn't exist.
                     logging.error(f"❌ Cannot summarize stats: Statistics report not found ({CYAN}{stats_report_path or 'N/A'}{RESET}). Ensure --generate-stats was successful.")
                 else:
                     # Proceed with AI summary generation.
                     logging.info(f"--- 🧠 {BOLD}Starting AI Summary of Statistics Report{RESET} ---")
                     logging.info(f"   📄 Reading stats report from: {CYAN}{stats_report_path}{RESET}")
                     try:
                         # Read the content of the generated stats report.
                         with open(stats_report_path, "r", encoding="utf-8") as f_stats:
                             stats_content = f_stats.read()

                         # Construct context about filters applied for the AI prompt.
                         filter_context = []
                         if args.focus_subreddit: filter_context.append(f"focused on subreddits: {', '.join(args.focus_subreddit)}")
                         if args.ignore_subreddit: filter_context.append(f"ignoring subreddits: {', '.join(args.ignore_subreddit)}")
                         if args.start_date or args.end_date:
                             start_str = args.start_date.strftime('%Y-%m-%d') if args.start_date else 'Beginning'
                             end_str = args.end_date.strftime('%Y-%m-%d') if args.end_date else 'End'
                             filter_context.append(f"date range: {start_str} to {end_str}")
                         filter_str = f" (Note: Data filters applied - {'; '.join(filter_context)})" if filter_context else ""

                         # Create the prompt for the AI summary.
                         summary_prompt = (f"Summarize the key findings (3-5 bullet points) from this Reddit user statistics report for /u/{username_target}{filter_str}:\n\n"
                                           f"--- REPORT START ---\n{stats_content}\n--- REPORT END ---\n\n**Summary:**")
                         logging.info("   🤖 Requesting summary from AI...")
                         summary_start_time = time.time() # Start timer for AI call
                         try:
                             # Make the AI call to generate the summary.
                             # Use a moderate temperature and limit output tokens.
                             # Apply specific safety settings for this task.
                             response = model.generate_content(summary_prompt,
                                         generation_config=genai.GenerationConfig(temperature=0.5, max_output_tokens=1024),
                                         safety_settings={'HARM_CATEGORY_HARASSMENT': 'BLOCK_LOW_AND_ABOVE', 'HARM_CATEGORY_HATE_SPEECH': 'BLOCK_LOW_AND_ABOVE',
                                                          'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'BLOCK_NONE', 'HARM_CATEGORY_DANGEROUS_CONTENT': 'BLOCK_LOW_AND_ABOVE'})
                             summary_duration = time.time() - summary_start_time # Calculate duration

                             # Process the AI response.
                             if response and hasattr(response, 'text') and response.text and response.text.strip():
                                 summary_text = response.text.strip()
                                 logging.info(f"   ✅ AI summary generated successfully ({summary_duration:.2f}s).")
                                 # Append the generated summary to the existing stats report Markdown file.
                                 with open(stats_report_path, "a", encoding="utf-8") as f_append:
                                     f_append.write(f"\n\n---\n\n## VII. AI-Generated Summary\n\n{summary_text}\n");
                                     logging.info(f"   💾 Summary appended to: {CYAN}{stats_report_path}{RESET}")
                             else:
                                 # Handle cases where summary generation failed or was blocked.
                                 block_reason = response.prompt_feedback.block_reason if response and response.prompt_feedback else 'Unknown'
                                 logging.error(f"   ❌ AI summary generation failed or was blocked ({summary_duration:.2f}s). Reason: {block_reason}")
                         except Exception as ai_err:
                             # Catch unexpected exceptions during the AI call.
                             logging.error(f"   ❌ Exception during AI summary generation: {ai_err}", exc_info=args.log_level == "DEBUG") # Log traceback if debug

                     except IOError as read_err:
                         # Handle errors reading the stats report file.
                         logging.error(f"   ❌ Error reading stats report file for summary: {read_err}")

                     logging.info(f"--- {BOLD}AI Summary Generation Finished{RESET} ---"); logging.info("-" * 70) # Separator line


            # --- AI Analysis Phase (if requested and possible) ---
            # Run AI analysis if requested (--run-analysis) AND AI model is available.
            if args.run_analysis:
                 if not model:
                     logging.critical("❌ AI Analysis requested but AI model failed to initialize.");
                     # Do not return here, allow other actions if possible.
                     return # Exit if analysis is requested but model is unavailable

                 try:
                     # Import analysis functions here, ensuring they are available.
                     from .analysis import generate_mapped_analysis, generate_raw_analysis, generate_subreddit_persona_analysis
                 except ImportError as e:
                     logging.critical(f"❌ Failed to import analysis functions from analysis.py: {e}");
                     return # Exit if analysis functions cannot be imported

                 logging.info(f"--- 🤖 {BOLD}Starting AI Analysis Phase{RESET} ({BOLD}{args.analysis_mode}{RESET} mode) ---")

                 logging.info(f"   📄 Loading System Prompt...")
                 prompt_file_path = args.prompt_file;
                 system_prompt = "" # Initialize system prompt
                 try:
                     # Read the system prompt file.
                     with open(prompt_file_path, "r", encoding="utf-8") as f:
                         system_prompt = f.read().strip()
                     if not system_prompt:
                         logging.warning(f"   ⚠️ Prompt file {CYAN}{prompt_file_path}{RESET} is empty.")
                     else:
                         logging.info(f"   ✅ System prompt loaded ({len(system_prompt)} chars).")
                 except Exception as e:
                     # Handle errors reading the prompt file.
                     logging.critical(f"❌ Failed to load system prompt {CYAN}{prompt_file_path}{RESET}: {e}");
                     return # Exit if prompt cannot be loaded

                 # Generate a timestamp for the analysis output filename.
                 ai_timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                 # Construct the analysis output filename including mode and timestamp.
                 output_filename = f"{username_target}_charc_{args.analysis_mode}_{ai_timestamp_str}.md"
                 # Construct the full output path in the user's data directory.
                 output_md_file = os.path.join(user_data_dir, output_filename)
                 logging.info(f"   💾 AI analysis output file: {CYAN}{output_md_file}{RESET}")

                 logging.info(f"   ⚙️ Preparing data and initiating AI analysis (applying filters)...")
                 analysis_success = False # Flag to track if analysis function reports success

                 # Prepare arguments dictionary common to both analysis modes.
                 analysis_func_args = {
                     "output_file": output_md_file, # Output file path
                     "model": model, # AI model instance
                     "system_prompt": system_prompt, # Loaded system prompt
                     "chunk_size": args.chunk_size, # Max tokens per chunk
                     "date_filter": date_filter, # Date filter tuple
                     # Pass filter lists to analysis functions. Note: The analysis functions
                     # themselves are responsible for *using* these filters on the data they process.
                     "focus_subreddits": args.focus_subreddit,
                     "ignore_subreddits": args.ignore_subreddit
                 }
                 
                 # --- Specialized Prompt for Persona Mode ---
                 if args.analysis_mode == "subreddit_persona" and not args.prompt_file:
                     # If the user didn't explicitly provide a custom prompt file, use this specialized one.
                     logging.info(f"   🎭 Using specialized system prompt for {BOLD}subreddit_persona{RESET} mode.")
                     system_prompt = (
                         "You are an expert OSINT profiler and behavioral analyst specialized in analyzing Reddit user history.\n"
                         "You have been provided with a Reddit user's activity history, AGGREGATED BY SUBREDDIT.\n"
                         "The data is structured in blocks starting with '=== SUBREDDIT: r/Name ==='.\n\n"
                         "YOUR TASK:\n"
                         "Perform a granular analysis of how the user's persona, opinions, tone, and behavior SHIFT depending on the subreddit they are in.\n"
                         "Do not just give a general summary. You must break down your analysis by community.\n\n"
                         "OUTPUT FORMAT:\n"
                         "For each major subreddit (or group of related subreddits) provided in the data, generate a section:\n\n"
                         "### In r/[Subreddit Name]\n"
                         "* **Persona:** (e.g., Expert, Learner, Troll, Supporter)\n"
                         "* **Key Opinions/Interests:** What did they talk about here? Be specific.\n"
                         "* **Tone:** (e.g., Formal, Aggressive, Casual)\n"
                         "* **Observations:** Any specific contradictions or unique traits revealed here compared to other subs?\n\n"
                         "### Overall Persona Profiling\n"
                         "* **Code Switching:** How does their identity change across communities?\n"
                         "* **Inconsistencies:** Any conflicting info?\n\n"
                         "Be specific and cite the subreddit for every claim."
                     )
                     analysis_func_args["system_prompt"] = system_prompt


                 # Determine which CSV files exist to pass to the analysis functions.
                 posts_csv_input = posts_csv_path if posts_csv_exists else None
                 comments_csv_input = comments_csv_path if comments_csv_exists else None

                 # If no CSV data is available after filtering, log error and skip analysis.
                 if not posts_csv_input and not comments_csv_input:
                     logging.error("❌ Cannot run AI analysis: No valid CSV data found after applying filters. Check filters or input data.");
                     return # Exit if no data remains

                 # Call the appropriate analysis function based on the selected mode.
                 from .analysis import generate_mapped_analysis, generate_raw_analysis, generate_subreddit_persona_analysis # Re-import within block for clarity

                 if args.analysis_mode == "raw":
                     # Call generate_raw_analysis with CSV paths and common arguments.
                     analysis_success = generate_raw_analysis(
                         posts_csv=posts_csv_input,
                         comments_csv=comments_csv_input,
                         **analysis_func_args
                     )
                 elif args.analysis_mode == "subreddit_persona":
                     # Call generate_subreddit_persona_analysis
                     analysis_success = generate_subreddit_persona_analysis(
                         posts_csv=posts_csv_input,
                         comments_csv=comments_csv_input,
                         **analysis_func_args
                     )
                 else: # mapped mode
                     # Call generate_mapped_analysis with CSV paths and common arguments.
                     # Mapped mode also requires the config for external context fetching.
                     analysis_success = generate_mapped_analysis(
                         posts_csv=posts_csv_input,
                         comments_csv=comments_csv_input,
                         config=current_config, # Pass config
                         no_cache_titles=args.no_cache_titles, # Pass mapped-specific args
                         fetch_external_context=args.fetch_external_context,
                         **analysis_func_args # Pass common arguments
                     )

                 # Log the outcome of the AI analysis generation.
                 if analysis_success:
                     logging.info(f"--- ✅ {BOLD}AI Analysis Complete{RESET} ---")
                 else:
                     logging.error(f"--- ❌ {BOLD}AI Analysis Failed{RESET}. Check logs for details.")

                 logging.info("-" * 70) # Separator line

        else:
             # Fallback for any action that was determined but not handled explicitly.
             logging.error(f"❌ Unhandled action scenario: {action}. This is an internal error.")


    except Exception as e:
         # Catch any unexpected critical errors that occur during the main execution flow.
         logging.critical(f"🔥 An unexpected CRITICAL error occurred in main execution: {e}", exc_info=True) # Log with traceback
         sys.exit(1) # Exit the script with a non-zero status code to indicate an error occurred

if __name__ == "__main__":
    # Entry point when the script is run directly.
    main() # Call the main function