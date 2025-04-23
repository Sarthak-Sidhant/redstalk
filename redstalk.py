#!/usr/bin/env python3
# redstalk.py
import os
import argparse
import logging
import sys
from datetime import datetime, timezone # Added timezone
import re # For date parsing
import time # For AI summary timing
import json # For saving stats JSON in helper

# --- Import Utilities ---
from config_utils import load_config, save_config, DEFAULT_CONFIG
# Import specific functions
from reddit_utils import save_reddit_data, _fetch_user_about_data
from data_utils import extract_csvs_from_json # Updated import signature usage later
from monitoring import monitor_user
# Import stats functions - including comparison

try:
    from stats.single_report import generate_stats_report # Updated import signature usage later
    from stats.comparison import generate_comparison_report
    stats_available = True
except ImportError as e:
    logging.error(f"❌ Failed to import from the 'stats' package. Statistics generation disabled. Error: {e}")
    stats_available = False
    # Define dummy functions if needed elsewhere in redstalk to avoid NameErrors
    def generate_stats_report(*args, **kwargs):
        logging.error("generate_stats_report called, but stats package failed to import.")
        return False, None
    def generate_comparison_report(*args, **kwargs):
        logging.error("generate_comparison_report called, but stats package failed to import.")
        return False

# --- ANSI Color Codes & Logging Setup (Keep from previous version) ---
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; RED = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; BLUE = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"
LOG_LEVEL_COLORS = { logging.DEBUG: DIM + BLUE, logging.INFO: BLUE, logging.WARNING: YELLOW, logging.ERROR: RED, logging.CRITICAL: BOLD + RED, }
class ColoredFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%'): super().__init__(fmt, datefmt, style)
    def format(self, record):
        log_message = super().format(record)
        level_color = LOG_LEVEL_COLORS.get(record.levelno, WHITE); levelname_colored = f"{level_color}{record.levelname:<8}{RESET}"
        log_prefix = f"{record.asctime} - {record.levelname:<8} - {record.filename}:{record.lineno:<4d} - "
        message_body = log_message[len(log_prefix):]
        formatted_message = f"{DIM}{record.asctime}{RESET} - {levelname_colored} - {DIM}{record.filename}:{record.lineno:<4d}{RESET} - {message_body}"
        # Highlight file paths using regex (simple example)
        formatted_message = re.sub(r'([\s\'"])(/[^/\s]+)+/([^/\s]+\.\w+)([\s\'"]|$)', rf'\1{CYAN}\2/\3{RESET}\4', formatted_message)
        return formatted_message

def setup_logging(log_level_str):
    log_level = getattr(logging, log_level_str.upper(), logging.INFO); root_logger = logging.getLogger(); root_logger.setLevel(log_level)
    for handler in root_logger.handlers[:]: root_logger.removeHandler(handler)
    ch = logging.StreamHandler(sys.stdout); ch.setLevel(log_level)
    formatter = ColoredFormatter(fmt='%(asctime)s - %(levelname)-8s - %(filename)s:%(lineno)-4d - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    ch.setFormatter(formatter); root_logger.addHandler(ch)
    if log_level > logging.DEBUG:
        logging.getLogger("requests").setLevel(logging.WARNING); logging.getLogger("urllib3").setLevel(logging.WARNING); logging.getLogger("google.generativeai").setLevel(logging.WARNING)
        logging.getLogger("vaderSentiment").setLevel(logging.WARNING)
    logging.debug(f"Logging initialized at level: {log_level_str}")


def valid_date(s):
    """Custom argparse type for user dates."""
    try:
        dt_naive = datetime.strptime(s, "%Y-%m-%d")
        return dt_naive.replace(tzinfo=timezone.utc)
    except ValueError:
        msg = f"Not a valid date: '{s}'. Expected format YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg)

# --- !! RESTORED Helper Function for Processing Single User !! ---
def process_single_user_for_stats(username, config, args):
    """Scrapes, converts CSV, fetches 'about', calculates stats for ONE user (UNFILTERED)."""
    logging.info(f"--- {BOLD}Processing User for Comparison: /u/{username}{RESET} ---")
    user_data_dir = os.path.join(args.output_dir, username); os.makedirs(user_data_dir, exist_ok=True)
    sort_descending = (args.sort_order == "descending")
    stats_results_data = None
    user_about_data = None
    success = False # Initialize success flag

    try:
        # 1. Scrape
        logging.info(f"   ⚙️ Running data fetch/update for /u/{BOLD}{username}{RESET}...")
        json_path_actual = save_reddit_data(user_data_dir, username, config, sort_descending, args.scrape_comments_only, args.force_scrape)
        if not json_path_actual or not os.path.exists(json_path_actual):
            json_path_expected = os.path.join(user_data_dir, f"{username}.json")
            if not os.path.exists(json_path_expected):
                logging.error(f"   ❌ Scrape/Update failed for {username}. Cannot proceed with comparison for this user."); return None
            json_path_actual = json_path_expected
            logging.warning(f"   ⚠️ Scrape function did not return path, using existing JSON: {CYAN}{json_path_actual}{RESET}")
        logging.info(f"   ✅ Scrape/Update successful for {username}.")

        # 2. CSV (No date/sub filtering needed for comparison source data)
        logging.info(f"   ⚙️ Converting JSON to CSV for {username} (unfiltered)...")
        # *** Pass None for filters to ensure base comparison data is unfiltered ***
        posts_csv, comments_csv = extract_csvs_from_json(
            json_path_actual,
            os.path.join(user_data_dir, username),
            date_filter=(0, float('inf')), # Ensure no date filter
            focus_subreddits=None,         # Ensure no focus sub filter
            ignore_subreddits=None         # Ensure no ignore sub filter
        )
        posts_csv_path = posts_csv if posts_csv else os.path.join(user_data_dir, f"{username}-posts.csv")
        comments_csv_path = comments_csv if comments_csv else os.path.join(user_data_dir, f"{username}-comments.csv")
        posts_csv_exists = os.path.exists(posts_csv_path)
        comments_csv_exists = os.path.exists(comments_csv_path)
        if not posts_csv_exists and not comments_csv_exists:
            logging.error(f"   ❌ CSV conversion failed or produced no files for {username}. Cannot generate stats.")
            return None
        logging.info(f"   ✅ CSV Conversion successful for {username}.")

        # 3. Fetch About Data
        logging.info(f"   ⚙️ Fetching 'about' data for {username}...")
        user_about_data = _fetch_user_about_data(username, config)
        if user_about_data is None: logging.warning(f"   ⚠️ Failed to fetch 'about' data for {username}.")

        # 4. Generate Stats (Return dict, don't write files here)
        logging.info(f"   ⚙️ Calculating stats for {username} (unfiltered)...")
        # *** Ensure filters are off when calling generate_stats_report here ***
        stats_success, stats_results_data = generate_stats_report(
             json_path=json_path_actual, about_data=user_about_data,
             posts_csv_path=posts_csv_path if posts_csv_exists else None,
             comments_csv_path=comments_csv_path if comments_csv_exists else None,
             username=username, output_path=None, # Don't write MD
             stats_json_path=None, # Don't write JSON
             date_filter=(0, float('inf')), # No date filter for comparison base
             focus_subreddits=None,         # No focus subreddit filter for comparison base
             ignore_subreddits=None,        # No ignore subreddit filter for comparison base
             top_n_words=args.top_words, top_n_items=args.top_items,
             write_md_report=False, write_json_report=False # Control flags
        )
        if not stats_success:
            logging.error(f"   ❌ Statistics generation failed for {username}.")
            return None # Return None if stats calc fails

        logging.info(f"--- ✅ {BOLD}Finished Processing for Comparison: /u/{username}{RESET} ---")
        return stats_results_data

    except Exception as e:
        logging.error(f"   ❌ Unexpected error processing user {username} for comparison: {e}", exc_info=True)
        return None


# --- Main Function ---
def main():
    current_config = load_config()

    parser = argparse.ArgumentParser(
        description=f"{BOLD}Redstalk v1.9.0{RESET} (Multiple Subreddit Filters)", # Version bump
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # --- Positional Argument (for single user processing) ---
    parser.add_argument("username", nargs='?', default=None, help="Target Reddit username (for single user analysis/stats).")

    # --- Mutually Exclusive Actions ---
    action_group = parser.add_mutually_exclusive_group()
    # Note: 'username' positional is NOT in this group, allowing it with --run-analysis etc.
    action_group.add_argument("--compare-user", nargs=2, metavar=('USER1', 'USER2'), default=None,
                              help="Compare stats between USER1 and USER2. Disables single-user processing and filters.")
    action_group.add_argument("--generate-prompt", action="store_true", help="Run interactive prompt generation assistant.")
    action_group.add_argument("--monitor", metavar='USERNAME', default=None, help="Monitor a user for new activity.")
    action_group.add_argument("--export-json-only", action="store_true", help="Only scrape/update the specified 'username' JSON and exit.")
    action_group.add_argument("--reset-config", action="store_true", help="Reset config.json to defaults and exit.")

    # --- Processing Actions (Combine with positional 'username') ---
    processing_group = parser.add_argument_group('Processing Actions (Combine with positional \'username\')')
    processing_group.add_argument("--run-analysis", action="store_true", help="Perform AI analysis on the specified username.")
    processing_group.add_argument("--generate-stats", action="store_true", help="Generate statistics report for the specified username.")

    # --- General Options ---
    general_group = parser.add_argument_group('General Options')
    general_group.add_argument("--output-dir", default=current_config['default_output_dir'], help="Base directory for output data.")
    general_group.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set logging level.")
    general_group.add_argument("--user-agent", default=None, help="Custom User-Agent for Reddit API. Overrides config.")


    # --- Data Filtering Options ---
    filter_group = parser.add_argument_group('Data Filtering Options (Applies ONLY to single \'username\' processing)')
    filter_group.add_argument("--start-date", type=valid_date, default=None, help="Only analyze activity ON or AFTER this date (YYYY-MM-DD, UTC).")
    filter_group.add_argument("--end-date", type=valid_date, default=None, help="Only analyze activity ON or BEFORE this date (YYYY-MM-DD, UTC).")
    # --- MODIFIED: --focus-subreddit ---
    filter_group.add_argument("--focus-subreddit", metavar='SUBREDDIT', default=None, nargs='+',
                              help="Only analyze activity within these specific subreddits (case-insensitive). Repeat for multiple.")
    # --- NEW: --ignore-subreddit ---
    filter_group.add_argument("--ignore-subreddit", metavar='SUBREDDIT', default=None, nargs='+',
                              help="Exclude activity from these specific subreddits (case-insensitive). Repeat for multiple.")


    # --- Scraping Options ---
    scraping_group = parser.add_argument_group('Scraping Options')
    scraping_group.add_argument("--force-scrape", action="store_true", help="Force re-scraping all data, updating existing JSON.")
    scraping_group.add_argument("--scrape-comments-only", action="store_true", help="Only scrape comments, skip submitted posts.")
    scraping_group.add_argument("--sort-order", default="descending", choices=["ascending", "descending"], help="Sort order for scraped data in JSON ('descending'=newest first).")


    # --- AI Analysis Options ---
    analysis_group = parser.add_argument_group('AI Analysis Options (used with --run-analysis)')
    analysis_group.add_argument("--prompt-file", default=current_config['default_prompt_file'], help="Path to system prompt text file.")
    analysis_group.add_argument("--api-key", default=None, help="Google Gemini API Key (Priority: ENV > config.json > flag).")
    analysis_group.add_argument("--model-name", default=None, help=f"Gemini model name. Overrides config (Default: {current_config['default_model_name']}).")
    analysis_group.add_argument("--chunk-size", type=int, default=current_config['default_chunk_size'], help="Target max tokens per chunk for large data.")
    analysis_group.add_argument("--analysis-mode", default="mapped", choices=["mapped", "raw"], help="Analysis format: 'mapped' (grouped) or 'raw' (sequential).")
    analysis_group.add_argument("--no-cache-titles", action="store_true", help="Disable caching fetched post titles (mapped analysis debug).")
    analysis_group.add_argument("--fetch-external-context", action="store_true", help="[Mapped Mode] Fetch titles for external posts (slower).")
    analysis_group.add_argument("--summarize-stats", action="store_true", help="[Requires --generate-stats] Use AI to generate a summary of the stats report.")


    # --- Statistics Options ---
    stats_group = parser.add_argument_group('Statistics Options (used with --generate-stats or --compare-user)')
    stats_group.add_argument("--top-words", type=int, default=50, help="Number of top words for frequency list.")
    stats_group.add_argument("--top-items", type=int, default=5, help="Number of top/bottom scored posts/comments.")
    stats_group.add_argument("--stats-output-json", metavar='FILEPATH', default=None, help="[Single User] Also save calculated stats data to a JSON file.")


    # --- Prompt Generation Options ---
    prompt_gen_group = parser.add_argument_group('Prompt Generation Options (used with --generate-prompt)')
    prompt_gen_group.add_argument("--prompt-dir", default=current_config['default_prompt_dir'], help="Directory to store generated prompts.")


    # --- Monitoring Options ---
    monitor_group = parser.add_argument_group('Monitoring Options (used with --monitor)')
    monitor_group.add_argument("--monitor-interval", type=int, default=None, help=f"Check interval in seconds. Overrides config (Default: {current_config['monitor_interval_seconds']}).")


    args = parser.parse_args()

    # --- Logging Setup ---
    setup_logging(args.log_level)

    # --- Argument Validation and Action Determination ---
    action = None
    username_target = args.username # User for single processing or export
    user1_comp, user2_comp = (args.compare_user[0], args.compare_user[1]) if args.compare_user else (None, None)
    monitor_target = args.monitor

    # Determine the primary action based on exclusive flags first
    if args.reset_config: action = "reset_config"
    elif args.generate_prompt: action = "generate_prompt"
    elif monitor_target: action = "monitor"; username_target = monitor_target
    elif user1_comp and user2_comp: action = "compare_users"
    elif args.export_json_only:
        action = "export_json_only"
        if not username_target: parser.error("--export-json-only requires the positional 'username' argument.")
    elif username_target and (args.run_analysis or args.generate_stats):
        action = "process_user_data" # Single user processing
    # Handle errors: No action selected or invalid combinations
    else:
        # If username was given, but no processing action
        if username_target:
             parser.error(f"Username '{username_target}' provided, but no processing action specified (--run-analysis or --generate-stats).")
        # If no exclusive action and no username
        else:
             parser.error("No action specified. Provide a username and processing actions, or use an exclusive action like --compare-user, --monitor, etc.")


    # Further validation for processing_user_data action
    if action == "process_user_data" and not (args.run_analysis or args.generate_stats):
         parser.error("When providing a username, you must specify at least one processing action: --run-analysis or --generate-stats.")

    # --- NEW: Validate focus/ignore subreddit overlap ---
    if args.focus_subreddit and args.ignore_subreddit:
        focus_set = {sub.lower() for sub in args.focus_subreddit}
        ignore_set = {sub.lower() for sub in args.ignore_subreddit}
        overlap = focus_set.intersection(ignore_set)
        if overlap:
            parser.error(f"The following subreddits cannot be in both --focus-subreddit and --ignore-subreddit: {', '.join(overlap)}")

    # Validate incompatible flags based on the determined action
    if action == "compare_users":
        if args.run_analysis or args.summarize_stats: logging.warning("⚠️ AI analysis (--run-analysis, --summarize-stats) is ignored when using --compare-user.")
        # Now warn about both focus and ignore filters
        if args.focus_subreddit or args.ignore_subreddit or args.start_date or args.end_date:
            logging.warning("⚠️ Filters (--focus-subreddit, --ignore-subreddit, --start-date, --end-date) are ignored for --compare-user base stats.")
        if args.export_json_only: parser.error("--export-json-only cannot be used with --compare-user.") # Should be caught by group
        if args.stats_output_json: logging.warning("⚠️ --stats-output-json is ignored when using --compare-user (comparison report is MD only).")
        args.generate_stats = True # Force stats generation implicitly
    elif action == "export_json_only":
         if args.run_analysis or args.generate_stats: logging.warning("⚠️ --run-analysis and --generate-stats are ignored when using --export-json-only.")
         # Now warn about both focus and ignore filters
         if args.focus_subreddit or args.ignore_subreddit or args.start_date or args.end_date:
             logging.warning("⚠️ Filters are ignored when using --export-json-only.")
    elif action == "process_user_data":
         # Validate flags specific to single user processing
         if args.compare_user: parser.error("Cannot use --compare-user with single username processing.") # Should be caught by group
         if args.summarize_stats and not args.generate_stats: parser.error("--summarize-stats requires --generate-stats to be enabled.")
    elif action in ["generate_prompt", "monitor", "reset_config"]:
         # Validate flags not applicable to these actions
         if args.run_analysis or args.generate_stats: logging.warning(f"⚠️ --run-analysis/--generate-stats ignored with action '{action}'.")
         # Now warn about both focus and ignore filters
         if args.focus_subreddit or args.ignore_subreddit or args.start_date or args.end_date:
             logging.warning(f"⚠️ Filters ignored with action '{action}'.")
         if args.stats_output_json: logging.warning(f"⚠️ --stats-output-json ignored with action '{action}'.")


    # Log the final determined action and target(s)
    if action == "compare_users": logging.info(f"🎬 Selected action: {BOLD}Compare Users{RESET} (/u/{user1_comp} vs /u/{user2_comp})")
    elif action == "process_user_data":
        action_details = []; filter_details = []
        if args.run_analysis: action_details.append(f"{BOLD}AI Analysis{RESET}")
        if args.generate_stats: action_details.append(f"{BOLD}Statistics{RESET}")
        logging.info(f"🎬 Selected action: {BOLD}Process User Data{RESET} ({', '.join(action_details)})")
        logging.info(f"👤 Target username: {BOLD}{username_target}{RESET}")
        # --- MODIFIED: Log focus/ignore filters ---
        if args.focus_subreddit: filter_details.append(f"Focus Subreddits = {args.focus_subreddit}")
        if args.ignore_subreddit: filter_details.append(f"Ignore Subreddits = {args.ignore_subreddit}")
        if args.start_date or args.end_date:
            start_str = args.start_date.strftime('%Y-%m-%d') if args.start_date else 'Beginning'
            end_str = args.end_date.strftime('%Y-%m-%d') if args.end_date else 'End'
            filter_details.append(f"Date = {start_str} to {end_str}")
        if filter_details: logging.info(f"    🔎 Filters Applied: {'; '.join(filter_details)}")
    elif action == "monitor": logging.info(f"🎬 Selected action: {BOLD}Monitor User{RESET}"); logging.info(f"👤 Target username: {BOLD}{username_target}{RESET}")
    elif action == "export_json_only": logging.info(f"🎬 Selected action: {BOLD}Export JSON Only{RESET}"); logging.info(f"👤 Target username: {BOLD}{username_target}{RESET}")
    elif action: logging.info(f"🎬 Selected action: {BOLD}{action.replace('_', ' ').title()}{RESET}")


    # --- Load AI Model (Conditionally) ---
    model = None
    genai = None
    ai_needed = action in ["generate_prompt", "monitor"] or \
                (action == "process_user_data" and args.run_analysis) or \
                (action == "process_user_data" and args.summarize_stats)

    if ai_needed:
        # (Keep AI loading logic - unchanged)
        logging.info(f"🤖 {BOLD}AI features required. Initializing AI Model...{RESET}")
        try:
            from ai_utils import generate_prompt_interactive, perform_ai_analysis
            from analysis import generate_mapped_analysis, generate_raw_analysis # Updated import signature usage later
            import google.generativeai as genai_imported
            genai = genai_imported
            logging.debug("AI modules imported successfully.")
        except ImportError as e:
            logging.critical(f"❌ Required Python packages for AI functionality not found: {e}")
            logging.critical("   Please install them: pip install google-generativeai")
            if action == "generate_prompt" or (action == "process_user_data" and (args.run_analysis or args.summarize_stats)): return
            logging.warning("   AI features will be unavailable.")

        if genai:
            if args.user_agent: current_config['user_agent'] = args.user_agent
            if args.model_name: current_config['default_model_name'] = args.model_name
            if args.monitor_interval: current_config['monitor_interval_seconds'] = args.monitor_interval

            api_key_to_use = os.environ.get("GOOGLE_API_KEY") or current_config.get("api_key") or args.api_key
            source_indicator = "ENV" if os.environ.get("GOOGLE_API_KEY") else ("config" if current_config.get("api_key") else ("flag" if args.api_key else "None"))

            if not api_key_to_use or api_key_to_use == "use_ur_own_keys_babe":
                logging.critical(f"❌ API Key missing or placeholder used for action '{BOLD}{action}{RESET}' (Source: {source_indicator}).")
                if action == "generate_prompt" or (action == "process_user_data" and (args.run_analysis or args.summarize_stats)): return
            else:
                logging.info(f"🔑 Using API key from {BOLD}{source_indicator}{RESET}.")
                model_name_to_use = current_config['default_model_name']
                logging.info(f"🧠 Attempting to configure Gemini model: {BOLD}{model_name_to_use}{RESET}")
                try:
                    genai.configure(api_key=api_key_to_use)
                    model = genai.GenerativeModel(model_name_to_use)
                    model.count_tokens("test") # Quick validation
                    logging.info(f"✅ Successfully configured Gemini model: {BOLD}{model_name_to_use}{RESET}")
                except Exception as e:
                    logging.critical(f"❌ Failed to configure Gemini model '{BOLD}{model_name_to_use}{RESET}': {e}", exc_info=args.log_level == "DEBUG")
                    model = None
                    if action == "generate_prompt" or (action == "process_user_data" and (args.run_analysis or args.summarize_stats)): return
                    logging.warning("   AI features will be unavailable.")
    else:
         logging.info(f"🤖 AI model not required for the selected action(s). Skipping AI setup.")

    logging.info("-" * 70)

    # --- Execute Action ---
    try:
        if action == "reset_config":
            # ... (Keep reset_config logic) ...
            logging.info(f"🔄 {BOLD}Resetting Configuration...{RESET}")
            try:
                save_config(DEFAULT_CONFIG)
                logging.info(f"✅ Configuration reset to defaults in {CYAN}config.json{RESET}")
            except Exception as e:
                logging.error(f"❌ Failed to reset configuration: {e}", exc_info=True)

        elif action == "generate_prompt":
            # ... (Keep generate_prompt logic) ...
            logging.info(f"🖊️ {BOLD}Starting Interactive Prompt Generation...{RESET}")
            if not model: logging.critical("❌ Model needed but failed to initialize."); return
            from ai_utils import generate_prompt_interactive # Import here
            prompt_dir = args.prompt_dir or current_config['default_prompt_dir']
            generate_prompt_interactive(model, prompt_dir)
            logging.info(f"✅ {BOLD}Interactive Prompt Generation Finished.{RESET}")

        elif action == "monitor":
            # ... (Keep monitor logic, using username_target) ...
            logging.info(f"👀 {BOLD}Starting User Monitoring...{RESET}")
            if not username_target: logging.critical("❌ Username required for monitoring."); return
            if not model: logging.warning("   ⚠️ AI Model not available for monitoring.")
            user_data_dir = os.path.join(args.output_dir, username_target); os.makedirs(user_data_dir, exist_ok=True)
            monitor_interval = current_config.get('monitor_interval_seconds', 180)
            if monitor_interval < 60: logging.warning(f"Monitor interval ({monitor_interval}s) low."); monitor_interval = 60
            prompt_file_path = args.prompt_file; system_prompt = "[Monitor Mode - Prompt Error]"
            try: logging.info(f"   Loading monitor base prompt from: {CYAN}{prompt_file_path}{RESET}"); f = open(prompt_file_path, "r", encoding="utf-8"); system_prompt = f.read().strip(); f.close();
            except Exception as e: logging.error(f"   ❌ Error reading monitor prompt file {CYAN}{prompt_file_path}{RESET}: {e}")
            if not system_prompt: logging.warning(f"   ⚠️ Monitor prompt file {CYAN}{prompt_file_path}{RESET} is empty.")
            if args.user_agent: current_config['user_agent'] = args.user_agent
            # Pass filter arguments to monitor_user (though it might not use them currently)
            monitor_user( username=username_target, user_data_dir=user_data_dir, config=current_config, interval_seconds=monitor_interval, model=model, system_prompt=system_prompt,
                chunk_size=args.chunk_size, sort_descending=(args.sort_order == "descending"), analysis_mode=args.analysis_mode, no_cache_titles=args.no_cache_titles, fetch_external_context=args.fetch_external_context,
                # Pass filters in case monitoring adds support later
                focus_subreddits=args.focus_subreddit, ignore_subreddits=args.ignore_subreddit)
            logging.info(f"🛑 {BOLD}Monitoring Stopped.{RESET}")

        elif action == "export_json_only":
             # ... (Keep export_json_only logic, using username_target) ...
             logging.info(f"--- {BOLD}Starting JSON Data Export Only{RESET} ---")
             if not username_target: logging.critical("❌ Username required for export."); return
             user_data_dir = os.path.join(args.output_dir, username_target); os.makedirs(user_data_dir, exist_ok=True)
             sort_descending = (args.sort_order == "descending")
             if args.user_agent: current_config['user_agent'] = args.user_agent
             logging.info(f"⚙️ Running data fetch/update for /u/{BOLD}{username_target}{RESET}...")
             json_path_actual = save_reddit_data( user_data_dir, username_target, current_config, sort_descending, args.scrape_comments_only, args.force_scrape )
             if json_path_actual and os.path.exists(json_path_actual): logging.info(f"✅ JSON data saved/updated successfully at: {CYAN}{json_path_actual}{RESET}"); logging.info(f"--- {BOLD}JSON Data Export Complete{RESET} ---")
             else: logging.error(f"❌ Failed to save/update JSON data for {username_target}."); logging.info(f"--- {BOLD}JSON Data Export Failed{RESET} ---")

        elif action == "compare_users":
            # --- Comparison Workflow ---
            logging.info(f"--- 👥 {BOLD}Starting User Comparison{RESET} ---")
            # Process user 1 (user1_comp) - UNFILTERED
            stats1 = process_single_user_for_stats(user1_comp, current_config, args)
            # Process user 2 (user2_comp) - UNFILTERED
            stats2 = process_single_user_for_stats(user2_comp, current_config, args)

            if stats1 and stats2:
                 logging.info(f"   ✅ Data processed for both users. Generating comparison report...")
                 comp_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                 comp_filename = f"comparison_{user1_comp}_vs_{user2_comp}_{comp_timestamp}.md"
                 comp_output_path = os.path.join(args.output_dir, comp_filename)
                 os.makedirs(args.output_dir, exist_ok=True) # Ensure main output dir exists
                 logging.info(f"   💾 Comparison report will be saved to: {CYAN}{comp_output_path}{RESET}")

                 # Call the comparison function from stats package
                 comp_success = generate_comparison_report(stats1, stats2, user1_comp, user2_comp, comp_output_path)
                 if comp_success: logging.info(f"--- ✅ {BOLD}User Comparison Complete{RESET} ---")
                 else: logging.error(f"--- ❌ {BOLD}User Comparison Report Generation Failed{RESET} ---")
            else:
                 logging.error(f"--- ❌ {BOLD}User Comparison Failed{RESET}. Could not process statistics for one or both users.")
            # --- End Comparison Workflow ---

        elif action == "process_user_data":
            # --- Single User Processing Workflow ---
            if not username_target: logging.critical("❌ Username required for processing."); return

            user_data_dir = os.path.join(args.output_dir, username_target); os.makedirs(user_data_dir, exist_ok=True)
            logging.info(f"📂 Using output directory: {CYAN}{user_data_dir}{RESET}")
            sort_descending = (args.sort_order == "descending")

            # Prepare filters
            start_ts = args.start_date.timestamp() if args.start_date else 0
            end_ts = (args.end_date.timestamp() + 86400) if args.end_date else float('inf')
            date_filter = (start_ts, end_ts)
            # focus_subreddits and ignore_subreddits are taken directly from args

            # --- Data Preparation ---
            logging.info(f"--- {BOLD}Starting Data Preparation Phase{RESET} ---")
            if args.user_agent: current_config['user_agent'] = args.user_agent
            logging.info(f"⚙️ Running data fetch/update for /u/{BOLD}{username_target}{RESET}...")
            json_path_actual = save_reddit_data( user_data_dir, username_target, current_config, sort_descending, args.scrape_comments_only, args.force_scrape )
            if not json_path_actual or not os.path.exists(json_path_actual):
                 json_path_expected = os.path.join(user_data_dir, f"{username_target}.json")
                 if os.path.exists(json_path_expected): json_path_actual = json_path_expected; logging.warning(f"   ⚠️ Using existing JSON: {CYAN}{json_path_actual}{RESET}")
                 else: logging.error(f"❌ Scraping failed and no JSON file found: {CYAN}{json_path_expected}{RESET}"); return
            else: logging.info(f"   ✅ Scrape/Update successful: {CYAN}{json_path_actual}{RESET}")

            logging.info(f"⚙️ Converting JSON data to CSV (applying filters if specified)...")
            # --- MODIFIED: Pass focus/ignore lists to extract_csvs_from_json ---
            posts_csv, comments_csv = extract_csvs_from_json(
                json_path_actual,
                os.path.join(user_data_dir, username_target),
                date_filter=date_filter,
                focus_subreddits=args.focus_subreddit, # Pass list or None
                ignore_subreddits=args.ignore_subreddit # Pass list or None
            )
            posts_csv_path = posts_csv if posts_csv else os.path.join(user_data_dir, f"{username_target}-posts.csv")
            comments_csv_path = comments_csv if comments_csv else os.path.join(user_data_dir, f"{username_target}-comments.csv")
            posts_csv_exists = os.path.exists(posts_csv_path); comments_csv_exists = os.path.exists(comments_csv_path)
            if not posts_csv_exists and not comments_csv_exists: logging.error("❌ CSV conversion produced no files (check filters). Cannot proceed."); return
            elif not posts_csv_exists: logging.info(f"   📄 Comments CSV: {CYAN}{comments_csv_path}{RESET} (Posts filtered/empty)")
            elif not comments_csv_exists: logging.info(f"   📄 Posts CSV: {CYAN}{posts_csv_path}{RESET} (Comments filtered/empty)")
            else: logging.info(f"   📄 Posts CSV: {CYAN}{posts_csv_path}{RESET}"); logging.info(f"   📄 Comments CSV: {CYAN}{comments_csv_path}{RESET}")
            logging.info(f"--- {BOLD}Data Preparation Complete{RESET} ---"); logging.info("-" * 70)


            # --- Fetch About Data ---
            user_about_data = None
            if args.generate_stats:
                 logging.info(f"--- {BOLD}Fetching User About Data{RESET} ---")
                 user_about_data = _fetch_user_about_data(username_target, current_config)
                 if user_about_data is None: logging.warning("   ⚠️ Failed to fetch user 'about' data.")
                 else: logging.info("   ✅ User 'about' data fetched successfully.")
                 logging.info("-" * 70)


            # --- Statistics Generation ---
            stats_report_path = None
            stats_results_data = None
            if args.generate_stats:
                 logging.info(f"--- 📊 {BOLD}Starting Statistics Generation Phase{RESET} ---")
                 stats_timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                 stats_output_filename = f"{username_target}_stats_{stats_timestamp_str}.md"
                 stats_output_path = os.path.join(user_data_dir, stats_output_filename)
                 stats_json_output_path = args.stats_output_json
                 logging.info(f"   ⚙️ Calculating statistics (applying filters)...")
                 # --- MODIFIED: Pass focus/ignore lists to generate_stats_report ---
                 stats_success, stats_results_data = generate_stats_report(
                     json_path=json_path_actual, about_data=user_about_data,
                     posts_csv_path=posts_csv_path if posts_csv_exists else None, comments_csv_path=comments_csv_path if comments_csv_exists else None,
                     username=username_target, output_path=stats_output_path, stats_json_path=stats_json_output_path,
                     date_filter=date_filter,
                     focus_subreddits=args.focus_subreddit, # Pass list or None
                     ignore_subreddits=args.ignore_subreddit, # Pass list or None
                     top_n_words=args.top_words, top_n_items=args.top_items,
                     write_md_report=True, write_json_report=True # Explicitly write reports
                 )
                 if stats_success: logging.info(f"--- ✅ {BOLD}Statistics Generation Complete{RESET} ---"); stats_report_path = stats_output_path
                 else: logging.error(f"--- ❌ {BOLD}Statistics Generation Failed{RESET} ---")
                 logging.info("-" * 70)


            # --- AI Summary of Stats ---
            # (Keep AI Summary logic - unchanged)
            if args.summarize_stats:
                 if not model: logging.critical("❌ AI Stats Summary requested but AI model failed."); return
                 if not stats_report_path or not os.path.exists(stats_report_path): logging.error(f"❌ Cannot summarize stats: Report not found ({CYAN}{stats_report_path or 'N/A'}{RESET}).")
                 else:
                     logging.info(f"--- 🧠 {BOLD}Starting AI Summary of Statistics Report{RESET} ---")
                     logging.info(f"   📄 Reading stats report: {CYAN}{stats_report_path}{RESET}")
                     try:
                         with open(stats_report_path, "r", encoding="utf-8") as f_stats: stats_content = f_stats.read()
                         # Add filter info to summary prompt if available
                         filter_context = []
                         if args.focus_subreddit: filter_context.append(f"focused on subreddits: {', '.join(args.focus_subreddit)}")
                         if args.ignore_subreddit: filter_context.append(f"ignoring subreddits: {', '.join(args.ignore_subreddit)}")
                         if args.start_date or args.end_date:
                             start_str = args.start_date.strftime('%Y-%m-%d') if args.start_date else 'Beginning'
                             end_str = args.end_date.strftime('%Y-%m-%d') if args.end_date else 'End'
                             filter_context.append(f"date range: {start_str} to {end_str}")
                         filter_str = f" (Note: Data filters applied - {'; '.join(filter_context)})" if filter_context else ""

                         summary_prompt = (f"Summarize the key findings (3-5 bullet points) from this Reddit user statistics report for /u/{username_target}{filter_str}:\n\n"
                                           f"--- REPORT START ---\n{stats_content}\n--- REPORT END ---\n\n**Summary:**")
                         logging.info("   🤖 Requesting summary from AI...")
                         summary_start_time = time.time()
                         try:
                             response = model.generate_content(summary_prompt,
                                         generation_config=genai.GenerationConfig(temperature=0.5, max_output_tokens=1024),
                                         safety_settings={'HARM_CATEGORY_HARASSMENT': 'BLOCK_LOW_AND_ABOVE', 'HARM_CATEGORY_HATE_SPEECH': 'BLOCK_LOW_AND_ABOVE',
                                                          'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'BLOCK_NONE', 'HARM_CATEGORY_DANGEROUS_CONTENT': 'BLOCK_LOW_AND_ABOVE'})
                             summary_duration = time.time() - summary_start_time
                             if response and hasattr(response, 'text') and response.text:
                                 summary_text = response.text.strip()
                                 logging.info(f"   ✅ AI summary generated successfully ({summary_duration:.2f}s).")
                                 with open(stats_report_path, "a", encoding="utf-8") as f_append: f_append.write(f"\n\n---\n\n## VII. AI-Generated Summary\n\n{summary_text}\n"); logging.info(f"   💾 Summary appended to: {CYAN}{stats_report_path}{RESET}")
                             else: logging.error(f"   ❌ AI summary generation failed or was blocked ({summary_duration:.2f}s). Reason: {response.prompt_feedback.block_reason if response and response.prompt_feedback else 'Unknown'}")
                         except Exception as ai_err: logging.error(f"   ❌ Exception during AI summary generation: {ai_err}", exc_info=args.log_level == "DEBUG")
                     except IOError as read_err: logging.error(f"   ❌ Error reading stats report file for summary: {read_err}")
                     logging.info(f"--- {BOLD}AI Summary Generation Finished{RESET} ---"); logging.info("-" * 70)


            # --- AI Analysis ---
            if args.run_analysis:
                 if not model: logging.critical("❌ AI Analysis requested but model failed."); return
                 try: from analysis import generate_mapped_analysis, generate_raw_analysis
                 except ImportError as e: logging.critical(f"❌ Failed to import analysis functions: {e}"); return

                 logging.info(f"--- 🤖 {BOLD}Starting AI Analysis Phase{RESET} ({BOLD}{args.analysis_mode}{RESET} mode) ---")
                 logging.info(f"   📄 Loading System Prompt...")
                 prompt_file_path = args.prompt_file; system_prompt = ""
                 try:
                     with open(prompt_file_path, "r", encoding="utf-8") as f: system_prompt = f.read().strip()
                     if not system_prompt: logging.warning(f"   ⚠️ Prompt file {CYAN}{prompt_file_path}{RESET} is empty.")
                     else: logging.info(f"   ✅ System prompt loaded ({len(system_prompt)} chars).")
                 except Exception as e: logging.critical(f"❌ Failed to load system prompt {CYAN}{prompt_file_path}{RESET}: {e}"); return

                 ai_timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                 output_filename = f"{username_target}_charc_{args.analysis_mode}_{ai_timestamp_str}.md"
                 output_md_file = os.path.join(user_data_dir, output_filename)
                 logging.info(f"   💾 AI analysis output file: {CYAN}{output_md_file}{RESET}")

                 logging.info(f"   ⚙️ Preparing data and initiating AI analysis (applying filters)...")
                 analysis_success = False
                 # --- MODIFIED: Pass focus/ignore lists to analysis functions ---
                 # (Note: analysis.py itself might not use them for filtering if it relies on CSVs)
                 analysis_func_args = {
                     "output_file": output_md_file,
                     "model": model,
                     "system_prompt": system_prompt,
                     "chunk_size": args.chunk_size,
                     "date_filter": date_filter,
                     "focus_subreddits": args.focus_subreddit,
                     "ignore_subreddits": args.ignore_subreddit
                 }

                 posts_csv_input = posts_csv_path if posts_csv_exists else None
                 comments_csv_input = comments_csv_path if comments_csv_exists else None
                 if not posts_csv_input and not comments_csv_input: logging.error("❌ Cannot run AI analysis: No valid CSV data found (check filters)."); return

                 from analysis import generate_mapped_analysis, generate_raw_analysis

                 if args.analysis_mode == "raw":
                     analysis_success = generate_raw_analysis( posts_csv=posts_csv_input, comments_csv=comments_csv_input, **analysis_func_args )
                 else: # mapped
                     analysis_success = generate_mapped_analysis( posts_csv=posts_csv_input, comments_csv=comments_csv_input, config=current_config,
                                                                no_cache_titles=args.no_cache_titles, fetch_external_context=args.fetch_external_context,
                                                                **analysis_func_args )

                 if analysis_success: logging.info(f"--- ✅ {BOLD}AI Analysis Complete{RESET} ---")
                 else: logging.error(f"--- ❌ {BOLD}AI Analysis Failed{RESET} ---")
                 logging.info("-" * 70)

        else:
             logging.error(f"❌ Unhandled action scenario: {action}")

    except Exception as e:
         logging.critical(f"🔥 An unexpected CRITICAL error occurred in main execution: {e}", exc_info=True)
         sys.exit(1)

if __name__ == "__main__":
    main()