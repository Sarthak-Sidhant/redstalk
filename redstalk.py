#!/usr/bin/env python3
# redstalk.py
import os
import argparse
import logging
import sys
from datetime import datetime

# --- Import Utilities ---
from config_utils import load_config, save_config, DEFAULT_CONFIG
from reddit_utils import save_reddit_data # Import only save_reddit_data from here
from stats_utils import generate_stats_report, _fetch_user_about_data # <--- CORRECT MODULE
from data_utils import extract_csvs_from_json
# Import AI utils conditionally later if needed
# from ai_utils import generate_prompt_interactive, perform_ai_analysis
# Import analysis functions conditionally later if needed
# from analysis import generate_mapped_analysis, generate_raw_analysis
from monitoring import monitor_user # monitor_user itself might conditionally import AI stuff if auto-analysis is enabled
from stats_utils import generate_stats_report

# Import google.generativeai conditionally later

def main():
    # --- Load Initial Config ---
    # Load config at the start to make defaults available to argparse
    current_config = load_config()

    parser = argparse.ArgumentParser(
        description="Reddit Scraper & AI Character Profiler v1.6", # Version bump
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # --- Actions ---
    # Action flags - These are not mutually exclusive anymore, allowing combinations like analysis + stats
    parser.add_argument("username", nargs='?', default=None,
                        help="Reddit username for analysis or stats generation. Required if not using --generate-prompt or --monitor.")
    parser.add_argument("--generate-prompt", action="store_true",
                        help="Run the interactive prompt generation assistant (exclusive action).")
    parser.add_argument("--monitor", metavar='USERNAME', default=None,
                        help="Monitor the specified Reddit user for new activity (exclusive action).")
    parser.add_argument("--reset-config", action="store_true",
                        help="Reset config.json to default values and exit (exclusive action).")
    parser.add_argument("--run-analysis", action="store_true",
                        help="Perform AI analysis on the specified username.")
    parser.add_argument("--generate-stats", action="store_true",
                        help="Generate a non-AI statistics report for the specified username.")

    # --- General Options ---
    parser.add_argument("--output-dir", default=current_config['default_output_dir'],
                        help="Base directory for all output data.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level.")
    parser.add_argument("--user-agent", default=None,
                        help="Custom User-Agent for Reddit API requests. Overrides config value.")

    # --- Scraping Options (Used by Stats & Analysis & Monitor) ---
    scraping_group = parser.add_argument_group('Scraping Options')
    scraping_group.add_argument("--force-scrape", action="store_true",
                                help="Force scraping even if username.json exists (updates existing).")
    scraping_group.add_argument("--scrape-comments-only", action="store_true",
                                help="Only scrape comments, skip submitted posts.")
    scraping_group.add_argument("--sort-order", default="descending", choices=["ascending", "descending"],
                                help="Sort order for scraped data. 'descending' = newest first.")

    # --- AI Analysis Options (Only relevant if --run-analysis is specified) ---
    analysis_group = parser.add_argument_group('AI Analysis Options (used with --run-analysis)')
    analysis_group.add_argument("--prompt-file", default=current_config['default_prompt_file'],
                                help="Path to the file containing the system prompt text for analysis.")
    analysis_group.add_argument("--api-key", default=None,
                                help="Google Gemini API Key. Priority: ENV -> config.json -> this flag.")
    analysis_group.add_argument("--model-name", default=None,
                                help=f"Gemini model name. Overrides config. Default: {current_config['default_model_name']}")
    analysis_group.add_argument("--chunk-size", type=int, default=current_config['default_chunk_size'],
                                help="Target maximum tokens per chunk for large data analysis.")
    analysis_group.add_argument("--analysis-mode", default="mapped", choices=["mapped", "raw"],
                                help="Analysis format: 'mapped' (grouped) or 'raw' (sequential).")
    analysis_group.add_argument("--no-cache-titles", action="store_true",
                                help="Disable caching of fetched post titles (for mapped analysis debugging).")
    analysis_group.add_argument("--fetch-external-context", action="store_true",
                                help="In mapped analysis, fetch titles for external posts (slower). Default is OFF.")

    # --- Statistics Options (Only relevant if --generate-stats is specified) ---
    stats_group = parser.add_argument_group('Statistics Options (used with --generate-stats)')
    stats_group.add_argument("--top-words", type=int, default=50,
                             help="Number of top words to show in the stats report frequency list.")
    stats_group.add_argument("--top-items", type=int, default=5,
                             help="Number of top/bottom scored posts/comments to show in the stats report.")

    # --- Prompt Generation Options (Only relevant if --generate-prompt is specified) ---
    prompt_gen_group = parser.add_argument_group('Prompt Generation Options (used with --generate-prompt)')
    prompt_gen_group.add_argument("--prompt-dir", default=current_config['default_prompt_dir'],
                                  help="Directory to store generated prompts.")
    # Prompt gen also uses --api-key and --model-name from analysis_group

    # --- Monitoring Options (Only relevant if --monitor is specified) ---
    monitor_group = parser.add_argument_group('Monitoring Options (used with --monitor)')
    monitor_group.add_argument("--monitor-interval", type=int, default=None,
                               help=f"Check interval in seconds for monitoring. Overrides config. Default: {current_config['monitor_interval_seconds']}")
    # Monitoring also uses relevant AI and scraping options

    args = parser.parse_args()

    # --- Logging Setup ---
    log_level = args.log_level.upper()
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
        level=log_level, datefmt='%Y-%m-%d %H:%M:%S',
    )
    if log_level != "DEBUG":
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

    # --- Handle Exclusive Actions First ---
    if args.reset_config:
        logging.info("Resetting configuration file config.json to defaults...")
        if save_config(DEFAULT_CONFIG): logging.info("Config reset successfully. Exiting.")
        else: logging.error("Failed to reset config file.")
        return

    if args.generate_prompt:
        action = "generate_prompt"
        username_to_process = None # Not directly needed for the action
        logging.info(f"Selected action: {action}")
    elif args.monitor:
        action = "monitor"
        username_to_process = args.monitor
        if not username_to_process: parser.error("--monitor requires a USERNAME.")
        logging.info(f"Selected action: {action}")
        logging.info(f"Target username: {username_to_process}")
    elif args.username and (args.run_analysis or args.generate_stats):
        # Handle the main processing action
        action = "process_user_data"
        username_to_process = args.username
        action_details = []
        if args.run_analysis: action_details.append("AI Analysis")
        if args.generate_stats: action_details.append("Statistics")
        logging.info(f"Selected action: Process User Data ({', '.join(action_details)})")
        logging.info(f"Target username: {username_to_process}")
    elif args.username:
        # Username provided but no specific action flag given
        parser.error(f"Username '{args.username}' provided, but no action specified. Use --run-analysis or --generate-stats.")
    else:
        # No exclusive action and no username provided
        parser.error("No action specified. Provide a username and use --run-analysis or --generate-stats, or use --generate-prompt or --monitor.")

    # --- Load AI Model (ONLY if needed for the determined action) ---
    model = None
    ai_needed = action in ["generate_prompt", "monitor"] or (action == "process_user_data" and args.run_analysis)

    if ai_needed:
        try:
            # Import AI-related modules only when needed
            from ai_utils import generate_prompt_interactive, perform_ai_analysis
            from analysis import generate_mapped_analysis, generate_raw_analysis
            import google.generativeai as genai
            logging.debug("AI modules imported successfully.")
        except ImportError as e:
            logging.critical(f"Required Python packages for AI functionality not found: {e}")
            logging.critical("Please install them: pip install google-generativeai")
            # Exit if AI is strictly required for the chosen action
            if action == "generate_prompt" or (action == "process_user_data" and args.run_analysis):
                 return
            # Allow monitor to potentially proceed without AI, but log critical error
            logging.warning("AI features will be unavailable for monitoring.")

        if 'genai' in sys.modules: # Check if import succeeded before proceeding
            # Consolidate relevant config updates from args that affect AI/API calls
            if args.user_agent: current_config['user_agent'] = args.user_agent
            if args.model_name: current_config['default_model_name'] = args.model_name
            if args.monitor_interval: current_config['monitor_interval_seconds'] = args.monitor_interval # Used by monitor action

            # Determine API Key
            api_key_to_use = os.environ.get("GOOGLE_API_KEY") or current_config.get("api_key") or args.api_key
            source_indicator = "ENV" if os.environ.get("GOOGLE_API_KEY") else ("config" if current_config.get("api_key") else ("flag" if args.api_key else "None"))

            if not api_key_to_use:
                logging.critical(f"API Key missing for action '{action}' (Source: {source_indicator}). Provide via ENV, config, or flag.")
                # Exit if AI is strictly required
                if action == "generate_prompt" or (action == "process_user_data" and args.run_analysis): return
            else:
                logging.info(f"Using API key from {source_indicator}.")

                # Configure Model
                model_name_to_use = current_config['default_model_name']
                logging.info(f"Attempting to use Gemini model: {model_name_to_use}")
                try:
                    genai.configure(api_key=api_key_to_use)
                    model = genai.GenerativeModel(model_name_to_use)
                    _ = model.count_tokens("test") # Quick check
                    logging.info(f"Successfully configured Gemini model: {model_name_to_use}")
                except Exception as e:
                    logging.critical(f"Failed to configure Gemini model '{model_name_to_use}': {e}", exc_info=True)
                    model = None # Ensure model is None if setup fails
                    # Exit if AI needed but failed
                    if action == "generate_prompt" or (action == "process_user_data" and args.run_analysis): return
                    logging.warning("AI features will be unavailable.")
    else:
         logging.info("AI model not required for the selected action(s). Skipping AI setup.")

    # --- Execute Action ---
    try:
        if action == "generate_prompt":
            if not model: logging.critical("Model needed for prompt generation but failed to initialize."); return
            # Import generate_prompt_interactive now that we know AI is needed and loaded
            from ai_utils import generate_prompt_interactive
            prompt_dir = args.prompt_dir or current_config['default_prompt_dir']
            generate_prompt_interactive(model, prompt_dir)

        elif action == "monitor":
            if not username_to_process: logging.critical("Username required for monitoring."); return
            if not model: logging.warning("AI Model not available. Monitoring cannot trigger AI analysis on update.")

            user_data_dir = os.path.join(args.output_dir, username_to_process); os.makedirs(user_data_dir, exist_ok=True)
            monitor_interval = current_config.get('monitor_interval_seconds', 180) # Use updated value or default
            if monitor_interval < 60: logging.warning(f"Monitor interval ({monitor_interval}s) low."); monitor_interval = 60

            # Load prompt details (even if model is None, for potential future use)
            prompt_file_path = args.prompt_file; system_prompt = "[Monitor Mode - Prompt Error]"
            try:
                with open(prompt_file_path, "r", encoding="utf-8") as f: system_prompt = f.read().strip()
                if not system_prompt: logging.warning(f"Monitor prompt file {prompt_file_path} is empty.")
            except Exception as e: logging.error(f"Error reading monitor prompt file {prompt_file_path}: {e}")

            # Update config user_agent from args if provided
            if args.user_agent: current_config['user_agent'] = args.user_agent

            monitor_user( # Function from monitoring.py
                username=username_to_process, user_data_dir=user_data_dir, config=current_config,
                interval_seconds=monitor_interval, model=model, system_prompt=system_prompt,
                chunk_size=args.chunk_size, sort_descending=(args.sort_order == "descending"),
                analysis_mode=args.analysis_mode, no_cache_titles=args.no_cache_titles,
                fetch_external_context=args.fetch_external_context
            )

        elif action == "process_user_data":
            if not username_to_process: logging.critical("Username required for processing."); return

            user_data_dir = os.path.join(args.output_dir, username_to_process); os.makedirs(user_data_dir, exist_ok=True)
            logging.info(f"Using output directory: {user_data_dir}")
            sort_descending = (args.sort_order == "descending")

            # --- Data Preparation (Common for Stats & Analysis) ---
            logging.info(f"--- Starting Data Preparation ---")
            # Update config user_agent from args if provided before scraping
            if args.user_agent: current_config['user_agent'] = args.user_agent
            json_path_actual = save_reddit_data( user_data_dir, username_to_process, current_config, sort_descending, args.scrape_comments_only, args.force_scrape )
            if not json_path_actual or not os.path.exists(json_path_actual):
                json_path_expected = os.path.join(user_data_dir, f"{username_to_process}.json")
                if os.path.exists(json_path_expected): json_path_actual = json_path_expected
                else: logging.error("Scraping failed and no JSON file found."); return
            logging.info("Scrape successful.")

            posts_csv, comments_csv = extract_csvs_from_json(json_path_actual, os.path.join(user_data_dir, username_to_process))
            posts_csv_path = posts_csv if posts_csv else os.path.join(user_data_dir, f"{username_to_process}-posts.csv")
            comments_csv_path = comments_csv if comments_csv else os.path.join(user_data_dir, f"{username_to_process}-comments.csv")
            if not os.path.exists(posts_csv_path) and not os.path.exists(comments_csv_path):
                 logging.error("CSV conversion failed or produced no files. Cannot generate stats or analysis.")
                 return
            logging.info("CSV conversion successful.")
            logging.info(f"--- Data Preparation Complete ---")

            # --- Fetch About Data (Needed for Stats) ---
            user_about_data = None
            if args.generate_stats:
                 logging.info(f"--- Fetching User About Data ---")
                 user_about_data = _fetch_user_about_data(username_to_process, current_config)
                 if user_about_data is None:
                      logging.warning("Failed to fetch user 'about' data. Total karma stats will be unavailable.")

            # --- Statistics Generation Phase (Conditional) ---
            if args.generate_stats:
                 logging.info(f"--- Starting Statistics Generation Phase ---")
                 stats_timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                 stats_output_filename = f"{username_to_process}_stats_{stats_timestamp_str}.md"
                 stats_output_path = os.path.join(user_data_dir, stats_output_filename)

                 stats_success = generate_stats_report( # from stats_utils.py
                     json_path=json_path_actual,
                     about_data=user_about_data, # Pass fetched data
                     posts_csv_path=posts_csv_path if os.path.exists(posts_csv_path) else None,
                     comments_csv_path=comments_csv_path if os.path.exists(comments_csv_path) else None,
                     username=username_to_process, output_path=stats_output_path,
                     top_n_words=args.top_words, top_n_items=args.top_items
                 )
                 if stats_success: logging.info("--- Statistics Generation Complete ---")
                 else: logging.error("--- Statistics Generation Failed ---")

            # --- AI Analysis Phase (Conditional) ---
            if args.run_analysis:
                if not model: logging.critical("AI Analysis requested but model failed to initialize."); return
                # Ensure analysis functions are available if AI is needed
                try:
                    from analysis import generate_mapped_analysis, generate_raw_analysis
                except ImportError as e:
                     logging.critical(f"Failed to import analysis functions: {e}"); return

                logging.info(f"--- Loading System Prompt for AI Analysis ---")
                prompt_file_path = args.prompt_file; system_prompt = ""
                try:
                    with open(prompt_file_path, "r", encoding="utf-8") as f: system_prompt = f.read().strip()
                    if not system_prompt: logging.warning(f"Prompt file {prompt_file_path} is empty.")
                except Exception as e: logging.critical(f"Failed to load system prompt {prompt_file_path}: {e}", exc_info=True); return

                logging.info(f"--- Starting AI Analysis Phase ({args.analysis_mode} mode) ---")
                ai_timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_filename = f"{username_to_process}_charc_{args.analysis_mode}_{ai_timestamp_str}.md"
                output_md_file = os.path.join(user_data_dir, output_filename)
                logging.info(f"AI analysis output file will be: {output_md_file}")

                analysis_success = False
                analysis_func_args = {"output_file": output_md_file, "model": model, "system_prompt": system_prompt, "chunk_size": args.chunk_size}

                if args.analysis_mode == "raw":
                    analysis_success = generate_raw_analysis( posts_csv=posts_csv_path if os.path.exists(posts_csv_path) else None, comments_csv=comments_csv_path if os.path.exists(comments_csv_path) else None, **analysis_func_args )
                else: # mapped
                    analysis_success = generate_mapped_analysis( posts_csv=posts_csv_path if os.path.exists(posts_csv_path) else None, comments_csv=comments_csv_path if os.path.exists(comments_csv_path) else None, config=current_config, no_cache_titles=args.no_cache_titles, fetch_external_context=args.fetch_external_context, **analysis_func_args )

                if analysis_success: logging.info("--- AI Analysis Complete ---")
                else: logging.error("--- AI Analysis Failed ---")

        else:
             # This path should not be reached due to argument parsing logic
             logging.error(f"Unhandled action scenario: {action}")

    except Exception as e:
         # Catch-all for unexpected errors in the main execution flow
         logging.critical(f"An unexpected error occurred in main execution: {e}", exc_info=True)
         sys.exit(1) # Exit with error status


if __name__ == "__main__":
    main()