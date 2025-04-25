# monitor.py
"""
This module provides functionality to monitor a specific Reddit user for
new activity (posts and comments) at a defined interval. When new activity
is detected, it triggers a re-scrape to update the local data files and
optionally triggers a new AI analysis on the updated dataset.
"""

# Import standard libraries
import logging # For logging information and errors
import time # For implementing sleep intervals between checks
import os # For file path manipulation and existence checks
from datetime import datetime # For getting the current time for logging iteration start/end

# Import necessary utility functions from other modules
# These functions are assumed to be available in the project structure.
from reddit_utils import get_reddit_data, save_reddit_data, get_modification_date, load_existing_data, format_timestamp
from data_utils import extract_csvs_from_json
# Analysis functions are imported conditionally *inside* monitor_user
# only when an analysis is triggered, to avoid circular dependencies if not needed.
# from analysis import generate_raw_analysis, generate_mapped_analysis # Not imported globally

# Import ANSI codes for coloring console output
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\031m"

def monitor_user(username, user_data_dir, config, interval_seconds, model, system_prompt, chunk_size, sort_descending, analysis_mode, no_cache_titles, fetch_external_context):
    """
    Starts a monitoring loop for a specific Reddit user.

    It periodically checks for new posts and comments since the last known
    activity timestamp recorded in the user's data JSON file. If new activity
    is found, it updates the JSON and CSV files and optionally triggers
    a new AI analysis on the updated dataset.

    Args:
        username (str): The Reddit username to monitor.
        user_data_dir (str): The directory where the user's data files are stored.
        config (dict): The application configuration dictionary.
        interval_seconds (int): The time interval (in seconds) between activity checks.
        model: The AI model instance (from genai) to use for analysis, or None
               if auto-analysis is disabled.
        system_prompt (str): The system prompt for AI analysis (only used if
                           auto-analysis is enabled).
        chunk_size (int): The maximum token size for AI analysis chunks (only used
                          if auto-analysis is enabled).
        sort_descending (bool): Whether to sort scraped data by timestamp descending.
                                (Used during re-scrape on update).
        analysis_mode (str): 'raw' or 'mapped' analysis mode (only used if
                             auto-analysis is enabled).
        no_cache_titles (bool): Disable caching for external post titles (only used
                                if mapped analysis + fetch_external_context enabled).
        fetch_external_context (bool): Fetch external post titles for comments
                                       (only used if mapped analysis enabled).
    """
    logging.info(f"👀 Starting real-time monitoring for user '{BOLD}{username}{RESET}'. Checking every {interval_seconds} seconds.")
    # Provide user feedback in the console indicating monitoring has started.
    print(f"👀 Monitoring /u/{username}... Press Ctrl+C to stop.")

    # Construct the expected path to the user's main JSON data file.
    json_path = os.path.join(user_data_dir, f"{username}.json")

    # Initialize variables to store the timestamps of the latest known post and comment.
    # These will be updated as new activity is detected and data is saved.
    last_known_post_time = 0
    last_known_comment_time = 0

    # Flag to determine if AI analysis should be run automatically when new data is found.
    # Currently hardcoded to False, but could be a command-line argument.
    run_analysis_on_update = False # Keep auto-analysis disabled by default

    def get_latest_timestamp_from_file(kind):
        """
        Helper function to find the timestamp of the newest item of a specific
        kind ('t3' for posts, 't1' for comments) in the user's JSON data file.
        Assumes the data for each kind is stored as a dictionary keyed by item ID.
        Assumes the data is saved in descending order by timestamp (newest first)
        by the `save_reddit_data` function.
        """
        logging.debug(f"      Checking latest timestamp for {kind} in {CYAN}{json_path}{RESET}")
        # Check if the JSON file exists.
        if not os.path.exists(json_path):
            logging.debug(f"         JSON file not found, returning 0.")
            return 0 # Return 0 if file is missing

        try:
            # Load the existing data from the JSON file using the utility function.
            # This function handles file reading errors and empty files.
            data = load_existing_data(json_path)
            # Check if the data contains the specified kind ('t3' or 't1') and if it's a non-empty dictionary.
            if kind in data and isinstance(data[kind], dict) and data[kind]:
                 # Get the first item's ID in the dictionary. Since `save_reddit_data`
                 # sorts by timestamp descending, the first item should be the newest.
                 first_item_id = next(iter(data[kind]))
                 # Get the modification date timestamp of the first item using the utility function.
                 latest_ts = get_modification_date(data[kind][first_item_id])
                 logging.debug(f"         Found latest {kind} timestamp: {format_timestamp(latest_ts)}")
                 return latest_ts # Return the timestamp

            # If the specified kind is not in the data or is empty/not a dict.
            logging.debug(f"         No data of kind '{kind}' found in JSON.")
            return 0 # Return 0 if no data of this kind exists

        except (KeyError, StopIteration, Exception) as e:
            # Catch errors during data access or iteration.
            logging.error(f"      ❌ Error reading latest timestamp for kind '{kind}' from {CYAN}{json_path}{RESET}: {e}")
            return 0 # Return 0 on error

    # Get the initial latest known timestamps from the existing data file before starting the loop.
    last_known_post_time = get_latest_timestamp_from_file("t3")
    last_known_comment_time = get_latest_timestamp_from_file("t1")
    logging.info(f"   Initial latest known times: Post={BOLD}{format_timestamp(last_known_post_time)}{RESET}, Comment={BOLD}{format_timestamp(last_known_comment_time)}{RESET}")

    try:
        iteration = 0
        # Start the infinite monitoring loop.
        while True:
            iteration += 1
            new_activity_detected = False # Flag to track if any new activity was found in this iteration
            logging.info(f"--- Monitor Iteration {iteration} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ---") # Log iteration start time
            logging.info(f"   🔍 Checking for new activity for /u/{BOLD}{username}{RESET}...")
            check_start_time = time.time() # Start timer for the check duration

            # --- Check Submitted Posts ---
            try:
                logging.debug("      Checking submitted posts (limit 5)...")
                # Fetch the 5 most recent submitted posts for the user.
                latest_posts_data = get_reddit_data(username, "submitted", config, limit=5)

                # Check if data was successfully fetched and has the expected structure.
                if latest_posts_data and isinstance(latest_posts_data.get("data"), dict) and isinstance(latest_posts_data["data"].get("children"), list):
                    # Iterate through the fetched posts (newest first).
                    for post_entry in latest_posts_data["data"]["children"]:
                         # Validate the structure of each individual entry.
                         if isinstance(post_entry, dict) and "data" in post_entry:
                            # Get the timestamp of the current post.
                            post_time = get_modification_date(post_entry)
                            # Compare with the last known post time.
                            if post_time > last_known_post_time:
                                logging.info(f"      ✨ {GREEN}New post detected!{RESET} ID: {post_entry['data'].get('id', 'N/A')}, Time: {format_timestamp(post_time)}")
                                new_activity_detected = True # Set flag
                                last_known_post_time = post_time # Update the marker immediately to avoid logging same item repeatedly
                            else:
                                # Since data is newest first, if we find an item that's not newer,
                                # the rest will also be older. We can stop checking posts.
                                logging.debug("         Post is older than last known, stopping post check.")
                                break # Exit the loop for posts

                         else:
                              # Log a warning if an individual entry has an unexpected structure.
                              logging.warning("      ⚠️ Malformed post entry structure during monitor check.")
                elif latest_posts_data is None:
                     # Log a warning if the data fetch itself failed.
                     logging.warning("      ⚠️ Failed to fetch recent posts during monitor check (error logged previously).")
                else:
                     # Log if no posts were returned or the 'children' list was empty.
                     logging.debug("      No recent posts found or empty children list.")
                time.sleep(1) # Add a small delay after the post check.
            except Exception as e:
                 # Catch and log any unexpected errors during the post checking process.
                 logging.error(f"   ❌ Error during monitoring check for posts: {e}", exc_info=True)

            # --- Check Comments ---
            # Similar logic as checking posts, but for comments ('comments' endpoint).
            try:
                logging.debug("      Checking comments (limit 5)...")
                # Fetch the 5 most recent comments for the user.
                latest_comments_data = get_reddit_data(username, "comments", config, limit=5)
                if latest_comments_data and isinstance(latest_comments_data.get("data"), dict) and isinstance(latest_comments_data["data"].get("children"), list):
                    # Iterate through the fetched comments (newest first).
                    for comment_entry in latest_comments_data["data"]["children"]:
                         # Validate individual entry structure.
                         if isinstance(comment_entry, dict) and "data" in comment_entry:
                            # Get the timestamp of the current comment.
                            comment_time = get_modification_date(comment_entry)
                            # Compare with the last known comment time.
                            if comment_time > last_known_comment_time:
                                logging.info(f"      ✨ {GREEN}New comment detected!{RESET} ID: {comment_entry['data'].get('id', 'N/A')}, Time: {format_timestamp(comment_time)}")
                                new_activity_detected = True # Set flag
                                last_known_comment_time = comment_time # Update the marker immediately
                            else:
                                # If not newer, the rest will also be older.
                                logging.debug("         Comment is older than last known, stopping comment check.")
                                break # Exit the loop for comments
                         else:
                              # Log warning for malformed entry.
                              logging.warning("      ⚠️ Malformed comment entry structure during monitor check.")
                elif latest_comments_data is None:
                     # Log warning if fetch failed.
                     logging.warning("      ⚠️ Failed to fetch recent comments during monitor check (error logged previously).")
                else:
                    # Log if no comments were returned or list was empty.
                    logging.debug("      No recent comments found or empty children list.")
                time.sleep(1) # Add a small delay after the comment check.
            except Exception as e:
                 # Catch and log any unexpected errors during comment checking.
                 logging.error(f"   ❌ Error during monitoring check for comments: {e}", exc_info=True)

            check_duration = time.time() - check_start_time # Calculate time taken for this check iteration
            logging.debug(f"      Activity check completed in {check_duration:.2f}s")

            # --- Handle New Activity ---
            if new_activity_detected:
                logging.info(f"   🔄 {BOLD}New activity detected! Re-scraping to update data files...{RESET}")
                # If new activity was found, re-run the data saving process.
                # Setting `force_scrape=False` is crucial here; it tells `save_reddit_data`
                # to only add newer/updated items, merging them with the existing data.
                json_path_actual = save_reddit_data(user_data_dir, username, config, sort_descending, scrape_comments_only=False, force_scrape=False)

                # After updating the JSON, re-extract the CSVs to reflect the new data.
                # This prepares the data in CSV format for potential analysis.
                if json_path_actual and os.path.exists(json_path_actual):
                    logging.info(f"   📄 Re-extracting CSVs from updated JSON ({CYAN}{os.path.basename(json_path_actual)}{RESET})...")
                    csv_prefix = os.path.join(user_data_dir, username)
                    # Extract CSVs from the updated JSON. No date filter is needed here,
                    # as we want to extract all currently saved data.
                    posts_csv, comments_csv = extract_csvs_from_json(json_path_actual, csv_prefix) # No date filter here

                    # Check if the CSV files were actually created.
                    posts_csv_exists = os.path.exists(f"{csv_prefix}-posts.csv")
                    comments_csv_exists = os.path.exists(f"{csv_prefix}-comments.csv")

                    # --- Optional: Trigger automatic analysis on update ---
                    # If auto-analysis is enabled (`run_analysis_on_update` is True) AND
                    # an AI model instance was provided:
                    if run_analysis_on_update and model:
                        # Check if CSVs are available for analysis.
                        if not posts_csv_exists and not comments_csv_exists:
                             logging.error("   ❌ Cannot run analysis on update: CSV extraction failed or produced no files.")
                        else:
                             logging.info(f"   🤖 {BOLD}Triggering new AI analysis based on updated data...{RESET}")
                             ai_start_time = time.time() # Start timer for analysis
                             # Generate a unique timestamp for the analysis output filename.
                             timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                             output_filename = f"{username}_charc_{analysis_mode}_MONITOR_{timestamp_str}.md" # Include MONITOR in name
                             output_md_file = os.path.join(user_data_dir, output_filename) # Full output path
                             logging.info(f"      Analysis output file will be: {CYAN}{output_md_file}{RESET}")

                             # Determine which CSV paths to pass to the analysis function.
                             posts_csv_path = f"{csv_prefix}-posts.csv" if posts_csv_exists else None
                             comments_csv_path = f"{csv_prefix}-comments.csv" if comments_csv_exists else None

                             success = False # Flag to track if analysis function reports success
                             try:
                                  # IMPORT ANALYSIS FUNCTIONS HERE to avoid circular dependency if monitoring is not used.
                                  from analysis import generate_raw_analysis, generate_mapped_analysis
                                  # Prepare arguments common to both analysis modes.
                                  analysis_args = {"output_file": output_md_file, "model": model, "system_prompt": system_prompt, "chunk_size": chunk_size}
                                  # Run the appropriate analysis function based on the configured mode.
                                  # Note: Date filter is NOT passed here; we want to analyze ALL updated data.
                                  # Subreddit filters are applied *within* the analysis functions based on config/args.
                                  if analysis_mode == "raw":
                                     success = generate_raw_analysis(posts_csv_path, comments_csv_path, **analysis_args)
                                  else: # mapped
                                     success = generate_mapped_analysis(posts_csv_path, comments_csv_path, config, no_cache_titles=no_cache_titles, fetch_external_context=fetch_external_context, **analysis_args)
                             except ImportError as e:
                                 # Catch error if analysis functions cannot be imported.
                                 logging.error(f"   ❌ Cannot run analysis: Failed to import analysis functions. Ensure analysis.py is available. Error: {e}")
                             except Exception as e:
                                  # Catch any other errors during the analysis execution.
                                  logging.error(f"   ❌ Error during triggered analysis execution: {e}", exc_info=True) # Log traceback

                             ai_duration = time.time() - ai_start_time # Calculate analysis duration
                             # Log the result of the analysis trigger.
                             if success:
                                logging.info(f"   ✅ Monitoring analysis complete ({ai_duration:.2f}s): {CYAN}{output_md_file}{RESET}")
                             else:
                                logging.error(f"   ❌ Monitoring analysis failed ({ai_duration:.2f}s). Check logs for details.")
                    elif run_analysis_on_update and not model:
                         # Warn if auto-analysis is on but no model was provided (likely configuration issue).
                         logging.warning("   ⚠️ Auto-analysis on update is enabled, but AI model is not available. Analysis skipped.")
                    else:
                         # If auto-analysis is off, just confirm data update success.
                         logging.info(f"   ✅ Data files updated successfully. Manual analysis can be run on latest data.")
                         # It's good practice to re-read the latest timestamps from the updated file
                         # to ensure the markers are correct for the next check.
                         last_known_post_time = get_latest_timestamp_from_file("t3")
                         last_known_comment_time = get_latest_timestamp_from_file("t1")
                         logging.debug(f"      Refreshed latest known times: Post={format_timestamp(last_known_post_time)}, Comment={format_timestamp(last_known_comment_time)}")
                else:
                    # Log error if the JSON file was not successfully saved after re-scraping.
                    logging.error(f"   ❌ Failed to save updated data after detecting new activity.")
                    # Could potentially add logic here to pause or stop monitoring if saving consistently fails.
            else:
                # Log if no new activity was found in this interval.
                logging.info(f"   ✅ No new activity detected this interval.")

            # --- Wait for the next check ---
            logging.info(f"   😴 Sleeping for {interval_seconds} seconds until next check...")
            time.sleep(interval_seconds) # Pause execution for the specified interval

    except KeyboardInterrupt:
        # Handle user interruption (Ctrl+C).
        print(f"\n{YELLOW}Monitoring stopped by user (Ctrl+C).{RESET}")
        logging.info("Monitoring stopped by user via KeyboardInterrupt.")
    except Exception as e:
        # Catch any unexpected critical errors that occur during the monitoring loop.
        logging.critical(f"🔥 An unexpected CRITICAL error occurred during monitoring: {e}", exc_info=True) # Log with traceback
        print(f"\n{BOLD}{RED}A critical error occurred: {e}. Stopping monitoring.{RESET}")