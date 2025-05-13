# monitor.py
"""
This module provides functionality to monitor a specific Reddit user for
new activity (posts and comments) at a defined interval. When new activity
is detected, it triggers a re-scrape using PRAW to update the local data files
and optionally triggers a new AI analysis on the updated dataset.
"""

# Import standard libraries
import logging # For logging information and errors
import time # For implementing sleep intervals between checks
import os # For file path manipulation and existence checks
from datetime import datetime # For getting the current time for logging iteration start/end

# Import PRAW libraries for direct checks and error handling
import praw
import prawcore

# Import necessary utility functions from other modules
# get_reddit_data is REMOVED as we use PRAW directly now.
from reddit_utils import save_reddit_data, get_modification_date, load_existing_data, format_timestamp
from data_utils import extract_csvs_from_json
# Analysis functions are imported conditionally *inside* monitor_user
# only when an analysis is triggered, to avoid circular dependencies if not needed.
# from analysis import generate_raw_analysis, generate_mapped_analysis # Not imported globally

# Import ANSI codes for coloring console output
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\031m" # Corrected RED code

# Renamed config parameter to praw_instance as it's required now.
# Removed 'config' from args as it's not directly used here anymore,
# except potentially by analysis functions (which should receive it if needed).
# Added praw_instance to the docstring.
def monitor_user(username, user_data_dir, praw_instance, interval_seconds, model, system_prompt, chunk_size, sort_descending, analysis_mode, no_cache_titles, fetch_external_context, config=None): # Added optional config for potential analysis use
    """
    Starts a monitoring loop for a specific Reddit user using PRAW.

    It periodically checks for new posts and comments since the last known
    activity timestamp recorded in the user's data JSON file. If new activity
    is found, it updates the JSON and CSV files and optionally triggers
    a new AI analysis on the updated dataset.

    Args:
        username (str): The Reddit username to monitor.
        user_data_dir (str): The directory where the user's data files are stored.
        praw_instance (praw.Reddit): An authenticated PRAW Reddit instance.
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
        config (dict, optional): The application configuration dictionary, passed down
                                 primarily for potential use by analysis functions.
    """
    # Validate PRAW instance
    if not isinstance(praw_instance, praw.Reddit):
        logging.critical("🔥 Monitor Error: Invalid PRAW instance received. Stopping monitoring.")
        print(f"{BOLD}{RED}Critical Error: Invalid PRAW instance passed to monitor. Cannot continue.{RESET}")
        return

    logging.info(f"👀 Starting real-time monitoring for user '{BOLD}{username}{RESET}'. Checking every {interval_seconds} seconds.")
    # Provide user feedback in the console indicating monitoring has started.
    print(f"👀 Monitoring /u/{username}... Press Ctrl+C to stop.")

    # Construct the expected path to the user's main JSON data file.
    json_path = os.path.join(user_data_dir, f"{username}.json")

    # Initialize variables to store the timestamps of the latest known post and comment.
    # These will be updated as new activity is detected and data is saved.
    # We use the created_utc timestamp for comparison in monitoring checks.
    last_known_post_time = 0.0
    last_known_comment_time = 0.0

    # Flag to determine if AI analysis should be run automatically when new data is found.
    # Needs to be configurable, likely via args passed into monitor_user.
    # Let's assume it's passed in or defaults to False.
    run_analysis_on_update = False # Example: Set this based on an argument if needed

    def get_latest_timestamp_from_file(kind):
        """
        Helper function to find the timestamp of the newest item of a specific
        kind ('t3' for posts, 't1' for comments) in the user's JSON data file.
        Relies on get_modification_date and assumes the file is sorted correctly
        by save_reddit_data if present.
        """
        logging.debug(f"      Checking latest timestamp for {kind} in {CYAN}{json_path}{RESET}")
        # Check if the JSON file exists.
        if not os.path.exists(json_path):
            logging.debug(f"         JSON file not found, returning 0.0.")
            return 0.0 # Return 0.0 if file is missing

        try:
            # Load the existing data from the JSON file using the utility function.
            data = load_existing_data(json_path)
            if kind in data and isinstance(data[kind], dict) and data[kind]:
                 # Items are stored as {id: entry_dict}. Get all valid items with dates.
                 items_with_date = []
                 for item_id, item_data in data[kind].items():
                     mod_date = get_modification_date(item_data) # Use the reliable function
                     if mod_date > 0.0:
                         items_with_date.append(mod_date)

                 if items_with_date:
                     latest_ts = max(items_with_date) # Find the maximum timestamp
                     logging.debug(f"         Found latest {kind} timestamp: {format_timestamp(latest_ts)}")
                     return latest_ts # Return the timestamp
                 else:
                      logging.debug(f"         No valid items with dates found for kind '{kind}' in JSON.")
                      return 0.0

            # If the specified kind is not in the data or is empty/not a dict.
            logging.debug(f"         No data of kind '{kind}' found in JSON.")
            return 0.0 # Return 0.0 if no data of this kind exists

        except Exception as e:
            # Catch errors during data access or processing.
            logging.error(f"      ❌ Error reading latest timestamp for kind '{kind}' from {CYAN}{json_path}{RESET}: {e}")
            return 0.0 # Return 0.0 on error

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
            logging.info(f"   🔍 Checking for new activity for /u/{BOLD}{username}{RESET} using PRAW...")
            check_start_time = time.time() # Start timer for the check duration

            try:
                # Get Redditor object inside the loop to handle potential transient issues
                redditor = praw_instance.redditor(username)

                # --- Check Submitted Posts using PRAW ---
                try:
                    logging.debug("      Checking submitted posts (limit 5)...")
                    # Fetch the 5 most recent submitted posts using PRAW
                    latest_posts = list(redditor.submissions.new(limit=5))

                    if latest_posts:
                        for post in latest_posts:
                            # Use created_utc for comparison in monitoring
                            post_time = getattr(post, 'created_utc', 0.0)
                            if post_time > 0.0 and post_time > (last_known_post_time + 1e-9): # Add epsilon for float compare
                                logging.info(f"      ✨ {GREEN}New post detected!{RESET} ID: {post.id}, Time: {format_timestamp(post_time)}")
                                new_activity_detected = True
                                # Update marker *only if* saving succeeds later, or we might miss items if save fails.
                                # We rely on save_reddit_data to correctly update the file and then we re-read the timestamp.
                                # last_known_post_time = post_time # Don't update marker here yet
                            else:
                                # Posts are sorted newest first, so we can break
                                logging.debug(f"         Post {post.id} ({format_timestamp(post_time)}) is not newer than last known post time ({format_timestamp(last_known_post_time)}), stopping post check.")
                                break
                    else:
                        logging.debug("      No recent posts found via PRAW.")
                    time.sleep(1) # Small delay after check

                except prawcore.exceptions.PrawcoreException as e:
                    logging.warning(f"      ⚠️ PRAW error checking posts: {e}")
                except Exception as e:
                    logging.error(f"   ❌ Unexpected error during post check: {e}", exc_info=True)


                # --- Check Comments using PRAW ---
                try:
                    logging.debug("      Checking comments (limit 5)...")
                    # Fetch the 5 most recent comments using PRAW
                    latest_comments = list(redditor.comments.new(limit=5))

                    if latest_comments:
                        for comment in latest_comments:
                            # Use created_utc for comparison in monitoring
                            comment_time = getattr(comment, 'created_utc', 0.0)
                            if comment_time > 0.0 and comment_time > (last_known_comment_time + 1e-9): # Add epsilon
                                logging.info(f"      ✨ {GREEN}New comment detected!{RESET} ID: {comment.id}, Time: {format_timestamp(comment_time)}")
                                new_activity_detected = True
                                # Don't update marker here yet
                                # last_known_comment_time = comment_time
                            else:
                                # Comments are sorted newest first
                                logging.debug(f"         Comment {comment.id} ({format_timestamp(comment_time)}) is not newer than last known comment time ({format_timestamp(last_known_comment_time)}), stopping comment check.")
                                break
                    else:
                        logging.debug("      No recent comments found via PRAW.")
                    time.sleep(1) # Small delay after check

                except prawcore.exceptions.PrawcoreException as e:
                    logging.warning(f"      ⚠️ PRAW error checking comments: {e}")
                except Exception as e:
                    logging.error(f"   ❌ Unexpected error during comment check: {e}", exc_info=True)

            except prawcore.exceptions.NotFound:
                logging.error(f"   ❌ User /u/{username} not found during monitoring check. Stopping monitoring.")
                print(f"{BOLD}{RED}Error: User /u/{username} not found. Stopping monitoring.{RESET}")
                break # Exit the while loop
            except prawcore.exceptions.Forbidden:
                logging.error(f"   ❌ Access denied to /u/{username} during monitoring check (private/suspended?). Stopping monitoring.")
                print(f"{BOLD}{RED}Error: Access denied to /u/{username}. Stopping monitoring.{RESET}")
                break # Exit the while loop
            except prawcore.exceptions.PrawcoreException as e:
                 logging.warning(f"   ⚠️ PRAW API error during user access in monitor check: {e}. Will retry next interval.")
                 # Continue loop, but skip processing this cycle
            except Exception as e:
                 logging.error(f"   ❌ Unexpected error accessing user /u/{username} in monitor check: {e}", exc_info=True)
                 # Continue loop, but skip processing this cycle


            check_duration = time.time() - check_start_time # Calculate time taken for this check iteration
            logging.debug(f"      Activity check completed in {check_duration:.2f}s")

            # --- Handle New Activity ---
            if new_activity_detected:
                logging.info(f"   🔄 {BOLD}New activity detected! Re-scraping to update data files...{RESET}")
                # Call save_reddit_data with the PRAW instance
                # Pass scrape_comments_only=False to get both, force_scrape=False for incremental update
                json_path_actual = save_reddit_data(
                    user_data_dir=user_data_dir,
                    username=username,
                    praw_instance=praw_instance, # Pass the PRAW instance
                    sort_descending=sort_descending,
                    scrape_comments_only=False,
                    force_scrape=False # IMPORTANT: Use incremental update
                )

                if json_path_actual and os.path.exists(json_path_actual):
                    logging.info(f"   📄 Re-extracting CSVs from updated JSON ({CYAN}{os.path.basename(json_path_actual)}{RESET})...")
                    csv_prefix = os.path.join(user_data_dir, username)
                    # Extract CSVs; consider if filters should be applied here based on args
                    # Currently extracts all data from the updated JSON without date/sub filters
                    posts_csv, comments_csv = extract_csvs_from_json(json_path_actual, csv_prefix)

                    posts_csv_exists = os.path.exists(f"{csv_prefix}-posts.csv")
                    comments_csv_exists = os.path.exists(f"{csv_prefix}-comments.csv")

                    # --- Optional: Trigger automatic analysis on update ---
                    if run_analysis_on_update and model:
                        if not posts_csv_exists and not comments_csv_exists:
                             logging.error("   ❌ Cannot run analysis on update: CSV extraction failed or produced no files.")
                        else:
                             logging.info(f"   🤖 {BOLD}Triggering new AI analysis based on updated data...{RESET}")
                             ai_start_time = time.time()
                             timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                             output_filename = f"{username}_charc_{analysis_mode}_MONITOR_{timestamp_str}.md"
                             output_md_file = os.path.join(user_data_dir, output_filename)
                             logging.info(f"      Analysis output file will be: {CYAN}{output_md_file}{RESET}")

                             posts_csv_path = f"{csv_prefix}-posts.csv" if posts_csv_exists else None
                             comments_csv_path = f"{csv_prefix}-comments.csv" if comments_csv_exists else None

                             success = False
                             try:
                                  from analysis import generate_raw_analysis, generate_mapped_analysis # Import locally
                                  # Pass required args. Include 'config' if analysis needs it.
                                  analysis_args = {
                                      "output_file": output_md_file,
                                      "model": model,
                                      "system_prompt": system_prompt,
                                      "chunk_size": chunk_size
                                  }
                                  if analysis_mode == "raw":
                                     success = generate_raw_analysis(posts_csv_path, comments_csv_path, **analysis_args)
                                  else: # mapped
                                     # Mapped analysis might need config, no_cache_titles, fetch_external_context
                                     success = generate_mapped_analysis(
                                         posts_csv_path=posts_csv_path,
                                         comments_csv_path=comments_csv_path,
                                         config=config, # Pass config dict here if needed by analysis
                                         no_cache_titles=no_cache_titles,
                                         fetch_external_context=fetch_external_context,
                                         **analysis_args
                                     )
                             except ImportError as e:
                                 logging.error(f"   ❌ Cannot run analysis: Failed to import analysis functions. Ensure analysis.py is available. Error: {e}")
                             except Exception as e:
                                  logging.error(f"   ❌ Error during triggered analysis execution: {e}", exc_info=True)

                             ai_duration = time.time() - ai_start_time
                             if success:
                                logging.info(f"   ✅ Monitoring analysis complete ({ai_duration:.2f}s): {CYAN}{output_md_file}{RESET}")
                             else:
                                logging.error(f"   ❌ Monitoring analysis failed ({ai_duration:.2f}s). Check logs for details.")
                    elif run_analysis_on_update and not model:
                         logging.warning("   ⚠️ Auto-analysis on update is enabled, but AI model is not available. Analysis skipped.")

                    # --- IMPORTANT: Refresh last known timestamps AFTER successful save ---
                    logging.info(f"   ✅ Data files updated successfully. Refreshing last known timestamps.")
                    last_known_post_time = get_latest_timestamp_from_file("t3")
                    last_known_comment_time = get_latest_timestamp_from_file("t1")
                    logging.info(f"      Refreshed latest known times: Post={BOLD}{format_timestamp(last_known_post_time)}{RESET}, Comment={BOLD}{format_timestamp(last_known_comment_time)}{RESET}")
                else:
                    logging.error(f"   ❌ Failed to save updated data after detecting new activity. Timestamps not refreshed.")
                    # Consider adding a longer sleep or retry mechanism if saving fails
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