import logging
import time
import os
from datetime import datetime
# Import necessary utils
from reddit_utils import get_reddit_data, save_reddit_data, get_modification_date, load_existing_data, format_timestamp
from data_utils import extract_csvs_from_json
# Import analysis functions conditionally later if needed
# from analysis import generate_raw_analysis, generate_mapped_analysis

# Import ANSI codes
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\033[31m"

def monitor_user(username, user_data_dir, config, interval_seconds, model, system_prompt, chunk_size, sort_descending, analysis_mode, no_cache_titles, fetch_external_context):
    """Monitors a user for new activity and updates data files."""
    logging.info(f"👀 Starting real-time monitoring for user '{BOLD}{username}{RESET}'. Checking every {interval_seconds} seconds.")
    print(f"👀 Monitoring /u/{username}... Press Ctrl+C to stop.")

    json_path = os.path.join(user_data_dir, f"{username}.json")
    last_known_post_time = 0
    last_known_comment_time = 0
    run_analysis_on_update = False # Keep auto-analysis disabled by default

    def get_latest_timestamp_from_file(kind):
        """Reads the JSON and returns the timestamp of the newest item."""
        logging.debug(f"      Checking latest timestamp for {kind} in {CYAN}{json_path}{RESET}")
        if not os.path.exists(json_path):
            logging.debug(f"         JSON file not found, returning 0.")
            return 0
        try:
            # Use load_existing_data which handles errors and empty files
            data = load_existing_data(json_path)
            if kind in data and isinstance(data[kind], dict) and data[kind]:
                 # Assumes data[kind] is sorted descending by save_reddit_data
                 first_item_id = next(iter(data[kind]))
                 latest_ts = get_modification_date(data[kind][first_item_id])
                 logging.debug(f"         Found latest {kind} timestamp: {format_timestamp(latest_ts)}")
                 return latest_ts
            logging.debug(f"         No data of kind '{kind}' found in JSON.")
            return 0
        except (KeyError, StopIteration, Exception) as e:
            logging.error(f"      ❌ Error reading latest timestamp for kind '{kind}' from {CYAN}{json_path}{RESET}: {e}")
            return 0

    last_known_post_time = get_latest_timestamp_from_file("t3")
    last_known_comment_time = get_latest_timestamp_from_file("t1")
    logging.info(f"   Initial latest known times: Post={BOLD}{format_timestamp(last_known_post_time)}{RESET}, Comment={BOLD}{format_timestamp(last_known_comment_time)}{RESET}")

    try:
        iteration = 0
        while True:
            iteration += 1
            new_activity_detected = False
            logging.info(f"--- Monitor Iteration {iteration} ({datetime.now().strftime('%H:%M:%S')}) ---")
            logging.info(f"   🔍 Checking for new activity for /u/{BOLD}{username}{RESET}...")
            check_start_time = time.time()

            # Check submitted posts
            try:
                logging.debug("      Checking submitted posts (limit 5)...")
                latest_posts_data = get_reddit_data(username, "submitted", config, limit=5)
                if latest_posts_data and isinstance(latest_posts_data.get("data"), dict) and isinstance(latest_posts_data["data"].get("children"), list):
                    for post_entry in latest_posts_data["data"]["children"]:
                         # Validate entry structure before getting date
                         if isinstance(post_entry, dict) and "data" in post_entry:
                            post_time = get_modification_date(post_entry)
                            if post_time > last_known_post_time:
                                logging.info(f"      ✨ {GREEN}New post detected!{RESET} ID: {post_entry['data'].get('id', 'N/A')}, Time: {format_timestamp(post_time)}")
                                new_activity_detected = True
                                last_known_post_time = post_time # Update marker immediately
                            else:
                                logging.debug("         Post is older than last known, stopping post check.")
                                break # Items are newest first
                         else:
                              logging.warning("      ⚠️ Malformed post entry structure during monitor check.")
                elif latest_posts_data is None:
                     logging.warning("      ⚠️ Failed to fetch recent posts during monitor check (error logged previously).")
                else:
                     logging.debug("      No recent posts found or empty children list.")
                time.sleep(1) # Small delay even if fetch failed
            except Exception as e:
                 logging.error(f"   ❌ Error during monitoring check for posts: {e}", exc_info=True)

            # Check comments
            try:
                logging.debug("      Checking comments (limit 5)...")
                latest_comments_data = get_reddit_data(username, "comments", config, limit=5)
                if latest_comments_data and isinstance(latest_comments_data.get("data"), dict) and isinstance(latest_comments_data["data"].get("children"), list):
                    for comment_entry in latest_comments_data["data"]["children"]:
                         if isinstance(comment_entry, dict) and "data" in comment_entry:
                            comment_time = get_modification_date(comment_entry)
                            if comment_time > last_known_comment_time:
                                logging.info(f"      ✨ {GREEN}New comment detected!{RESET} ID: {comment_entry['data'].get('id', 'N/A')}, Time: {format_timestamp(comment_time)}")
                                new_activity_detected = True
                                last_known_comment_time = comment_time # Update marker immediately
                            else:
                                logging.debug("         Comment is older than last known, stopping comment check.")
                                break # Items are newest first
                         else:
                              logging.warning("      ⚠️ Malformed comment entry structure during monitor check.")
                elif latest_comments_data is None:
                     logging.warning("      ⚠️ Failed to fetch recent comments during monitor check (error logged previously).")
                else:
                    logging.debug("      No recent comments found or empty children list.")
                time.sleep(1)
            except Exception as e:
                 logging.error(f"   ❌ Error during monitoring check for comments: {e}", exc_info=True)

            check_duration = time.time() - check_start_time
            logging.debug(f"      Activity check completed in {check_duration:.2f}s")

            if new_activity_detected:
                logging.info(f"   🔄 {BOLD}New activity detected! Re-scraping to update data files...{RESET}")
                # Run scrape/save to merge new data. force_scrape=False ensures only newer added/updated.
                json_path_actual = save_reddit_data(user_data_dir, username, config, sort_descending, scrape_comments_only=False, force_scrape=False)

                if json_path_actual and os.path.exists(json_path_actual):
                    logging.info(f"   📄 Re-extracting CSVs from updated JSON ({CYAN}{os.path.basename(json_path_actual)}{RESET})...")
                    csv_prefix = os.path.join(user_data_dir, username)
                    # Run CSV extraction without date filter for monitoring updates
                    posts_csv, comments_csv = extract_csvs_from_json(json_path_actual, csv_prefix) # No date filter here

                    posts_csv_exists = os.path.exists(f"{csv_prefix}-posts.csv")
                    comments_csv_exists = os.path.exists(f"{csv_prefix}-comments.csv")


                    # --- Optional: Trigger automatic analysis on update ---
                    if run_analysis_on_update and model: # Check if model is available
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
                             try: # Import analysis functions only if needed here
                                  from analysis import generate_raw_analysis, generate_mapped_analysis
                                  # Run analysis without date filter for monitoring
                                  analysis_args = {"output_file": output_md_file, "model": model, "system_prompt": system_prompt, "chunk_size": chunk_size}
                                  if analysis_mode == "raw":
                                     success = generate_raw_analysis(posts_csv_path, comments_csv_path, **analysis_args)
                                  else: # mapped
                                     success = generate_mapped_analysis(posts_csv_path, comments_csv_path, config, no_cache_titles=no_cache_titles, fetch_external_context=fetch_external_context, **analysis_args)
                             except ImportError as e:
                                 logging.error(f"   ❌ Cannot run analysis: Failed to import analysis functions: {e}")
                             except Exception as e:
                                  logging.error(f"   ❌ Error during triggered analysis execution: {e}", exc_info=True)

                             ai_duration = time.time() - ai_start_time
                             if success:
                                logging.info(f"   ✅ Monitoring analysis complete ({ai_duration:.2f}s): {CYAN}{output_md_file}{RESET}")
                             else:
                                logging.error(f"   ❌ Monitoring analysis failed ({ai_duration:.2f}s).")
                    elif run_analysis_on_update and not model:
                         logging.warning("   ⚠️ Auto-analysis on update is enabled, but AI model is not available.")
                    else:
                         # Just log success if auto-analysis is off
                         logging.info(f"   ✅ Data files updated successfully. Manual analysis can be run on latest data.")
                         # Refresh latest known times from file after update
                         last_known_post_time = get_latest_timestamp_from_file("t3")
                         last_known_comment_time = get_latest_timestamp_from_file("t1")
                         logging.debug(f"      Refreshed latest known times: Post={format_timestamp(last_known_post_time)}, Comment={format_timestamp(last_known_comment_time)}")
                else:
                    logging.error(f"   ❌ Failed to save updated data after detecting new activity.")
                    # Consider maybe pausing or stopping monitor if saving fails?
            else:
                logging.info(f"   ✅ No new activity detected this interval.")

            logging.info(f"   😴 Sleeping for {interval_seconds} seconds until next check...")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}Monitoring stopped by user (Ctrl+C).{RESET}")
        logging.info("Monitoring stopped by user.")
    except Exception as e:
        logging.critical(f"🔥 An unexpected CRITICAL error occurred during monitoring: {e}", exc_info=True)
        print(f"\n{BOLD}{RED}A critical error occurred: {e}. Stopping monitoring.{RESET}")