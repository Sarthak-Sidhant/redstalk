# monitoring.py
import logging
import time
import os
from datetime import datetime
# Import necessary utils
from reddit_utils import get_reddit_data, save_reddit_data, get_modification_date, load_existing_data
from data_utils import extract_csvs_from_json, format_timestamp
# Import analysis functions if auto-analysis on update is desired
from analysis import generate_raw_analysis, generate_mapped_analysis

def monitor_user(username, user_data_dir, config, interval_seconds, model, system_prompt, chunk_size, sort_descending, analysis_mode, no_cache_titles, fetch_external_context):
    """Monitors a user for new activity and updates data files."""
    logging.info(f"Starting real-time monitoring for user '{username}'. Checking every {interval_seconds} seconds.")
    print(f"Monitoring /u/{username}... Press Ctrl+C to stop.")

    json_path = os.path.join(user_data_dir, f"{username}.json")
    last_known_post_time = 0
    last_known_comment_time = 0

    def get_latest_timestamp_from_file(kind):
        """Reads the JSON and returns the timestamp of the newest item for the given kind."""
        if not os.path.exists(json_path):
            return 0
        try:
            # Load only necessary part? No, full load is easier with current structure
            data = load_existing_data(json_path) # Use the util for safety
            if kind in data and isinstance(data[kind], dict) and data[kind]:
                 # Assumes data[kind] is a dict sorted descending (newest first) by save_reddit_data
                 first_item_id = next(iter(data[kind]))
                 return get_modification_date(data[kind][first_item_id])
            return 0 # No data of this kind
        except (KeyError, StopIteration, Exception) as e:
            logging.error(f"Error reading latest timestamp for kind '{kind}' from {json_path}: {e}")
            return 0

    last_known_post_time = get_latest_timestamp_from_file("t3")
    last_known_comment_time = get_latest_timestamp_from_file("t1")
    logging.info(f"Initial latest known times: Post={format_timestamp(last_known_post_time)}, Comment={format_timestamp(last_known_comment_time)}")

    try:
        while True:
            new_activity_detected = False
            logging.info(f"Checking for new activity for /u/{username}...")

            # Check submitted posts (limit 5 should be enough to catch recent)
            try:
                logging.debug("Checking submitted posts (limit 5)...")
                # Pass config to get_reddit_data
                latest_posts_data = get_reddit_data(username, "submitted", config, limit=5)
                if latest_posts_data and latest_posts_data.get("data", {}).get("children"):
                    for post_entry in latest_posts_data["data"]["children"]:
                        post_time = get_modification_date(post_entry)
                        if post_time > last_known_post_time:
                            logging.info(f"✨ New post detected! ID: {post_entry['data'].get('id')}, Time: {format_timestamp(post_time)}")
                            new_activity_detected = True
                            last_known_post_time = post_time # Update marker immediately
                        else:
                            break # Items are newest first
                    time.sleep(1) # Small delay between checks
            except Exception as e:
                 logging.error(f"Error during monitoring check for posts: {e}", exc_info=True)


            # Check comments (limit 5)
            try:
                logging.debug("Checking comments (limit 5)...")
                # Pass config to get_reddit_data
                latest_comments_data = get_reddit_data(username, "comments", config, limit=5)
                if latest_comments_data and latest_comments_data.get("data", {}).get("children"):
                    for comment_entry in latest_comments_data["data"]["children"]:
                        comment_time = get_modification_date(comment_entry)
                        if comment_time > last_known_comment_time:
                            logging.info(f"✨ New comment detected! ID: {comment_entry['data'].get('id')}, Time: {format_timestamp(comment_time)}")
                            new_activity_detected = True
                            last_known_comment_time = comment_time # Update marker immediately
                        else:
                            break # Items are newest first
                    time.sleep(1)
            except Exception as e:
                 logging.error(f"Error during monitoring check for comments: {e}", exc_info=True)


            if new_activity_detected:
                logging.info("New activity detected. Re-scraping to update data files...")
                # Run scrape/save to merge new data. force_scrape=False ensures only newer are added/updated.
                # Pass config to save_reddit_data
                json_path_actual = save_reddit_data(user_data_dir, username, config, sort_descending, scrape_comments_only=False, force_scrape=False)

                if json_path_actual:
                    logging.info("Data file updated. Re-extracting CSVs...")
                    csv_prefix = os.path.join(user_data_dir, username)
                    # Pass sort_descending? CSVs only reflect JSON order.
                    posts_csv, comments_csv = extract_csvs_from_json(json_path_actual, csv_prefix)

                    # --- Optional: Trigger automatic analysis on update ---
                    # Set run_analysis_on_update = True to enable this
                    run_analysis_on_update = False
                    # -----------------------------------------------------

                    if run_analysis_on_update:
                        if not posts_csv and not comments_csv:
                             logging.error("Cannot run analysis on update: CSV extraction failed.")
                        else:
                             logging.info("Triggering new analysis based on updated data...")
                             # Use detailed timestamp for monitor-triggered analysis
                             timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
                             output_filename = f"{username}_charc_{analysis_mode}_MONITOR_{timestamp_str}.md"
                             output_md_file = os.path.join(user_data_dir, output_filename)
                             logging.info(f"Analysis output file will be: {output_md_file}")

                             posts_csv_path = posts_csv if posts_csv else f"{csv_prefix}-posts.csv" # Use expected path if None
                             comments_csv_path = comments_csv if comments_csv else f"{csv_prefix}-comments.csv"

                             success = False
                             if analysis_mode == "raw":
                                success = generate_raw_analysis(posts_csv_path, comments_csv_path, output_md_file, model, system_prompt, chunk_size)
                             else: # mapped
                                success = generate_mapped_analysis(posts_csv_path, comments_csv_path, config, output_md_file, model, system_prompt, chunk_size, no_cache_titles, fetch_external_context)

                             if success:
                                logging.info(f"✅ Monitoring analysis complete: {output_md_file}")
                             else:
                                logging.error("❌ Monitoring analysis failed.")
                    else:
                         logging.info("✅ Data files updated. Manual analysis can be run on the latest data.")
                         # Refresh latest known times from file after update
                         last_known_post_time = get_latest_timestamp_from_file("t3")
                         last_known_comment_time = get_latest_timestamp_from_file("t1")
                         logging.debug(f"Refreshed latest known times: Post={format_timestamp(last_known_post_time)}, Comment={format_timestamp(last_known_comment_time)}")
                else:
                    logging.error("Failed to save updated data after detecting new activity.")
            else:
                logging.info("No new activity detected.")

            logging.debug(f"Sleeping for {interval_seconds} seconds...")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nMonitoring stopped by user (Ctrl+C).")
        logging.info("Monitoring stopped by user.")
    except Exception as e:
        logging.critical(f"An unexpected error occurred during monitoring: {e}", exc_info=True)
        print(f"\nAn critical error occurred: {e}. Stopping monitoring.")