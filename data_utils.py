# data_utils.py
"""
This module handles the process of reading raw data from a JSON file
(typically produced by a scraping process) and exporting it into separate
CSV files for posts and comments. It includes functionality to apply
date and subreddit filters during the extraction process.
"""

# Import standard libraries
import csv # For writing data in CSV format
import logging # For logging process information and errors
import json # For reading the input JSON file
import os # For file path manipulation and existence checks
from datetime import datetime, timezone # For date/time handling and filtering
from typing import List, Optional, Tuple # Imports for type hints, improving code readability

# Import necessary functions from other utils
# These functions are assumed to be available in the project structure.
# Using a try-except block to handle potential import errors gracefully.
try:
    # These functions are expected to operate on the dictionary structure loaded from JSON
    from reddit_utils import get_modification_date, format_timestamp # Utility functions for date handling
    REDDIT_UTILS_AVAILABLE = True
except ImportError:
    logging.warning("Could not import from reddit_utils. Date handling might be limited.")
    REDDIT_UTILS_AVAILABLE = False
    # Define dummy functions if reddit_utils is unavailable to prevent crashes
    def get_modification_date(entry): return 0
    def format_timestamp(ts): return "DATE_ERROR"


# Import ANSI codes for coloring output messages in the console.
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; YELLOW = "\033[33m"

def extract_csvs_from_json(
    json_path: str, # Path to the source JSON file
    output_prefix: str, # Prefix for the output CSV filenames (e.g., 'user_data')
    date_filter: Tuple[float, float] = (0, float('inf')), # Date filter as a tuple of (start_ts, end_ts)
    focus_subreddits: Optional[List[str]] = None, # Optional list of subreddits to include
    ignore_subreddits: Optional[List[str]] = None  # Optional list of subreddits to exclude
) -> Tuple[Optional[str], Optional[str]]:
    """
    Reads a JSON file containing Reddit user data (posts and comments,
    usually structured by Kind 't3' for posts and 't1' for comments),
    applies optional date and subreddit filters, and writes the filtered
    data into two separate CSV files: one for posts and one for comments.

    Args:
        json_path (str): The absolute or relative path to the input JSON file.
        output_prefix (str): The base name for the output CSV files.
                             The posts file will be {output_prefix}-posts.csv
                             and the comments file will be {output_prefix}-comments.csv.
        date_filter (tuple[float, float]): A tuple representing the start and end
                                           Unix timestamps (inclusive of start,
                                           exclusive of end) for filtering data
                                           based on the 'modified_utc' or 'created_utc'
                                           timestamp. Defaults to (0, infinity),
                                           meaning no date filtering.
        focus_subreddits (List[str] | None): A list of subreddit names (case-insensitive)
                                             to include in the output. If provided,
                                             only items from these subreddits will be
                                             included. If None, this filter is not applied.
        ignore_subreddits (List[str] | None): A list of subreddit names (case-insensitive)
                                              to exclude from the output. If provided,
                                              items from these subreddits will be
                                              excluded. If None, this filter is not applied.
                                              Note: ignore_subreddits is applied *after*
                                              focus_subreddits if both are present.

    Returns:
        tuple(str | None, str | None): A tuple containing the absolute paths to the
                                       created posts CSV file and comments CSV file,
                                       respectively. If a file is not created (e.g.,
                                       due to no data or all data being filtered out),
                                       its corresponding path in the tuple will be None.
                                       Returns (None, None) if the input JSON file
                                       cannot be read or decoded.
    """
    # Construct the full paths for the output CSV files.
    posts_csv_path = f"{output_prefix}-posts.csv"
    comments_csv_path = f"{output_prefix}-comments.csv"

    # Initialize counters for tracking items processed and filtered.
    posts_written, comments_written = 0, 0
    posts_filtered_date, comments_filtered_date = 0, 0
    posts_filtered_sub, comments_filtered_sub = 0, 0 # Counts items filtered by EITHER focus or ignore
    posts_skipped_invalid, comments_skipped_invalid = 0, 0 # Counts items with structural issues
    posts_csv_created, comments_csv_created = False, False # Flags to track if a CSV was actually written
    start_ts, end_ts = date_filter # Unpack the date filter tuple

    logging.info(f"   ⚙️ Extracting data from {CYAN}{json_path}{RESET} to CSV files...")

    # --- Log Filters Being Applied ---
    filter_log_parts = []
    # Format date filter for logging if active.
    if start_ts > 0 or end_ts != float('inf'):
        # Convert timestamps back to human-readable dates for the log message.
        start_str = datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d') if start_ts > 0 else 'Beginning'
        end_str = datetime.fromtimestamp(end_ts - 1, timezone.utc).strftime('%Y-%m-%d') if end_ts != float('inf') else 'End'
        filter_log_parts.append(f"Date: {start_str} to {end_str} (UTC)")
    # Log subreddit filters if provided.
    if focus_subreddits:
        filter_log_parts.append(f"Focusing on: {', '.join(focus_subreddits)}")
    if ignore_subreddits:
        filter_log_parts.append(f"Ignoring: {', '.join(ignore_subreddits)}")

    # Print the filter summary log message if any filters are active.
    if filter_log_parts:
        logging.info(f"      Applying Filters: {'; '.join(filter_log_parts)}")

    # --- Pre-process filter lists for efficient lookup ---
    focus_lower_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_lower_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None

    # --- Load Data from JSON ---
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error(f"   ❌ JSON file not found: {CYAN}{json_path}{RESET}");
        return None, None
    except json.JSONDecodeError:
        logging.error(f"   ❌ Error decoding JSON: {CYAN}{json_path}{RESET}");
        return None, None
    except Exception as e:
        logging.error(f"   ❌ Error reading JSON {CYAN}{json_path}{RESET}: {e}", exc_info=True);
        return None, None

    if not isinstance(data, dict):
        logging.error(f"   ❌ JSON file {CYAN}{json_path}{RESET} is not a dict (expected structure: {{'t3': {{...}}, 't1': {{...}}}}).");
        return None, None

    # --- Define CSV Fieldnames ---
    post_fieldnames = ['title', 'selftext', 'permalink', 'created_utc_iso', 'modified_utc_iso', 'subreddit', 'score', 'num_comments', 'link_flair_text', 'is_self']
    comment_fieldnames = ['id', 'parent_id', 'body', 'permalink', 'created_utc_iso', 'modified_utc_iso', 'subreddit', 'score', 'author_flair_text']

    # --- Define CSV Quoting Behavior (Optional Change) ---
    # QUOTE_MINIMAL: Only quotes fields containing delimiter, quotechar, or line breaks (default)
    # QUOTE_NONNUMERIC: Quotes all fields that are not integers or floats.
    # QUOTE_ALL: Quotes all fields.
    csv_quoting_style = csv.QUOTE_NONNUMERIC # <-- Set desired quoting style here

    # --- Write Posts CSV ---
    if "t3" in data and isinstance(data.get("t3"), dict) and data["t3"]:
        logging.debug(f"      Processing {len(data['t3'])} posts (t3) for CSV...")
        try:
            with open(posts_csv_path, 'w', newline='', encoding='utf-8') as pfile:
                # Apply the chosen quoting style
                post_writer = csv.writer(pfile, quoting=csv_quoting_style)
                post_writer.writerow(post_fieldnames) # Write header

                for i, (entry_id, entry_data) in enumerate(data["t3"].items()):
                    # Basic validation of the structure loaded from JSON
                    if not isinstance(entry_data, dict) or 'data' not in entry_data or not isinstance(entry_data['data'], dict):
                        posts_skipped_invalid += 1; continue

                    # Get timestamps using helpers (which expect the dict structure)
                    if not REDDIT_UTILS_AVAILABLE:
                         logging.warning("Skipping date filtering/formatting due to missing reddit_utils.");
                         modified_utc_ts = entry_data.get('data',{}).get('created_utc', 0)
                         created_utc = modified_utc_ts
                    else:
                         modified_utc_ts = get_modification_date(entry_data)
                         created_utc = entry_data.get('data',{}).get('created_utc', 0)

                    # Skip if modification date is invalid (e.g., 0)
                    if modified_utc_ts == 0:
                        posts_skipped_invalid += 1; continue

                    # Apply date filter
                    if not (start_ts <= modified_utc_ts < end_ts):
                         posts_filtered_date += 1; continue

                    # Extract the core 'data' dictionary
                    edata = entry_data['data']
                    item_subreddit_lower = edata.get('subreddit', '').lower()

                    # Apply subreddit filters
                    focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                    ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)
                    if not (focus_match and ignore_match):
                        posts_filtered_sub += 1; continue

                    # Extract data fields from the dictionary - uses .get for safety
                    title = edata.get('title', '')
                    # Note: Selftext can be legitimately empty for link posts or empty body self-posts
                    selftext = edata.get('selftext', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                    permalink = edata.get('permalink', '')
                    created_iso = format_timestamp(created_utc) # Format timestamps
                    modified_iso = format_timestamp(modified_utc_ts)
                    subreddit = edata.get('subreddit', '')
                    score = edata.get('score', 0)
                    num_comments = edata.get('num_comments', 0)
                    link_flair = edata.get('link_flair_text', '')
                    is_self = edata.get('is_self', False)

                    # Write the row to the CSV
                    post_writer.writerow([title, selftext, permalink, created_iso, modified_iso, subreddit, score, num_comments, link_flair, is_self])
                    posts_written += 1

            # Logging and cleanup after processing posts
            if posts_written > 0:
                logging.info(f"      📄 Created posts CSV: {CYAN}{posts_csv_path}{RESET} ({posts_written} posts written)")
                posts_csv_created = True
            if posts_filtered_date > 0 or posts_filtered_sub > 0 or posts_skipped_invalid > 0:
                 logging.info(f"         (Filtered Posts: {posts_filtered_date} date, {posts_filtered_sub} sub. Skipped: {posts_skipped_invalid} invalid)")
                 if posts_written == 0 and os.path.exists(posts_csv_path):
                      try: os.remove(posts_csv_path); logging.info(f"         Removed empty posts CSV file: {CYAN}{posts_csv_path}{RESET}")
                      except OSError as e: logging.warning(f"      ⚠️ Could not remove empty/filtered posts CSV: {e}")
            elif posts_written == 0:
                 logging.info("      ℹ️ No posts written (likely due to filters or empty data).")

        except IOError as e:
            logging.error(f"      ❌ IOError writing posts CSV {CYAN}{posts_csv_path}{RESET}: {e}", exc_info=True);
            posts_csv_created = False
        except Exception as e:
            logging.error(f"      ❌ Unexpected error writing posts CSV {CYAN}{posts_csv_path}{RESET}: {e}", exc_info=True);
            posts_csv_created = False

        # Ensure temp file cleanup if error occurred
        if not posts_csv_created and os.path.exists(posts_csv_path):
            try: os.remove(posts_csv_path)
            except OSError: pass

    else:
        logging.info("      ℹ️ No 't3' (posts) data found in JSON.")

    # --- Write Comments CSV ---
    if "t1" in data and isinstance(data.get("t1"), dict) and data["t1"]:
        logging.debug(f"      Processing {len(data['t1'])} comments (t1) for CSV...")
        try:
            with open(comments_csv_path, 'w', newline='', encoding='utf-8') as cfile:
                # Apply the chosen quoting style
                comment_writer = csv.writer(cfile, quoting=csv_quoting_style)
                comment_writer.writerow(comment_fieldnames) # Write header

                for i, (entry_id, entry_data) in enumerate(data["t1"].items()):
                     # Basic validation of the structure loaded from JSON
                     if not isinstance(entry_data, dict) or 'data' not in entry_data or not isinstance(entry_data['data'], dict):
                         comments_skipped_invalid += 1; continue

                     # Get timestamps using helpers
                     if not REDDIT_UTILS_AVAILABLE:
                         logging.warning("Skipping date filtering/formatting due to missing reddit_utils.");
                         modified_utc_ts = entry_data.get('data',{}).get('created_utc', 0)
                         created_utc = modified_utc_ts
                     else:
                         modified_utc_ts = get_modification_date(entry_data)
                         created_utc = entry_data.get('data',{}).get('created_utc', 0)

                     # Skip if modification date is invalid
                     if modified_utc_ts == 0:
                         comments_skipped_invalid += 1; continue

                     # Apply date filter
                     if not (start_ts <= modified_utc_ts < end_ts):
                          comments_filtered_date += 1; continue

                     # Extract the core 'data' dictionary
                     edata = entry_data['data']
                     item_subreddit_lower = edata.get('subreddit', '').lower()

                     # Apply subreddit filters
                     focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                     ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)
                     if not (focus_match and ignore_match):
                         comments_filtered_sub += 1; continue

                     # --- Extract fields for comment CSV ---
                     # Uses .get for safety - relies on keys existing in the input JSON dict (edata)
                     full_comment_id = edata.get('name', f't1_{edata.get("id", "")}') # Prefer 'name', fallback using 'id'

                     # IMPORTANT: This relies on 'parent_id' being present in the 'edata' dictionary
                     # loaded from the JSON file. If it's missing in the JSON, this will be an empty string.
                     # The fix for missing 'parent_id' must happen in the script that *creates* the JSON file
                     # (likely reddit_utils.py's _praw_object_to_dict function).
                     parent_id = edata.get('parent_id', '')

                     body = edata.get('body', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                     permalink = edata.get('permalink', '')
                     created_iso = format_timestamp(created_utc)
                     modified_iso = format_timestamp(modified_utc_ts)
                     subreddit = edata.get('subreddit', '')
                     score = edata.get('score', 0)
                     author_flair = edata.get('author_flair_text', '')

                     # Write the row to the CSV
                     comment_writer.writerow([full_comment_id, parent_id, body, permalink, created_iso, modified_iso, subreddit, score, author_flair])
                     comments_written += 1

            # Logging and cleanup after processing comments
            if comments_written > 0:
                logging.info(f"      📄 Created comments CSV: {CYAN}{comments_csv_path}{RESET} ({comments_written} comments written)")
                comments_csv_created = True
            if comments_filtered_date > 0 or comments_filtered_sub > 0 or comments_skipped_invalid > 0:
                 logging.info(f"         (Filtered Comments: {comments_filtered_date} date, {comments_filtered_sub} sub. Skipped: {comments_skipped_invalid} invalid)")
                 if comments_written == 0 and os.path.exists(comments_csv_path):
                      try: os.remove(comments_csv_path); logging.info(f"         Removed empty comments CSV file: {CYAN}{comments_csv_path}{RESET}")
                      except OSError as e: logging.warning(f"      ⚠️ Could not remove empty/filtered comments CSV: {e}")
            elif comments_written == 0:
                 logging.info("      ℹ️ No comments written (likely due to filters or empty data).")

        except IOError as e:
            logging.error(f"      ❌ IOError writing comments CSV {CYAN}{comments_csv_path}{RESET}: {e}", exc_info=True);
            comments_csv_created = False
        except Exception as e:
            logging.error(f"      ❌ Unexpected error writing comments CSV {CYAN}{comments_csv_path}{RESET}: {e}", exc_info=True);
            comments_csv_created = False

        # Ensure temp file cleanup if error occurred
        if not comments_csv_created and os.path.exists(comments_csv_path):
            try: os.remove(comments_csv_path)
            except OSError: pass

    else:
        logging.info("      ℹ️ No 't1' (comments) data found in JSON.")

    # Return the paths to the created files (or None if not created)
    final_posts_path = posts_csv_path if posts_csv_created else None
    final_comments_path = comments_csv_path if comments_csv_created else None
    logging.info(f"   ✅ CSV Extraction complete.")
    return final_posts_path, final_comments_path