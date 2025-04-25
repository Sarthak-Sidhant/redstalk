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
from reddit_utils import get_modification_date, format_timestamp # Utility functions for date handling

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
    # Convert focus and ignore subreddit lists to lowercase sets. Checking membership
    # in a set is much faster (O(1) on average) than checking membership in a list (O(n)).
    focus_lower_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_lower_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None

    # --- Load Data from JSON ---
    try:
        # Open and load the data from the specified JSON file.
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        # Handle case where the input JSON file doesn't exist.
        logging.error(f"   ❌ JSON file not found: {CYAN}{json_path}{RESET}");
        return None, None # Return None for both paths to indicate failure
    except json.JSONDecodeError:
        # Handle case where the file is not valid JSON.
        logging.error(f"   ❌ Error decoding JSON: {CYAN}{json_path}{RESET}");
        return None, None # Return None for both paths
    except Exception as e:
        # Handle any other exceptions during file reading.
        logging.error(f"   ❌ Error reading JSON {CYAN}{json_path}{RESET}: {e}", exc_info=True); # Log traceback
        return None, None # Return None for both paths

    # Validate that the loaded JSON data is in the expected dictionary format.
    if not isinstance(data, dict):
        logging.error(f"   ❌ JSON file {CYAN}{json_path}{RESET} is not a dict (expected structure: {{'t3': {{...}}, 't1': {{...}}}}).");
        return None, None # Return None if the structure is unexpected

    # --- Define CSV Fieldnames ---
    # These are the column headers for the output CSV files.
    # They should correspond to the data extracted from the JSON entries.
    # Added 'subreddit', 'score', 'num_comments' (posts), 'link_flair_text' (posts), 'author_flair_text' (comments).
    post_fieldnames = ['title', 'selftext', 'permalink', 'created_utc_iso', 'modified_utc_iso', 'subreddit', 'score', 'num_comments', 'link_flair_text']
    comment_fieldnames = ['body', 'permalink', 'created_utc_iso', 'modified_utc_iso', 'subreddit', 'score', 'author_flair_text']

    # --- Write Posts CSV ---
    # Check if the JSON data contains a 't3' key (representing posts) and if it's a non-empty dictionary.
    if "t3" in data and isinstance(data.get("t3"), dict) and data["t3"]:
        logging.debug(f"      Processing {len(data['t3'])} posts (t3) for CSV...")
        try:
            # Open the posts CSV file for writing. 'newline=''' is important to prevent blank rows.
            with open(posts_csv_path, 'w', newline='', encoding='utf-8') as pfile:
                post_writer = csv.writer(pfile, quoting=csv.QUOTE_MINIMAL) # Use QUOTE_MINIMAL for handling commas/quotes in text
                post_writer.writerow(post_fieldnames) # Write the header row

                # Iterate through each post entry in the 't3' dictionary.
                for i, (entry_id, entry_data) in enumerate(data["t3"].items()):
                    # Basic validation of entry structure.
                    if not isinstance(entry_data, dict) or 'data' not in entry_data or not isinstance(entry_data['data'], dict):
                        posts_skipped_invalid += 1; continue # Skip invalid entries and count them

                    # Get the modification timestamp using the utility function. Skip if it fails.
                    modified_utc_ts = get_modification_date(entry_data)
                    if modified_utc_ts == 0: # get_modification_date returns 0 on failure
                        posts_skipped_invalid += 1; continue # Skip entries with no valid timestamp

                    # --- Date Filtering ---
                    # Check if the item's timestamp falls within the specified date range [start_ts, end_ts).
                    if not (start_ts <= modified_utc_ts < end_ts):
                         posts_filtered_date += 1; continue # Skip if outside date range and count it

                    # Get the actual data payload of the Reddit item.
                    edata = entry_data['data']
                    item_subreddit_lower = edata.get('subreddit', '').lower() # Get subreddit lowercase for filtering

                    # --- Combined Subreddit Filtering ---
                    # Check if the item's subreddit is in the focus list OR if there is no focus list.
                    focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                    # Check if the item's subreddit is NOT in the ignore list OR if there is no ignore list.
                    ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)

                    # An item is kept ONLY if it passes BOTH the focus and ignore filter checks.
                    if not (focus_match and ignore_match):
                        posts_filtered_sub += 1 # Increment the combined sub filter counter
                        continue # Skip this item

                    # --- Extract remaining data fields ---
                    # Extract fields from the item's data payload (edata). Use get() with default values
                    # to handle missing fields gracefully.
                    title = edata.get('title', '')
                    # Replace newlines and carriage returns in selftext with '<br>' and spaces for single CSV row.
                    selftext = edata.get('selftext', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                    permalink = edata.get('permalink', '')
                    created_utc = edata.get('created_utc', 0)
                    created_iso = format_timestamp(created_utc) # Format timestamps to ISO 8601 string
                    modified_iso = format_timestamp(modified_utc_ts)
                    subreddit = edata.get('subreddit', '') # Get original case for CSV column
                    score = edata.get('score', 0)
                    num_comments = edata.get('num_comments', 0)
                    link_flair = edata.get('link_flair_text', '') # Extract link flair text

                    # Write the extracted data as a row in the CSV file.
                    post_writer.writerow([title, selftext, permalink, created_iso, modified_iso, subreddit, score, num_comments, link_flair])
                    posts_written += 1 # Increment the count of posts successfully written

            # --- Post-writing checks and logging for Posts CSV ---
            if posts_written > 0:
                logging.info(f"      📄 Created posts CSV: {CYAN}{posts_csv_path}{RESET} ({posts_written} posts written)")
                posts_csv_created = True # Set flag indicating the CSV was created and is non-empty
            # Log summary of filtered/skipped posts if any occurred.
            if posts_filtered_date > 0 or posts_filtered_sub > 0 or posts_skipped_invalid > 0:
                 logging.info(f"         (Filtered: {posts_filtered_date} by date, {posts_filtered_sub} by subreddit rules. Skipped: {posts_skipped_invalid} invalid)")
                 # If no posts were written BUT the file exists (meaning it was created but remained empty after headers/filters), remove it.
                 if posts_written == 0 and os.path.exists(posts_csv_path):
                      try: os.remove(posts_csv_path); logging.info(f"         Removed empty posts CSV file: {CYAN}{posts_csv_path}{RESET}")
                      except OSError as e: logging.warning(f"      ⚠️ Could not remove empty/filtered posts CSV: {e}") # Log error if removal fails
            # If no posts were written at all (file might not have been created or was empty), log info.
            elif posts_written == 0:
                 logging.info("      ℹ️ No posts written (likely due to filters or empty data).")


        except IOError as e:
            # Handle errors specifically related to file writing.
            logging.error(f"      ❌ IOError writing posts CSV {CYAN}{posts_csv_path}{RESET}: {e}", exc_info=True);
            posts_csv_created = False # Ensure flag is False on error
        except Exception as e:
            # Handle any other unexpected errors during post processing.
            logging.error(f"      ❌ Unexpected error writing posts CSV {CYAN}{posts_csv_path}{RESET}: {e}", exc_info=True);
            posts_csv_created = False # Ensure flag is False on error

        # Clean up the posts CSV file if it was not successfully created (e.g., error occurred after opening but before writing).
        if not posts_csv_created and os.path.exists(posts_csv_path):
            try: os.remove(posts_csv_path)
            except OSError: pass # Ignore errors during cleanup

    else:
        # Log if no 't3' data was found in the JSON initially.
        logging.info("      ℹ️ No 't3' (posts) data found in JSON.")

    # --- Write Comments CSV ---
    # Check if the JSON data contains a 't1' key (representing comments) and if it's a non-empty dictionary.
    if "t1" in data and isinstance(data.get("t1"), dict) and data["t1"]:
        logging.debug(f"      Processing {len(data['t1'])} comments (t1) for CSV...")
        try:
            # Open the comments CSV file for writing.
            with open(comments_csv_path, 'w', newline='', encoding='utf-8') as cfile:
                comment_writer = csv.writer(cfile, quoting=csv.QUOTE_MINIMAL) # Use QUOTE_MINIMAL
                comment_writer.writerow(comment_fieldnames) # Write the header row

                # Iterate through each comment entry in the 't1' dictionary.
                for i, (entry_id, entry_data) in enumerate(data["t1"].items()):
                     # Basic validation of entry structure.
                     if not isinstance(entry_data, dict) or 'data' not in entry_data or not isinstance(entry_data['data'], dict):
                         comments_skipped_invalid += 1; continue # Skip invalid

                     # Get the modification timestamp. Skip if it fails.
                     modified_utc_ts = get_modification_date(entry_data)
                     if modified_utc_ts == 0:
                         comments_skipped_invalid += 1; continue # Skip entries with no valid timestamp

                     # --- Date Filtering ---
                     # Check if the item's timestamp is within the date range.
                     if not (start_ts <= modified_utc_ts < end_ts):
                          comments_filtered_date += 1; continue # Skip if outside date range

                     # Get the data payload.
                     edata = entry_data['data']
                     item_subreddit_lower = edata.get('subreddit', '').lower() # Get subreddit lowercase

                     # --- Combined Subreddit Filtering ---
                     # Apply focus and ignore filters similar to posts.
                     focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                     ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)

                     # Keep item ONLY if it passes BOTH checks.
                     if not (focus_match and ignore_match):
                         comments_filtered_sub += 1 # Increment combined sub filter counter
                         continue # Skip this item

                     # --- Extract remaining data fields ---
                     # Extract fields from the item's data payload. Replace newlines/tabs with spaces/breaks.
                     body = edata.get('body', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                     permalink = edata.get('permalink', '')
                     created_utc = edata.get('created_utc', 0)
                     created_iso = format_timestamp(created_utc)
                     modified_iso = format_timestamp(modified_utc_ts)
                     subreddit = edata.get('subreddit', '') # Original case
                     score = edata.get('score', 0)
                     author_flair = edata.get('author_flair_text', '') # Extract author flair text

                     # Write the extracted data as a row.
                     comment_writer.writerow([body, permalink, created_iso, modified_iso, subreddit, score, author_flair])
                     comments_written += 1 # Increment written count

            # --- Post-writing checks and logging for Comments CSV ---
            if comments_written > 0:
                logging.info(f"      📄 Created comments CSV: {CYAN}{comments_csv_path}{RESET} ({comments_written} comments written)")
                comments_csv_created = True # Set flag if CSV was created and non-empty
            # Log summary of filtered/skipped comments if any occurred.
            if comments_filtered_date > 0 or comments_filtered_sub > 0 or comments_skipped_invalid > 0:
                 logging.info(f"         (Filtered: {comments_filtered_date} by date, {comments_filtered_sub} by subreddit rules. Skipped: {comments_skipped_invalid} invalid)")
                 # If no comments were written BUT the file exists, remove it.
                 if comments_written == 0 and os.path.exists(comments_csv_path):
                      try: os.remove(comments_csv_path); logging.info(f"         Removed empty comments CSV file: {CYAN}{comments_csv_path}{RESET}")
                      except OSError as e: logging.warning(f"      ⚠️ Could not remove empty/filtered comments CSV: {e}")
            # If no comments were written at all, log info.
            elif comments_written == 0:
                 logging.info("      ℹ️ No comments written (likely due to filters or empty data).")

        except IOError as e:
            # Handle file writing errors for comments CSV.
            logging.error(f"      ❌ IOError writing comments CSV {CYAN}{comments_csv_path}{RESET}: {e}", exc_info=True);
            comments_csv_created = False
        except Exception as e:
            # Handle other unexpected errors during comment processing.
            logging.error(f"      ❌ Unexpected error writing comments CSV {CYAN}{comments_csv_path}{RESET}: {e}", exc_info=True);
            comments_csv_created = False

        # Clean up the comments CSV file if it was not successfully created.
        if not comments_csv_created and os.path.exists(comments_csv_path):
            try: os.remove(comments_csv_path)
            except OSError: pass # Ignore errors during cleanup

    else:
        # Log if no 't1' data was found in the JSON initially.
        logging.info("      ℹ️ No 't1' (comments) data found in JSON.")

    # --- Return Results ---
    # Return the paths to the created CSV files. If a file wasn't created (e.g., due to filters), return None for that path.
    final_posts_path = posts_csv_path if posts_csv_created else None
    final_comments_path = comments_csv_path if comments_csv_created else None
    logging.info(f"   ✅ CSV Extraction complete.")
    return final_posts_path, final_comments_path