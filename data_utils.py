import csv
import logging
import json
import os
from datetime import datetime, timezone
from typing import List, Optional, Tuple # Import typing for clarity

# Import necessary functions from other utils
from reddit_utils import get_modification_date, format_timestamp # Ensure these are available

# Import ANSI codes
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; YELLOW = "\033[33m"

def extract_csvs_from_json(
    json_path: str,
    output_prefix: str,
    date_filter: Tuple[float, float] = (0, float('inf')),
    focus_subreddits: Optional[List[str]] = None, # NEW
    ignore_subreddits: Optional[List[str]] = None  # NEW
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts posts and comments from JSON to CSV, applying optional filters.

    Args:
        json_path (str): Path to the input JSON file.
        output_prefix (str): Prefix for the output CSV filenames.
        date_filter (tuple): A tuple (start_timestamp, end_timestamp) for date filtering.
        focus_subreddits (list[str] | None): List of subreddits to *include*. If None, include all.
        ignore_subreddits (list[str] | None): List of subreddits to *exclude*. If None, exclude none.

    Returns:
        tuple(str | None, str | None): Paths to the created posts and comments CSV files, or None if not created/empty.
    """
    posts_csv_path = f"{output_prefix}-posts.csv"
    comments_csv_path = f"{output_prefix}-comments.csv"
    posts_written, comments_written = 0, 0
    posts_filtered_date, comments_filtered_date = 0, 0
    posts_filtered_sub, comments_filtered_sub = 0, 0 # Counts items filtered by EITHER focus or ignore
    posts_skipped_invalid, comments_skipped_invalid = 0, 0
    posts_csv_created, comments_csv_created = False, False
    start_ts, end_ts = date_filter

    logging.info(f"   ⚙️ Extracting data from {CYAN}{json_path}{RESET} to CSV files...")

    # Log filters being applied
    filter_log_parts = []
    if start_ts > 0 or end_ts != float('inf'):
        start_str = datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d') if start_ts > 0 else 'Beginning'
        end_str = datetime.fromtimestamp(end_ts - 1, timezone.utc).strftime('%Y-%m-%d') if end_ts != float('inf') else 'End'
        filter_log_parts.append(f"Date: {start_str} to {end_str} (UTC)")
    if focus_subreddits:
        filter_log_parts.append(f"Focusing on: {focus_subreddits}")
    if ignore_subreddits:
        filter_log_parts.append(f"Ignoring: {ignore_subreddits}")

    if filter_log_parts:
        logging.info(f"      Applying Filters: {'; '.join(filter_log_parts)}")

    # --- Pre-process filter lists for efficient lookup ---
    focus_lower_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_lower_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None


    try:
        with open(json_path, "r", encoding="utf-8") as f: data = json.load(f)
    except FileNotFoundError: logging.error(f"   ❌ JSON file not found: {CYAN}{json_path}{RESET}"); return None, None
    except json.JSONDecodeError: logging.error(f"   ❌ Error decoding JSON: {CYAN}{json_path}{RESET}"); return None, None
    except Exception as e: logging.error(f"   ❌ Error reading JSON {CYAN}{json_path}{RESET}: {e}"); return None, None

    if not isinstance(data, dict): logging.error(f"   ❌ JSON file {CYAN}{json_path}{RESET} is not a dict."); return None, None

    # Ensure standard fields plus subreddit, score, num_comments are included
    post_fieldnames = ['title', 'selftext', 'permalink', 'created_utc_iso', 'modified_utc_iso', 'subreddit', 'score', 'num_comments', 'link_flair_text'] # Added flair
    comment_fieldnames = ['body', 'permalink', 'created_utc_iso', 'modified_utc_iso', 'subreddit', 'score', 'author_flair_text'] # Added flair

    # --- Write Posts CSV ---
    if "t3" in data and isinstance(data.get("t3"), dict) and data["t3"]:
        logging.debug(f"      Processing {len(data['t3'])} posts (t3) for CSV...")
        try:
            with open(posts_csv_path, 'w', newline='', encoding='utf-8') as pfile:
                post_writer = csv.writer(pfile, quoting=csv.QUOTE_MINIMAL)
                post_writer.writerow(post_fieldnames)
                for i, (entry_id, entry_data) in enumerate(data["t3"].items()):
                    if not isinstance(entry_data, dict) or 'data' not in entry_data or not isinstance(entry_data['data'], dict):
                        posts_skipped_invalid += 1; continue

                    modified_utc_ts = get_modification_date(entry_data)
                    if modified_utc_ts == 0:
                        posts_skipped_invalid += 1; continue

                    # --- Date Filtering ---
                    if not (start_ts <= modified_utc_ts < end_ts):
                         posts_filtered_date += 1; continue

                    edata = entry_data['data']
                    item_subreddit_lower = edata.get('subreddit', '').lower()

                    # --- Combined Subreddit Filtering ---
                    # Apply focus filter (if provided): MUST be in focus_lower_set
                    focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                    # Apply ignore filter (if provided): MUST NOT be in ignore_lower_set
                    ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)

                    # Keep item ONLY if it passes BOTH checks
                    if not (focus_match and ignore_match):
                        posts_filtered_sub += 1 # Increment the combined sub filter counter
                        continue

                    # --- Extract remaining data ---
                    title = edata.get('title', '')
                    selftext = edata.get('selftext', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                    permalink = edata.get('permalink', '')
                    created_utc = edata.get('created_utc', 0)
                    created_iso = format_timestamp(created_utc)
                    modified_iso = format_timestamp(modified_utc_ts)
                    subreddit = edata.get('subreddit', '') # Get original case for CSV
                    score = edata.get('score', 0)
                    num_comments = edata.get('num_comments', 0)
                    link_flair = edata.get('link_flair_text', '') # Get link flair

                    post_writer.writerow([title, selftext, permalink, created_iso, modified_iso, subreddit, score, num_comments, link_flair])
                    posts_written += 1

            if posts_written > 0:
                logging.info(f"      📄 Created posts CSV: {CYAN}{posts_csv_path}{RESET} ({posts_written} posts written)")
                posts_csv_created = True
            if posts_filtered_date > 0 or posts_filtered_sub > 0 or posts_skipped_invalid > 0:
                 logging.info(f"         (Filtered: {posts_filtered_date} by date, {posts_filtered_sub} by subreddit rules. Skipped: {posts_skipped_invalid} invalid)")
                 if posts_written == 0 and os.path.exists(posts_csv_path): # Remove file if filters left it empty
                      try: os.remove(posts_csv_path); logging.info(f"         Removed empty posts CSV file: {CYAN}{posts_csv_path}{RESET}")
                      except OSError as e: logging.warning(f"      ⚠️ Could not remove empty/filtered posts CSV: {e}")
            elif posts_written == 0:
                 logging.info("      ℹ️ No posts written (likely due to filters or empty data).")


        except IOError as e: logging.error(f"      ❌ IOError writing posts CSV {CYAN}{posts_csv_path}{RESET}: {e}"); posts_csv_created = False
        except Exception as e: logging.error(f"      ❌ Unexpected error writing posts CSV {CYAN}{posts_csv_path}{RESET}: {e}"); posts_csv_created = False
        if not posts_csv_created and os.path.exists(posts_csv_path):
            try: os.remove(posts_csv_path)
            except OSError: pass

    else: logging.info("      ℹ️ No 't3' (posts) data found in JSON.")

    # --- Write Comments CSV ---
    if "t1" in data and isinstance(data.get("t1"), dict) and data["t1"]:
        logging.debug(f"      Processing {len(data['t1'])} comments (t1) for CSV...")
        try:
            with open(comments_csv_path, 'w', newline='', encoding='utf-8') as cfile:
                comment_writer = csv.writer(cfile, quoting=csv.QUOTE_MINIMAL)
                comment_writer.writerow(comment_fieldnames)
                for i, (entry_id, entry_data) in enumerate(data["t1"].items()):
                     if not isinstance(entry_data, dict) or 'data' not in entry_data or not isinstance(entry_data['data'], dict):
                         comments_skipped_invalid += 1; continue

                     modified_utc_ts = get_modification_date(entry_data)
                     if modified_utc_ts == 0:
                         comments_skipped_invalid += 1; continue

                     # --- Date Filtering ---
                     if not (start_ts <= modified_utc_ts < end_ts):
                          comments_filtered_date += 1; continue

                     edata = entry_data['data']
                     item_subreddit_lower = edata.get('subreddit', '').lower()

                     # --- Combined Subreddit Filtering ---
                     focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                     ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)

                     if not (focus_match and ignore_match):
                         comments_filtered_sub += 1 # Increment the combined sub filter counter
                         continue

                     # --- Extract remaining data ---
                     body = edata.get('body', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                     permalink = edata.get('permalink', '')
                     created_utc = edata.get('created_utc', 0)
                     created_iso = format_timestamp(created_utc)
                     modified_iso = format_timestamp(modified_utc_ts)
                     subreddit = edata.get('subreddit', '') # Get original case for CSV
                     score = edata.get('score', 0)
                     author_flair = edata.get('author_flair_text', '') # Get author flair

                     comment_writer.writerow([body, permalink, created_iso, modified_iso, subreddit, score, author_flair])
                     comments_written += 1

            if comments_written > 0:
                logging.info(f"      📄 Created comments CSV: {CYAN}{comments_csv_path}{RESET} ({comments_written} comments written)")
                comments_csv_created = True
            if comments_filtered_date > 0 or comments_filtered_sub > 0 or comments_skipped_invalid > 0:
                 logging.info(f"         (Filtered: {comments_filtered_date} by date, {comments_filtered_sub} by subreddit rules. Skipped: {comments_skipped_invalid} invalid)")
                 if comments_written == 0 and os.path.exists(comments_csv_path): # Remove file if filters left it empty
                      try: os.remove(comments_csv_path); logging.info(f"         Removed empty comments CSV file: {CYAN}{comments_csv_path}{RESET}")
                      except OSError as e: logging.warning(f"      ⚠️ Could not remove empty/filtered comments CSV: {e}")
            elif comments_written == 0:
                 logging.info("      ℹ️ No comments written (likely due to filters or empty data).")

        except IOError as e: logging.error(f"      ❌ IOError writing comments CSV {CYAN}{comments_csv_path}{RESET}: {e}"); comments_csv_created = False
        except Exception as e: logging.error(f"      ❌ Unexpected error writing comments CSV {CYAN}{comments_csv_path}{RESET}: {e}"); comments_csv_created = False
        if not comments_csv_created and os.path.exists(comments_csv_path):
            try: os.remove(comments_csv_path)
            except OSError: pass

    else: logging.info("      ℹ️ No 't1' (comments) data found in JSON.")

    final_posts_path = posts_csv_path if posts_csv_created else None
    final_comments_path = comments_csv_path if comments_csv_created else None
    logging.info(f"   ✅ CSV Extraction complete.")
    return final_posts_path, final_comments_path