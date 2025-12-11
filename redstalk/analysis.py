# analysis.py
"""
This module handles the process of loading, filtering, formatting, and preparing
Reddit user data (posts and comments) for AI analysis. It supports two main
modes: 'mapped' analysis, which attempts to group comments under their parent
posts, and 'raw' analysis, which treats all data entries sequentially.
It applies various filters (date, subreddit) before passing the prepared data
to the core AI analysis function in `ai_utils.py`.
"""

# Import standard libraries
import csv # For reading CSV files (where scraped data is stored)
import logging # For logging information, warnings, and errors during processing
import os # For checking file existence and paths
import re # For using regular expressions (specifically for parsing dates and permalinks)
from datetime import datetime, timezone # For handling date and time conversions and filtering
import time # To measure the duration of data preparation steps

# Import necessary utility functions from other modules
from .reddit_utils import get_post_title_from_permalink # Used in 'mapped' mode to get context for comments on external posts
from .ai_utils import perform_ai_analysis # The core function to send data to the AI model

# --- ANSI Codes (for logging ONLY) ---
# Define ANSI escape codes specifically for use within logging messages
# (ai_utils also defines these, but redefining locally keeps this file self-contained for logging colors)
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; YELLOW = "\033[33m"


def _apply_date_filter_to_entries(entries, date_filter):
    """
    Filters a list of already formatted analysis entry strings based on a date range.
    It looks for a specific date/timestamp pattern within each entry string.

    Args:
        entries: A list of strings, where each string is a formatted data entry
                 (either a post or a comment block).
        date_filter: A tuple (start_timestamp, end_timestamp) representing the
                     date range in Unix timestamps. Entries with a date/timestamp
                     within [start_timestamp, end_timestamp) are kept. If
                     start_timestamp is 0 and end_timestamp is infinity, no filter is applied.

    Returns:
        A new list containing only the entry strings that fall within the specified date range,
        or the original list if no date filter is active or parsing fails.
    """
    start_ts, end_ts = date_filter
    # Check if the date filter is effectively disabled.
    if start_ts <= 0 and end_ts == float('inf'):
        logging.debug("      No date filter applied to analysis entries.")
        return entries # Return the original list if no filter is active

    logging.debug(f"      Filtering {len(entries)} formatted analysis entries by date...")
    filtered_entries = []
    # Regex pattern to find the date string within the entry header.
    # It looks for "Date:" or "timestamp:" followed by a date/time pattern.
    # The '?' makes 'UTC' optional. re.IGNORECASE makes the pattern case-insensitive.
    date_pattern = re.compile(r"(?:Date|timestamp):\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s*(?:UTC)?)", re.IGNORECASE)

    items_kept = 0
    items_filtered = 0

    # Iterate through each entry string to check its date.
    for entry_index, entry in enumerate(entries):
        match = date_pattern.search(entry) # Try to find the date pattern in the entry string
        keep = True # Assume we keep the entry by default

        if match:
            try:
                date_str = match.group(1).strip() # Extract the date string found by the regex
                # Attempt to parse the date string into a datetime object.
                # Try parsing formats with or without explicit timezone (assuming UTC if missing).
                try:
                    dt_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)
                except ValueError:
                    dt_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)

                # Convert the datetime object to a Unix timestamp for easy comparison.
                entry_ts = dt_obj.timestamp()
                # Check if the entry's timestamp is within the specified range [start_ts, end_ts).
                if not (start_ts <= entry_ts < end_ts):
                    keep = False # If outside the range, mark for filtering out
                    # logging.debug(f" Filtering out entry {entry_index+1} (Date: {date_str})...") # Verbose logging
            except (ValueError, IndexError, AttributeError) as e:
                 # If date parsing or extraction fails, log a warning but keep the entry
                 # because we cannot definitively say it falls outside the filter range.
                 logging.warning(f" Could not parse date '{match.group(1) if match else 'N/A'}' for filtering in entry {entry_index+1}: {e}. Keeping.")
                 # 'keep' remains True

        else:
             # If the date pattern is not found in the entry, log a warning but keep the entry.
             # This might happen if the entry format is unexpected.
             logging.warning(f" Could not find date pattern for filtering in entry {entry_index+1}. Keeping.")
             # 'keep' remains True

        # Add the entry to the filtered list if it should be kept.
        if keep:
            filtered_entries.append(entry)
            items_kept += 1
        else:
            items_filtered += 1

    # Log a summary of the filtering results only if some filtering actually happened.
    if items_filtered > 0 or items_kept < len(entries):
         logging.info(f"      📊 Date filter applied to analysis entries: {items_kept} kept, {items_filtered} filtered out.")
         # Add a specific warning if all items were filtered out by date.
         if items_kept == 0 and items_filtered > 0:
             logging.warning("      ⚠️ All analysis entries were filtered out by the specified date range.")

    return filtered_entries # Return the list of entries that passed the date filter


# --- Mapped Analysis ---
# **** MODIFIED Function Signature ****
def generate_mapped_analysis(posts_csv, comments_csv, config, output_file, model, system_prompt, chunk_size,
                             date_filter=(0, float('inf')),
                             focus_subreddits=None, # Parameter name CHANGED for clarity (was subreddit_filter)
                             ignore_subreddits=None, # NEW Parameter added for ignoring subreddits
                             no_cache_titles=False, fetch_external_context=False):
    """
    Generates analysis in 'mapped' mode. Reads posts and comments from CSVs,
    attempts to map comments to their parent posts, applies subreddit and date
    filters, formats the data entries with comments nested under posts where possible,
    and then passes the formatted entries to the AI analysis core.

    Args:
        posts_csv: Path to the CSV file containing scraped post data.
        comments_csv: Path to the CSV file containing scraped comment data.
        config: Application configuration dictionary (needed for external context fetching).
        output_file: Path where the final AI analysis result should be saved.
        model: The AI model instance (from genai) to use for analysis.
        system_prompt: The system prompt string for the AI analysis.
        chunk_size: The maximum token size for data chunks sent to the AI.
        date_filter: A tuple (start_timestamp, end_timestamp) for date filtering.
        focus_subreddits: A list of subreddit names (case-insensitive) to *include*.
                          If None, all subreddits are included unless in `ignore_subreddits`.
        ignore_subreddits: A list of subreddit names (case-insensitive) to *exclude*.
                           Applied *after* `focus_subreddits` if both are provided.
                           If None, no subreddits are ignored.
        no_cache_titles: If True, disables caching when fetching external post titles.
        fetch_external_context: If True, attempts to fetch the title of posts
                                for comments that don't belong to a loaded post.

    Returns:
        True if the analysis process completed (even if with AI errors in the output),
        False if a critical error occurred during data loading/preparation.
    """
    logging.info(f"   Analysis Prep ({BOLD}Mapped Mode{RESET}): Reading & Filtering CSV data...")
    start_time = time.time() # Start the timer for data preparation

    # --- Prepare Filters ---
    # Determine if any filters are active for logging purposes.
    filter_active = date_filter[0] > 0 or date_filter[1] != float('inf') or focus_subreddits or ignore_subreddits
    # Convert focus and ignore subreddit lists to lowercase sets for efficient case-insensitive checking.
    focus_subs_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_subs_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None

    # --- Logging Filter Status ---
    # Construct a log message detailing which filters are active.
    filter_log_parts = []
    if date_filter[0] > 0 or date_filter[1] != float('inf'): filter_log_parts.append("Date")
    if focus_subs_set: filter_log_parts.append(f"Focus Subs ({len(focus_subs_set)})")
    if ignore_subs_set: filter_log_parts.append(f"Ignore Subs ({len(ignore_subs_set)})")
    if filter_log_parts: logging.info(f"      (Filters: {', '.join(filter_log_parts)} applied during CSV read)")

    # Log the status of external context fetching.
    if not fetch_external_context: logging.info(f"      ℹ️ External post context fetching is {BOLD}DISABLED{RESET}.")
    else: logging.info(f"      🌐 External post context fetching is {BOLD}ENABLED{RESET}.")
    # Warn the user if title caching is disabled, as it might increase runtime.
    if no_cache_titles: logging.warning("      ⚠️ Post title caching is disabled.")


    entries = [] # This list will hold the final formatted string entries for the AI
    posts_read = 0; comments_read = 0 # Counters for total rows read from CSVs
    posts_kept_count = 0; comments_kept_count = 0 # Counters for rows that pass subreddit filters
    posts_data_filtered = {} # Dictionary to store filtered post data, keyed by permalink (and potentially post_id)
    comments_data_filtered = [] # List to store filtered comment data dictionaries

    # --- Load & Filter Posts from CSV ---
    # Process the posts CSV if a path is provided and the file exists.
    if posts_csv and os.path.exists(posts_csv):
        logging.debug(f"      Loading & Filtering posts from {CYAN}{posts_csv}{RESET}")
        try:
            # Open and read the CSV file as a dictionary reader.
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                # Define the minimum required fields in the posts CSV.
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'title', 'selftext']
                # Check if all required fields are present in the CSV header.
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                    logging.error(f"      ❌ Required fields missing in posts CSV {CYAN}{os.path.basename(posts_csv)}{RESET}. Needed: {required_fields}"); return False # Abort if fields are missing
                # Iterate through each row (post) in the CSV.
                for i, row in enumerate(reader):
                    posts_read += 1 # Increment total posts read counter
                    current_sub = row.get('subreddit', '').lower() # Get subreddit and convert to lowercase for filtering

                    # **** MODIFIED Subreddit Filter Logic ****
                    # Apply focus filter: if a focus list exists, skip the row if the subreddit is NOT in the focus list.
                    if focus_subs_set and current_sub not in focus_subs_set:
                        continue # Skip this row and go to the next one
                    # Apply ignore filter: if an ignore list exists, skip the row if the subreddit IS in the ignore list.
                    # This is applied *after* the focus filter.
                    if ignore_subs_set and current_sub in ignore_subs_set:
                        continue # Skip this row and go to the next one

                    # If the row passes subreddit filters, extract relevant data.
                    permalink = row.get('permalink','').strip()
                    if not permalink: continue # Skip entries with no permalink

                    # Extract the post ID from the permalink using regex.
                    match = re.search(r'/comments/([^/]+)/', permalink)
                    post_id = match.group(1) if match else None

                    # Get the timestamp, preferring modified over created if available.
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')

                    # Store the filtered post data in the dictionary, keyed by permalink.
                    post_entry_data = { 'title': row.get('title','[NO TITLE]').strip(), 'selftext': row.get('selftext','').strip(),
                                        'permalink': permalink, 'timestamp': timestamp.strip(), 'comments': [], 'subreddit': row.get('subreddit','') } # Store original case sub name
                    posts_data_filtered[permalink] = post_entry_data
                    # Also store it keyed by post_id if found, to easily link comments later.
                    if post_id: posts_data_filtered[post_id] = post_entry_data # Link by ID too
                    posts_kept_count += 1 # Increment the count of posts that passed filters

        except Exception as e:
            # Catch and log any errors during CSV reading or processing.
            logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv}{RESET}: {e}", exc_info=True); return False # Abort on error

        # Log the number of posts read and kept after subreddit filters.
        logging.debug(f"         Loaded {posts_read} post rows, kept {posts_kept_count} after subreddit filters.")

    # --- Load & Filter Comments from CSV ---
    # Process the comments CSV if a path is provided and the file exists.
    if comments_csv and os.path.exists(comments_csv):
        logging.debug(f"      Loading & Filtering comments from {CYAN}{comments_csv}{RESET}")
        try:
            # Open and read the CSV file as a dictionary reader.
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                # Define the minimum required fields in the comments CSV.
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'body']
                # Check if all required fields are present.
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                     logging.error(f"      ❌ Required fields missing in comments CSV {CYAN}{os.path.basename(comments_csv)}{RESET}. Needed: {required_fields}"); return False # Abort
                # Iterate through each row (comment) in the CSV.
                for i, row in enumerate(reader):
                    comments_read += 1 # Increment total comments read counter
                    current_sub = row.get('subreddit', '').lower() # Get subreddit and convert to lowercase

                    # **** MODIFIED Subreddit Filter Logic ****
                    # Apply focus filter: if a focus list exists, skip if subreddit is NOT in the focus list.
                    if focus_subs_set and current_sub not in focus_subs_set:
                        continue # Skip this row
                    # Apply ignore filter: if an ignore list exists, skip if subreddit IS in the ignore list.
                    if ignore_subs_set and current_sub in ignore_subs_set:
                        continue # Skip this row

                    # If the row passes subreddit filters, extract relevant data.
                    comment_permalink = row.get('permalink','').strip();
                    if not comment_permalink: continue # Skip entries with no permalink
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    # Store the filtered comment data dictionary in the list.
                    comments_data_filtered.append({ 'body': row.get('body','[NO BODY]').strip(), 'permalink': comment_permalink,
                                                    'timestamp': timestamp.strip(), 'subreddit': row.get('subreddit','') }) # Store original case sub name
                    comments_kept_count += 1 # Increment the count of comments that passed filters

        except Exception as e:
            # Catch and log any errors during CSV reading or processing.
            logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv}{RESET}: {e}", exc_info=True); return False # Abort

        # Log the number of comments read and kept after subreddit filters.
        logging.debug(f"         Loaded {comments_read} comment rows, kept {comments_kept_count} after subreddit filters.")

    # --- Assemble Filtered Entries ---
    # Now, combine the filtered post and comment data into structured entries for the AI.
    logging.debug(f"      Assembling {posts_kept_count} filtered posts and {comments_kept_count} filtered comments into analysis entries...")
    processed_comment_permalinks = set() # Keep track of comments that have been added to a post's section to avoid adding them again later

    # First, process the filtered posts.
    for post_permalink, post_data in posts_data_filtered.items():
         # Ensure we are processing an actual post entry (keyed by permalink starting with '/')
         # and not a duplicate entry keyed by just the post_id.
         if not isinstance(post_permalink, str) or not post_permalink.startswith('/'): continue

         # Format the post data into a string block.
         post_header = f"USER'S POST TITLE: {post_data['title']} (Date: {post_data['timestamp']}) (Sub: /r/{post_data['subreddit']}) (Permalink: https://www.reddit.com{post_data['permalink']})"
         post_body_text = post_data['selftext'].replace('<br>', ' ').strip() or '[No Body]' # Clean up HTML breaks and strip whitespace
         post_block = f"{post_header}\nPOST BODY:\n{post_body_text}"

         # Find comments associated with this post.
         post_id_match = re.search(r'/comments/([^/]+)/', post_permalink)
         post_id_for_comments = post_id_match.group(1) if post_id_match else None
         comments_for_this_post = []
         if post_id_for_comments:
              for comment_info in comments_data_filtered:
                  c_perm = comment_info['permalink']
                  # A comment permalink contains the parent post's ID. Check if the comment's permalink
                  # contains the ID of the current post being processed, and if the comment hasn't
                  # already been added to a post's comment section (shouldn't happen often but safe check).
                  if f"/comments/{post_id_for_comments}/" in c_perm and c_perm not in processed_comment_permalinks:
                       comments_for_this_post.append(comment_info)
                       processed_comment_permalinks.add(c_perm) # Mark the comment as processed

         # If comments were found for this post, append them to the post's block.
         if comments_for_this_post:
              post_block += "\n\n  --- Comments on this Post ---"
              for comment_info in comments_for_this_post:
                  comment_body_cleaned = comment_info['body'].replace('<br>', ' ').strip() or '[No Body]'
                  # Indent comments slightly to visually group them under the post.
                  post_block += (f"\n  ↳ USER'S COMMENT (Date: {comment_info['timestamp']}):\n"
                                 f"    {comment_body_cleaned}\n"
                                 f"    (Permalink: https://www.reddit.com{comment_info['permalink']})")
              post_block += "\n  --- End Comments on this Post ---"

         # Add the complete post block (with nested comments if any) to the list of entries for the AI.
         entries.append(post_block)

    # Now, add comments that were filtered but did *not* belong to any post that was kept.
    # These are typically comments made by the user on other people's posts that were not scraped.
    external_comments_added = 0
    for comment_info in comments_data_filtered:
        comment_permalink = comment_info['permalink']
        # Check if this comment's permalink was *not* added to the processed_comment_permalinks set.
        # This means it wasn't attached to a kept post.
        if comment_permalink not in processed_comment_permalinks:
            comment_body_cleaned = comment_info['body'].replace('<br>', ' ').strip() or '[No Body]'
            comment_timestamp = comment_info['timestamp']; comment_subreddit = comment_info['subreddit']

            # Try to fetch the title of the post this comment is on, if fetch_external_context is enabled.
            post_title = "[Context Fetch Disabled]" # Default text if fetching is off
            if fetch_external_context:
                # Use the helper function to get the post title. Caching can be disabled via `no_cache_titles`.
                fetched_title = get_post_title_from_permalink(comment_permalink, config, use_cache=(not no_cache_titles))
                post_title = fetched_title # Use the fetched title or error string from the utility function

            # Format the external comment into its own block.
            comment_block = (
                f"--- USER'S COMMENT ON EXTERNAL/OTHER POST ---\n"
                f"COMMENT IN SUBREDDIT: /r/{comment_subreddit}\n"
                f"EXTERNAL POST CONTEXT (Title): {post_title}\n" # No ANSI codes here, keep output plain text
                f"USER'S COMMENT (Date: {comment_timestamp}):\n{comment_body_cleaned}\n"
                f"(Comment Permalink: https://www.reddit.com{comment_permalink})\n"
                f"--- END COMMENT ON EXTERNAL/OTHER POST ---"
            )
            # Add the external comment block to the list of entries and increment the counter.
            entries.append(comment_block); external_comments_added += 1
            processed_comment_permalinks.add(comment_permalink) # Mark as processed so it's not double-counted

    # Log how many external comments were added if any.
    if external_comments_added > 0: logging.debug(f"         Added {external_comments_added} comments on external/other posts.")


    prep_duration = time.time() - start_time # Calculate total preparation time
    logging.info(f"   ✅ Analysis Prep ({BOLD}Mapped Mode{RESET}): Prepared {len(entries)} potential entries ({prep_duration:.2f}s) after subreddit filters.")

    # --- APPLY DATE FILTER to assembled `entries` list ---
    # Apply the date filter to the list of formatted string entries.
    final_entries = _apply_date_filter_to_entries(entries, date_filter)

    # Check if any entries remain after all filters.
    if not final_entries:
        logging.error(f"   ❌ No data entries remaining after all filters for mapped analysis.")
        return False # Abort if no data is left

    # --- Pass final entries to AI analysis core ---
    logging.info(f"   🚀 Passing {len(final_entries)} filtered entries to AI analysis core...")
    # Call the core analysis function from ai_utils with the prepared data.
    return perform_ai_analysis(model, system_prompt, final_entries, output_file, chunk_size)


# --- Raw Analysis ---
# **** MODIFIED Function Signature ****
def generate_raw_analysis(posts_csv, comments_csv, output_file, model, system_prompt, chunk_size,
                          date_filter=(0, float('inf')),
                          focus_subreddits=None, # Parameter name CHANGED
                          ignore_subreddits=None): # NEW Parameter added
    """
    Generates analysis in 'raw' mode. Reads posts and comments from CSVs,
    applies subreddit and date filters, formats each post/comment as a separate,
    sequential entry, and then passes the formatted entries to the AI analysis core.
    Comments are NOT mapped to their parent posts in this mode.

    Args:
        posts_csv: Path to the CSV file containing scraped post data.
        comments_csv: Path to the CSV file containing scraped comment data.
        output_file: Path where the final AI analysis result should be saved.
        model: The AI model instance (from genai) to use for analysis.
        system_prompt: The system prompt string for the AI analysis.
        chunk_size: The maximum token size for data chunks sent to the AI.
        date_filter: A tuple (start_timestamp, end_timestamp) for date filtering.
        focus_subreddits: A list of subreddit names (case-insensitive) to *include*.
                          If None, all subreddits are included unless in `ignore_subreddits`.
        ignore_subreddits: A list of subreddit names (case-insensitive) to *exclude*.
                           Applied *after* `focus_subreddits` if both are provided.
                           If None, no subreddits are ignored.

    Returns:
        True if the analysis process completed (even if with AI errors in the output),
        False if a critical error occurred during data loading/preparation.
    """
    logging.info(f"   Analysis Prep ({BOLD}Raw Mode{RESET}): Reading & Filtering CSV data...")
    start_time = time.time() # Start the timer for data preparation

    # --- Prepare Filters ---
    # Determine if any filters are active for logging purposes.
    filter_active = date_filter[0] > 0 or date_filter[1] != float('inf') or focus_subreddits or ignore_subreddits
    # Convert focus and ignore subreddit lists to lowercase sets for efficient case-insensitive checking.
    focus_subs_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_subs_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None

    # --- Logging Filter Status ---
    # Construct a log message detailing which filters are active.
    filter_log_parts = []
    if date_filter[0] > 0 or date_filter[1] != float('inf'): filter_log_parts.append("Date")
    if focus_subs_set: filter_log_parts.append(f"Focus Subs ({len(focus_subs_set)})")
    if ignore_subs_set: filter_log_parts.append(f"Ignore Subs ({len(ignore_subs_set)})")
    if filter_log_parts: logging.info(f"      (Filters: {', '.join(filter_log_parts)} applied during CSV read)")

    entries = [] # This list will hold all filtered and formatted entries (posts and comments mixed)
    posts_read = 0; comments_read = 0 # Counters for total rows read from CSVs
    posts_kept_count = 0; comments_kept_count = 0 # Counters for rows that pass subreddit filters

    # --- Process & Filter Posts from CSV ---
    # Process the posts CSV if a path is provided and the file exists.
    if posts_csv and os.path.exists(posts_csv):
        logging.debug(f"      Loading & Filtering posts from {CYAN}{posts_csv}{RESET}")
        try:
            # Open and read the CSV file as a dictionary reader.
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                # Define the minimum required fields for posts in raw mode.
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'title', 'selftext']
                # Check if required fields are present.
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                     logging.error(f"      ❌ Required fields missing in posts CSV {CYAN}{os.path.basename(posts_csv)}{RESET}. Needed: {required_fields}"); return False # Abort
                # Iterate through each row (post).
                for i, row in enumerate(reader):
                    posts_read += 1 # Increment total posts read
                    current_sub = row.get('subreddit', '').lower() # Get subreddit lowercase

                    # **** MODIFIED Subreddit Filter Logic ****
                    # Apply focus filter.
                    if focus_subs_set and current_sub not in focus_subs_set:
                        continue # Skip
                    # Apply ignore filter.
                    if ignore_subs_set and current_sub in ignore_subs_set:
                        continue # Skip

                    # If filters pass, format the post into a string entry immediately.
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    body_cleaned = row.get('selftext', '').replace('<br>', ' ').strip() or '[No Body]' # Clean up HTML breaks
                    permalink = row.get('permalink', 'UNKNOWN_PERMALINK').strip()
                    full_permalink = f"https://www.reddit.com{permalink}" if permalink.startswith('/') else permalink
                    entry = (f"--- POST START ---\n"
                             f"Date: {timestamp.strip()}\n" # Include date for later date filtering
                             f"Subreddit: /r/{row.get('subreddit', 'N/A')}\n" # Include subreddit for context
                             f"Permalink: {full_permalink}\n"
                             f"Title: {row.get('title', '[NO TITLE]').strip()}\n" # Include title
                             f"Body:\n{body_cleaned}\n" # Include body
                             f"--- POST END ---")
                    entries.append(entry) # Add the formatted entry to the main list
                    posts_kept_count += 1 # Increment count of posts kept

        except Exception as e:
            # Catch and log errors during post processing.
            logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv}{RESET}: {e}", exc_info=True); return False # Abort on error

        # Log post read and kept counts.
        logging.debug(f"         Loaded {posts_read} post rows, kept {posts_kept_count} after subreddit filters.")


    # --- Process & Filter Comments from CSV ---
    # Process the comments CSV if a path is provided and the file exists.
    if comments_csv and os.path.exists(comments_csv):
        logging.debug(f"      Loading & Filtering comments from {CYAN}{comments_csv}{RESET}")
        try:
            # Open and read the comments CSV.
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                # Define required fields for comments in raw mode.
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'body']
                # Check required fields.
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                     logging.error(f"      ❌ Required fields missing in comments CSV {CYAN}{os.path.basename(comments_csv)}{RESET}. Needed: {required_fields}"); return False # Abort
                # Iterate through each row (comment).
                for i, row in enumerate(reader):
                    comments_read += 1 # Increment total comments read
                    current_sub = row.get('subreddit', '').lower() # Get subreddit lowercase

                    # **** MODIFIED Subreddit Filter Logic ****
                    # Apply focus filter.
                    if focus_subs_set and current_sub not in focus_subs_set:
                        continue # Skip
                    # Apply ignore filter.
                    if ignore_subs_set and current_sub in ignore_subs_set:
                        continue # Skip

                    # If filters pass, format the comment into a string entry immediately.
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    body_cleaned = row.get('body', '[NO BODY]').replace('<br>', ' ').strip() or '[No Body]' # Clean up HTML breaks
                    permalink = row.get('permalink', 'UNKNOWN_PERMALINK').strip()
                    full_permalink = f"https://www.reddit.com{permalink}" if permalink.startswith('/') else permalink
                    entry = (f"--- COMMENT START ---\n"
                             f"Date: {timestamp.strip()}\n" # Include date for filtering
                             f"Subreddit: /r/{row.get('subreddit', 'N/A')}\n" # Include subreddit
                             f"Permalink: {full_permalink}\n"
                             f"Body:\n{body_cleaned}\n" # Include body
                             f"--- COMMENT END ---")
                    entries.append(entry) # Add the formatted entry to the main list
                    comments_kept_count += 1 # Increment count of comments kept

        except Exception as e:
            # Catch and log errors during comment processing.
            logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv}{RESET}: {e}", exc_info=True); return False # Abort on error

        # Log comment read and kept counts.
        logging.debug(f"         Loaded {comments_read} comment rows, kept {comments_kept_count} after subreddit filters.")

    # Log total counts after subreddit filters.
    prep_duration = time.time() - start_time # Calculate total preparation time
    logging.info(f"   ✅ Analysis Prep ({BOLD}Raw Mode{RESET}): Prepared {len(entries)} potential entries ({prep_duration:.2f}s) after subreddit filters.")
    logging.info(f"      (Total rows read: {posts_read+comments_read}. Kept after sub filters: Posts={posts_kept_count}, Comments={comments_kept_count})")


    # --- APPLY DATE FILTER to assembled `entries` list ---
    # Apply the date filter to the flat list of formatted string entries.
    final_entries = _apply_date_filter_to_entries(entries, date_filter)

    # Check if any entries remain after all filters (subreddit + date).
    if not final_entries:
        logging.error(f"   ❌ No data entries remaining after all filters for raw analysis.")
        return False # Abort if no data is left

    # --- Pass final entries to AI analysis core ---
    logging.info(f"   🚀 Passing {len(final_entries)} filtered entries to AI analysis core...")
    # Call the core analysis function from ai_utils with the prepared data.
    # In raw mode, `final_entries` is just a flat list of post and comment strings.
    return perform_ai_analysis(model, system_prompt, final_entries, output_file, chunk_size)
# --- Subreddit Persona Analysis ---
def generate_subreddit_persona_analysis(posts_csv, comments_csv, output_file, model, system_prompt, chunk_size,
                                      date_filter=(0, float('inf')),
                                      focus_subreddits=None,
                                      ignore_subreddits=None):
    """
    Generates analysis in 'subreddit_persona' mode. 
    Aggregates user activity by subreddit to facilitate personality analysis 
    based on community interactions.

    Args:
        posts_csv: Path to the CSV file containing scraped post data.
        comments_csv: Path to the CSV file containing scraped comment data.
        output_file: Path where the final AI analysis result should be saved.
        model: The AI model instance to use for analysis.
        system_prompt: The system prompt string for the AI analysis.
        chunk_size: The maximum token size for data chunks sent to the AI.
        date_filter: A tuple (start_timestamp, end_timestamp) for date filtering.
        focus_subreddits: A list of subreddit names to *include*.
        ignore_subreddits: A list of subreddit names to *exclude*.

    Returns:
        True if analysis completed, False otherwise.
    """
    logging.info(f"   Analysis Prep ({BOLD}Subreddit Persona Mode{RESET}): Aggregating data by subreddit...")
    start_time = time.time()

    # --- Prepare Filters ---
    focus_subs_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_subs_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None

    # Data structure: {subreddit_name: {'posts': [], 'comments': []}}
    subreddit_data = {}

    total_items_read = 0
    items_kept = 0

    # Helper to process a row
    def process_row(row, is_post):
        current_sub = row.get('subreddit', '').lower()
        original_sub_name = row.get('subreddit', 'Unknown')
        
        # Filters
        if focus_subs_set and current_sub not in focus_subs_set: return
        if ignore_subs_set and current_sub in ignore_subs_set: return

        # Date Filter
        timestamp_str = row.get('modified_utc_iso') or row.get('created_utc_iso', '')
        try:
             # Simple timestamp check similar to other modes
             if timestamp_str:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                ts = dt.timestamp()
                if not (date_filter[0] <= ts < date_filter[1]):
                    return
        except ValueError:
            pass # Keep if date parse fails

        if current_sub not in subreddit_data:
            subreddit_data[current_sub] = {'name': original_sub_name, 'posts': [], 'comments': []}
        
        if is_post:
            subreddit_data[current_sub]['posts'].append(row)
        else:
            subreddit_data[current_sub]['comments'].append(row)
        
        nonlocal items_kept
        items_kept += 1

    # Load Posts
    if posts_csv and os.path.exists(posts_csv):
        try:
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    total_items_read += 1
                    process_row(row, is_post=True)
        except Exception as e:
            logging.error(f"      ❌ Error reading posts CSV: {e}")
            return False

    # Load Comments
    if comments_csv and os.path.exists(comments_csv):
        try:
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    total_items_read += 1
                    process_row(row, is_post=False)
        except Exception as e:
            logging.error(f"      ❌ Error reading comments CSV: {e}")
            return False

    if not subreddit_data:
         logging.error(f"   ❌ No data found after filtering.")
         return False

    # Format entries
    # Each entry will be a summary of activity in one subreddit
    entries = []
    
    # Sort subreddits by activity volume
    sorted_subs = sorted(subreddit_data.values(), 
                         key=lambda x: len(x['posts']) + len(x['comments']), 
                         reverse=True)

    for sub_info in sorted_subs:
        sub_name = sub_info['name']
        posts = sub_info['posts']
        comments = sub_info['comments']
        
        entry_text = f"=== SUBREDDIT: r/{sub_name} ===\n"
        entry_text += f"Activity Summary: {len(posts)} Posts, {len(comments)} Comments\n\n"
        
        if posts:
            entry_text += "--- POSTS ---\n"
            for p in posts:
                entry_text += f"Title: {p.get('title', 'N/A')}\n"
                entry_text += f"Date: {p.get('created_utc_iso', 'N/A')}\n"
                body = p.get('selftext', '').replace('<br>', ' ').strip()
                if body: entry_text += f"Body: {body[:500]}{'...' if len(body)>500 else ''}\n"
                entry_text += "\n"
        
        if comments:
            entry_text += "--- COMMENTS ---\n"
            for c in comments:
                body = c.get('body', '').replace('<br>', ' ').strip()
                entry_text += f"Date: {c.get('created_utc_iso', 'N/A')}\n"
                entry_text += f"Comment: {body}\n\n"
        
        entry_text += f"=== END SUBREDDIT r/{sub_name} ===\n"
        entries.append(entry_text)

    prep_duration = time.time() - start_time
    logging.info(f"   ✅ Prepared {len(entries)} subreddit blocks in {prep_duration:.2f}s.")

    # In this mode, we might want to augment the system prompt if the user used the default
    # but that's hard to know. We'll trust the user or the default prompt to be generic enough.
    
    return perform_ai_analysis(model, system_prompt, entries, output_file, chunk_size)
