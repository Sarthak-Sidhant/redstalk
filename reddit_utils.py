# reddit_utils.py
"""
This module provides utility functions for interacting with the Reddit API.
It handles fetching user posts and comments, saving and loading this data
to/from JSON files, extracting specific information like modification dates,
and fetching external data like post titles from comment permalinks.
It also includes helpers for timestamp formatting.
"""

# Import standard libraries
import requests # For making HTTP requests to the Reddit API
import logging # For logging information and errors
import json # For working with JSON data
import time # For adding delays (e.g., between API requests)
import os # For file path manipulation and existence checks
import re # For using regular expressions (e.g., extracting IDs from permalinks)
from datetime import datetime, timezone # For date/time handling, specifically UTC timezone

# Import ANSI codes for coloring output messages.
CYAN = "\036m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; YELLOW = "\033[33m"


# --- Globals specific to this module ---
# A simple in-memory cache for post titles fetched externally.
# This prevents repeated API calls for the same post title if multiple comments
# link back to it within a single script execution.
post_title_cache = {}

# --- Helper Functions ---

def format_timestamp(utc_timestamp):
    """
    Formats a Unix timestamp (seconds since epoch in UTC) into a human-readable
    string in 'YYYY-MM-DD HH:MM:SS UTC' format.

    Handles potential errors in input and returns a fallback string.

    Args:
        utc_timestamp: The Unix timestamp (float or int).

    Returns:
        A formatted string representation of the timestamp, or "UNKNOWN_DATE"
        if the input is invalid or out of a reasonable range.
    """
    try:
        # Attempt to convert the input to a float. Return fallback if conversion fails.
        ts = float(utc_timestamp) if utc_timestamp is not None else 0
        # Add basic sanity check for the timestamp value. Unix timestamps started
        # in 1970. Checking for post-2000 and not too far in the future helps
        # catch corrupted data.
        if ts <= 946684800 or ts > time.time() * 1.1: # 946684800 is approx 2000-01-01
            raise ValueError("Timestamp out of reasonable range (expected post-2000, not too far future)")

        # Convert the timestamp to a datetime object in UTC.
        dt_object = datetime.fromtimestamp(ts, timezone.utc)
        # Format the datetime object into the desired string format.
        return dt_object.strftime('%Y-%m-%d %H:%M:%S UTC')
    except (ValueError, TypeError, OSError) as e:
        # Catch errors during conversion or formatting. Log a warning and return the fallback.
        logging.warning(f"      ⚠️ Could not format timestamp '{utc_timestamp}': {e}. Using fallback.")
        return "UNKNOWN_DATE"

def get_modification_date(entry):
    """
    Determines the most recent modification date (either 'edited' or 'created_utc')
    for a Reddit API entry (post or comment).

    Prioritizes 'edited' timestamp if it's valid and after 'created_utc'.

    Args:
        entry (dict): A dictionary representing a single Reddit API item, expected
                      to contain a 'data' key which is also a dictionary.

    Returns:
        The most recent Unix timestamp (float) if successful, or 0 if the input
        structure is invalid or timestamps cannot be determined/parsed.
    """
    # Validate that the input is a dictionary and has a 'data' key.
    if not isinstance(entry, dict) or "data" not in entry:
         logging.warning(f"   ⚠️ Cannot get modification date from invalid entry structure: {str(entry)[:100]}")
         return 0 # Return 0 for invalid structure

    # Get the 'data' dictionary from the entry using .get() for safety.
    entry_data = entry.get("data", {})
    # Validate that 'data' is actually a dictionary.
    if not isinstance(entry_data, dict):
         logging.warning(f"   ⚠️ Cannot get modification date, entry 'data' is not a dict: {str(entry_data)[:100]}")
         return 0 # Return 0 if 'data' is not a dict

    # Get the 'edited' and 'created_utc' timestamps using .get() with a default of 0.
    edited_time = entry_data.get("edited")
    created_utc = entry_data.get("created_utc", 0)

    # Handle the 'edited' field. It can be a timestamp (int/float), boolean False, or string 'false'.
    if edited_time and str(edited_time).lower() != 'false':
        try:
            edited_ts = float(edited_time) # Convert edited time to float
            # Check if the edited timestamp is *after* the created timestamp.
            # Reddit's API documentation implies edited is 0 or False if not edited,
            # but sometimes edited might hold a value equal to created_utc or slightly before
            # due to API quirks or timing. We only consider it a true 'modification'
            # if the timestamp is strictly later.
            if edited_ts > float(created_utc): # Compare as floats
                return edited_ts # Return the edited time if it's later
            else:
                 # Log a debug message if edited time is not later than created time.
                 logging.debug(f"      Edited timestamp {edited_ts} is not strictly after created timestamp {created_utc}. Using created_utc.")
                 # Fallback to created_utc if edited timestamp is not strictly later.
                 try: return float(created_utc)
                 except (ValueError, TypeError): return 0 # Ultimate fallback if created_utc is also invalid
        except (ValueError, TypeError):
            # If 'edited' value is not a valid number format, log debug and fall back to created_utc.
            logging.debug(f"      Invalid 'edited' timestamp '{edited_time}', falling back to created_utc.")
            try: return float(created_utc)
            except (ValueError, TypeError): return 0 # Ultimate fallback
    else:
        # If 'edited' is None, False, or 'false', use the 'created_utc' timestamp.
        try: return float(created_utc) # Convert created_utc to float
        except (ValueError, TypeError): return 0 # Ultimate fallback if created_utc is invalid


def load_existing_data(filepath):
    """
    Loads previously scraped Reddit data from a JSON file.

    Args:
        filepath (str): The path to the JSON file.

    Returns:
        A dictionary containing the loaded data (expected structure: {'t1': {...}, 't3': {...}}),
        or a new dictionary with empty 't1' and 't3' keys if the file does not exist,
        is empty, or contains invalid JSON/structure.
    """
    # Check if the specified file exists.
    if os.path.exists(filepath):
        logging.debug(f"   Attempting to load existing data from: {CYAN}{filepath}{RESET}")
        try:
            # Open the file for reading.
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f) # Load JSON data

                # Basic validation: ensure the loaded data is a dictionary.
                if not isinstance(data, dict):
                    logging.error(f"   ❌ Existing file {CYAN}{filepath}{RESET} does not contain a valid JSON object (expected dict). Starting fresh.")
                    # Return a dictionary with empty 't1' and 't3' keys to signify starting fresh but with correct structure.
                    return {"t1": {}, "t3": {}}

                logging.info(f"   ✅ Loaded existing data successfully from {CYAN}{filepath}{RESET}")
                # Ensure 't1' and 't3' keys exist in the returned dictionary, even if they were missing in the file.
                if "t1" not in data: data["t1"] = {}
                if "t3" not in data: data["t3"] = {}
                return data # Return the loaded data

        except json.JSONDecodeError:
            # Handle cases where the file exists but contains invalid JSON.
            logging.error(f"   ❌ Error decoding existing JSON file: {CYAN}{filepath}{RESET}. Starting fresh.")
            return {"t1": {}, "t3": {}} # Return fresh data structure

        except Exception as e:
             # Handle any other unexpected errors during file reading.
             logging.error(f"   ❌ Error reading existing file {CYAN}{filepath}{RESET}: {e}", exc_info=True) # Log traceback
             return {"t1": {}, "t3": {}} # Return fresh data structure

    # If the file does not exist, log this and return a fresh data structure.
    logging.debug(f"   No existing data file found at {CYAN}{filepath}{RESET}. Starting fresh.")
    return {"t1": {}, "t3": {}} # Return fresh data structure


# --- Reddit API Interaction ---
def get_reddit_data(username, category, config, limit=100, after=None, before=None):
    """
    Fetches a page of data (posts or comments) for a specific user from the Reddit API.

    Handles constructing the correct URL, setting headers (including User-Agent),
    adding pagination parameters, and basic error handling for API requests.

    Args:
        username (str): The Reddit username.
        category (str): The category to fetch, e.g., "submitted" for posts,
                        "comments" for comments.
        config (dict): The application configuration, expected to contain 'user_agent'.
        limit (int): The number of items to request per page (max 100).
        after (str | None): A fullname (e.g., 't3_xyz') of the last item from the
                            previous page to get the next page of results.
        before (str | None): A fullname to get results before this item. Used for
                             fetching older items.

    Returns:
        A dictionary containing the JSON response data from the API, or None if
        the request failed (timeout, HTTP error, network error, JSON decode error).
    """
    # Construct the API endpoint URL for the user's activity in the specified category.
    url = f"https://www.reddit.com/user/{username}/{category}.json"

    # Get the User-Agent string from the configuration. Use a fallback if not found or invalid.
    user_agent = config.get('user_agent')
    if not isinstance(user_agent, str) or not user_agent.strip(): # Also check for empty/whitespace string
        user_agent = "Python:RedditProfilerScript:v1.7" # Fallback UA
        logging.warning(f"      ⚠️ Invalid or missing User-Agent in config, using fallback: {user_agent}")
    headers = {"User-Agent": user_agent} # Set the User-Agent header

    # Define the request parameters. 'raw_json=1' is often needed for consistent timestamps.
    params = {"limit": limit, "raw_json": 1}
    # Add pagination parameters if provided.
    if after: params["after"] = after
    if before: params["before"] = before

    logging.debug(f"      🌐 Requesting URL: {DIM}{url}{RESET} with params: {params}")

    try:
        # Make the GET request to the Reddit API. Set a timeout.
        response = requests.get(url, headers=headers, params=params, timeout=30)

        # Log the status code received, even if it's not an error, before raising HTTPError.
        if response.status_code != 200:
             logging.warning(f"      ⚠️ Received non-200 status code {response.status_code} from {DIM}{url}{RESET}")

        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx status codes).

        logging.debug(f"      ✅ Received {response.status_code} from {DIM}{url}{RESET}")

        # Check the Content-Type header to ensure the response is JSON.
        if 'application/json' not in response.headers.get('Content-Type', ''):
             # If not JSON, log an error and return None. Include a snippet of the response body.
             logging.error(f"   ❌ Unexpected content type received from {DIM}{url}{RESET}: {response.headers.get('Content-Type', 'N/A')}. Body: {DIM}{response.text[:200]}...{RESET}")
             return None

        # Attempt to parse the JSON response body.
        return response.json()

    except requests.exceptions.Timeout:
        # Handle request timeout specifically.
        logging.error(f"   ❌ Timeout fetching {category} for {username} (URL: {DIM}{url}{RESET})")
        return None # Indicate failure

    except requests.exceptions.HTTPError as http_err:
        # Handle HTTP errors (e.g., 404 Not Found, 403 Forbidden, 429 Rate Limited).
        status_code = response.status_code if response is not None else "N/A" # Get status code if response object exists
        logging.error(f"   ❌ HTTP error {status_code} fetching {category} for {username} (URL: {DIM}{url}{RESET}): {http_err}")
        # Provide more specific messages for common HTTP errors.
        if status_code == 404: logging.error(f"      User '{username}' or category '{category}' not found.")
        elif status_code in [401, 403]: logging.error(f"      Unauthorized/Forbidden access fetching {category} for {username}. Profile might be private/suspended.")
        elif status_code == 429: logging.warning(f"      Rate limit (429) hit fetching {category} for {username}. Consider increasing sleep time or reducing frequency.")
        # Log a snippet of the response body for other HTTP errors if possible.
        elif response is not None and hasattr(response, 'text'):
             logging.error(f"      Response body snippet: {DIM}{response.text[:200]}...{RESET}")
        return None # Indicate failure

    except requests.exceptions.RequestException as req_err:
        # Handle any other request-related errors (e.g., network connectivity issues).
        logging.error(f"   ❌ Network error fetching {category} for {username} (URL: {DIM}{url}{RESET}): {req_err}")
        return None # Indicate failure

    except json.JSONDecodeError:
        # Handle errors during JSON decoding.
        response_text_snippet = response.text[:500] if response and hasattr(response, 'text') else "N/A" # Get snippet if response exists
        logging.error(f"   ❌ Failed to decode JSON response for {category} from {username} (URL: {DIM}{url}{RESET}). Response snippet: {DIM}{response_text_snippet}...{RESET}")
        return None # Indicate failure


def save_reddit_data(user_data_dir, username, config, sort_descending, scrape_comments_only=False, force_scrape=False):
    """
    Scrapes Reddit posts and/or comments for a user, merges new data with
    existing data from a JSON file, sorts the combined data, and saves it
    back to the JSON file.

    Supports incremental fetching to only get newer items, or force scraping
    to re-fetch everything.

    Args:
        user_data_dir (str): The base directory to store user data.
        username (str): The Reddit username to scrape.
        config (dict): Application configuration (for user_agent, potentially sleep time).
        sort_descending (bool): True to sort newest items first, False for oldest first.
        scrape_comments_only (bool): If True, only scrapes comments, not posts.
        force_scrape (bool): If True, ignores existing data and scrapes all accessible
                             pages from scratch.

    Returns:
        The path to the saved JSON file if successful, or None if saving fails.
    """
    # Ensure the user-specific data directory exists.
    os.makedirs(user_data_dir, exist_ok=True)
    # Construct the full path to the JSON data file.
    filepath = os.path.join(user_data_dir, f"{username}.json")

    # Load existing data from the file if it exists and we are not force scraping.
    existing_data = load_existing_data(filepath) if not force_scrape else {"t1": {}, "t3": {}}

    # Log whether existing data was found or force scrape is active.
    if os.path.exists(filepath) and not force_scrape:
        logging.info(f"   📂 Existing data file found: {CYAN}{filepath}{RESET}")
        logging.info(f"      Attempting incremental fetch (only newer items)...")
    elif force_scrape:
         logging.info(f"   ⚠️ {BOLD}Force scraping{RESET} enabled. Re-fetching all accessible data for {username}.")
         # If force scraping, the existing data is loaded but will be overwritten by new data.
         # This log message clarifies the behavior.
    else:
        logging.info(f"   ℹ️ No existing data file ({CYAN}{filepath}{RESET}) or force_scrape used. Starting full scrape.")

    # Determine which categories to scrape ('submitted' for posts, 'comments').
    categories = ["comments"] if scrape_comments_only else ["submitted", "comments"]

    # Initialize counters for tracking fetched and updated items, and skipped items.
    newly_fetched_count = 0
    updated_count = 0 # Track updates to existing items separately
    skipped_malformed = 0 # Track items with unexpected structure or invalid dates

    # API call parameters and retry logic settings.
    max_retries = 3 # Maximum number of times to retry a failed page fetch
    sleep_time = 2 # Base sleep time in seconds between API calls
    fetch_limit = 100 # Number of items to request per page (max allowed by Reddit API)

    logging.info(f"   ⚙️ Starting data fetch process (Sort: {BOLD}{'Newest First' if sort_descending else 'Oldest First'}{RESET}, Comments Only: {scrape_comments_only})...")

    # Loop through the selected categories (submitted, comments).
    for category in categories:
        # Determine the 'kind' identifier for the category ('t3' for posts, 't1' for comments).
        kind = "t3" if category == "submitted" else "t1"
        after = None # 'after' parameter for pagination, initially None
        page_count = 0 # Counter for pages fetched for the current category
        retries = 0 # Retry counter for the current category fetch loop
        stop_fetching = False # Flag to signal when to stop fetching pages for this category

        logging.info(f"      Fetching '{BOLD}{category}{RESET}'...")

        # --- Pagination Loop ---
        while True:
            page_count += 1
            logging.debug(f"         Fetching page {page_count} of {category} (after: {after}, limit: {fetch_limit})")
            # Call the get_reddit_data function to fetch a page of data.
            data = get_reddit_data(username, category, config, limit=fetch_limit, after=after)

            # --- Handle Fetch Errors ---
            if data is None:
                retries += 1 # Increment retry counter
                if retries >= max_retries:
                    # Stop fetching this category if max retries are reached.
                    logging.error(f"      ❌ Stopping fetch for {BOLD}{category}{RESET} after {max_retries} consecutive errors.")
                    break # Exit the pagination loop for this category
                else:
                    # Calculate exponential backoff sleep time.
                    wait = sleep_time * (2**retries)
                    logging.warning(f"      ⚠️ Fetch failed for {category} (attempt {retries}/{max_retries}). Retrying after {wait:.1f} seconds...")
                    time.sleep(wait) # Wait before retrying
                    continue # Go to the next iteration of the while loop (retry the same page)

            retries = 0 # Reset retry counter on a successful fetch

            # --- Validate Response Structure ---
            # Check if the returned data has the expected structure ('data' dict with 'children' list).
            if "data" not in data or not isinstance(data["data"], dict) or "children" not in data["data"]:
                 logging.warning(f"      ⚠️ Invalid or missing 'data'/'children' structure in response for {category} page {page_count}. Ending fetch.")
                 break # Stop fetching this category if structure is bad

            # Get the list of items ('children') from the response data.
            fetched_items = data["data"].get("children") # Use .get for safety
            # If the list is empty or not a list, and it's the first page with no 'after' token,
            # it means there's no data for this category. Otherwise, it means we've reached the end of results.
            if not isinstance(fetched_items, list) or not fetched_items:
                 if page_count == 1 and not data["data"].get("after"):
                     logging.info(f"      ✅ No '{category}' found for user {username}.")
                 else:
                     logging.debug(f"      No more children found for {category} (or empty list) on page {page_count}. Ending {category} fetch.")
                 break # Exit the pagination loop for this category

            # Log a progress indicator periodically for long fetches.
            if page_count % 10 == 0:
                logging.debug(f"         ... processed {page_count} pages for {category} ...")

            found_new_in_page = False # Flag to track if any new or updated items were found in this page

            # --- Process Fetched Items ---
            for entry in fetched_items:
                # More thorough validation of each individual entry's structure.
                if not isinstance(entry, dict) or \
                   "kind" not in entry or entry["kind"] != kind or \
                   "data" not in entry or not isinstance(entry["data"], dict) or \
                   "id" not in entry["data"]:
                    # Log and skip entries with unexpected structure.
                    logging.warning(f"      ⚠️ Skipping malformed or unexpected kind entry: {DIM}{str(entry)[:100]}...{RESET}")
                    skipped_malformed += 1
                    continue # Go to the next entry

                # Extract the item's ID and modification date.
                entry_id = entry["data"]["id"]
                current_mod_date = get_modification_date(entry)
                if current_mod_date == 0: # Skip if cannot determine a valid date for sorting/comparison
                    logging.warning(f"      ⚠️ Skipping entry {kind}_{entry_id} due to invalid modification date.")
                    skipped_malformed += 1
                    continue

                # Ensure the 'kind' key exists in the existing_data dictionary and is a dictionary itself.
                # This is a defensive check in case load_existing_data failed partially or returned a malformed dict.
                if kind not in existing_data or not isinstance(existing_data.get(kind), dict):
                     existing_data[kind] = {} # Initialize as empty dict if missing or wrong type

                # Check if the entry already exists in the loaded data.
                existing_entry_data = existing_data[kind].get(entry_id)

                if existing_entry_data:
                    # --- Handle Existing Entry ---
                    # Validate the structure of the stored entry before getting its date.
                    if isinstance(existing_entry_data, dict) and "data" in existing_entry_data:
                        stored_mod_date = get_modification_date(existing_entry_data)
                        if stored_mod_date == 0:
                             # If the stored entry has an invalid date, treat the fetched one as an update.
                             logging.warning(f"         Found existing entry {kind}_{entry_id} with invalid stored date ({stored_mod_date}), updating.")
                             existing_data[kind][entry_id] = entry # Overwrite the invalid stored entry
                             updated_count += 1
                             found_new_in_page = True
                        elif current_mod_date > stored_mod_date:
                             # If the fetched item is newer than the stored one, update it.
                             logging.debug(f"         ✨ Updating existing entry {kind}_{entry_id} (new mod date {format_timestamp(current_mod_date)} > stored {format_timestamp(stored_mod_date)}).")
                             existing_data[kind][entry_id] = entry # Overwrite with the newer data
                             updated_count += 1
                             found_new_in_page = True
                        elif force_scrape and current_mod_date == stored_mod_date:
                             # If force scraping is enabled, update the item even if the date is the same.
                             # This ensures data freshness in force scrape mode. Log only if force_scrape
                             # to avoid excessive logging for older items in incremental mode.
                             logging.debug(f"         ✨ Re-adding/updating existing entry {kind}_{entry_id} (force_scrape, same date {format_timestamp(current_mod_date)}).")
                             existing_data[kind][entry_id] = entry
                             updated_count += 1 # Count as an update if forced
                             found_new_in_page = True
                        elif not force_scrape:
                            # In incremental fetch mode (not force_scrape), if we encounter an existing
                            # item that is *not* newer, we can stop fetching pages for this category
                            # because the API returns items in reverse chronological order.
                            logging.debug(f"         Found existing, non-updated {kind}_{entry_id}. Stopping pagination for {category}.")
                            stop_fetching = True # Set flag to stop fetching pages
                            break # Exit the loop for items in this page

                        # If current_mod_date < stored_mod_date, we don't do anything unless force_scrape.
                        # The older fetched item is simply ignored as the stored one is newer.
                    else:
                        # If the stored entry's structure was invalid, log and overwrite it with the fetched one.
                        logging.warning(f"      ⚠️ Found invalid stored entry structure for ID {entry_id}, overwriting with fetched data.")
                        existing_data[kind][entry_id] = entry # Overwrite with fetched data
                        updated_count += 1 # Count as an update
                        found_new_in_page = True
                else:
                    # --- Handle New Entry ---
                    # If the entry does not exist in the loaded data, add it.
                    logging.debug(f"         ✨ Adding new entry {kind}_{entry_id}")
                    existing_data[kind][entry_id] = entry # Add the new entry
                    newly_fetched_count += 1 # Increment count of newly fetched items
                    found_new_in_page = True # Set flag

            if stop_fetching:
                break # Exit the pagination loop for this category if the flag was set

            # Get the 'after' token for the next page. If it's None, we've reached the end of results.
            after = data["data"].get("after")
            if not after:
                logging.debug(f"      No 'after' token received for {category} on page {page_count}. Ending {category} fetch.")
                break # Exit the pagination loop for this category

            # Add a sleep delay between page requests to avoid hitting rate limits.
            # This delay happens regardless of whether new items were found on the page.
            logging.debug(f"         😴 Sleeping for {sleep_time} seconds before next {category} request...")
            time.sleep(sleep_time)

    # Log summary of skipped malformed entries.
    if skipped_malformed > 0:
         logging.warning(f"   ⚠️ Skipped {skipped_malformed} malformed entries during fetch.")

    # If no new or updated items were found, and the file already exists (and wasn't force scraped),
    # we don't need to sort or save.
    if newly_fetched_count == 0 and updated_count == 0 and os.path.exists(filepath) and not force_scrape:
        logging.info("   ✅ No new items fetched or updated. Data file remains unchanged.")
        return filepath # Return the path to the existing file

    # Log summary of fetched and updated items.
    logging.info(f"   📊 Fetch summary: {BOLD}{newly_fetched_count}{RESET} new items fetched, {BOLD}{updated_count}{RESET} items updated.")

    # --- Sort the Data ---
    # Sort the items within each category ('t1' and 't3') by their modification date.
    # This ensures the newest items are consistently at the beginning (if sort_descending is True),
    # which is useful for incremental fetches and display.
    logging.info(f"   ⚙️ Sorting fetched data by modification date ({'Newest First' if sort_descending else 'Oldest First'})...")

    # Iterate through the keys of the existing_data dictionary. Use list() to iterate
    # over a copy of keys in case the dictionary is modified during iteration (though unlikely here).
    for kind in list(existing_data.keys()):
         # Skip if the value for this key is not a non-empty dictionary.
         if not isinstance(existing_data.get(kind), dict) or not existing_data[kind]: continue

         valid_items = {} # Dictionary to hold items that have a valid date for sorting
         invalid_sort_count = 0 # Counter for items that cannot be sorted
         # Iterate through the items of the current kind.
         for item_id, item_data in existing_data[kind].items():
              mod_date = get_modification_date(item_data) # Get the modification date

              # Check if the item has a valid structure and a positive modification date.
              # Items without a valid date cannot be sorted reliably.
              if isinstance(item_data, dict) and "data" in item_data and mod_date is not None and mod_date > 0:
                  valid_items[item_id] = item_data # Add to the list of sortable items
              else:
                  invalid_sort_count += 1
                  logging.debug(f"      Excluding item {kind}_{item_id} from sorting due to invalid data or date ({mod_date}).")

         # Log how many items were excluded from sorting if any.
         if invalid_sort_count > 0:
              logging.warning(f"      Excluded {invalid_sort_count} items of kind '{kind}' from sorting due to invalid data/date.")

         try:
            # Sort the items. Use a lambda function to specify the sorting key:
            # the modification date obtained from get_modification_date.
            # Python's sort is stable, preserving the relative order of items with equal keys.
            sorted_items = sorted(
                valid_items.items(), # Sort list of (id, item_data) tuples
                key=lambda item: get_modification_date(item[1]), # item[1] is the item_data dict
                reverse=sort_descending # Sort descending if requested (newest first)
            )
            # Replace the original dictionary for this kind with a new dictionary created from the sorted items.
            existing_data[kind] = dict(sorted_items)
         except Exception as e:
            # Log an error if sorting fails unexpectedly.
            logging.error(f"   ❌ Error sorting items for kind '{kind}': {e}. Data for this kind might be unsorted.")
            # As a fallback, keep the valid items, but they might not be sorted correctly.
            existing_data[kind] = valid_items

    # --- Save the Updated Data ---
    # Save the merged, potentially updated, and sorted data to the JSON file.
    # Use a temporary file and atomic replace to prevent data loss if saving is interrupted.
    temp_filepath = filepath + ".tmp"
    logging.info(f"   💾 Saving updated data to {CYAN}{filepath}{RESET}...")
    try:
        # Open the temporary file for writing.
        with open(temp_filepath, "w", encoding="utf-8") as f:
            # Write the dictionary as JSON. indent=2 makes it somewhat readable.
            json.dump(existing_data, f, indent=2)

        # Check if the temporary file was successfully created and written.
        if os.path.exists(temp_filepath):
             # Atomically replace the original file with the temporary file.
             os.replace(temp_filepath, filepath)
             logging.info(f"   ✅ Data saved successfully.")
        else:
             # This case is unlikely if the `json.dump` didn't raise an exception,
             # but it's a safety check.
             logging.error(f"   ❌ Temporary file {CYAN}{temp_filepath}{RESET} not found after write attempt. Save failed.")
             return None # Indicate save failure

    except Exception as e:
        # Catch any exceptions during the saving process (e.g., disk full, permission error).
        logging.error(f"   ❌ An unexpected error occurred during file save to {CYAN}{filepath}{RESET}: {e}", exc_info=True) # Log traceback
        # Attempt to clean up the temporary file if it was created.
        if os.path.exists(temp_filepath):
            try: os.remove(temp_filepath)
            except OSError as rm_err: logging.error(f"      Could not remove temporary file {CYAN}{temp_filepath}{RESET}: {rm_err}")
        return None # Indicate save failure

    # Return the path to the successfully saved file.
    return filepath


def get_post_title_from_permalink(comment_permalink, config, use_cache=True, max_retries=2):
    """
    Fetches the title of the parent post for a given comment permalink by
    calling the Reddit API's /api/info endpoint.

    Uses an in-memory cache to avoid repeated API calls for the same post.

    Args:
        comment_permalink (str): The permalink URL of a comment.
        config (dict): Application configuration (for user_agent).
        use_cache (bool): If True, checks and updates the in-memory title cache.
        max_retries (int): Maximum number of retries for the API call in case of errors.

    Returns:
        The title of the parent post (str) if successful, or an error message string
        if fetching or processing failed.
    """
    global post_title_cache # Declare use of the module-level global cache

    # Extract the post ID from the comment permalink using regex.
    post_id_match = re.search(r'/comments/([^/]+)/', comment_permalink)
    if not post_id_match:
        logging.warning(f"      ⚠️ Could not extract post ID from comment permalink: {DIM}{comment_permalink}{RESET}")
        return "[Could not extract Post ID]" # Return specific error if ID extraction fails

    post_id = post_id_match.group(1) # Extracted post ID (e.g., 'abcde')
    full_post_id = f"t3_{post_id}" # Construct the full fullname (e.g., 't3_abcde')

    # Check the cache first if caching is enabled.
    if use_cache and full_post_id in post_title_cache:
        logging.debug(f"      Cache hit for post title ID {full_post_id}")
        return post_title_cache[full_post_id] # Return title from cache

    # If not in cache or caching is disabled, fetch from API.
    info_url = f"https://www.reddit.com/api/info.json" # Endpoint to get info by fullname
    params = {"id": full_post_id, "raw_json": 1} # Request the specific post by its fullname
    user_agent = config.get('user_agent', 'Python:RedditProfilerScript:v1.7') # Get User-Agent from config, fallback if needed
    headers = {"User-Agent": user_agent} # Set User-Agent header

    logging.debug(f"      🌐 Fetching post title for ID {full_post_id} from URL: {DIM}{info_url}{RESET}")

    # Implement retry logic for the API call.
    for attempt in range(max_retries + 1): # Loop from 0 up to max_retries (total attempts = max_retries + 1)
        try:
            # Calculate sleep duration, increasing with each retry attempt (exponential backoff).
            sleep_duration = 0.5 + attempt * 0.75
            if attempt > 0: logging.debug(f"         Retrying title fetch ({attempt}/{max_retries}), sleeping {sleep_duration:.1f}s...")
            time.sleep(sleep_duration) # Wait before making the request

            # Make the GET request to the /api/info endpoint. Set a timeout.
            response = requests.get(info_url, headers=headers, params=params, timeout=15)
            if response.status_code != 200:
                 # Log warning for non-200 status, continue retrying if attempts remain.
                 logging.warning(f"      ⚠️ Received non-200 status {response.status_code} fetching title for {full_post_id} (Attempt {attempt + 1}/{max_retries})")
            response.raise_for_status() # Raise HTTPError for 4xx/5xx

            # Check the Content-Type header.
            if 'application/json' not in response.headers.get('Content-Type', ''):
                 logging.warning(f"      ⚠️ Unexpected content type for title fetch {full_post_id}: {response.headers.get('Content-Type', 'N/A')}. Body: {DIM}{response.text[:200]}...{RESET}")
                 # If unexpected content type, retry if attempts remain, otherwise fail.
                 if attempt < max_retries: continue
                 else: return "[Fetch Error: Wrong Content Type]" # Return specific error on final failed attempt

            # Decode the JSON response.
            info_data = response.json()

            # Validate the structure of the response JSON to ensure it contains post data.
            # Expected: {'data': {'children': [{'kind': 't3', 'data': {...}}, ...]}}
            if (isinstance(info_data, dict) and 'data' in info_data and
                isinstance(info_data['data'], dict) and 'children' in info_data['data'] and
                isinstance(info_data['data']['children'], list) and
                len(info_data['data']['children']) > 0): # Must have at least one child

                # The list of children should contain the requested post entry.
                post_entry = info_data['data']['children'][0]
                # Validate that the first child is a 't3' (post) and has 'data'.
                if isinstance(post_entry, dict) and post_entry.get('kind') == 't3' and 'data' in post_entry:
                    # Extract the 'title' from the post's data payload.
                    title = post_entry['data'].get('title')
                    if title is not None:
                        logging.debug(f"      {GREEN}✅ Fetched title{RESET} for post ID {full_post_id}: '{title[:50]}...'")
                        if use_cache: post_title_cache[full_post_id] = title # Add to cache if enabled
                        return title # Return the fetched title
                    else:
                        # If 'title' key is missing, log warning and return specific error.
                        logging.warning(f"      ⚠️ Could not find 'title' key in post data for ID {full_post_id} via /api/info.")
                        return "[Title Key Missing]"

            # If the JSON structure is unexpected, log warning and return specific error.
            logging.warning(f"      ⚠️ Unexpected JSON structure or missing data for post {full_post_id} via /api/info. Data: {DIM}{str(info_data)[:300]}{RESET}")
            # This kind of error might be permanent, so don't necessarily retry unless it's the last attempt.
            # Returning immediately avoids retrying for potentially bad data structure.
            return "[Fetch Error: Unexpected JSON]"

        except requests.exceptions.Timeout:
            # Handle timeout errors. Log and retry if attempts remain.
            logging.warning(f"      ⚠️ Timeout fetching title for post {full_post_id} (Attempt {attempt + 1}/{max_retries})")
            if attempt >= max_retries: return "[Fetch Error: Timeout]" # Fail on last attempt
            # Otherwise, loop continues to the next attempt after sleep

        except requests.exceptions.HTTPError as http_err:
            # Handle HTTP errors. Log and handle specific common errors (404, 403, 429).
            status_code = http_err.response.status_code if hasattr(http_err, 'response') and http_err.response is not None else 'N/A'
            logging.warning(f"      ⚠️ HTTP error {status_code} fetching title for post {full_post_id} (Attempt {attempt + 1}/{max_retries})")
            if status_code == 404: return "[Post Not Found]" # Post not found -> permanent error
            if status_code in [401, 403]: return "[Access Denied to Post]" # Access denied -> permanent error
            if status_code == 429:
                 # Handle rate limit with a longer exponential backoff wait.
                 wait = 5 * (attempt + 2) # Longer wait on rate limit retry
                 logging.warning(f"         Rate limited fetching title for {full_post_id}. Sleeping {wait}s...")
                 time.sleep(wait)
                 # Loop continues to next attempt after sleep
            elif attempt >= max_retries: # For other HTTP errors, fail on the last attempt.
                 return f"[Fetch Error: HTTP {status_code}]"
            # Otherwise, continue to retry for non-fatal HTTP errors

        except (requests.exceptions.RequestException, json.JSONDecodeError) as req_err:
            # Handle other request exceptions (network errors) or JSON decoding errors.
            logging.warning(f"      ⚠️ Error fetching/decoding title for post {full_post_id}: {req_err} (Attempt {attempt + 1}/{max_retries})")
            if attempt >= max_retries: return "[Fetch Error: Request/Decode]" # Fail on last attempt
            # Loop continues to next attempt after sleep

    # This part is reached only if all retry attempts fail for any reason not handled above.
    logging.error(f"      ❌ Failed to fetch title for post {full_post_id} after all attempts.")
    return "[Failed to Fetch Title]" # Ultimate fallback error message


# Helper function added previously to fetch user about data for stats
def _fetch_user_about_data(username, config):
    """
    Fetches the user's 'about.json' data from the Reddit API.
    This endpoint provides general information about the user account.

    Args:
        username (str): The Reddit username.
        config (dict): Application configuration (for user_agent).

    Returns:
        A dictionary containing the user's 'about' data if successful,
        or None if fetching or processing fails.
    """
    # Check if the requests library is available (basic sanity check).
    if not requests:
        logging.error("   ❌ Cannot fetch 'about' data: 'requests' library not installed.")
        return None

    # Construct the URL for the user's 'about.json' endpoint.
    url = f"https://www.reddit.com/user/{username}/about.json"
    # Get User-Agent, using a default specifically for this stats fetching part if config is missing.
    user_agent = config.get('user_agent', 'Python:RedditStatsUtil:v1.0')
    headers = {"User-Agent": user_agent}

    logging.debug(f"   🌐 Fetching user 'about' data from: {DIM}{url}{RESET}")

    try:
        # Make the GET request with a timeout.
        response = requests.get(url, headers=headers, timeout=15)
        # Log status code before raising error.
        if response.status_code != 200:
             logging.warning(f"   ⚠️ Received non-200 status {response.status_code} fetching 'about' for /u/{username}.")
        response.raise_for_status() # Raise HTTPError for bad responses

        # Check content type.
        if 'application/json' not in response.headers.get('Content-Type', ''):
             logging.error(f"   ❌ Unexpected content type received for 'about.json' /u/{username}: {response.headers.get('Content-Type', 'N/A')}.")
             return None # Fail if not JSON

        # Decode JSON response.
        about_json = response.json()

        # Validate structure: expected to be a dict with 'kind' == 't2' and 'data' dict.
        if isinstance(about_json, dict) and about_json.get("kind") == "t2" and "data" in about_json:
             logging.debug(f"   ✅ Successfully fetched 'about' data for /u/{username}.")
             return about_json["data"] # Return the 'data' payload which contains user info
        else:
             logging.warning(f"   ⚠️ Unexpected structure in 'about.json' response for /u/{username}.")
             return None # Fail if structure is unexpected

    except requests.exceptions.RequestException as e:
        # Handle request exceptions. Include status code in error message if available.
        status_code_str = f" (Status: {e.response.status_code})" if hasattr(e, 'response') and e.response is not None else ""
        logging.error(f"   ❌ Error fetching 'about.json' for /u/{username}{status_code_str}: {e}", exc_info=True) # Log traceback
        return None # Fail on request error

    except json.JSONDecodeError:
        # Handle JSON decoding errors.
        logging.error(f"   ❌ Failed to decode JSON from 'about.json' for /u/{username}.")
        return None # Fail on decode error

    except Exception as e: # Catch broader exceptions during 'about' fetch
        # Handle any other unexpected errors.
        logging.error(f"   ❌ Unexpected error fetching 'about.json' for /u/{username}: {e}", exc_info=True) # Log traceback
        return None # Fail on unexpected error