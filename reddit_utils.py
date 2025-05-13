# reddit_utils.py
"""
This module provides utility functions for interacting with the Reddit API using PRAW.
It handles fetching user posts and comments, saving and loading this data
to/from JSON files, extracting specific information like modification dates,
and fetching external data like post titles from comment permalinks.
It also includes helpers for timestamp formatting.

Relies on PRAW (Python Reddit API Wrapper) and expects an authenticated PRAW instance
to be passed where needed. PRAW should be configured via praw.ini or environment variables.
"""

# Import standard libraries
import logging # For logging information and errors
import json # For working with JSON data
import time # For adding delays (e.g., between API requests)
import os # For file path manipulation and existence checks
import re # For using regular expressions (e.g., extracting IDs from permalinks)
from datetime import datetime, timezone # For date/time handling, specifically UTC timezone

# Import PRAW library
import praw # The main PRAW library
import prawcore # For PRAW-specific exceptions

# Import ANSI codes for coloring output messages.
# (Keep these as they are used for logging formatting)
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\033[31m"

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
        # catch corrupted data. We allow slightly past current time for clock skew tolerance.
        # 946684800 is approx 2000-01-01 UTC
        # Allow 0 as a special case (often indicates missing data)
        if ts != 0 and (ts <= 946684800 or ts > time.time() + 3600): # Allow up to 1 hour in the future
             # Log invalid timestamps that are outside the expected range
             logging.warning(f"      ⚠️ Timestamp {ts} ({datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') if ts > 0 else '0'}) is out of reasonable range (expected post-2000, not too far future). Treating as invalid.")
             # Keep the original value for potential downstream checks, but formatting will fail gracefully
             raise ValueError("Timestamp out of reasonable range")

        # Convert the timestamp to a datetime object in UTC. Handle 0 case.
        if ts == 0:
             return "UNKNOWN_DATE" # Explicitly return fallback for 0
        dt_object = datetime.fromtimestamp(ts, timezone.utc)
        # Format the datetime object into the desired string format.
        return dt_object.strftime('%Y-%m-%d %H:%M:%S UTC')
    except (ValueError, TypeError, OSError) as e:
        # Catch errors during conversion or formatting. Log a warning and return the fallback.
        logging.warning(f"      ⚠️ Could not format timestamp '{utc_timestamp}': {e}. Using fallback 'UNKNOWN_DATE'.")
        return "UNKNOWN_DATE"

def get_modification_date(entry):
    """
    Determines the most recent modification date (either 'edited' or 'created_utc')
    for a Reddit item represented as a dictionary (from our saved JSON structure).

    Prioritizes 'edited' timestamp if it's valid and after 'created_utc'.

    Args:
        entry (dict): A dictionary representing a single Reddit API item, expected
                      to contain a 'data' key which is also a dictionary holding
                      'created_utc' and potentially 'edited'.

    Returns:
        The most recent Unix timestamp (float) if successful, or 0.0 if the input
        structure is invalid or timestamps cannot be determined/parsed/validated.
        Returns float for consistent comparison.
    """
    # Validate that the input is a dictionary and has a 'data' key which is also a dict.
    if not isinstance(entry, dict) or "data" not in entry or not isinstance(entry.get("data"), dict):
         logging.warning(f"   ⚠️ Cannot get modification date from invalid entry structure: {DIM}{str(entry)[:100]}{RESET}")
         return 0.0 # Return 0.0 for invalid structure

    entry_data = entry["data"]

    # Get the 'edited' and 'created_utc' timestamps using .get() with a default of None or 0.0.
    # Ensure created_utc defaults to a float for comparison.
    edited_time = entry_data.get("edited") # PRAW provides False or float timestamp
    created_utc = entry_data.get("created_utc", 0.0)

    # --- Validate created_utc ---
    try:
        created_ts = float(created_utc) if created_utc is not None else 0.0
        # Basic validation for created_ts similar to format_timestamp
        # 946684800 is approx 2000-01-01 UTC
        # Allow 0.0 as a potential initial/missing value
        if created_ts != 0.0 and (created_ts <= 946684800 or created_ts > time.time() + 3600):
             logging.warning(f"      ⚠️ Invalid 'created_utc' timestamp {created_ts} found in entry data {entry_data.get('name', 'UNKNOWN')}. Using 0.0.")
             created_ts = 0.0
    except (ValueError, TypeError):
        logging.warning(f"      ⚠️ Could not parse 'created_utc' timestamp '{created_utc}' in entry {entry_data.get('name', 'UNKNOWN')}. Using 0.0.")
        created_ts = 0.0

    # If created_ts is invalid (0.0) at this point, no point checking edited time.
    if created_ts == 0.0:
        return 0.0

    # --- Handle the 'edited' field ---
    # PRAW provides False or a float timestamp.
    # Our stored JSON might have string 'false' from older versions, or null/None.
    if edited_time and str(edited_time).lower() != 'false':
        try:
            edited_ts = float(edited_time) # Convert edited time to float

            # --- Validate edited_ts ---
            # Check if it's within a reasonable range (post-2000, not too far future)
            if edited_ts <= 946684800 or edited_ts > time.time() + 3600:
                 logging.warning(f"      ⚠️ Invalid 'edited' timestamp {edited_ts} found in entry data {entry_data.get('name', 'UNKNOWN')}. Ignoring edit time.")
                 return created_ts # Fallback to validated created_ts

            # Check if the edited timestamp is strictly *after* the created timestamp.
            # Allow a small tolerance (e.g., 1 second) for potential float precision issues or near-simultaneous edits.
            # Use a small epsilon for float comparison robustness
            if edited_ts > created_ts + 1e-6:
                # logging.debug(f"      Using edited timestamp {edited_ts} for {entry_data.get('name', 'UNKNOWN')} (created: {created_ts})")
                return edited_ts # Return the validated edited time if it's later
            else:
                 # Log a debug message if edited time is not strictly later than created time.
                 logging.debug(f"      Edited timestamp {edited_ts} is not strictly after created timestamp {created_ts} for {entry_data.get('name', 'UNKNOWN')}. Using created_utc.")
                 return created_ts # Fallback to created_ts if edited timestamp is not strictly later.
        except (ValueError, TypeError):
            # If 'edited' value is not a valid number format, log debug and fall back to created_utc.
            logging.warning(f"      ⚠️ Invalid 'edited' timestamp format '{edited_time}' in entry {entry_data.get('name', 'UNKNOWN')}, falling back to created_utc.")
            return created_ts # Fallback to validated created_ts
    else:
        # If 'edited' is None, False, or 'false', use the 'created_utc' timestamp.
        # logging.debug(f"      Using created timestamp {created_ts} for {entry_data.get('name', 'UNKNOWN')} (no valid edit time)")
        return created_ts # Return the validated created_ts


def _praw_object_to_dict(item):
    """
    Converts a PRAW Submission or Comment object into the dictionary format
    expected by the rest of this module (matching the old requests-based structure).

    Args:
        item: A PRAW Submission or Comment object.

    Returns:
        A dictionary in the format {'kind': 't1'/'t3', 'data': {...}}, or None if
        the input item is invalid or lacks essential data.
    """
    data = {}
    kind = None

    try:
        # --- Common Attributes ---
        author_name = "[deleted]"
        if item.author:
             try:
                 author_name = item.author.name
             except AttributeError: # Might happen if author object exists but name doesn't (rare?)
                 pass # Keep "[deleted]"

        subreddit_name = "[unknown]"
        if item.subreddit:
             try:
                 subreddit_name = item.subreddit.display_name
             except AttributeError: # Might happen for quarantined/private subs?
                 pass # Keep "[unknown]"

        # --- Type-Specific Attributes ---
        if isinstance(item, praw.models.Submission):
            kind = "t3"
            data = {
                "id": item.id,
                "name": item.fullname, # e.g., t3_abcde
                "author": author_name,
                "created_utc": item.created_utc,
                "edited": item.edited, # PRAW provides False or float timestamp
                "title": getattr(item, 'title', '[Title Missing]'), # Use getattr for safety
                "selftext": getattr(item, 'selftext', ''), # Use getattr for safety
                "permalink": getattr(item, 'permalink', ''), # Use getattr for safety
                "url": getattr(item, 'url', ''), # Link URL for link posts
                "subreddit": subreddit_name,
                "subreddit_id": getattr(item, 'subreddit_id', None),
                "score": getattr(item, 'score', 0),
                "num_comments": getattr(item, 'num_comments', 0),
                "is_self": getattr(item, 'is_self', False), # True for text post, False for link post
                "over_18": getattr(item, 'over_18', False),
                
                "link_flair_text": getattr(item, 'link_flair_text', None),
                "author_flair_text": getattr(item, 'author_flair_text', None),
                "distinguished": getattr(item, 'distinguished', None),
            }
        elif isinstance(item, praw.models.Comment):
            kind = "t1"
            data = {
                "id": item.id,
                "name": item.fullname, # e.g., t1_abcde
                "author": author_name,
                "created_utc": item.created_utc,
                "edited": item.edited, # PRAW provides False or float timestamp
                "body": getattr(item, 'body', '[Body Missing]'), # Use getattr for safety
                "permalink": getattr(item, 'permalink', ''), # Use getattr for safety
                "subreddit": subreddit_name,
                "subreddit_id": getattr(item, 'subreddit_id', None),
                "link_id": getattr(item, 'link_id', None), # Fullname of the submission (t3_xxxxx)
                "parent_id": getattr(item, 'parent_id', None), # Fullname of the parent (t1_... or t3_...)
                # Fetching link details can be slow/add API calls, get only essentials here
                "link_permalink": f"/r/{subreddit_name}/comments/{getattr(item, 'link_id', '').replace('t3_', '')}/", # Construct basic permalink
                "link_url": getattr(item, 'link_url', None), # URL the submission links to (if link post) - may not always be loaded
                "link_title": getattr(item, 'link_title', None), # Title of the submission - may not always be loaded
                "score": getattr(item, 'score', 0),
                "author_flair_text": getattr(item, 'author_flair_text', None),
                "distinguished": getattr(item, 'distinguished', None),
                # Add other fields if needed
            }
        else:
            logging.warning(f"      ⚠️ Unexpected PRAW item type: {type(item)}. Cannot convert.")
            return None

        # Basic validation: ensure essential fields like id and created_utc exist
        # created_utc should always be present for valid items
        if not data.get("id") or data.get("created_utc") is None:
             logging.warning(f"      ⚠️ PRAW item missing essential data (id or created_utc): {getattr(item, 'fullname', 'UNKNOWN')}. Cannot convert.")
             return None

        return {"kind": kind, "data": data}

    except AttributeError as e:
        # Catch cases where an expected attribute might be missing (e.g., deleted user/subreddit)
        # Log specific attribute that failed if possible
        logging.warning(f"      ⚠️ Attribute error converting PRAW item {getattr(item, 'fullname', 'UNKNOWN')}: {e}. Item data might be incomplete.")
        # Return potentially incomplete data if kind and id are present
        if kind and data.get("id"):
             data['error_incomplete_conversion'] = str(e) # Mark as potentially incomplete
             return {"kind": kind, "data": data}
        return None # Cannot form basic structure
    except Exception as e:
        # Catch any other unexpected errors during conversion
        logging.error(f"      ❌ Unexpected error converting PRAW item {getattr(item, 'fullname', 'UNKNOWN')} to dict: {e}", exc_info=True)
        return None


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
            # Open the file for reading. Use utf-8-sig to handle potential BOM
            with open(filepath, "r", encoding="utf-8-sig") as f:
                # Handle potentially empty file
                content = f.read()
                if not content.strip():
                    logging.warning(f"   ⚠️ Existing file {CYAN}{filepath}{RESET} is empty. Starting fresh.")
                    return {"t1": {}, "t3": {}}

                data = json.loads(content) # Load JSON data from content

                # Basic validation: ensure the loaded data is a dictionary.
                if not isinstance(data, dict):
                    logging.error(f"   ❌ Existing file {CYAN}{filepath}{RESET} does not contain a valid JSON object (expected dict). Starting fresh.")
                    # Return a dictionary with empty 't1' and 't3' keys to signify starting fresh but with correct structure.
                    return {"t1": {}, "t3": {}}

                logging.info(f"   ✅ Loaded existing data successfully from {CYAN}{filepath}{RESET}")
                # Ensure 't1' and 't3' keys exist in the returned dictionary, even if they were missing in the file.
                # Use .setdefault for cleaner code
                data.setdefault("t1", {})
                data.setdefault("t3", {})

                # Optional: Validate that 't1' and 't3' values are dictionaries
                if not isinstance(data["t1"], dict):
                     logging.warning(f"   ⚠️ Loaded 't1' data is not a dictionary. Resetting 't1'. File: {CYAN}{filepath}{RESET}")
                     data["t1"] = {}
                if not isinstance(data["t3"], dict):
                     logging.warning(f"   ⚠️ Loaded 't3' data is not a dictionary. Resetting 't3'. File: {CYAN}{filepath}{RESET}")
                     data["t3"] = {}

                return data # Return the loaded data

        except json.JSONDecodeError as e:
            # Handle cases where the file exists but contains invalid JSON.
            logging.error(f"   ❌ Error decoding existing JSON file: {CYAN}{filepath}{RESET}. JSON Error: {e}. Starting fresh.")
            return {"t1": {}, "t3": {}} # Return fresh data structure

        except Exception as e:
             # Handle any other unexpected errors during file reading.
             logging.error(f"   ❌ Error reading existing file {CYAN}{filepath}{RESET}: {e}", exc_info=True) # Log traceback
             return {"t1": {}, "t3": {}} # Return fresh data structure

    # If the file does not exist, log this and return a fresh data structure.
    logging.debug(f"   No existing data file found at {CYAN}{filepath}{RESET}. Starting fresh.")
    return {"t1": {}, "t3": {}} # Return fresh data structure


# --- PRAW Interaction ---

def save_reddit_data(user_data_dir, username, praw_instance, sort_descending, scrape_comments_only=False, force_scrape=False):
    """
    Scrapes Reddit posts and/or comments for a user using PRAW, merges new data
    with existing data from a JSON file, sorts the combined data, and saves it
    back to the JSON file.

    Supports incremental fetching (only getting items newer than the latest stored)
    or force scraping to re-fetch all accessible items.

    Args:
        user_data_dir (str): The base directory to store user data.
        username (str): The Reddit username to scrape.
        praw_instance (praw.Reddit): An authenticated PRAW Reddit instance.
        sort_descending (bool): True to sort newest items first, False for oldest first.
        scrape_comments_only (bool): If True, only scrapes comments, not posts.
        force_scrape (bool): If True, ignores existing data timestamps and scrapes
                             all accessible items (up to Reddit's listing limits).

    Returns:
        The path to the saved JSON file if successful, or None if fetching or saving fails.
    """
    if not isinstance(praw_instance, praw.Reddit):
         logging.error(f"   ❌ Invalid PRAW instance provided to save_reddit_data.")
         return None
    # Check if PRAW instance is read-only, might affect ability to fetch some data
    if praw_instance.read_only:
         logging.debug(f"   ℹ️ PRAW instance is in read-only mode.")
    else:
         # Check if authenticated user matches target user (usually not the case)
         try:
              auth_user = praw_instance.user.me()
              if auth_user and auth_user.name.lower() == username.lower():
                   logging.debug(f"   ℹ️ Authenticated as the target user ({username}).")
              # else: pass # Authenticated as someone else, normal for scraping
         except Exception: # Handle cases where .me() fails if not fully authenticated
              logging.debug("   ℹ️ PRAW instance not fully authenticated or cannot determine authenticated user.")


    # Ensure the user-specific data directory exists.
    os.makedirs(user_data_dir, exist_ok=True)
    # Construct the full path to the JSON data file.
    filepath = os.path.join(user_data_dir, f"{username}.json")

    # --- Load Existing Data & Determine Newest Timestamps ---
    existing_data = load_existing_data(filepath)
    newest_stored_ts = {'t1': 0.0, 't3': 0.0} # Store newest timestamp for comments (t1) and posts (t3)

    if os.path.exists(filepath) and not force_scrape:
        logging.info(f"   📂 Existing data file found: {CYAN}{filepath}{RESET}")
        logging.info(f"      Attempting incremental fetch (only items newer than stored)...")
        # Find the timestamp of the newest item in existing data for incremental fetch
        for kind in ['t1', 't3']:
            if existing_data.get(kind):
                # Sort items by modification date to find the newest
                # Items are stored as {id: entry_dict}
                items_with_date = []
                for item_id, item_data in existing_data[kind].items():
                    mod_date = get_modification_date(item_data)
                    if mod_date > 0: # Only consider items with valid dates
                        items_with_date.append((mod_date, item_id))
                    # else: item has invalid date, won't be used for newest check

                if items_with_date:
                    items_with_date.sort(key=lambda x: x[0], reverse=True) # Sort newest first
                    newest_stored_ts[kind] = items_with_date[0][0] # Get the timestamp of the newest item
                    newest_id = items_with_date[0][1]
                    logging.info(f"      Newest existing '{kind}' item ID {newest_id} has timestamp: {format_timestamp(newest_stored_ts[kind])}")
                else:
                     logging.info(f"      No valid items found in existing '{kind}' data to determine newest timestamp.")

    elif force_scrape:
         logging.info(f"   ⚠️ {BOLD}Force scraping{RESET} enabled. Re-fetching all accessible data for {username} (up to API limits).")
         # Reset existing data if force scraping to ensure only newly fetched items are saved
         existing_data = {"t1": {}, "t3": {}}
    else:
        logging.info(f"   ℹ️ No existing data file ({CYAN}{filepath}{RESET}) found. Starting full scrape.")
        existing_data = {"t1": {}, "t3": {}} # Ensure clean start

    # Determine which categories to scrape ('submitted' for posts, 'comments').
    categories_to_fetch = ["comments"] if scrape_comments_only else ["submitted", "comments"]

    # Initialize counters
    newly_fetched_count = 0
    updated_count = 0
    skipped_malformed = 0
    skipped_older = 0 # Count items skipped because they are not newer than existing data
    initial_item_count = {
        't1': len(existing_data.get('t1', {})),
        't3': len(existing_data.get('t3', {}))
    }

    # PRAW fetch settings
    fetch_limit = None # Fetch as many items as PRAW allows (typically ~1000 per category)

    logging.info(f"   ⚙️ Starting PRAW fetch process (Sort: {BOLD}{'Newest First' if sort_descending else 'Oldest First'}{RESET}, Comments Only: {scrape_comments_only}, Force: {force_scrape})...")

    try:
        # Get the Redditor object
        redditor = praw_instance.redditor(username)
        # Trigger a fetch of a basic attribute to check if user exists and is accessible
        # Use getattr to avoid exception if user is suspended/deleted right away
        redditor_id = getattr(redditor, 'id', None)
        if redditor_id is None:
             # Try accessing name, might trigger Forbidden/NotFound specifically
             _ = redditor.name
        logging.info(f"   👤 Accessing data for user: /u/{BOLD}{username}{RESET} (ID: {redditor_id or 'N/A'})")

    except prawcore.exceptions.NotFound:
        logging.error(f"   ❌ {RED}User '{username}' not found.{RESET}")
        return None
    except prawcore.exceptions.Forbidden:
        logging.error(f"   ❌ {RED}Access denied to user '{username}'. Profile may be private, suspended, or deleted.{RESET}")
        return None
    except prawcore.exceptions.PrawcoreException as e:
        logging.error(f"   ❌ {RED}Error accessing user '{username}': {e}{RESET}", exc_info=True)
        return None
    except Exception as e: # Catch potential non-PRAW errors during redditor init
         logging.error(f"   ❌ {RED}Unexpected error initializing Redditor object for '{username}': {e}{RESET}", exc_info=True)
         return None

    # Loop through the selected categories (submitted, comments).
    for category in categories_to_fetch:
        kind = "t3" if category == "submitted" else "t1"
        items_fetched_this_category = 0
        items_processed_this_category = 0
        fetch_start_time = time.time()

        logging.info(f"      Fetching '{BOLD}{category}{RESET}' (limit: {fetch_limit or 'API max'})...")

        try:
            # Use the appropriate PRAW listing generator (newest first)
            if category == "submitted":
                listing_generator = redditor.submissions.new(limit=fetch_limit)
            else: # category == "comments"
                listing_generator = redditor.comments.new(limit=fetch_limit)

            # --- Process Fetched PRAW Items ---
            for item in listing_generator:
                items_fetched_this_category += 1
                if items_fetched_this_category % 200 == 0: # Log progress periodically
                    logging.debug(f"         ... fetched {items_fetched_this_category} {category} ...")

                # Convert PRAW object to our standard dictionary format
                entry_dict = _praw_object_to_dict(item)

                if entry_dict is None:
                    skipped_malformed += 1
                    logging.warning(f"      ⚠️ Skipping item (failed PRAW conversion): {getattr(item, 'fullname', 'UNKNOWN')}")
                    continue # Skip to next item

                # Get ID and modification date from the converted dictionary
                # The 'kind' is already determined by _praw_object_to_dict
                entry_id = entry_dict["data"]["id"]
                current_mod_date = get_modification_date(entry_dict) # Use float for comparison

                # We need a valid modification date to process the item (for sorting and incremental)
                if current_mod_date == 0.0:
                    skipped_malformed += 1
                    logging.warning(f"      ⚠️ Skipping entry {kind}_{entry_id} due to invalid modification date after conversion.")
                    continue # Skip items where date couldn't be determined or was invalid

                items_processed_this_category += 1

                # --- Incremental Fetch Logic ---
                # If not force scraping, check if item is strictly newer than the newest stored item for this kind
                # Add a small epsilon to handle potential float precision issues if newest_stored_ts came from the same item
                is_newer = current_mod_date > (newest_stored_ts[kind] + 1e-9)

                # Process if force_scrape is True OR if it's newer than stored data
                if force_scrape or is_newer:
                    existing_entry_data = existing_data[kind].get(entry_id)

                    if existing_entry_data:
                        # Item exists. Check if fetched version is newer OR if force_scrape is on.
                        stored_mod_date = get_modification_date(existing_entry_data)
                        # Only update if fetched is strictly newer or forced
                        if force_scrape or (stored_mod_date > 0.0 and current_mod_date > stored_mod_date + 1e-9):
                             logging.debug(f"         ✨ Updating existing entry {kind}_{entry_id} (Date: {format_timestamp(current_mod_date)})")
                             existing_data[kind][entry_id] = entry_dict
                             updated_count += 1
                        # else: Item exists but fetched is not newer (or stored has invalid date). Keep existing.
                        #       This case shouldn't happen often if `is_newer` check passed, but protects against weird edits.
                    else:
                        # Item is new to our dataset and meets the date criteria (or force_scrape)
                        logging.debug(f"         ➕ Adding new entry {kind}_{entry_id} (Date: {format_timestamp(current_mod_date)})")
                        existing_data[kind][entry_id] = entry_dict
                        newly_fetched_count += 1
                else:
                    # Item is not newer than the newest stored item, skip it in incremental mode
                    logging.debug(f"         ⏭️ Skipping older/same entry {kind}_{entry_id} (Date: {format_timestamp(current_mod_date)} <= Newest Stored {kind}: {format_timestamp(newest_stored_ts[kind])})")
                    skipped_older += 1
                    # Optimization Check: If PRAW guarantees newest-first and we are not forcing,
                    # and we hit an item older than our *absolute newest* stored item,
                    # we *might* be able to break. However, edits can mess up strict ordering.
                    # Fetching all available (up to limit) and filtering is safer.
                    # Let's keep fetching all for robustness for now.
                    # if not force_scrape and category != "comments": # Edits affect comments more often? Maybe still risky.
                    #    logging.info(f"      Optimisation: Found item not newer than newest stored ({kind}_{entry_id}). Assuming rest are older. Stopping {category} fetch early.")
                    #    break # POTENTIAL OPTIMIZATION BUT LESS ROBUST

            fetch_duration = time.time() - fetch_start_time
            logging.info(f"      ✅ Finished fetching {category}. Processed {items_processed_this_category} valid items ({GREEN}{newly_fetched_count}{RESET} new, {YELLOW}{updated_count}{RESET} updated in total across categories) out of {items_fetched_this_category} fetched in {fetch_duration:.2f}s.")

        except prawcore.exceptions.Forbidden as e:
             logging.warning(f"      ⚠️ {RED}Access denied{RESET} fetching {category} for {username}: {e}. Skipping category.")
        except prawcore.exceptions.NotFound as e:
             logging.warning(f"      ⚠️ {YELLOW}Resource not found{RESET} fetching {category} for {username}: {e}. Skipping category.")
        except prawcore.exceptions.RequestException as e:
             # Network errors
             logging.warning(f"      ⚠️ {YELLOW}Network/Request error{RESET} fetching {category} for {username}: {e}. Skipping category.")
             time.sleep(5) # Add a small delay after network errors
        except prawcore.exceptions.ResponseException as e:
             # Specific Reddit API errors (e.g., 5xx, 429)
             status_code = getattr(e.response, 'status_code', 'N/A')
             logging.warning(f"      ⚠️ {YELLOW}Reddit API response error{RESET} fetching {category} for {username}: {e} (Status: {status_code}). Skipping category.")
             if status_code == 429: # Rate limit
                 logging.warning(f"         {RED}Hit rate limit (429){RESET}. PRAW should handle delays, but if this persists, check usage.")
                 # PRAW usually waits, but we add a small extra pause just in case.
                 time.sleep(5)
             elif status_code >= 500: # Server error
                  logging.warning(f"         Reddit server error ({status_code}). Waiting briefly before potentially continuing...")
                  time.sleep(10)
             else:
                  time.sleep(2) # Small delay for other response errors
        except Exception as e:
             # Catch-all for unexpected errors during the fetch loop
             logging.error(f"      ❌ Unexpected error fetching {category} for {username}: {e}", exc_info=True)
             logging.warning(f"      Continuing fetch process despite error in {category}.")

    # --- Post-Fetch Summary and Processing ---

    if skipped_malformed > 0:
         logging.warning(f"   ⚠️ Skipped {skipped_malformed} malformed/unconvertible entries during fetch.")
    if not force_scrape and skipped_older > 0:
         logging.info(f"   ℹ️ Skipped {skipped_older} entries older than or same age as newest stored data.")

    # Check if any changes were actually made to the data dictionary content
    final_item_count = {
        't1': len(existing_data.get('t1', {})),
        't3': len(existing_data.get('t3', {}))
    }
    # total_changes = newly_fetched_count + updated_count
    content_changed = (final_item_count['t1'] != initial_item_count['t1'] or
                       final_item_count['t3'] != initial_item_count['t3'] or
                       updated_count > 0)

    # If we started with an empty dataset (no file or force_scrape), we should always save, even if empty.
    initial_data_was_empty = not os.path.exists(filepath) or force_scrape

    if not content_changed and not initial_data_was_empty:
        logging.info(f"   ✅ No changes detected in data content. Data file remains unchanged: {CYAN}{filepath}{RESET}")
        return filepath # Return the path to the existing file

    logging.info(f"   📊 Fetch summary: {BOLD}{GREEN}{newly_fetched_count}{RESET} new items added, {BOLD}{YELLOW}{updated_count}{RESET} items updated.")

    # --- Sort the Data ---
    # This sorts the *entire* dataset (old + new) before saving.
    logging.info(f"   ⚙️ Sorting combined data by modification date ({'Newest First' if sort_descending else 'Oldest First'})...")
    valid_items_count_after_sort = 0
    items_excluded_from_sort = 0

    # Use a temporary dictionary to store sorted data
    sorted_data = {"t1": {}, "t3": {}}

    for kind in list(existing_data.keys()): # Process 't1', 't3'
         if not isinstance(existing_data.get(kind), dict) or not existing_data[kind]:
              logging.debug(f"      No data of kind '{kind}' to sort.")
              continue # Skip if kind doesn't exist or is empty

         items_to_sort = [] # List of (mod_date, item_id, item_data) tuples
         current_kind_invalid = 0
         current_kind_valid = 0

         for item_id, item_data in existing_data[kind].items():
              mod_date = get_modification_date(item_data)
              if mod_date > 0.0: # Only sort items with a valid positive timestamp
                   items_to_sort.append((mod_date, item_id, item_data))
                   current_kind_valid += 1
              else:
                   items_excluded_from_sort += 1
                   current_kind_invalid += 1
                   logging.debug(f"      Excluding item {kind}_{item_id} from sorting due to invalid/zero modification date ({mod_date}). It will not be saved.")

         if current_kind_invalid > 0:
              logging.warning(f"      Will exclude {current_kind_invalid} items of kind '{kind}' from final saved data due to invalid date for sorting.")

         # Sort the valid items based on modification date
         items_to_sort.sort(key=lambda x: x[0], reverse=sort_descending)
         valid_items_count_after_sort += len(items_to_sort)

         # Reconstruct the dictionary for this kind with sorted items
         # Using dict comprehension for efficiency
         sorted_data[kind] = {item_id: item_data for mod_date, item_id, item_data in items_to_sort}
         logging.debug(f"      Sorted {len(sorted_data[kind])} valid items for kind '{kind}'.")

    if items_excluded_from_sort > 0:
         logging.warning(f"   ⚠️ Excluded a total of {items_excluded_from_sort} items across all types from the final dataset due to invalid/zero dates.")

    # --- Save the Updated and Sorted Data ---
    temp_filepath = filepath + ".tmp"
    logging.info(f"   💾 Saving {valid_items_count_after_sort} sorted items to {CYAN}{filepath}{RESET}...")
    try:
        with open(temp_filepath, "w", encoding="utf-8") as f:
            # Save the 'sorted_data' dictionary, which only contains valid, sorted items
            json.dump(sorted_data, f, indent=2) # indent=2 for readability

        os.replace(temp_filepath, filepath) # Atomic replace
        logging.info(f"   ✅ Data saved successfully.")

    except Exception as e:
        logging.error(f"   ❌ An unexpected error occurred during file save to {CYAN}{filepath}{RESET}: {e}", exc_info=True)
        # Attempt cleanup of temp file
        if os.path.exists(temp_filepath):
            try: os.remove(temp_filepath)
            except OSError as rm_err: logging.error(f"      Could not remove temporary file {CYAN}{temp_filepath}{RESET}: {rm_err}")
        return None # Indicate save failure

    return filepath # Return path to saved file


def get_post_title_from_permalink(comment_permalink, praw_instance, use_cache=True):
    """
    Fetches the title of the parent post for a given comment permalink using PRAW.

    Uses an in-memory cache to avoid repeated API calls for the same post.

    Args:
        comment_permalink (str): The permalink URL of a comment (relative or absolute).
        praw_instance (praw.Reddit): An authenticated PRAW Reddit instance.
        use_cache (bool): If True, checks and updates the in-memory title cache.

    Returns:
        The title of the parent post (str) if successful, or an error message string
        like "[Post Not Found]" or "[PRAW Error]" if fetching or processing failed.
    """
    global post_title_cache # Use the module-level cache

    if not isinstance(praw_instance, praw.Reddit):
         logging.error(f"      ❌ Invalid PRAW instance provided to get_post_title_from_permalink.")
         return "[PRAW Error]"

    if not comment_permalink or not isinstance(comment_permalink, str):
        logging.warning(f"      ⚠️ Invalid comment permalink provided: {comment_permalink}")
        return "[Invalid Permalink]"

    # Extract the post ID (the part after /comments/)
    # Regex handles optional leading/trailing slashes and query params/fragments
    # Example: /r/subreddit/comments/abc123/post_slug/xyz789/?context=3 -> abc123
    # Example: /r/subreddit/comments/abc123/ -> abc123
    # Example: abc123 (if only ID is somehow passed, though less likely)
    post_id_match = re.search(r'(?:comments/|^)([a-zA-Z0-9]+)', comment_permalink)
    if not post_id_match:
        logging.warning(f"      ⚠️ Could not extract post ID from comment permalink: {DIM}{comment_permalink}{RESET}")
        return "[Invalid Permalink Format]"

    post_id = post_id_match.group(1)
    # PRAW's submission() method takes the base36 ID directly.
    # Use fullname format 't3_{id}' for cache key consistency.
    cache_key = f"t3_{post_id}"

    # Check cache first
    if use_cache and cache_key in post_title_cache:
        cached_title = post_title_cache[cache_key]
        logging.debug(f"      Cache hit for post title ID {cache_key}: '{str(cached_title)[:50]}...'")
        return cached_title # Return cached value (could be title or error string)

    logging.debug(f"      🌐 Fetching post title for ID {post_id} using PRAW...")

    try:
        # Fetch the submission object using PRAW by base36 ID
        submission = praw_instance.submission(id=post_id)

        # Access the title attribute. Check if submission itself was found.
        # PRAW might return a Submission object even if deleted, but attributes might be limited.
        title = getattr(submission, 'title', None)

        if title is not None:
            logging.debug(f"      {GREEN}✅ Fetched title{RESET} for post ID {post_id}: '{title[:50]}...'")
            if use_cache: post_title_cache[cache_key] = title # Add successful fetch to cache
            return title
        else:
            # This might happen if the post was deleted after the comment was made.
            logging.warning(f"      ⚠️ Fetched submission object for {post_id} but title attribute was None or missing. Post might be deleted.")
            result = "[Post Title Missing/Deleted]"
            if use_cache: post_title_cache[cache_key] = result # Cache this specific failure state
            return result

    except prawcore.exceptions.NotFound:
        logging.warning(f"      ⚠️ Post ID '{post_id}' not found via PRAW (404).")
        result = "[Post Not Found]"
        if use_cache: post_title_cache[cache_key] = result # Cache failure
        return result
    except prawcore.exceptions.Forbidden:
        logging.warning(f"      ⚠️ Access denied to post ID '{post_id}' via PRAW (e.g., private subreddit, quarantined).")
        result = "[Access Denied to Post]"
        if use_cache: post_title_cache[cache_key] = result # Cache failure
        return result
    # Redirect might indicate the post ID was valid but maybe merged or has unusual state
    except prawcore.exceptions.Redirect:
         logging.warning(f"      ⚠️ Encountered redirect fetching post ID '{post_id}'. Treating as not directly accessible.")
         result = "[Post Not Accessible/Redirect]"
         if use_cache: post_title_cache[cache_key] = result # Cache failure
         return result
    except prawcore.exceptions.PrawcoreException as e:
        # Catch other PRAW-related errors (network, API issues)
        logging.error(f"      ❌ PRAW error fetching title for post ID '{post_id}': {e}")
        # Don't cache generic PRAW errors as they might be temporary
        return "[PRAW Fetch Error]"
    except Exception as e:
        # Catch any other unexpected exceptions
        logging.error(f"      ❌ Unexpected error fetching title for post ID '{post_id}': {e}", exc_info=True)
        return "[Unexpected Fetch Error]"


def _fetch_user_about_data(username, praw_instance):
    """
    Fetches basic user account information using PRAW's Redditor object.

    Args:
        username (str): The Reddit username.
        praw_instance (praw.Reddit): An authenticated PRAW Reddit instance.

    Returns:
        A dictionary containing basic user info (like karma, creation date, id, suspension status)
        if successful, or None if the user doesn't exist or fetching fails critically.
        May return partial data if some attributes are missing (e.g., for suspended users).
    """
    if not isinstance(praw_instance, praw.Reddit):
         logging.error("   ❌ Cannot fetch 'about' data: Invalid PRAW instance provided.")
         return None

    logging.debug(f"   🌐 Fetching user 'about' data for /u/{username} using PRAW...")

    try:
        redditor = praw_instance.redditor(username)

        # Attempt to fetch attributes. Use getattr for resilience against missing attributes
        # (common with suspended/deleted/shadowbanned accounts).
        user_data = {
            "name": getattr(redditor, 'name', username), # Fallback to input username if name attr fails
            "id": getattr(redditor, 'id', None),
            "created_utc": getattr(redditor, 'created_utc', None),
            "link_karma": getattr(redditor, 'link_karma', 0),
            "comment_karma": getattr(redditor, 'comment_karma', 0),
            "awardee_karma": getattr(redditor, 'awardee_karma', 0), # Added more karma types
            "awarder_karma": getattr(redditor, 'awarder_karma', 0),
            "total_karma": getattr(redditor, 'total_karma', 0), # PRAW >= 7.0
            # Suspension status check - access directly, might raise Forbidden/NotFound on problematic accounts
            "is_suspended": getattr(redditor, 'is_suspended', None), # None indicates status couldn't be determined
            "is_blocked": None, # PRAW doesn't directly expose is_blocked easily, requires authenticated user context
            # Add other potentially useful fields if needed:
            # "icon_img": getattr(redditor, 'icon_img', None),
            # "is_mod": getattr(redditor, 'is_mod', False), # Might not be accurate without full scope?
        }

        # Check suspension status explicitly if possible (attribute might exist but be None)
        # Note: Accessing .is_suspended can sometimes trigger exceptions itself for weird accounts
        try:
             # Re-fetch is_suspended if it was None initially, might trigger specific exception
             if user_data["is_suspended"] is None:
                 user_data["is_suspended"] = redditor.is_suspended
        except prawcore.exceptions.NotFound:
            logging.warning(f"   ⚠️ User /u/{username} not found when checking suspension status (might be deleted).")
            # Keep id/created if already fetched, but mark as likely deleted/gone.
            user_data["status_note"] = "User not found during suspension check (likely deleted)."
        except prawcore.exceptions.Forbidden:
            logging.warning(f"   ⚠️ Access denied when checking suspension status for /u/{username} (private/suspended?).")
            user_data["is_suspended"] = True # Assume suspended if Forbidden
            user_data["status_note"] = "Access denied during suspension check (assumed suspended/private)."
        except AttributeError:
            # is_suspended attribute might not exist on very old/weird accounts
            logging.warning(f"   ⚠️ Attribute 'is_suspended' not found for /u/{username}. Cannot determine suspension status.")
            user_data["is_suspended"] = None # Mark as unknown explicitly
            user_data["status_note"] = "Suspension status attribute missing."
        except Exception as susp_e:
             logging.warning(f"   ⚠️ Error checking suspension status for /u/{username}: {susp_e}")
             user_data["status_note"] = f"Error during suspension check: {susp_e}"


        # Validate essential data was retrieved (ID is crucial)
        if user_data["id"] is None:
             # This usually means the initial redditor() call failed subtly or user doesn't exist
             # Let's try one more check that might raise NotFound/Forbidden clearly
             try:
                 _ = redditor.created_utc # Access another attribute
                 # If we get here, ID is missing but account seems somewhat accessible? Weird.
                 logging.warning(f"   ⚠️ Could not retrieve user ID for /u/{username}, but other attributes might exist. Data may be unreliable.")
                 user_data["status_note"] = "User ID missing, data unreliable."
                 # Return partial data anyway
             except (prawcore.exceptions.NotFound, prawcore.exceptions.Forbidden):
                 # This confirms the user is likely gone or inaccessible.
                 logging.error(f"   ❌ User /u/{username} confirmed not found or inaccessible when fetching details.")
                 return None # Return None as user seems truly gone/inaccessible


        logging.debug(f"   ✅ Successfully fetched 'about' data for /u/{username} (ID: {user_data['id']}). Suspended: {user_data.get('is_suspended', 'Unknown')}")
        return user_data

    except prawcore.exceptions.NotFound:
        logging.error(f"   ❌ User /u/{username} not found via PRAW.")
        return None
    except prawcore.exceptions.Forbidden:
        # This might happen for profiles that are blocked, private, or require special access
        logging.error(f"   ❌ Access denied to /u/{username}'s profile via PRAW (private/suspended/deleted?).")
        # Try to return minimal info indicating the state if possible
        return {
             "name": username,
             "id": None,
             "created_utc": None,
             "is_suspended": True, # Assume suspended/private
             "status_note": "Access denied to profile (Forbidden)."
        }
    except prawcore.exceptions.PrawcoreException as e:
        logging.error(f"   ❌ PRAW error fetching 'about' data for /u/{username}: {e}", exc_info=False) # Keep log cleaner
        return None
    except AttributeError as e:
        # Catch if expected attributes are missing from the Redditor object after creation
        logging.error(f"   ❌ Attribute error fetching 'about' data for /u/{username}: {e}. Account might be in an unusual state (e.g., partially deleted).")
        # Attempt to return minimal data if possible
        return {
             "name": username,
             "id": getattr(redditor, 'id', None) if 'redditor' in locals() else None,
             "created_utc": None,
             "status_note": f"Attribute error during fetch: {e}"
        }
    except Exception as e:
        # Catch any other unexpected errors
        logging.error(f"   ❌ Unexpected error fetching 'about' data for /u/{username}: {e}", exc_info=True)
        return None