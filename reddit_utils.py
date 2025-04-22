import requests
import logging
import json
import time
import os
import re
from datetime import datetime, timezone # Added timezone

# Import ANSI codes
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; YELLOW = "\033[33m"


# --- Globals specific to this module ---
post_title_cache = {}

# --- Helper Functions ---

def format_timestamp(utc_timestamp):
    """Formats a UTC timestamp into a human-readable string."""
    try:
        # Ensure input is treated as float
        ts = float(utc_timestamp) if utc_timestamp is not None else 0
        # Add basic range check for sanity (e.g., year > 2000 and not too far in future)
        if ts <= 946684800 or ts > time.time() * 1.1: # 946684800 is approx 2000-01-01
            raise ValueError("Timestamp out of reasonable range (expected post-2000, not too far future)")
        dt_object = datetime.fromtimestamp(ts, timezone.utc)
        return dt_object.strftime('%Y-%m-%d %H:%M:%S UTC')
    except (ValueError, TypeError, OSError) as e:
        logging.warning(f"      ⚠️ Could not format timestamp '{utc_timestamp}': {e}. Using fallback.")
        return "UNKNOWN_DATE"

def get_modification_date(entry):
    """Gets the modification date (edited or created) from a Reddit entry's data."""
    if not isinstance(entry, dict) or "data" not in entry:
         logging.warning(f"   ⚠️ Cannot get modification date from invalid entry: {str(entry)[:100]}")
         return 0

    entry_data = entry.get("data", {}) # Use .get for safety
    if not isinstance(entry_data, dict):
         logging.warning(f"   ⚠️ Cannot get modification date, entry 'data' is not a dict: {str(entry_data)[:100]}")
         return 0

    edited_time = entry_data.get("edited")
    created_utc = entry_data.get("created_utc", 0)

    # Handle potential string 'false' or boolean False for edited
    if edited_time and str(edited_time).lower() != 'false':
        try:
            edited_ts = float(edited_time)
            # Return edited time only if it's later than created time
            if edited_ts > float(created_utc):
                return edited_ts
            else:
                 # Log if edited time is earlier than created time (data anomaly)
                 logging.debug(f"      Edited timestamp {edited_ts} is not after created timestamp {created_utc}. Using created_utc.")
                 return float(created_utc)
        except (ValueError, TypeError):
            logging.debug(f"      Invalid 'edited' timestamp '{edited_time}', falling back to created_utc.")
            # Fallback to created_utc if edited is invalid format
            try: return float(created_utc)
            except (ValueError, TypeError): return 0 # Ultimate fallback
    else:
        # Use created_utc if not edited or edited is False
        try: return float(created_utc)
        except (ValueError, TypeError): return 0 # Ultimate fallback


def load_existing_data(filepath):
    """Loads existing scraped data from a JSON file."""
    if os.path.exists(filepath):
        logging.debug(f"   Attempting to load existing data from: {CYAN}{filepath}{RESET}")
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Basic validation: check if it's a dictionary
                if not isinstance(data, dict):
                    logging.error(f"   ❌ Existing file {CYAN}{filepath}{RESET} does not contain a valid JSON object. Starting fresh.")
                    return {"t1": {}, "t3": {}}

                logging.info(f"   ✅ Loaded existing data successfully from {CYAN}{filepath}{RESET}")
                # Ensure keys exist even after loading
                if "t1" not in data: data["t1"] = {}
                if "t3" not in data: data["t3"] = {}
                return data
        except json.JSONDecodeError:
            logging.error(f"   ❌ Error decoding existing JSON file: {CYAN}{filepath}{RESET}. Starting fresh.")
            return {"t1": {}, "t3": {}}
        except Exception as e:
             logging.error(f"   ❌ Error reading existing file {CYAN}{filepath}{RESET}: {e}. Starting fresh.")
             return {"t1": {}, "t3": {}}
    logging.debug(f"   No existing data file found at {CYAN}{filepath}{RESET}. Starting fresh.")
    return {"t1": {}, "t3": {}}

# --- Reddit API Interaction ---
def get_reddit_data(username, category, config, limit=100, after=None, before=None):
    """Fetches data from the Reddit API for a specific user and category."""
    url = f"https://www.reddit.com/user/{username}/{category}.json"
    # Ensure user agent exists and is a non-empty string
    user_agent = config.get('user_agent')
    if not isinstance(user_agent, str) or not user_agent:
        user_agent = "Python:RedditProfilerScript:v1.7" # Fallback UA
        logging.warning(f"      ⚠️ Invalid or missing User-Agent in config, using fallback: {user_agent}")
    headers = {"User-Agent": user_agent}

    params = {"limit": limit, "raw_json": 1}
    if after: params["after"] = after
    if before: params["before"] = before

    logging.debug(f"      🌐 Requesting URL: {DIM}{url}{RESET} with params: {params}")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        # Log status before raising exception for non-2xx codes
        if response.status_code != 200:
             logging.warning(f"      ⚠️ Received non-200 status code {response.status_code} from {DIM}{url}{RESET}")
        response.raise_for_status() # Raises HTTPError for 4xx/5xx
        logging.debug(f"      ✅ Received {response.status_code} from {DIM}{url}{RESET}")
        # Check content type before decoding JSON
        if 'application/json' not in response.headers.get('Content-Type', ''):
             logging.error(f"   ❌ Unexpected content type received from {DIM}{url}{RESET}: {response.headers.get('Content-Type', 'N/A')}. Body: {DIM}{response.text[:200]}...{RESET}")
             return None
        return response.json()
    except requests.exceptions.Timeout:
        logging.error(f"   ❌ Timeout fetching {category} for {username} (URL: {DIM}{url}{RESET})")
        return None
    except requests.exceptions.HTTPError as http_err:
        status_code = response.status_code if response else "N/A"
        logging.error(f"   ❌ HTTP error {status_code} fetching {category} for {username} (URL: {DIM}{url}{RESET}): {http_err}")
        if status_code == 404: logging.error(f"      User '{username}' or category '{category}' not found.")
        elif status_code in [401, 403]: logging.error(f"      Unauthorized/Forbidden access fetching {category} for {username}. Profile might be private/suspended.")
        elif status_code == 429: logging.warning(f"      Rate limit (429) hit fetching {category} for {username}. Consider increasing sleep time or reducing frequency.")
        # Log response body snippet for other errors if possible
        elif response is not None and hasattr(response, 'text'):
             logging.error(f"      Response body snippet: {DIM}{response.text[:200]}...{RESET}")
        return None
    except requests.exceptions.RequestException as req_err:
        logging.error(f"   ❌ Network error fetching {category} for {username} (URL: {DIM}{url}{RESET}): {req_err}")
        return None
    except json.JSONDecodeError:
        response_text_snippet = response.text[:500] if response and hasattr(response, 'text') else "N/A"
        logging.error(f"   ❌ Failed to decode JSON response for {category} from {username} (URL: {DIM}{url}{RESET}). Response snippet: {DIM}{response_text_snippet}...{RESET}")
        return None


def save_reddit_data(user_data_dir, username, config, sort_descending, scrape_comments_only=False, force_scrape=False):
    """Scrapes Reddit data for a user, updates existing data, and saves to JSON."""
    os.makedirs(user_data_dir, exist_ok=True)
    filepath = os.path.join(user_data_dir, f"{username}.json")

    # Load existing data or initialize fresh
    existing_data = load_existing_data(filepath)

    if os.path.exists(filepath) and not force_scrape:
        logging.info(f"   📂 Existing data file found: {CYAN}{filepath}{RESET}")
        logging.info(f"      Attempting incremental fetch (only newer items)...")
    elif force_scrape:
         logging.info(f"   ⚠️ {BOLD}Force scraping{RESET} enabled. Re-fetching all accessible data for {username}.")
    else:
        logging.info(f"   ℹ️ No existing data file ({CYAN}{filepath}{RESET}) or force_scrape used. Starting full scrape.")

    categories = ["comments"] if scrape_comments_only else ["submitted", "comments"]
    newly_fetched_count = 0
    updated_count = 0 # Track updates separately
    skipped_malformed = 0
    max_retries = 3
    sleep_time = 2 # Base sleep time
    fetch_limit = 100

    logging.info(f"   ⚙️ Starting data fetch process (Sort: {BOLD}{'Newest First' if sort_descending else 'Oldest First'}{RESET}, Comments Only: {scrape_comments_only})...")

    for category in categories:
        kind = "t3" if category == "submitted" else "t1"
        after = None
        page_count = 0
        retries = 0
        stop_fetching = False
        logging.info(f"      Fetching '{BOLD}{category}{RESET}'...")

        while True:
            page_count += 1
            logging.debug(f"         Fetching page {page_count} of {category} (after: {after}, limit: {fetch_limit})")
            data = get_reddit_data(username, category, config, limit=fetch_limit, after=after)

            if data is None:
                retries += 1
                if retries >= max_retries:
                    logging.error(f"      ❌ Stopping fetch for {BOLD}{category}{RESET} after {max_retries} consecutive errors.")
                    break
                else:
                    wait = sleep_time * (2**retries)
                    logging.warning(f"      ⚠️ Fetch failed for {category} (attempt {retries}/{max_retries}). Retrying after {wait:.1f} seconds...")
                    time.sleep(wait)
                    continue

            retries = 0 # Reset retries on success

            if "data" not in data or not isinstance(data["data"], dict) or "children" not in data["data"]:
                 logging.warning(f"      ⚠️ Invalid or missing 'data'/'children' structure in response for {category} page {page_count}. Ending fetch.")
                 break

            fetched_items = data["data"].get("children") # Use .get for safety
            if not isinstance(fetched_items, list) or not fetched_items:
                 if page_count == 1 and not data["data"].get("after"):
                     logging.info(f"      ✅ No '{category}' found for user {username}.")
                 else:
                     logging.debug(f"      No more children found for {category} (or empty list) on page {page_count}. Ending {category} fetch.")
                 break

            # Progress indicator for DEBUG level
            if page_count % 10 == 0:
                logging.debug(f"         ... processed {page_count} pages for {category} ...")

            found_new_in_page = False
            for entry in fetched_items:
                # Validate entry structure more thoroughly
                if not isinstance(entry, dict) or \
                   "kind" not in entry or entry["kind"] != kind or \
                   "data" not in entry or not isinstance(entry["data"], dict) or \
                   "id" not in entry["data"]:
                    logging.warning(f"      ⚠️ Skipping malformed or unexpected kind entry: {DIM}{str(entry)[:100]}...{RESET}")
                    skipped_malformed += 1
                    continue

                entry_id = entry["data"]["id"]
                current_mod_date = get_modification_date(entry)
                if current_mod_date == 0: # Skip if cannot determine date
                    logging.warning(f"      ⚠️ Skipping entry {kind}_{entry_id} due to invalid modification date.")
                    skipped_malformed += 1
                    continue

                # Ensure the 'kind' key exists in existing_data
                if kind not in existing_data or not isinstance(existing_data.get(kind), dict):
                     existing_data[kind] = {}

                # Check if entry already exists
                existing_entry_data = existing_data[kind].get(entry_id)

                if existing_entry_data:
                    # Check if stored entry is valid before getting its date
                    if isinstance(existing_entry_data, dict) and "data" in existing_entry_data:
                        stored_mod_date = get_modification_date(existing_entry_data)
                        if stored_mod_date == 0: # Treat stored invalid date as needing update
                             logging.warning(f"         Found existing entry {kind}_{entry_id} with invalid stored date, updating.")
                             existing_data[kind][entry_id] = entry
                             updated_count += 1
                             found_new_in_page = True
                        elif current_mod_date > stored_mod_date:
                             logging.debug(f"         ✨ Updating existing entry {kind}_{entry_id} (new mod date).")
                             existing_data[kind][entry_id] = entry
                             updated_count += 1
                             found_new_in_page = True
                        elif force_scrape and current_mod_date == stored_mod_date:
                             # Only log if force_scrape and date is same, avoid noise if older
                             logging.debug(f"         ✨ Re-adding/updating existing entry {kind}_{entry_id} (force_scrape, same date).")
                             existing_data[kind][entry_id] = entry
                             updated_count += 1 # Count as update if forced
                             found_new_in_page = True
                        elif not force_scrape:
                            # Stop fetching if we hit an existing, non-updated item
                            logging.debug(f"         Found existing, non-updated {kind}_{entry_id}. Stopping pagination for {category}.")
                            stop_fetching = True
                            break
                        # else: current_mod_date < stored_mod_date, do nothing unless force_scrape
                    else: # Stored entry was invalid structure
                        logging.warning(f"      ⚠️ Found invalid stored entry structure for ID {entry_id}, overwriting.")
                        existing_data[kind][entry_id] = entry
                        updated_count += 1 # Count as update
                        found_new_in_page = True
                else: # Entry is completely new
                    logging.debug(f"         ✨ Adding new entry {kind}_{entry_id}")
                    existing_data[kind][entry_id] = entry
                    newly_fetched_count += 1
                    found_new_in_page = True

            if stop_fetching:
                break # Stop pagination loop for this category

            after = data["data"].get("after")
            if not after:
                logging.debug(f"      No 'after' token received for {category} on page {page_count}. Ending {category} fetch.")
                break

            # Add small delay between pages regardless of found_new_in_page
            logging.debug(f"         😴 Sleeping for {sleep_time} seconds before next {category} request...")
            time.sleep(sleep_time)

    if skipped_malformed > 0:
         logging.warning(f"   ⚠️ Skipped {skipped_malformed} malformed entries during fetch.")

    if newly_fetched_count == 0 and updated_count == 0 and os.path.exists(filepath) and not force_scrape:
        logging.info("   ✅ No new items fetched or updated.")
        return filepath # Return existing path, no save needed

    logging.info(f"   📊 Fetch summary: {BOLD}{newly_fetched_count}{RESET} new items fetched, {BOLD}{updated_count}{RESET} items updated.")

    # Sort the data within each kind (t1, t3) based on modification date
    logging.info(f"   ⚙️ Sorting fetched data by modification date ({'Newest First' if sort_descending else 'Oldest First'})...")
    for kind in list(existing_data.keys()): # Iterate over keys copy
         if not isinstance(existing_data.get(kind), dict) or not existing_data[kind]: continue # Skip empty or invalid kinds

         valid_items = {}
         invalid_sort_count = 0
         for item_id, item_data in existing_data[kind].items():
              mod_date = get_modification_date(item_data)
              # Check if mod_date is valid (non-zero) for sorting
              if isinstance(item_data, dict) and "data" in item_data and mod_date is not None and mod_date > 0:
                  valid_items[item_id] = item_data
              else:
                  invalid_sort_count += 1
                  logging.debug(f"      Excluding item {kind}_{item_id} from sorting due to invalid data or date ({mod_date}).")

         if invalid_sort_count > 0:
              logging.warning(f"      Excluded {invalid_sort_count} items of kind '{kind}' from sorting due to invalid data/date.")

         try:
            # Use a stable sort (Python's default)
            sorted_items = sorted(
                valid_items.items(),
                key=lambda item: get_modification_date(item[1]), # item[1] is the entry dict
                reverse=sort_descending
            )
            existing_data[kind] = dict(sorted_items) # Replace with sorted valid items
         except Exception as e:
            logging.error(f"   ❌ Error sorting items for kind '{kind}': {e}. Data for this kind might be unsorted.")
            existing_data[kind] = valid_items # Keep unsorted valid items as fallback


    # Save the potentially updated and sorted data
    temp_filepath = filepath + ".tmp"
    logging.info(f"   💾 Saving updated data to {CYAN}{filepath}{RESET}...")
    try:
        with open(temp_filepath, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=2) # Use indent=2 for potentially smaller files
        # Ensure temp file exists before replacing
        if os.path.exists(temp_filepath):
             os.replace(temp_filepath, filepath)
             logging.info(f"   ✅ Data saved successfully.")
        else:
             # This case should be rare if writing didn't raise exception, but good to handle
             logging.error(f"   ❌ Temporary file {CYAN}{temp_filepath}{RESET} not found after write attempt. Save failed.")
             return None

    except Exception as e:
        logging.error(f"   ❌ An unexpected error occurred during file save to {CYAN}{filepath}{RESET}: {e}")
        if os.path.exists(temp_filepath):
            try: os.remove(temp_filepath)
            except OSError as rm_err: logging.error(f"      Could not remove temporary file {CYAN}{temp_filepath}{RESET}: {rm_err}")
        return None # Indicate save failure
    return filepath


def get_post_title_from_permalink(comment_permalink, config, use_cache=True, max_retries=2):
    """Fetches the title of the post associated with a comment permalink."""
    global post_title_cache # Use the module-level cache
    post_id_match = re.search(r'/comments/([^/]+)/', comment_permalink)
    if not post_id_match:
        logging.warning(f"      ⚠️ Could not extract post ID from comment permalink: {DIM}{comment_permalink}{RESET}")
        return "[Could not extract Post ID]"

    post_id = post_id_match.group(1)
    full_post_id = f"t3_{post_id}"

    if use_cache and full_post_id in post_title_cache:
        logging.debug(f"      Cache hit for post title ID {full_post_id}")
        return post_title_cache[full_post_id]

    info_url = f"https://www.reddit.com/api/info.json"
    params = {"id": full_post_id, "raw_json": 1}
    user_agent = config.get('user_agent', 'Python:RedditProfilerScript:v1.7') # Use current UA
    headers = {"User-Agent": user_agent}
    logging.debug(f"      🌐 Fetching post title for ID {full_post_id} from URL: {DIM}{info_url}{RESET}")

    for attempt in range(max_retries + 1): # Allow max_retries attempts (0 to max_retries-1)
        try:
            # Slightly longer sleep, especially on retries
            sleep_duration = 0.5 + attempt * 0.75
            if attempt > 0: logging.debug(f"         Retrying title fetch ({attempt}/{max_retries}), sleeping {sleep_duration:.1f}s...")
            time.sleep(sleep_duration)

            response = requests.get(info_url, headers=headers, params=params, timeout=15)
            if response.status_code != 200:
                 logging.warning(f"      ⚠️ Received non-200 status {response.status_code} fetching title for {full_post_id} (Attempt {attempt + 1}/{max_retries})")
            response.raise_for_status() # Check for 4xx/5xx

            # Check content type
            if 'application/json' not in response.headers.get('Content-Type', ''):
                 logging.warning(f"      ⚠️ Unexpected content type for title fetch {full_post_id}: {response.headers.get('Content-Type', 'N/A')}. Body: {DIM}{response.text[:200]}...{RESET}")
                 # Decide whether to retry on wrong content type or fail immediately
                 if attempt < max_retries: continue # Retry
                 else: return "[Fetch Error: Wrong Content Type]"

            info_data = response.json()

            # Validate structure before accessing
            if (isinstance(info_data, dict) and 'data' in info_data and
                isinstance(info_data['data'], dict) and 'children' in info_data['data'] and
                isinstance(info_data['data']['children'], list) and
                len(info_data['data']['children']) > 0):

                post_entry = info_data['data']['children'][0]
                if isinstance(post_entry, dict) and post_entry.get('kind') == 't3' and 'data' in post_entry:
                    title = post_entry['data'].get('title')
                    if title is not None:
                        logging.debug(f"      {GREEN}✅ Fetched title{RESET} for post ID {full_post_id}: '{title[:50]}...'")
                        if use_cache: post_title_cache[full_post_id] = title
                        return title
                    else:
                        logging.warning(f"      ⚠️ Could not find 'title' key in post data for ID {full_post_id} via /api/info.")
                        return "[Title Key Missing]" # More specific error

            # Log unexpected structure if title wasn't found
            logging.warning(f"      ⚠️ Unexpected JSON structure or missing data for post {full_post_id} via /api/info. Data: {DIM}{str(info_data)[:300]}{RESET}")
            # Don't retry immediately on bad structure, might be permanent issue
            return "[Fetch Error: Unexpected JSON]"

        except requests.exceptions.Timeout:
            logging.warning(f"      ⚠️ Timeout fetching title for post {full_post_id} (Attempt {attempt + 1}/{max_retries})")
            if attempt >= max_retries: return "[Fetch Error: Timeout]"
            # Continue to retry
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code if http_err.response else 'N/A'
            logging.warning(f"      ⚠️ HTTP error {status_code} fetching title for post {full_post_id} (Attempt {attempt + 1}/{max_retries})")
            if status_code == 404: return "[Post Not Found]"
            if status_code in [401, 403]: return "[Access Denied to Post]"
            if status_code == 429:
                 wait = 5 * (attempt + 2) # Longer wait on rate limit retry
                 logging.warning(f"         Rate limited fetching title for {full_post_id}. Sleeping {wait}s...")
                 time.sleep(wait)
                 # Continue to retry after sleep
            elif attempt >= max_retries: # Fail on last attempt for other HTTP errors
                 return f"[Fetch Error: HTTP {status_code}]"
            # Otherwise, continue to retry for non-fatal HTTP errors
        except (requests.exceptions.RequestException, json.JSONDecodeError) as req_err:
            logging.warning(f"      ⚠️ Error fetching/decoding title for post {full_post_id}: {req_err} (Attempt {attempt + 1}/{max_retries})")
            if attempt >= max_retries: return "[Fetch Error: Request/Decode]"
            # Continue to retry

    # Fallback if all retries fail
    logging.error(f"      ❌ Failed to fetch title for post {full_post_id} after all attempts.")
    return "[Failed to Fetch Title]"

# Helper function added previously to fetch user about data for stats
def _fetch_user_about_data(username, config):
    """Fetches data from the user's 'about.json' endpoint."""
    if not requests:
        logging.error("   ❌ Cannot fetch 'about' data: 'requests' library not installed.")
        return None

    url = f"https://www.reddit.com/user/{username}/about.json"
    user_agent = config.get('user_agent', 'Python:RedditStatsUtil:v1.0') # Default specific to this usage
    headers = {"User-Agent": user_agent}
    logging.debug(f"   🌐 Fetching user 'about' data from: {DIM}{url}{RESET}")
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
             logging.warning(f"   ⚠️ Received non-200 status {response.status_code} fetching 'about' for /u/{username}.")
        response.raise_for_status()

        if 'application/json' not in response.headers.get('Content-Type', ''):
             logging.error(f"   ❌ Unexpected content type received for 'about.json' /u/{username}: {response.headers.get('Content-Type', 'N/A')}.")
             return None

        about_json = response.json()
        if isinstance(about_json, dict) and about_json.get("kind") == "t2" and "data" in about_json:
             logging.debug(f"   ✅ Successfully fetched 'about' data for /u/{username}.")
             return about_json["data"]
        else:
             logging.warning(f"   ⚠️ Unexpected structure in 'about.json' response for /u/{username}.")
             return None
    except requests.exceptions.RequestException as e:
        status_code_str = f" (Status: {e.response.status_code})" if hasattr(e, 'response') and e.response is not None else ""
        logging.error(f"   ❌ Error fetching 'about.json' for /u/{username}{status_code_str}: {e}")
        return None
    except json.JSONDecodeError:
        logging.error(f"   ❌ Failed to decode JSON from 'about.json' for /u/{username}.")
        return None
    except Exception as e: # Catch broader exceptions during 'about' fetch
        logging.error(f"   ❌ Unexpected error fetching 'about.json' for /u/{username}: {e}")
        return None