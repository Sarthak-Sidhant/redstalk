# reddit_utils.py
import requests
import logging
import json
import time
import os
import re

# --- Globals specific to this module ---
post_title_cache = {}

# --- Helper Functions ---
def get_modification_date(entry):
    """Gets the modification date (edited or created) from a Reddit entry's data."""
    if not isinstance(entry, dict) or "data" not in entry:
         logging.warning(f"Cannot get modification date from invalid entry: {str(entry)[:100]}")
         return 0 # Default to epoch start if data is bad

    entry_data = entry["data"]
    if not isinstance(entry_data, dict):
         logging.warning(f"Cannot get modification date, entry 'data' is not a dict: {str(entry_data)[:100]}")
         return 0

    edited_time = entry_data.get("edited")
    if edited_time and edited_time is not False:
        try:
            return float(edited_time)
        except (ValueError, TypeError):
            logging.debug(f"Invalid 'edited' timestamp '{edited_time}', falling back to created_utc.")
            # Fallback to created_utc if edited is invalid format
            return float(entry_data.get("created_utc", 0))
    # Use created_utc if not edited or edited is False
    return float(entry_data.get("created_utc", 0))

def load_existing_data(filepath):
    """Loads existing scraped data from a JSON file."""
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.error(f"Error decoding existing JSON file: {filepath}. Starting fresh.")
            return {"t1": {}, "t3": {}}
        except Exception as e:
             logging.error(f"Error reading existing file {filepath}: {e}. Starting fresh.")
             return {"t1": {}, "t3": {}}
    return {"t1": {}, "t3": {}}

# --- Reddit API Interaction ---
def get_reddit_data(username, category, config, limit=100, after=None, before=None):
    """Fetches data from the Reddit API for a specific user and category."""
    url = f"https://www.reddit.com/user/{username}/{category}.json"
    headers = {"User-Agent": config.get('user_agent', 'Python:RedditProfilerScript:v1.5')}
    params = {"limit": limit, "raw_json": 1} # raw_json=1 helps avoid HTML entities
    if after:
        params["after"] = after
    if before:
        params["before"] = before

    logging.debug(f"Requesting URL: {url} with params: {params}")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching {category} for {username} (URL: {url})")
        return None
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error fetching {category} for {username}: {http_err} (Status: {response.status_code}, URL: {url})")
        if response.status_code == 404:
            logging.error(f"User '{username}' or category '{category}' not found.")
        elif response.status_code in [401, 403]:
             logging.error(f"Unauthorized/Forbidden access fetching {category} for {username}. User profile might be private or suspended.")
        elif response.status_code == 429:
            logging.warning(f"Rate limit potentially hit fetching {category} for {username}. Consider increasing sleep time or reducing frequency.")
        # Add more specific handling as needed
        return None
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Network error fetching {category} for {username}: {req_err} (URL: {url})")
        return None
    except json.JSONDecodeError:
        response_text_snippet = response.text[:500] if response and hasattr(response, 'text') else "N/A"
        logging.error(f"Failed to decode JSON response for {category} from {username} (URL: {url}). Response snippet: {response_text_snippet}")
        return None


def save_reddit_data(user_data_dir, username, config, sort_descending, scrape_comments_only=False, force_scrape=False):
    """Scrapes Reddit data for a user, updates existing data, and saves to JSON."""
    os.makedirs(user_data_dir, exist_ok=True)
    filepath = os.path.join(user_data_dir, f"{username}.json")

    existing_data = load_existing_data(filepath) if os.path.exists(filepath) else {"t1": {}, "t3": {}}

    if os.path.exists(filepath) and not force_scrape:
        logging.info(f"JSON file found at {filepath}.")
        logging.info("Attempting to fetch only newer items since last scrape.")
    elif force_scrape:
         logging.info(f"Force scraping enabled. Existing data in {filepath} (if any) will be updated.")
         # Don't clear existing_data if force_scrape, just update it
    else:
        logging.info(f"No existing JSON file at {filepath} or force_scrape used. Starting full scrape.")

    categories = ["comments"] if scrape_comments_only else ["submitted", "comments"]
    newly_fetched_count = 0
    max_retries = 3
    sleep_time = 2 # Base sleep time

    for category in categories:
        kind = "t3" if category == "submitted" else "t1"
        after = None
        page_count = 0
        retries = 0
        stop_fetching = False

        while True:
            page_count += 1
            logging.debug(f"Fetching page {page_count} of {category} for {username} (after: {after})")
            # Pass config to get_reddit_data
            data = get_reddit_data(username, category, config, limit=100, after=after)

            if data is None:
                retries += 1
                if retries >= max_retries:
                    logging.error(f"Stopping fetch for {category} after {max_retries} consecutive errors.")
                    break # Stop fetching this category
                else:
                    wait = sleep_time * (2**retries) # Exponential backoff
                    logging.warning(f"Fetch failed for {category} (attempt {retries}/{max_retries}). Retrying after {wait:.1f} seconds...")
                    time.sleep(wait)
                    continue # Retry the same page

            retries = 0 # Reset retries on success

            if "data" not in data or not data["data"].get("children"):
                logging.debug(f"No more children found for {category} (or invalid data format) on page {page_count}.")
                break

            fetched_items = data["data"]["children"]
            if not fetched_items:
                 logging.debug(f"Empty 'children' list received for {category} on page {page_count}.")
                 break

            found_new_in_page = False
            for entry in fetched_items:
                if not isinstance(entry, dict) or "kind" not in entry or entry["kind"] != kind or "data" not in entry or "id" not in entry["data"]:
                    logging.warning(f"Skipping malformed or unexpected kind entry: {str(entry)[:100]}...")
                    continue

                entry_id = entry["data"]["id"]
                current_mod_date = get_modification_date(entry)

                # Ensure the 'kind' key exists in existing_data before checking 'in'
                if kind not in existing_data:
                    existing_data[kind] = {}

                if entry_id in existing_data[kind]:
                    stored_entry = existing_data[kind][entry_id]
                    # Check if stored entry is valid before getting its date
                    if isinstance(stored_entry, dict) and "data" in stored_entry:
                        stored_mod_date = get_modification_date(stored_entry)
                        if current_mod_date <= stored_mod_date and not force_scrape:
                            logging.debug(f"Found existing, non-updated entry {kind}_{entry_id}. Stopping pagination for {category}.")
                            stop_fetching = True
                            break # Stop processing this page
                        elif current_mod_date > stored_mod_date:
                             logging.debug(f"Updating existing entry {kind}_{entry_id} due to newer modification date.")
                             existing_data[kind][entry_id] = entry
                             newly_fetched_count += 1
                             found_new_in_page = True
                        elif force_scrape:
                             # force_scrape is true, re-add it anyway
                             logging.debug(f"Re-adding existing entry {kind}_{entry_id} due to force_scrape.")
                             existing_data[kind][entry_id] = entry
                             found_new_in_page = True # Treat as progress if force_scrape
                        # else: entry is older or same date, and not force_scrape - do nothing
                    else: # Stored entry was invalid
                        logging.warning(f"Found invalid stored entry for ID {entry_id}, overwriting.")
                        existing_data[kind][entry_id] = entry
                        newly_fetched_count += 1
                        found_new_in_page = True
                else: # Entry is completely new
                    logging.debug(f"Adding new entry {kind}_{entry_id}")
                    existing_data[kind][entry_id] = entry
                    newly_fetched_count += 1
                    found_new_in_page = True

            if stop_fetching:
                break # Stop pagination loop for this category

            after = data["data"].get("after")
            if not after:
                logging.debug(f"No 'after' token received for {category} on page {page_count}. Ending fetch for this category.")
                break

            logging.debug(f"Sleeping for {sleep_time} seconds before next request...")
            time.sleep(sleep_time)

    if newly_fetched_count == 0 and os.path.exists(filepath) and not force_scrape:
        logging.info("No new items fetched or updated.")
        return filepath # Return existing path, no save needed

    logging.info(f"Fetched/Updated {newly_fetched_count} total items across categories.")

    # Sort the data within each kind (t1, t3) based on modification date
    for kind in list(existing_data.keys()): # Iterate over keys copy
         if not existing_data[kind]: # Skip empty kinds
             continue
         valid_items = {
            item_id: item_data for item_id, item_data in existing_data[kind].items()
            if isinstance(item_data, dict) and "data" in item_data and get_modification_date(item_data) is not None
         }
         try:
            sorted_items = sorted(
                valid_items.items(),
                key=lambda item: get_modification_date(item[1]),
                reverse=sort_descending
            )
            existing_data[kind] = dict(sorted_items)
         except Exception as e:
            logging.error(f"Error sorting items for kind '{kind}': {e}. Data for this kind might be unsorted.")
            existing_data[kind] = valid_items # Keep unsorted valid items

    # Save the potentially updated and sorted data
    temp_filepath = filepath + ".tmp"
    try:
        with open(temp_filepath, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=4)
        os.replace(temp_filepath, filepath)
        logging.info(f"Saved data to {filepath}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during file save: {e}")
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except OSError as rm_err:
                 logging.error(f"Could not remove temporary file {temp_filepath}: {rm_err}")
        return None # Indicate save failure
    return filepath


def get_post_title_from_permalink(comment_permalink, config, use_cache=True, max_retries=2):
    """Fetches the title of the post associated with a comment permalink."""
    global post_title_cache # Use the module-level cache
    post_id_match = re.search(r'/comments/([^/]+)/', comment_permalink)
    if not post_id_match:
        logging.warning(f"Could not extract post ID from comment permalink: {comment_permalink}")
        return "[Could not extract Post ID]"

    post_id = post_id_match.group(1)
    full_post_id = f"t3_{post_id}" # Reddit API often uses the full ID

    if use_cache and full_post_id in post_title_cache:
        logging.debug(f"Cache hit for post ID {full_post_id}")
        return post_title_cache[full_post_id]

    # Use the /api/info endpoint which is designed for getting data by full ID
    info_url = f"https://www.reddit.com/api/info.json"
    params = {"id": full_post_id, "raw_json": 1}
    headers = {"User-Agent": config.get('user_agent', 'Python:RedditProfilerScript:v1.5')}
    logging.debug(f"Fetching post title for ID {full_post_id} from URL: {info_url}")

    for attempt in range(max_retries):
        try:
            # Slightly reduced sleep as info endpoint might be lighter
            time.sleep(0.5 + attempt * 0.5) # Increase sleep slightly on retry
            response = requests.get(info_url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            info_data = response.json()

            # /api/info returns data under ['data']['children']
            if (isinstance(info_data, dict) and 'data' in info_data and
                'children' in info_data['data'] and
                isinstance(info_data['data']['children'], list) and
                len(info_data['data']['children']) > 0):

                post_entry = info_data['data']['children'][0]
                if isinstance(post_entry, dict) and post_entry.get('kind') == 't3' and 'data' in post_entry:
                    title = post_entry['data'].get('title')
                    if title is not None: # Check for None explicitly, empty string is valid
                        logging.debug(f"Fetched title for post ID {full_post_id}: '{title[:50]}...'")
                        if use_cache:
                            post_title_cache[full_post_id] = title
                        return title
                    else:
                        logging.warning(f"Could not find 'title' in post data for ID {full_post_id} via /api/info.")
                        return "[Title Not Found in Data]"
            # Log unexpected structure if title wasn't found
            logging.warning(f"Unexpected JSON structure or missing data received for post {full_post_id} via /api/info. Data: {str(info_data)[:300]}")
            return "[Unexpected JSON structure]"

        except requests.exceptions.Timeout:
            logging.warning(f"Timeout fetching title for post {full_post_id} (Attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code
            logging.warning(f"HTTP error {status_code} fetching title for post {full_post_id} (Attempt {attempt + 1}/{max_retries})")
            if status_code == 404:
                 return "[Post Not Found]"
            if status_code == 429:
                 wait = 5 * (attempt + 1)
                 logging.warning(f"Rate limited fetching title for {full_post_id}. Sleeping {wait}s...")
                 time.sleep(wait)
            elif status_code in [401, 403]:
                 return "[Access Denied to Post]"
            # For other HTTP errors, continue retrying unless it's the last attempt
        except (requests.exceptions.RequestException, json.JSONDecodeError) as req_err:
            logging.warning(f"Error fetching/decoding title for post {full_post_id}: {req_err} (Attempt {attempt + 1}/{max_retries})")

        if attempt >= max_retries - 1:
             logging.error(f"Failed to fetch title for post {full_post_id} after {max_retries} attempts.")
             return "[Failed to Fetch Title]"
        # Implicit else: continue loop (retry)

    # Should not be reached if loop logic is correct, but as a final fallback:
    return "[Failed to Fetch Title after retries]"