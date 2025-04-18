#!/usr/bin/env python3
import os
import json
import csv
import argparse
import logging
import time
import requests
import google.generativeai as genai
import re
from datetime import datetime, timezone

CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "default_output_dir": "data",
    "default_prompt_file": "prompt.txt",
    "default_chunk_size": 500000,
    "api_key": None
}

def load_config():
    config = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                user_config = json.load(f)
                config.update(user_config)
                if not isinstance(config.get("default_chunk_size"), int):
                    logging.warning(f"Invalid 'default_chunk_size' type in {CONFIG_FILE}, using default: {DEFAULT_CONFIG['default_chunk_size']}")
                    config["default_chunk_size"] = DEFAULT_CONFIG["default_chunk_size"]
                if not config.get("api_key"):
                    config["api_key"] = None
                return config
        except json.JSONDecodeError:
            logging.error(f"Error decoding {CONFIG_FILE}. Using default configuration values.")
            return DEFAULT_CONFIG.copy()
        except Exception as e:
            logging.error(f"Error loading {CONFIG_FILE}: {e}. Using default configuration values.")
            return DEFAULT_CONFIG.copy()
    else:
        logging.info(f"{CONFIG_FILE} not found. Using default configuration.")
        return config

def save_config(config_data):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4)
        logging.info(f"Configuration saved to {CONFIG_FILE}")
        return True
    except IOError as e:
        logging.error(f"Error saving configuration to {CONFIG_FILE}: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred saving config: {e}")
        return False

current_config = load_config()

TOTAL_TOKEN_THRESHOLD = 1_000_000

def get_modification_date(entry):
    edited_time = entry["data"].get("edited")
    if edited_time and edited_time is not False:
        try:
            return float(edited_time)
        except (ValueError, TypeError):
            return float(entry["data"]["created_utc"])
    return float(entry["data"]["created_utc"])

def load_existing_data(filepath):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.error(f"Error decoding existing JSON file: {filepath}. Starting fresh.")
            return {"t1": {}, "t3": {}}
    return {"t1": {}, "t3": {}}

def get_reddit_data(username, category, limit=100, after=None):
    url = f"https://www.reddit.com/user/{username}/{category}.json"
    headers = {"User-Agent": "Python:RedditProfilerScript:v1.3 (by /u/YourRedditUsername)"}
    params = {"limit": limit, "after": after}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching {category} for {username} (URL: {url})")
        return None
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error fetching {category} for {username}: {http_err} (Status: {response.status_code}, URL: {url})")
        if response.status_code == 404:
            logging.error(f"User '{username}' or category '{category}' not found.")
        elif response.status_code == 429:
            logging.warning(f"Rate limit potentially hit fetching {category} for {username}. Consider increasing sleep time or reducing frequency.")
        return None
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Network error fetching {category} for {username}: {req_err} (URL: {url})")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON response for {category} from {username} (URL: {url})")
        return None

def save_reddit_data(user_data_dir, username, scrape_comments_only=False, force_scrape=False):
    os.makedirs(user_data_dir, exist_ok=True)
    filepath = os.path.join(user_data_dir, f"{username}.json")

    if os.path.exists(filepath) and not force_scrape:
        logging.info(f"JSON file already exists at {filepath}. Skipping scrape. Use --force-scrape to override.")
        return filepath

    existing_data = load_existing_data(filepath)
    categories = ["comments"] if scrape_comments_only else ["submitted", "comments"]
    newly_fetched_count = 0

    for category in categories:
        kind = "t3" if category == "submitted" else "t1"
        after = None
        page_count = 0
        while True:
            page_count += 1
            logging.debug(f"Fetching page {page_count} of {category} for {username} (after: {after})")
            data = get_reddit_data(username, category, limit=100, after=after)

            if data is None:
                logging.error(f"Stopping fetch for {category} due to previous error.")
                break
            if "data" not in data or not data["data"].get("children"):
                logging.debug(f"No more children found for {category} (or invalid data format).")
                break

            fetched_items = data["data"]["children"]
            found_new_in_page = False
            for entry in fetched_items:
                if not isinstance(entry, dict) or "data" not in entry or "id" not in entry["data"]:
                    logging.warning(f"Skipping malformed entry: {str(entry)[:100]}")
                    continue
                entry_id = entry["data"]["id"]
                current_mod_date = get_modification_date(entry)
                if entry_id in existing_data[kind]:
                    stored_entry = existing_data[kind][entry_id]
                    if isinstance(stored_entry, dict) and "data" in stored_entry:
                        stored_mod_date = get_modification_date(stored_entry)
                        if current_mod_date <= stored_mod_date:
                            logging.debug(f"Skipping existing/older entry {kind}_{entry_id}")
                            continue
                    else:
                        logging.warning(f"Found invalid stored entry for ID {entry_id}, will overwrite.")
                logging.debug(f"Adding/Updating entry {kind}_{entry_id}")
                existing_data[kind][entry_id] = entry
                newly_fetched_count += 1
                found_new_in_page = True
            after = data["data"].get("after")
            if not after:
                logging.debug(f"No 'after' token received for {category}. Ending fetch.")
                break
            logging.debug("Sleeping for 2 seconds before next request...")
            time.sleep(2)

    if newly_fetched_count == 0 and os.path.exists(filepath) and not force_scrape:
        logging.info("No new items fetched.")
        return filepath

    logging.info(f"Fetched/Updated {newly_fetched_count} total items.")

    for kind in existing_data:
        valid_items = {
            item_id: item_data for item_id, item_data in existing_data[kind].items()
            if isinstance(item_data, dict) and "data" in item_data
        }
        try:
            existing_data[kind] = dict(sorted(
                valid_items.items(), key=lambda item: get_modification_date(item[1])
            ))
        except Exception as e:
            logging.error(f"Error sorting items for kind '{kind}': {e}. Data might be unsorted.")

    temp_filepath = filepath + ".tmp"
    try:
        with open(temp_filepath, "w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=4)
        os.replace(temp_filepath, filepath)
        logging.info(f"Saved data to {filepath}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during file save: {e}")
        if os.path.exists(temp_filepath): os.remove(temp_filepath)
        return None
    return filepath

def format_timestamp(utc_timestamp):
    try:
        dt_object = datetime.fromtimestamp(float(utc_timestamp), timezone.utc)
        return dt_object.strftime('%Y-%m-%d %H:%M:%S UTC')
    except (ValueError, TypeError, OSError):
        logging.warning(f"Could not format timestamp: {utc_timestamp}. Using fallback.")
        return "UNKNOWN_DATE"

def extract_csvs_from_json(json_path, output_prefix):
    posts_csv_path = f"{output_prefix}-posts.csv"
    comments_csv_path = f"{output_prefix}-comments.csv"

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error(f"JSON file not found: {json_path}")
        return None, None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON file: {json_path}")
        return None, None
    except Exception as e:
        logging.error(f"Error reading JSON file {json_path}: {e}")
        return None, None

    if not isinstance(data, dict) or "t3" not in data or "t1" not in data:
        logging.error(f"JSON file {json_path} does not have the expected structure ('t1', 't3' keys).")
        return None, None

    posts_written = 0
    comments_written = 0

    try:
        with open(posts_csv_path, 'w', newline='', encoding='utf-8') as pfile, \
             open(comments_csv_path, 'w', newline='', encoding='utf-8') as cfile:

            post_fieldnames = ['title', 'selftext', 'permalink', 'created_utc_iso']
            comment_fieldnames = ['body', 'permalink', 'created_utc_iso']

            post_writer = csv.writer(pfile, quoting=csv.QUOTE_MINIMAL)
            comment_writer = csv.writer(cfile, quoting=csv.QUOTE_MINIMAL)

            post_writer.writerow(post_fieldnames)
            comment_writer.writerow(comment_fieldnames)

            for entry_id, entry_data in data.get("t3", {}).items():
                if isinstance(entry_data, dict) and 'data' in entry_data:
                    edata = entry_data['data']
                    if isinstance(edata, dict):
                        title = edata.get('title', '')
                        selftext = edata.get('selftext', '').replace('\n', ' ').replace('\r', ' ')
                        permalink = edata.get('permalink', '')
                        created_utc = edata.get('created_utc', 0)
                        created_iso = format_timestamp(created_utc)
                        post_writer.writerow([title, selftext, permalink, created_iso])
                        posts_written += 1
                    else:
                        logging.warning(f"Skipping post entry {entry_id} due to invalid 'data' field format.")
                else:
                    logging.warning(f"Skipping post entry {entry_id} due to invalid structure.")

            for entry_id, entry_data in data.get("t1", {}).items():
                if isinstance(entry_data, dict) and 'data' in entry_data:
                    edata = entry_data['data']
                    if isinstance(edata, dict):
                        body = edata.get('body', '').replace('\n', ' ').replace('\r', ' ')
                        permalink = edata.get('permalink', '')
                        created_utc = edata.get('created_utc', 0)
                        created_iso = format_timestamp(created_utc)
                        comment_writer.writerow([body, permalink, created_iso])
                        comments_written += 1
                    else:
                        logging.warning(f"Skipping comment entry {entry_id} due to invalid 'data' field format.")
                else:
                    logging.warning(f"Skipping comment entry {entry_id} due to invalid structure.")

        logging.info(f"Created {posts_csv_path} with {posts_written} posts.")
        logging.info(f"Created {comments_csv_path} with {comments_written} comments.")
        return posts_csv_path, comments_csv_path

    except IOError as e:
        logging.error(f"Error writing CSV files: {e}")
        return None, None
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV creation: {e}")
        return None, None

def count_tokens(model, text):
    try:
        if not text: return 0
        return model.count_tokens(text).total_tokens
    except Exception as e:
        logging.error(f"Error counting tokens ({type(e).__name__}): {e}. Text starts with: '{str(text)[:50]}...'")
        return None

def split_into_chunks(text, model, max_chunk_tokens, delimiter="\n\n---\n\n"):
    chunks = []
    current_chunk_parts = []
    current_token_count = 0

    blocks = text.split(delimiter)
    if len(blocks) <= 1 and delimiter != "\n\n":
        logging.debug(f"Primary delimiter '{delimiter}' resulted in <= 1 block. Trying '\\n\\n' instead.")
        delimiter = "\n\n"
        blocks = text.split(delimiter)

    logging.info(f"Attempting to split content into chunks based on {len(blocks)} blocks using delimiter '{delimiter.encode('unicode_escape').decode()}'. Target max tokens/chunk: {max_chunk_tokens}")

    for i, block in enumerate(blocks):
        if not block.strip(): continue
        block_to_add = (delimiter + block) if (i > 0 or current_chunk_parts) else block
        block_token_count = count_tokens(model, block_to_add)
        if block_token_count is None:
            logging.error("Token counting failed for a block, cannot proceed with chunking.")
            return None

        if block_token_count > max_chunk_tokens:
            logging.warning(f"Single block exceeds max chunk tokens ({block_token_count} > {max_chunk_tokens}). Including it as its own chunk.")
            if current_chunk_parts: chunks.append("".join(current_chunk_parts))
            chunks.append(block_to_add)
            current_chunk_parts = []
            current_token_count = 0
            continue

        if current_token_count + block_token_count <= max_chunk_tokens:
            current_chunk_parts.append(block_to_add)
            current_token_count += block_token_count
        else:
            if current_chunk_parts: chunks.append("".join(current_chunk_parts))
            current_chunk_parts = [block_to_add]
            current_token_count = block_token_count

    if current_chunk_parts: chunks.append("".join(current_chunk_parts))
    logging.info(f"Split content into {len(chunks)} chunks.")
    return chunks

def perform_ai_analysis(model, system_prompt, all_reddit_data, output_file, chunk_size):
    logging.info("Counting tokens for the combined Reddit data...")
    total_tokens = count_tokens(model, all_reddit_data)

    if total_tokens is None:
        logging.error("Failed to count tokens. Cannot proceed with analysis.")
        return False
    logging.info(f"Total estimated tokens: {total_tokens}")

    max_chunk_tokens = chunk_size
    final_result = ""
    generation_config = genai.GenerationConfig(
        temperature=0.8, top_p=1, top_k=32, max_output_tokens=16384,
    )
    chunk_delimiter = "\n\n---\n\n" if "POST TITLE:" in all_reddit_data else "\n\n"

    if total_tokens > TOTAL_TOKEN_THRESHOLD:
        logging.warning(f"Token count ({total_tokens}) exceeds threshold ({TOTAL_TOKEN_THRESHOLD}). Splitting into chunks (target size: {max_chunk_tokens})...")
        chunks = split_into_chunks(all_reddit_data, model, max_chunk_tokens, delimiter=chunk_delimiter)

        if chunks is None:
            logging.error("Failed to split data into chunks.")
            return False

        all_chunk_results = []
        for i, chunk in enumerate(chunks):
            logging.info(f"Generating analysis for chunk {i+1}/{len(chunks)}...")
            chunk_token_count = count_tokens(model, chunk)
            logging.info(f"Chunk {i+1} token count: {chunk_token_count}")
            full_prompt = f"{system_prompt}\n\n--- START OF DATA CHUNK {i+1}/{len(chunks)} ---\n\n{chunk}\n\n--- END OF DATA CHUNK {i+1}/{len(chunks)} ---\n\nCONTINUE ANALYSIS BASED ON THIS CHUNK AND PREVIOUS CONTEXT IF APPLICABLE."
            try:
                response = model.generate_content(contents=full_prompt, generation_config=generation_config)
                chunk_result = response.text if hasattr(response, 'text') else f"[ERROR: No text content in response for chunk {i+1}]"
                all_chunk_results.append(chunk_result)
                logging.info(f"Successfully received analysis for chunk {i+1}.")
            except Exception as e:
                logging.error(f"Error generating content for chunk {i+1}: {e}")
                all_chunk_results.append(f"\n\n---------- ERROR PROCESSING CHUNK {i+1}/{len(chunks)} ----------\n\nError: {e}\n\n")

        final_result = f"--- ANALYSIS BASED ON {len(chunks)} CHUNK(S) ---\n\n"
        for i, result in enumerate(all_chunk_results):
            final_result += f"---------- ANALYSIS FOR CHUNK {i+1}/{len(chunks)} ----------\n\n{result}\n\n"
    else:
        logging.info("Token count within limit. Performing single analysis.")
        full_prompt = f"{system_prompt}\n\n{all_reddit_data}"
        try:
            response = model.generate_content(contents=full_prompt, generation_config=generation_config)
            final_result = response.text if hasattr(response, 'text') else "[ERROR: No text content in response]"
            logging.info("Successfully received analysis.")
        except Exception as e:
            logging.error(f"Error generating content: {e}")
            final_result = f"Error during analysis: {e}"
            return False

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(final_result)
        logging.info(f"✅ Analysis saved to {output_file}")
        return True
    except IOError as e:
        logging.error(f"❌ Error saving analysis to {output_file}: {e}")
        return False

def generate_mapped_analysis(posts_csv, comments_csv, output_file, model, system_prompt, chunk_size):
    logging.info("Starting MAPPED analysis (includes date/permalink)...")
    posts = {}
    post_id_map = {}
    try:
        with open(posts_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=['title', 'selftext', 'permalink', 'created_utc_iso'])
            next(reader)
            for row in reader:
                permalink = row.get('permalink','').strip()
                title = row.get('title','').strip()
                selftext = row.get('selftext','').strip()
                timestamp = row.get('created_utc_iso','UNKNOWN_DATE').strip()
                if not permalink:
                    logging.warning(f"Skipping post row due to missing permalink: {row}")
                    continue
                match = re.search(r'/comments/([^/]+)/', permalink)
                if match:
                    post_id = match.group(1)
                    post_data = {'title': title, 'selftext': selftext, 'permalink': permalink, 'timestamp': timestamp, 'comments': []}
                    posts[permalink] = post_data
                    post_id_map[post_id] = post_data
                else:
                    logging.warning(f"Could not extract post ID from post permalink: {permalink}")
    except FileNotFoundError:
        logging.error(f"Posts CSV file not found: {posts_csv}")
        return False
    except Exception as e:
        logging.error(f"Error reading posts CSV {posts_csv}: {e}")
        return False

    comments_unmapped = 0
    try:
        with open(comments_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, fieldnames=['body', 'permalink', 'created_utc_iso'])
            next(reader)
            for row in reader:
                comment_permalink = row.get('permalink','').strip()
                comment_body = row.get('body','').strip()
                comment_timestamp = row.get('created_utc_iso','UNKNOWN_DATE').strip()
                if not comment_permalink:
                    logging.warning(f"Skipping comment row due to missing permalink: {row}")
                    comments_unmapped += 1
                    continue
                match = re.search(r'/comments/([^/]+)/', comment_permalink)
                if match:
                    post_id = match.group(1)
                    if post_id in post_id_map:
                        post_id_map[post_id]['comments'].append((comment_body, comment_permalink, comment_timestamp))
                    else:
                        comments_unmapped += 1
                        logging.debug(f"Comment {comment_permalink} belongs to post ID {post_id} not found.")
                else:
                    logging.warning(f"Could not extract post ID from comment permalink: {comment_permalink}")
                    comments_unmapped += 1
        if comments_unmapped > 0:
            logging.warning(f"{comments_unmapped} comments could not be mapped or had missing permalinks.")
    except FileNotFoundError:
        logging.warning(f"Comments CSV file not found: {comments_csv}. Proceeding without comments if posts exist.")
    except Exception as e:
        logging.error(f"Error reading comments CSV {comments_csv}: {e}")

    if not posts and comments_unmapped == 0:
        logging.error("No posts loaded and no comments found/failed to load.")
        return False

    entries = []
    for post_permalink, post_data in posts.items():
        post_header = f"POST TITLE: {post_data['title']} (Date: {post_data['timestamp']}) (Permalink: https://www.reddit.com{post_data['permalink']})"
        post_block = f"{post_header}\nPOST BODY: {post_data['selftext']}"
        for comment_body, comment_permalink, comment_timestamp in post_data['comments']:
            post_block += f"\n  ↳ COMMENT: {comment_body} (Date: {comment_timestamp}) (Permalink: https://www.reddit.com{comment_permalink})"
        entries.append(post_block)

    if not entries and comments_unmapped > 0:
        logging.warning("No posts found/loaded to map comments to. Mapped analysis will be empty.")

    all_reddit_data = "\n\n---\n\n".join(entries)
    if not all_reddit_data.strip():
        logging.error("No data available to analyze after processing CSVs for mapped analysis.")
        return False

    return perform_ai_analysis(model, system_prompt, all_reddit_data, output_file, chunk_size)

def generate_raw_analysis(posts_csv, comments_csv, output_file, model, system_prompt, chunk_size):
    logging.info("Starting RAW analysis (includes date/permalink)...")
    entries = []
    try:
        if os.path.exists(posts_csv):
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, fieldnames=['title', 'selftext', 'permalink', 'created_utc_iso'])
                next(reader)
                for row in reader:
                    title=row.get('title','').strip()
                    body=row.get('selftext','').strip()
                    permalink=row.get('permalink','UNKNOWN_PERMALINK').strip()
                    timestamp=row.get('created_utc_iso','UNKNOWN_DATE').strip()
                    entries.append(f"--- POST START ---\nPOST TITLE: {title}\nPOST BODY: {body}\n(Date: {timestamp}) (Permalink: https://www.reddit.com{permalink})\n--- POST END ---")
        else:
            logging.warning(f"Posts CSV file not found: {posts_csv}.")
    except Exception as e:
        logging.error(f"Error reading posts CSV {posts_csv}: {e}")
    try:
        if os.path.exists(comments_csv):
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, fieldnames=['body', 'permalink', 'created_utc_iso'])
                next(reader)
                for row in reader:
                    body=row.get('body','').strip()
                    permalink=row.get('permalink','UNKNOWN_PERMALINK').strip()
                    timestamp=row.get('created_utc_iso','UNKNOWN_DATE').strip()
                    entries.append(f"--- COMMENT START ---\nCOMMENT BODY: {body}\n(Date: {timestamp}) (Permalink: https://www.reddit.com{permalink})\n--- COMMENT END ---")
        else:
            logging.warning(f"Comments CSV file not found: {comments_csv}.")
    except Exception as e:
        logging.error(f"Error reading comments CSV {comments_csv}: {e}")

    if not entries:
        logging.error("No posts or comments data found in CSV files for raw analysis.")
        return False
    all_reddit_data = "\n\n".join(entries)

    return perform_ai_analysis(model, system_prompt, all_reddit_data, output_file, chunk_size)

def main():
    parser = argparse.ArgumentParser(
        description="Reddit Scraper & AI Character Profiler v1.3",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("username", help="Reddit username to analyze")
    parser.add_argument("--output-dir",
                        default=current_config['default_output_dir'],
                        help="Base directory for all output data.")
    parser.add_argument("--prompt-file",
                        default=current_config['default_prompt_file'],
                        help="Path to the file containing the system prompt text.")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level.")
    parser.add_argument("--api-key", default=None,
                        help="Google Gemini API Key (optional fallback). Priority: ENV -> config.json -> this flag.")
    parser.add_argument("--chunk-size", type=int,
                        default=current_config['default_chunk_size'],
                        help="Target maximum tokens per chunk for large data analysis.")
    parser.add_argument("--force-scrape", action="store_true", help="Force scraping even if username.json exists.")
    parser.add_argument("--scrape-comments-only", action="store_true", help="Only scrape comments, skip submitted posts.")
    parser.add_argument("--raw-analysis", action="store_true", help="Perform raw analysis (sequential listing) instead of mapped analysis.")
    parser.add_argument("--reset-config", action="store_true", help=f"Reset {CONFIG_FILE} to default values and exit.")

    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                        level=args.log_level.upper(), datefmt='%Y-%m-%d %H:%M:%S')

    if args.reset_config:
        logging.info(f"Resetting configuration file {CONFIG_FILE} to defaults...")
        if save_config(DEFAULT_CONFIG):
            logging.info("Config reset successfully. Exiting.")
        else:
            logging.error("Failed to reset config file.")
        return

    logging.info(f"Using configuration: {current_config}")

    api_key_to_use = None
    source_used = "None"

    env_key = os.environ.get("GOOGLE_API_KEY")
    if env_key:
        api_key_to_use = env_key
        source_used = "environment variable (GOOGLE_API_KEY)"
        logging.info(f"Using API key from {source_used}.")
    else:
        config_key = current_config.get("api_key")
        if config_key:
            api_key_to_use = config_key
            source_used = f"config file ({CONFIG_FILE})"
            logging.info(f"Using API key from {source_used}.")
        else:
            arg_key = args.api_key
            if arg_key:
                api_key_to_use = arg_key
                source_used = "command-line argument (--api-key)"
                logging.info(f"Using API key from {source_used}.")

    if not api_key_to_use:
        logging.critical("API Key missing. Provide via GOOGLE_API_KEY environment variable, 'api_key' in config.json, or --api-key flag.")
        return

    prompt_file_path = args.prompt_file
    try:
        with open(prompt_file_path, "r", encoding="utf-8") as f:
            system_prompt = f.read()
        logging.info(f"Successfully loaded system prompt from: {prompt_file_path}")
        if not system_prompt.strip():
            logging.warning(f"Prompt file {prompt_file_path} appears to be empty.")
    except FileNotFoundError:
        logging.critical(f"System prompt file not found: {prompt_file_path}")
        return
    except IOError as e:
        logging.critical(f"Error reading system prompt file {prompt_file_path}: {e}")
        return

    try:
        genai.configure(api_key=api_key_to_use)
        model_name = "gemini-2.0-flash"
        model = genai.GenerativeModel(model_name)
        logging.info(f"Using Gemini model: {model_name}")
    except Exception as e:
        logging.critical(f"Failed to configure or initialize Generative AI model (using key from {source_used}): {e}")
        return

    user_data_dir = os.path.join(args.output_dir, args.username)
    os.makedirs(user_data_dir, exist_ok=True)
    json_path_expected = os.path.join(user_data_dir, f"{args.username}.json")
    csv_prefix = os.path.join(user_data_dir, args.username)

    logging.info(f"--- Starting Scrape for user: {args.username} ---")
    json_path_actual = save_reddit_data(user_data_dir, args.username, args.scrape_comments_only, args.force_scrape)
    if not json_path_actual or not os.path.exists(json_path_actual):
        if os.path.exists(json_path_expected):
            json_path_actual = json_path_expected
            logging.info(f"Using existing JSON file: {json_path_actual}")
        else:
            logging.error("Scraping/JSON check failed. Cannot proceed.")
            return
    logging.info("--- Scrape Phase Complete ---")

    logging.info(f"--- Starting CSV Conversion from: {json_path_actual} ---")
    posts_csv, comments_csv = extract_csvs_from_json(json_path_actual, csv_prefix)
    posts_exist = posts_csv and os.path.exists(posts_csv)
    comments_exist = comments_csv and os.path.exists(comments_csv)
    if not posts_exist and not comments_exist:
        logging.error("CSV conversion failed entirely.")
        return
    elif not posts_exist or not comments_exist:
        logging.warning("CSV conversion partial or created empty files.")
    posts_csv = posts_csv or f"{csv_prefix}-posts.csv"
    comments_csv = comments_csv or f"{csv_prefix}-comments.csv"
    logging.info("--- CSV Conversion Complete ---")

    logging.info(f"--- Starting AI Analysis Phase ---")
    timestamp_str = datetime.now().strftime('%Y%m%d')
    analysis_type = "raw" if args.raw_analysis else "mapped"
    output_filename = f"{args.username}_charc_{analysis_type}_{timestamp_str}.md"
    output_md_file = os.path.join(user_data_dir, output_filename)
    logging.info(f"Analysis output file will be: {output_md_file}")

    current_chunk_size = args.chunk_size

    if args.raw_analysis:
        success = generate_raw_analysis(posts_csv, comments_csv, output_md_file, model, system_prompt, current_chunk_size)
    else:
        success = generate_mapped_analysis(posts_csv, comments_csv, output_md_file, model, system_prompt, current_chunk_size)

    if success:
        logging.info("--- AI Analysis Complete ---")
    else:
        logging.error("--- AI Analysis Failed ---")

if __name__ == "__main__":
    main()
