import csv
import logging
import os
import re
from datetime import datetime, timezone
import time # Keep time for prep duration logging

# Import necessary utils
from reddit_utils import get_post_title_from_permalink # Only need this one here
from ai_utils import perform_ai_analysis

# --- ANSI Codes (for logging ONLY) ---
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; YELLOW = "\033[33m"


def _apply_date_filter_to_entries(entries, date_filter):
    """Filters a list of pre-formatted entry strings based on date regex."""
    start_ts, end_ts = date_filter
    if start_ts <= 0 and end_ts == float('inf'):
        logging.debug("      No date filter applied to analysis entries.")
        return entries # No filter needed

    logging.debug(f"      Filtering {len(entries)} formatted analysis entries by date...")
    filtered_entries = []
    # Regex to find the specific date format used in entry headers
    date_pattern = re.compile(r"\(Date:\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+UTC)\)")

    items_kept = 0
    items_filtered = 0

    for entry_index, entry in enumerate(entries):
        match = date_pattern.search(entry)
        keep = True # Default to keeping if date pattern fails
        if match:
            try:
                date_str = match.group(1).strip()
                dt_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S %Z').replace(tzinfo=timezone.utc)
                entry_ts = dt_obj.timestamp()
                if not (start_ts <= entry_ts < end_ts):
                    keep = False # Filter out if outside range
                    # logging.debug(f" Filtering out entry {entry_index+1} (Date: {date_str})...") # Verbose
            except (ValueError, IndexError) as e:
                 logging.warning(f" Could not parse date '{match.group(1)}' for filtering in entry {entry_index+1}: {e}. Keeping.")
                 # keep remains True
        else:
             logging.warning(f" Could not find date pattern for filtering in entry {entry_index+1}. Keeping.")
             # keep remains True

        if keep:
            filtered_entries.append(entry)
            items_kept += 1
        else:
            items_filtered += 1

    # Log summary only if filtering occurred
    if items_filtered > 0 or items_kept < len(entries):
         logging.info(f"      📊 Date filter applied to analysis entries: {items_kept} kept, {items_filtered} filtered out.")
         if items_kept == 0 and items_filtered > 0:
             logging.warning("      ⚠️ All analysis entries were filtered out by the specified date range.")
    return filtered_entries

# --- Mapped Analysis ---
def generate_mapped_analysis(posts_csv, comments_csv, config, output_file, model, system_prompt, chunk_size,
                             date_filter=(0, float('inf')), subreddit_filter=None, # Added filter
                             no_cache_titles=False, fetch_external_context=False):
    """Generates 'mapped' analysis: Formats posts/comments, applies filters."""
    logging.info(f"   Analysis Prep ({BOLD}Mapped Mode{RESET}): Reading & Filtering CSV data...")
    start_time = time.time()
    filter_active = date_filter[0] > 0 or date_filter[1] != float('inf') or subreddit_filter
    if filter_active: logging.info(f"      (Filters: {'Date ' if date_filter[0] > 0 or date_filter[1] != float('inf') else ''}{'Subreddit ' if subreddit_filter else ''}applied during CSV read)")
    if not fetch_external_context: logging.info(f"      ℹ️ External post context fetching is {BOLD}DISABLED{RESET}.")
    else: logging.info(f"      🌐 External post context fetching is {BOLD}ENABLED{RESET}.")
    if no_cache_titles: logging.warning("      ⚠️ Post title caching is disabled.")

    entries = [] # Build list of potential entries FIRST
    posts_read = 0; comments_read = 0
    posts_data_filtered = {} # Store post data keyed by permalink and post_id
    comments_data_filtered = [] # Store comment dicts that pass filters

    # --- Load & Filter Posts from CSV ---
    if posts_csv and os.path.exists(posts_csv):
        logging.debug(f"      Loading & Filtering posts from {CYAN}{posts_csv}{RESET}")
        try:
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'title', 'selftext']
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                    logging.error(f"      ❌ Required fields missing in posts CSV {CYAN}{os.path.basename(posts_csv)}{RESET}. Needed: {required_fields}"); return False
                for i, row in enumerate(reader):
                    posts_read += 1
                    current_sub = row.get('subreddit', '').lower()
                    # Apply Subreddit Filter Here
                    if subreddit_filter and current_sub != subreddit_filter: continue
                    permalink = row.get('permalink','').strip()
                    if not permalink: continue
                    match = re.search(r'/comments/([^/]+)/', permalink)
                    post_id = match.group(1) if match else None
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    post_entry_data = { 'title': row.get('title','[NO TITLE]').strip(), 'selftext': row.get('selftext','').strip(),
                                        'permalink': permalink, 'timestamp': timestamp.strip(), 'comments': [], 'subreddit': row.get('subreddit','') } # Store original case sub
                    posts_data_filtered[permalink] = post_entry_data
                    if post_id: posts_data_filtered[post_id] = post_entry_data # Link by ID too
        except Exception as e: logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv}{RESET}: {e}", exc_info=True); return False
        logging.debug(f"         Loaded and kept {len(posts_data_filtered)//2} posts from {posts_read} rows (after subreddit filter).")

    # --- Load & Filter Comments from CSV ---
    if comments_csv and os.path.exists(comments_csv):
        logging.debug(f"      Loading & Filtering comments from {CYAN}{comments_csv}{RESET}")
        try:
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'body']
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                     logging.error(f"      ❌ Required fields missing in comments CSV {CYAN}{os.path.basename(comments_csv)}{RESET}. Needed: {required_fields}"); return False
                for i, row in enumerate(reader):
                    comments_read += 1
                    current_sub = row.get('subreddit', '').lower()
                    # Apply Subreddit Filter Here
                    if subreddit_filter and current_sub != subreddit_filter: continue
                    comment_permalink = row.get('permalink','').strip();
                    if not comment_permalink: continue
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    comments_data_filtered.append({ 'body': row.get('body','[NO BODY]').strip(), 'permalink': comment_permalink,
                                                    'timestamp': timestamp.strip(), 'subreddit': row.get('subreddit','') }) # Store original case sub
        except Exception as e: logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv}{RESET}: {e}", exc_info=True); return False
        logging.debug(f"         Loaded and kept {len(comments_data_filtered)} comments from {comments_read} rows (after subreddit filter).")

    # --- Assemble Filtered Entries ---
    logging.debug(f"      Assembling {len(posts_data_filtered)//2} filtered posts and {len(comments_data_filtered)} filtered comments into analysis entries...")
    processed_comment_permalinks = set()

    # Add filtered posts and map their filtered comments
    for post_permalink, post_data in posts_data_filtered.items():
         if not isinstance(post_permalink, str) or not post_permalink.startswith('/'): continue # Ensure it's a permalink key
         # Use plain subreddit name in header
         post_header = f"USER'S POST TITLE: {post_data['title']} (Date: {post_data['timestamp']}) (Sub: /r/{post_data['subreddit']}) (Permalink: https://www.reddit.com{post_data['permalink']})"
         post_body_text = post_data['selftext'].replace('<br>', ' ').strip() or '[No Body]'
         post_block = f"{post_header}\nPOST BODY:\n{post_body_text}"
         post_id_match = re.search(r'/comments/([^/]+)/', post_permalink)
         post_id_for_comments = post_id_match.group(1) if post_id_match else None
         comments_for_this_post = []
         if post_id_for_comments:
              for comment_info in comments_data_filtered:
                  c_perm = comment_info['permalink']
                  # Check if comment belongs to this post's ID and hasn't been processed
                  if f"/comments/{post_id_for_comments}/" in c_perm and c_perm not in processed_comment_permalinks:
                       comments_for_this_post.append(comment_info); processed_comment_permalinks.add(c_perm)
         if comments_for_this_post:
              post_block += "\n\n  --- Comments on this Post ---"
              for comment_info in comments_for_this_post:
                  comment_body_cleaned = comment_info['body'].replace('<br>', ' ').strip() or '[No Body]'
                  post_block += (f"\n  ↳ USER'S COMMENT (Date: {comment_info['timestamp']}):\n"
                                 f"    {comment_body_cleaned}\n"
                                 f"    (Permalink: https://www.reddit.com{comment_info['permalink']})")
              post_block += "\n  --- End Comments on this Post ---"
         entries.append(post_block)

    # Add filtered comments not mapped above
    external_comments_added = 0
    for comment_info in comments_data_filtered:
        comment_permalink = comment_info['permalink']
        if comment_permalink not in processed_comment_permalinks:
            comment_body_cleaned = comment_info['body'].replace('<br>', ' ').strip() or '[No Body]'
            comment_timestamp = comment_info['timestamp']; comment_subreddit = comment_info['subreddit']
            post_title = "[Context Fetch Disabled]" # Default, plain text
            if fetch_external_context:
                fetched_title = get_post_title_from_permalink(comment_permalink, config, use_cache=(not no_cache_titles))
                post_title = fetched_title # Use plain fetched title/error marker
            comment_block = (
                f"--- USER'S COMMENT ON EXTERNAL/OTHER POST ---\n"
                f"COMMENT IN SUBREDDIT: /r/{comment_subreddit}\n"
                f"EXTERNAL POST CONTEXT (Title): {post_title}\n" # No ANSI here
                f"USER'S COMMENT (Date: {comment_timestamp}):\n{comment_body_cleaned}\n"
                f"(Comment Permalink: https://www.reddit.com{comment_permalink})\n"
                f"--- END COMMENT ON EXTERNAL/OTHER POST ---"
            )
            entries.append(comment_block); external_comments_added += 1
    if external_comments_added > 0: logging.debug(f"         Added {external_comments_added} comments on external/other posts.")

    prep_duration = time.time() - start_time
    logging.info(f"   ✅ Analysis Prep ({BOLD}Mapped Mode{RESET}): Prepared {len(entries)} filtered entries ({prep_duration:.2f}s).")

    # --- APPLY DATE FILTER to assembled `entries` list ---
    final_entries = _apply_date_filter_to_entries(entries, date_filter)

    if not final_entries:
        logging.error(f"   ❌ No data entries remaining after all filters for mapped analysis.")
        return False

    logging.info(f"   🚀 Passing {len(final_entries)} filtered entries to AI analysis core...")
    return perform_ai_analysis(model, system_prompt, final_entries, output_file, chunk_size)


# --- Raw Analysis ---
def generate_raw_analysis(posts_csv, comments_csv, output_file, model, system_prompt, chunk_size,
                          date_filter=(0, float('inf')), subreddit_filter=None):
    """Generates 'raw' analysis: Sequential list, applies filters."""
    logging.info(f"   Analysis Prep ({BOLD}Raw Mode{RESET}): Reading & Filtering CSV data...")
    start_time = time.time()
    filter_active = date_filter[0] > 0 or date_filter[1] != float('inf') or subreddit_filter
    if filter_active: logging.info(f"      (Filters: {'Date ' if date_filter[0] > 0 or date_filter[1] != float('inf') else ''}{'Subreddit ' if subreddit_filter else ''}applied during CSV read)")

    entries = [] # Build list of potential entries FIRST
    posts_read = 0; comments_read = 0
    posts_added = 0; comments_added = 0

    # --- Process & Filter Posts from CSV ---
    if posts_csv and os.path.exists(posts_csv):
        logging.debug(f"      Loading & Filtering posts from {CYAN}{posts_csv}{RESET}")
        try:
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'title', 'selftext']
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                     logging.error(f"      ❌ Required fields missing in posts CSV {CYAN}{os.path.basename(posts_csv)}{RESET}. Needed: {required_fields}"); return False
                for i, row in enumerate(reader):
                    posts_read += 1
                    current_sub = row.get('subreddit', '').lower()
                    # Apply Subreddit Filter
                    if subreddit_filter and current_sub != subreddit_filter: continue
                    # Format entry if sub filter passes
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    body_cleaned = row.get('selftext', '').replace('<br>', ' ').strip() or '[No Body]'
                    permalink = row.get('permalink', 'UNKNOWN_PERMALINK').strip()
                    full_permalink = f"https://www.reddit.com{permalink}" if permalink.startswith('/') else permalink
                    entry = (f"--- POST START ---\n"
                             f"Date: {timestamp.strip()}\n" # Date used for filtering later
                             f"Subreddit: /r/{row.get('subreddit', 'N/A')}\n"
                             f"Permalink: {full_permalink}\n"
                             f"Title: {row.get('title', '[NO TITLE]').strip()}\n"
                             f"Body:\n{body_cleaned}\n"
                             f"--- POST END ---")
                    entries.append(entry); posts_added += 1
        except Exception as e: logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv}{RESET}: {e}", exc_info=True)

    # --- Process & Filter Comments from CSV ---
    if comments_csv and os.path.exists(comments_csv):
        logging.debug(f"      Loading & Filtering comments from {CYAN}{comments_csv}{RESET}")
        try:
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                required_fields = ['subreddit', 'permalink', 'modified_utc_iso', 'created_utc_iso', 'body']
                if not reader.fieldnames or not all(f in reader.fieldnames for f in required_fields):
                     logging.error(f"      ❌ Required fields missing in comments CSV {CYAN}{os.path.basename(comments_csv)}{RESET}. Needed: {required_fields}"); return False
                for i, row in enumerate(reader):
                    comments_read += 1
                    current_sub = row.get('subreddit', '').lower()
                    # Apply Subreddit Filter
                    if subreddit_filter and current_sub != subreddit_filter: continue
                    # Format entry if sub filter passes
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    body_cleaned = row.get('body', '[NO BODY]').replace('<br>', ' ').strip() or '[No Body]'
                    permalink = row.get('permalink', 'UNKNOWN_PERMALINK').strip()
                    full_permalink = f"https://www.reddit.com{permalink}" if permalink.startswith('/') else permalink
                    entry = (f"--- COMMENT START ---\n"
                             f"Date: {timestamp.strip()}\n"
                             f"Subreddit: /r/{row.get('subreddit', 'N/A')}\n"
                             f"Permalink: {full_permalink}\n"
                             f"Body:\n{body_cleaned}\n"
                             f"--- COMMENT END ---")
                    entries.append(entry); comments_added += 1
        except Exception as e: logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv}{RESET}: {e}", exc_info=True)

    prep_duration = time.time() - start_time
    logging.info(f"   ✅ Analysis Prep ({BOLD}Raw Mode{RESET}): Prepared {len(entries)} potential entries from {posts_read+comments_read} rows ({prep_duration:.2f}s).")
    logging.info(f"      (Posts added: {posts_added}, Comments added: {comments_added} after subreddit filter, before date filter)")

    # --- APPLY DATE FILTER to assembled `entries` list ---
    final_entries = _apply_date_filter_to_entries(entries, date_filter)

    if not final_entries:
        logging.error(f"   ❌ No data entries remaining after all filters for raw analysis.")
        return False

    logging.info(f"   🚀 Passing {len(final_entries)} filtered entries to AI analysis core...")
    return perform_ai_analysis(model, system_prompt, final_entries, output_file, chunk_size)