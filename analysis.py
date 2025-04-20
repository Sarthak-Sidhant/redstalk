# analysis.py
import csv
import logging
import os
import re
# Import necessary utils
from reddit_utils import get_post_title_from_permalink
# Import the updated perform_ai_analysis which handles conditional chunking
from ai_utils import perform_ai_analysis

# --- Mapped Analysis ---
def generate_mapped_analysis(posts_csv, comments_csv, config, output_file, model, system_prompt, chunk_size, no_cache_titles=False, fetch_external_context=False):
    """
    Generates 'mapped' analysis:
    1. Loads user's posts.
    2. Loads user's comments.
    3. Maps comments to user's posts where possible.
    4. Formats posts with their comments as single entries.
    5. Formats comments on external posts as separate entries (optionally fetching context).
    6. Passes the list of formatted entries to perform_ai_analysis.
    """
    logging.info("Starting MAPPED analysis...")
    if not fetch_external_context:
        logging.info("External post context fetching is DISABLED by default. Use --fetch-external-context to enable (slower).")
    else:
         logging.info("External post context fetching is ENABLED.")

    # Initialize data structures
    posts = {}
    post_id_map = {}
    processed_comment_permalinks = set()
    comments_data = []
    entries = [] # Final list of formatted strings

    # 1. Load User's Posts
    if posts_csv and os.path.exists(posts_csv):
        logging.debug(f"Loading posts from {posts_csv}")
        try:
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                expected_post_fields = ['title', 'selftext', 'permalink', 'created_utc_iso', 'modified_utc_iso']
                if not reader.fieldnames or not all(field in reader.fieldnames for field in expected_post_fields):
                     logging.warning(f"Post CSV headers mismatch/missing. Expected ~{expected_post_fields}, Got {reader.fieldnames}. Trying...")

                for i, row in enumerate(reader):
                    permalink = row.get('permalink','').strip()
                    if not permalink:
                        logging.warning(f"Skipping post row {i+1}: missing permalink.")
                        continue
                    # Extract post ID early for mapping
                    match = re.search(r'/comments/([^/]+)/', permalink)
                    if not match:
                        logging.warning(f"Could not extract post ID from user's post permalink: {permalink} (Row {i+1})")
                        continue
                    post_id = match.group(1)

                    title = row.get('title','[NO TITLE]').strip()
                    selftext = row.get('selftext','').strip() # Keep <br> etc from CSV write
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    post_data = {'title': title, 'selftext': selftext, 'permalink': permalink, 'timestamp': timestamp.strip(), 'comments': [] }
                    posts[permalink] = post_data
                    post_id_map[post_id] = post_data

        except Exception as e:
            logging.error(f"Error processing posts CSV {posts_csv}: {e}", exc_info=True)
            return False # Stop analysis if posts can't be read
    else:
         logging.warning(f"Posts CSV not found or path not provided: {posts_csv}. Proceeding without user posts.")
         posts = {}
         post_id_map = {}

    # 2. Load User's Comments and Store/Map
    if comments_csv and os.path.exists(comments_csv):
        logging.debug(f"Loading comments from {comments_csv}")
        try:
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                expected_comment_fields = ['body', 'permalink', 'created_utc_iso', 'modified_utc_iso']
                if not reader.fieldnames or not all(field in reader.fieldnames for field in expected_comment_fields):
                     logging.warning(f"Comment CSV headers mismatch/missing. Expected ~{expected_comment_fields}, Got {reader.fieldnames}. Trying...")

                for i, row in enumerate(reader):
                    comment_permalink = row.get('permalink','').strip()
                    if not comment_permalink:
                        logging.warning(f"Skipping comment row {i+1}: missing permalink.")
                        continue

                    comment_body = row.get('body','[NO BODY]').strip() # Keep <br>
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    comment_info = {'body': comment_body, 'permalink': comment_permalink, 'timestamp': timestamp.strip()}
                    comments_data.append(comment_info) # Store all comment info

                    # Try to map comments belonging to user's posts
                    match = re.search(r'/comments/([^/]+)/(\w+)?/?(\w+)?', comment_permalink) # Include comment ID parts
                    if match:
                        post_id = match.group(1)
                        if post_id in post_id_map:
                            # Add to the specific post's comment list
                            post_id_map[post_id]['comments'].append(comment_info) # Store full info dict
                            processed_comment_permalinks.add(comment_permalink) # Mark as mapped
        except Exception as e:
            logging.error(f"Error processing comments CSV {comments_csv}: {e}", exc_info=True)
            return False # Stop analysis if comments can't be read
    else:
         logging.warning(f"Comments CSV not found or path not provided: {comments_csv}. Proceeding without comments.")
         comments_data = [] # Ensure it's empty

    # 3. Assemble the list of entry strings for analysis
    logging.debug(f"Assembling {len(posts)} posts and {len(comments_data)} total comments for analysis entries.")

    # Add user's posts with their mapped comments
    # Iterate based on the order posts were added (reflects CSV order)
    for post_permalink, post_data in posts.items():
        post_header = f"USER'S POST TITLE: {post_data['title']} (Date: {post_data['timestamp']}) (Permalink: https://www.reddit.com{post_data['permalink']})"
        post_body_text = post_data['selftext'] if post_data['selftext'] else '[No Body]'
        post_block = f"{post_header}\nPOST BODY:\n{post_body_text}"

        # Add mapped comments for *this* post
        if post_data['comments']:
             post_block += "\n\n  --- Comments on this Post ---"
             # Sort comments within post by timestamp? Optional. Using list order for now.
             for comment_info in post_data['comments']:
                 post_block += (f"\n  ↳ USER'S COMMENT (Date: {comment_info['timestamp']}):\n"
                                f"  {comment_info['body']}\n"
                                f"  (Permalink: https://www.reddit.com{comment_info['permalink']})")
             post_block += "\n  --- End Comments on this Post ---"
        entries.append(post_block) # Add the complete post block

    # Add comments made on *other* posts (unmapped comments)
    unmapped_comments_added = 0
    for comment_info in comments_data:
        comment_permalink = comment_info['permalink']
        # Check if this comment was already processed as part of a user's post
        if comment_permalink not in processed_comment_permalinks:
            comment_body = comment_info['body']
            comment_timestamp = comment_info['timestamp']

            post_title = "[Context Fetching Disabled]"
            if fetch_external_context:
                logging.debug(f"Fetching context for external comment: {comment_permalink}")
                post_title = get_post_title_from_permalink(comment_permalink, config, use_cache=(not no_cache_titles))

            # Format the external comment block as a distinct entry
            comment_block = (
                f"--- USER'S COMMENT ON EXTERNAL POST ---\n"
                f"EXTERNAL POST CONTEXT (Title): {post_title}\n"
                f"USER'S COMMENT (Date: {comment_timestamp}):\n{comment_body}\n"
                f"(Comment Permalink: https://www.reddit.com{comment_permalink})\n"
                f"--- END COMMENT ON EXTERNAL POST ---"
            )
            entries.append(comment_block) # Add as a separate entry
            unmapped_comments_added += 1

    if unmapped_comments_added > 0:
         logging.info(f"Included {unmapped_comments_added} comments made on external posts as separate entries.")

    if not entries:
        logging.error("No data entries generated for mapped analysis (check CSV content and logs).")
        return False

    logging.info(f"Passing {len(entries)} formatted entries to AI analysis core.")
    # Pass the LIST of entries to the core analysis function
    return perform_ai_analysis(model, system_prompt, entries, output_file, chunk_size)


# --- Raw Analysis ---
def generate_raw_analysis(posts_csv, comments_csv, output_file, model, system_prompt, chunk_size):
    """
    Generates 'raw' analysis:
    1. Loads posts and formats each as a string entry.
    2. Loads comments and formats each as a string entry.
    3. Passes the combined list of entries to perform_ai_analysis.
    """
    logging.info("Starting RAW analysis (sequential listing)...")
    entries = [] # List to hold formatted strings

    # Process Posts
    if posts_csv and os.path.exists(posts_csv):
        logging.debug(f"Loading posts from {posts_csv} for raw analysis")
        try:
            with open(posts_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    title = row.get('title', '[NO TITLE]').strip()
                    body = row.get('selftext', '').strip() # Keep <br> etc.
                    permalink = row.get('permalink', 'UNKNOWN_PERMALINK').strip()
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    full_permalink = f"https://www.reddit.com{permalink}" if permalink.startswith('/') else permalink
                    entry = f"--- POST START ---\nDate: {timestamp.strip()}\nPermalink: {full_permalink}\nTitle: {title}\nBody:\n{body if body else '[No Body]'}\n--- POST END ---"
                    entries.append(entry)
        except Exception as e:
            logging.error(f"Error reading posts CSV {posts_csv} for raw analysis: {e}", exc_info=True)
            # Continue processing comments even if posts fail

    # Process Comments
    if comments_csv and os.path.exists(comments_csv):
        logging.debug(f"Loading comments from {comments_csv} for raw analysis")
        try:
            with open(comments_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    body = row.get('body', '[NO BODY]').strip() # Keep <br> etc.
                    permalink = row.get('permalink', 'UNKNOWN_PERMALINK').strip()
                    timestamp = row.get('modified_utc_iso') or row.get('created_utc_iso', 'UNKNOWN_DATE')
                    full_permalink = f"https://www.reddit.com{permalink}" if permalink.startswith('/') else permalink
                    entry = f"--- COMMENT START ---\nDate: {timestamp.strip()}\nPermalink: {full_permalink}\nBody:\n{body}\n--- COMMENT END ---"
                    entries.append(entry)
        except Exception as e:
            logging.error(f"Error reading comments CSV {comments_csv} for raw analysis: {e}", exc_info=True)
            # Continue even if comments fail

    if not entries:
        logging.error("No posts or comments data found/processed for raw analysis.")
        return False

    logging.info(f"Passing {len(entries)} formatted entries to AI analysis core.")
    # Pass the LIST of entries to the core analysis function
    return perform_ai_analysis(model, system_prompt, entries, output_file, chunk_size)