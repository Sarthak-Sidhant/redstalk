# data_utils.py
import csv
import logging
import json
import os
from datetime import datetime, timezone
# Import necessary functions from other utils if needed (e.g., get_modification_date)
from reddit_utils import get_modification_date

def format_timestamp(utc_timestamp):
    """Formats a UTC timestamp into a human-readable string."""
    try:
        ts = float(utc_timestamp) if utc_timestamp else 0
        dt_object = datetime.fromtimestamp(ts, timezone.utc)
        return dt_object.strftime('%Y-%m-%d %H:%M:%S UTC')
    except (ValueError, TypeError, OSError) as e:
        logging.warning(f"Could not format timestamp '{utc_timestamp}': {e}. Using fallback.")
        return "UNKNOWN_DATE"

def extract_csvs_from_json(json_path, output_prefix):
    """Extracts posts and comments from the JSON data file into separate CSV files."""
    posts_csv_path = f"{output_prefix}-posts.csv"
    comments_csv_path = f"{output_prefix}-comments.csv"
    posts_written = 0
    comments_written = 0
    posts_csv_created = False
    comments_csv_created = False

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error(f"JSON file not found for CSV extraction: {json_path}")
        return None, None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON file for CSV extraction: {json_path}")
        return None, None
    except Exception as e:
        logging.error(f"Error reading JSON file {json_path} for CSV extraction: {e}")
        return None, None

    if not isinstance(data, dict):
        logging.error(f"JSON file {json_path} does not contain a dictionary.")
        return None, None

    post_fieldnames = ['title', 'selftext', 'permalink', 'created_utc_iso', 'modified_utc_iso']
    comment_fieldnames = ['body', 'permalink', 'created_utc_iso', 'modified_utc_iso']

    # Write posts
    if "t3" in data and isinstance(data["t3"], dict) and data["t3"]:
        try:
            with open(posts_csv_path, 'w', newline='', encoding='utf-8') as pfile:
                post_writer = csv.writer(pfile, quoting=csv.QUOTE_MINIMAL)
                post_writer.writerow(post_fieldnames)
                # Iterate in the order provided by the (potentially sorted) dict
                for entry_id, entry_data in data["t3"].items():
                    if isinstance(entry_data, dict) and 'data' in entry_data and isinstance(entry_data['data'], dict):
                        edata = entry_data['data']
                        title = edata.get('title', '')
                        # Replace newlines/tabs in selftext to avoid breaking CSV structure badly
                        selftext = edata.get('selftext', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                        permalink = edata.get('permalink', '')
                        created_utc = edata.get('created_utc', 0)
                        # Use get_modification_date for consistency with sorting
                        modified_utc = get_modification_date(entry_data)
                        created_iso = format_timestamp(created_utc)
                        modified_iso = format_timestamp(modified_utc)
                        post_writer.writerow([title, selftext, permalink, created_iso, modified_iso])
                        posts_written += 1
                    else:
                        logging.warning(f"Skipping invalid post entry {entry_id} during CSV write.")
            logging.info(f"Created {posts_csv_path} with {posts_written} posts.")
            posts_csv_created = True
        except IOError as e:
             logging.error(f"IOError writing posts CSV {posts_csv_path}: {e}")
             if os.path.exists(posts_csv_path): os.remove(posts_csv_path) # Clean up partial file
             posts_csv_created = False # Ensure flag is false
        except Exception as e:
             logging.error(f"Unexpected error writing posts CSV {posts_csv_path}: {e}")
             if os.path.exists(posts_csv_path): os.remove(posts_csv_path)
             posts_csv_created = False
    else:
        logging.info("No valid 't3' (posts) data found in JSON to write to CSV.")

    # Write comments
    if "t1" in data and isinstance(data["t1"], dict) and data["t1"]:
        try:
            with open(comments_csv_path, 'w', newline='', encoding='utf-8') as cfile:
                comment_writer = csv.writer(cfile, quoting=csv.QUOTE_MINIMAL)
                comment_writer.writerow(comment_fieldnames)
                for entry_id, entry_data in data["t1"].items():
                     if isinstance(entry_data, dict) and 'data' in entry_data and isinstance(entry_data['data'], dict):
                        edata = entry_data['data']
                        # Replace newlines/tabs in body
                        body = edata.get('body', '').replace('\n', ' <br> ').replace('\r', '').replace('\t', ' ')
                        permalink = edata.get('permalink', '')
                        created_utc = edata.get('created_utc', 0)
                        modified_utc = get_modification_date(entry_data)
                        created_iso = format_timestamp(created_utc)
                        modified_iso = format_timestamp(modified_utc)
                        comment_writer.writerow([body, permalink, created_iso, modified_iso])
                        comments_written += 1
                     else:
                         logging.warning(f"Skipping invalid comment entry {entry_id} during CSV write.")
            logging.info(f"Created {comments_csv_path} with {comments_written} comments.")
            comments_csv_created = True
        except IOError as e:
             logging.error(f"IOError writing comments CSV {comments_csv_path}: {e}")
             if os.path.exists(comments_csv_path): os.remove(comments_csv_path)
             comments_csv_created = False
        except Exception as e:
             logging.error(f"Unexpected error writing comments CSV {comments_csv_path}: {e}")
             if os.path.exists(comments_csv_path): os.remove(comments_csv_path)
             comments_csv_created = False
    else:
        logging.info("No valid 't1' (comments) data found in JSON to write to CSV.")

    # Return paths only if files were successfully created
    final_posts_path = posts_csv_path if posts_csv_created else None
    final_comments_path = comments_csv_path if comments_csv_created else None
    return final_posts_path, final_comments_path