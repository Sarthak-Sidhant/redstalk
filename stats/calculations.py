# stats/calculations.py

"""
This module contains a collection of helper functions designed to perform
various statistical calculations on a user's Reddit data (posts and comments).

These functions are intended to be called by a higher-level report generation
script (like single_report.py) after the data has been loaded, filtered,
and saved to CSV files.

Some calculations require optional external libraries like vaderSentiment
(for sentiment analysis) and pandas (for efficient CSV processing for
specific tasks). These dependencies are handled gracefully, and calculations
requiring them will be skipped if the libraries are not available.
"""

import logging
import os
import csv
import time
import math
import statistics
import re # Added for Mention Frequency
from collections import Counter, defaultdict # Added defaultdict for Sentiment Arc
from datetime import datetime, timezone

# --- Import from sibling module ---
# These are utility functions and constants shared within the 'stats' package.
# core_utils.py provides helpers like text cleaning, timestamp retrieval,
# string formatting for durations, n-gram generation, and shared constants
# like stop words and color codes for logging.
from .core_utils import clean_text, _get_timestamp, _format_timedelta, _generate_ngrams, STOP_WORDS, CYAN, RESET, BOLD, YELLOW, RED, GREEN # Added GREEN

# --- Import from modules OUTSIDE the 'stats' package ---
# This specific import is for formatting Reddit timestamps into human-readable
# strings, typically found in the main project directory.
# We use a try/except block because this file might conceptually be reusable,
# or the main project structure might change. If it fails, define a placeholder
# function to avoid errors later, though it will indicate the failure.
try:
    from reddit_utils import format_timestamp
except ImportError:
    logging.critical(f"{BOLD}{RED}❌ Critical Error: Failed to import 'format_timestamp' from reddit_utils.py needed by calculations.{RESET}")
    # Define a fallback function to prevent NameError if import fails
    def format_timestamp(ts):
        """Fallback timestamp formatter if reddit_utils is not available."""
        logging.error(f"Attempted to format timestamp {ts} but format_timestamp was not imported.")
        return f"TIMESTAMP_ERROR({ts})"

# --- Optional Dependency: VADER Sentiment ---
# VADER is specifically for sentiment analysis. It's an optional dependency.
# If not installed (`pip install vaderSentiment`), sentiment-related
# calculations (_calculate_sentiment_ratio, _calculate_sentiment_arc) will be skipped.
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    vader_available = True
    logging.debug("VADER SentimentIntensityAnalyzer imported successfully.")
except ImportError:
    vader_available = False
    SentimentIntensityAnalyzer = None # Explicitly set to None if import fails
    logging.warning(f"{YELLOW}⚠️ VADER sentiment library not found. Sentiment analysis (ratio, arc) will be skipped.{RESET}")
    logging.warning(f"{YELLOW}   To enable, run: pip install vaderSentiment{RESET}")

# --- Optional Dependency: Pandas ---
# Pandas is used for more efficient processing of large CSV files for specific
# calculations like Question Ratio and Mention Frequency. It's an optional dependency.
# If not installed (`pip install pandas`), calculations relying *only* on Pandas
# (currently Question Ratio and Mention Frequency) will be skipped. Note that
# Text Stats and Word Frequency were updated to use the standard `csv` module
# to reduce the core dependency on Pandas for more fundamental stats.
try:
    import pandas as pd
    pandas_available = True
    logging.debug("Pandas library imported successfully.")
except ImportError:
    pandas_available = False
    pd = None # Explicitly set to None if import fails
    # Inform the user how to install Pandas if they want these features
    logging.warning(f"{YELLOW}⚠️ Pandas library not found. Calculations requiring CSV reading via Pandas (Question Ratio, Mention Frequency) will be skipped.{RESET}")
    logging.warning(f"{YELLOW}   To enable, run: pip install pandas{RESET}")


# --- Calculation Helpers ---
# These functions take the filtered data (or paths to filtered CSVs)
# and the 'about' data as input and return dictionaries containing
# specific statistical results. They are prefixed with `_` to indicate
# they are internal helpers, typically not called directly from outside
# this module, but orchestrated by a main report function.

# Note: Data loading and primary filtering are assumed to be handled
# by the calling script (e.g., single_report.py) and the filtered data
# or paths to the filtered CSVs are passed into these functions.

def _calculate_basic_counts(data):
    """
    Calculate the total number of posts and comments in the filtered data.

    Args:
        data (dict): A dictionary containing filtered Reddit items,
                     structured by kind (e.g., {'t3': {...}, 't1': {...}}).

    Returns:
        dict: A dictionary with keys 'total_posts' and 'total_comments'.
    """
    logging.debug("      Calculating basic counts...")
    # Safely access counts, defaulting to an empty dict if a kind is missing
    return {"total_posts": len(data.get("t3", {})), "total_comments": len(data.get("t1", {}))}

def _calculate_time_range(data):
    """
    Determine the time range of activity (first and last item) based on
    creation timestamps of the filtered data.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with keys for first/last activity timestamp (as epoch seconds)
              and formatted date/time strings. Returns 0/None if no valid timestamps found.
    """
    logging.debug("      Calculating time range (based on creation time of filtered items)...")
    # Extract all valid creation timestamps from posts and comments
    all_timestamps = []
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                # Use _get_timestamp helper to get the creation time
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0: # Ensure timestamp is valid and positive
                    all_timestamps.append(ts)
            except Exception as e:
                 logging.debug(f"         Error extracting timestamp for time range on {kind} {item_id}: {e}")
                 continue # Skip this item and continue

    if not all_timestamps:
        # If no valid timestamps were found
        logging.debug("         No valid timestamps found for time range calculation.")
        return {"first_activity": None, "last_activity": None, "first_activity_ts": 0, "last_activity_ts": 0}

    # Find the minimum and maximum timestamps
    min_ts, max_ts = min(all_timestamps), max(all_timestamps)

    # Format timestamps using the potentially imported format_timestamp function
    first_formatted = format_timestamp(min_ts) if format_timestamp else "N/A"
    last_formatted = format_timestamp(max_ts) if format_timestamp else "N/A"

    return {"first_activity": first_formatted, "last_activity": last_formatted,
            "first_activity_ts": min_ts, "last_activity_ts": max_ts}

def _calculate_subreddit_activity(data):
    """
    Count the number of posts and comments made in each subreddit
    present in the filtered data, and list all unique subreddits.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with counts per subreddit for posts and comments,
              and lists of unique subreddits.
    """
    logging.debug("      Calculating subreddit activity...")
    # Use Counters to easily count occurrences
    post_subs, comment_subs = Counter(), Counter()
    # Use sets to track unique subreddits quickly
    posted_set, commented_set = set(), set()

    # Process posts
    for item_id, item in data.get("t3", {}).items():
        try:
            subreddit = item.get("data", {}).get("subreddit")
            if subreddit and isinstance(subreddit, str):
                post_subs[subreddit] += 1
                posted_set.add(subreddit)
        except AttributeError:
            logging.warning(f"      ⚠️ Could not process post {item_id} for subreddit activity (invalid structure).")
            # Continue processing other items even if one fails

    # Process comments
    for item_id, item in data.get("t1", {}).items():
        try:
            subreddit = item.get("data", {}).get("subreddit")
            if subreddit and isinstance(subreddit, str):
                comment_subs[subreddit] += 1
                commented_set.add(subreddit)
        except AttributeError:
            logging.warning(f"      ⚠️ Could not process comment {item_id} for subreddit activity (invalid structure).")
            # Continue processing other items even if one fails

    # Get a sorted list of all subreddits the user was active in
    all_active = sorted(list(posted_set.union(commented_set)), key=str.lower)

    return {"posts_per_subreddit": dict(post_subs),
            "comments_per_subreddit": dict(comment_subs),
            "unique_subs_posted": len(posted_set),
            "unique_subs_commented": len(commented_set),
            "all_active_subs": all_active}


def _calculate_text_stats(posts_csv_path, comments_csv_path):
    """
    Calculate basic statistics about the text content of posts and comments,
    including total words, unique words, lexical diversity, and average
    word count per item. Reads content from the provided CSV file paths.

    Args:
        posts_csv_path (str): Path to the CSV file containing filtered post data.
        comments_csv_path (str): Path to the CSV file containing filtered comment data.

    Returns:
        dict: A dictionary with text statistics. Returns default values or "N/A"
              if CSV files are not found or text cannot be processed.
    """
    logging.debug("      Calculating text stats (from filtered CSVs)...")
    # Initialize stats counters
    stats = {"total_post_words": 0, "total_comment_words": 0,
             "num_posts_with_text": 0, "num_comments_with_text": 0}
    valid_csv_found = False

    # --- Process Posts CSV ---
    if posts_csv_path and os.path.exists(posts_csv_path):
        valid_csv_found = True
        logging.debug(f"         Reading post text from {CYAN}{posts_csv_path}{RESET}")
        try:
            with open(posts_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    # Combine title and selftext for posts
                    title = row.get('title', '')
                    selftext = row.get('selftext', '').replace('<br>', ' ')
                    full_text = f"{title} {selftext}".strip()

                    # Only count words if the text is not empty or a placeholder
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]', '[no title]'):
                        # Clean text and count words. Set remove_stopwords=False here
                        # as we want to count *all* words for total/average length.
                        words = clean_text(full_text, remove_stopwords=False);
                        stats["total_post_words"] += len(words);
                        stats["num_posts_with_text"] += 1
        except Exception as e:
            logging.error(f"      ❌ Error reading posts CSV {CYAN}{posts_csv_path}{RESET} for text stats: {e}")
    else:
        logging.debug(f"         Posts CSV for text stats not found or not provided: {CYAN}{posts_csv_path}{RESET}")

    # --- Process Comments CSV ---
    if comments_csv_path and os.path.exists(comments_csv_path):
        valid_csv_found = True
        logging.debug(f"         Reading comment text from {CYAN}{comments_csv_path}{RESET}")
        try:
            with open(comments_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    body = row.get('body', '').replace('<br>', ' ').strip()

                    # Only count words if the text is not empty or a placeholder
                    if body and body.lower() not in ('[no body]', '[deleted]', '[removed]'):
                        # Clean text and count words. Set remove_stopwords=False.
                        words = clean_text(body, remove_stopwords=False);
                        stats["total_comment_words"] += len(words);
                        stats["num_comments_with_text"] += 1
        except Exception as e:
            logging.error(f"      ❌ Error reading comments CSV {CYAN}{comments_csv_path}{RESET} for text stats: {e}")
    else:
        logging.debug(f"         Comments CSV for text stats not found or not provided: {CYAN}{comments_csv_path}{RESET}")

    # --- Handle Case with No Valid CSVs ---
    if not valid_csv_found:
        logging.warning("      ⚠️ No valid CSV files found for text stats calculation.")
        return {"total_words": 0, "total_post_words": 0, "total_comment_words": 0,
                "total_unique_words": 0, "lexical_diversity": "N/A",
                "avg_post_word_length": "N/A", "avg_comment_word_length": "N/A"}

    # --- Calculate unique words separately across both files ---
    unique_words_set = set()
    # List of files and the columns within them that contain text
    files_for_unique = []
    if posts_csv_path and os.path.exists(posts_csv_path): files_for_unique.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files_for_unique.append((comments_csv_path, ['body']))

    for file_path, cols in files_for_unique:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Combine text from specified columns for this row
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]', '[no title]'):
                         # Clean text and get words. Note: Stop words are *not* removed here
                         # for unique word count, as they are still distinct words.
                         # Words are converted to lowercase in clean_text by default.
                         words = clean_text(full_text, remove_stopwords=False)
                         # Add words to the set, skipping very short words (often punctuation)
                         unique_words_set.update(word for word in words if len(word) > 1)
        except Exception as e:
            logging.error(f"      ❌ Error during unique word count read for {CYAN}{file_path}{RESET}: {e}")

    # --- Final calculations using collected stats ---
    total_words = stats["total_post_words"] + stats["total_comment_words"];
    total_unique_words = len(unique_words_set)

    # Lexical diversity (Unique Words / Total Words)
    lex_div = (total_unique_words / total_words) if total_words > 0 else 0

    # Average words per post (only considering posts with text)
    avg_p = (stats["total_post_words"] / stats["num_posts_with_text"]) if stats["num_posts_with_text"] > 0 else 0

    # Average words per comment (only considering comments with text)
    avg_c = (stats["total_comment_words"] / stats["num_comments_with_text"]) if stats["num_comments_with_text"] > 0 else 0

    return {"total_words": total_words,
            "total_post_words": stats["total_post_words"],
            "total_comment_words": stats["total_comment_words"],
            "total_unique_words": total_unique_words,
            "lexical_diversity": f"{lex_div:.3f}", # Format to 3 decimal places
            "avg_post_word_length": f"{avg_p:.1f}", # Format to 1 decimal place
            "avg_comment_word_length": f"{avg_c:.1f}"}


def _calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=50):
    """
    Calculate the frequency of individual words across all text, excluding
    common stop words. Returns the top N most frequent words.
    Reads content from the provided CSV file paths.

    Args:
        posts_csv_path (str): Path to the CSV file containing filtered post data.
        comments_csv_path (str): Path to the CSV file containing filtered comment data.
        top_n (int): The number of most common words to return.

    Returns:
        dict: A dictionary containing a single key 'word_frequency' mapping
              words to their counts. Empty if no text processed or N=0.
    """
    logging.debug(f"      Calculating word frequency (top {top_n}, from filtered CSVs)...")
    word_counter = Counter() # Counter for easy word counting
    files = []
    # List of files and the columns within them that contain text
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate word frequency.")
        return {"word_frequency": {}} # Return empty results if no files

    total_rows_processed = 0
    # Process each specified CSV file
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for word frequency...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                     # Combine text from specified columns
                     full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                     # Process text if it's not empty or a placeholder
                     if full_text and full_text.lower() not in ['[deleted]', '[removed]', '[no body]', '[no title]']:
                        # Clean text and remove stop words before counting frequency
                        word_counter.update(clean_text(full_text, remove_stopwords=True))
                     total_rows_processed += 1 # Count all rows attempted
        except Exception as e:
            logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for word freq: {e}")

    logging.debug(f"         Finished word freq calculation from {total_rows_processed} total rows read.")
    # Return the N most common words and their counts
    return {"word_frequency": dict(word_counter.most_common(top_n))}


def _calculate_post_types(data):
    """
    Categorize posts from the filtered data into 'link' posts (is_self=False)
    and 'self' posts (is_self=True).

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with counts for 'link_posts' and 'self_posts'.
              Includes a warning if posts have an unknown 'is_self' status.
    """
    logging.debug("      Calculating post types...")
    link_p, self_p, unknown_p = 0, 0, 0

    # Iterate through posts in the filtered data
    for item_id, item in data.get("t3", {}).items():
         try:
             is_self = item.get("data", {}).get("is_self")
             if is_self is True:
                 self_p += 1
             elif is_self is False:
                 link_p += 1
             else:
                 # Handle cases where 'is_self' is missing or not a boolean
                 unknown_p += 1
         except Exception as e:
             logging.warning(f"      ⚠️ Error processing post {item_id} for type: {e}")
             unknown_p += 1; continue # Count as unknown and continue

    if unknown_p > 0:
        logging.warning(f"      ⚠️ Found {unknown_p} posts with unknown or error in 'is_self' field.")

    return { "link_posts": link_p, "self_posts": self_p }


def _calculate_engagement_stats(data, about_data):
    """
    Calculate average scores for filtered posts and comments. Also include
    total link, comment, and combined karma if the user's 'about' data is available.

    Args:
        data (dict): A dictionary containing filtered Reddit items.
        about_data (dict or None): Dictionary containing data from the user's
                                   'about' page, or None if not fetched/available.

    Returns:
        dict: A dictionary with average item scores and total karma information.
    """
    logging.debug("      Calculating engagement stats (item scores & overall karma)...")
    post_scores, comment_scores = [], []

    # Collect scores from posts
    for item_id, item in data.get("t3", {}).items():
        try:
            score = item.get("data", {}).get("score")
            if isinstance(score, (int, float)): # Accept float just in case, convert to int
                 post_scores.append(int(score))
            elif score is not None:
                 logging.debug(f"         Score for post {item_id} is not an integer/float: {type(score)}. Skipping.")
        except (AttributeError, ValueError, TypeError, KeyError):
            logging.debug(f"         Could not parse score for post {item_id}. Skipping.")
            pass # Ignore items with missing or invalid score data

    # Collect scores from comments
    for item_id, item in data.get("t1", {}).items():
        try:
            score = item.get("data", {}).get("score")
            if isinstance(score, (int, float)): # Accept float just in case
                comment_scores.append(int(score))
            elif score is not None:
                 logging.debug(f"         Score for comment {item_id} is not an integer/float: {type(score)}. Skipping.")
        except (AttributeError, ValueError, TypeError, KeyError):
            logging.debug(f"         Could not parse score for comment {item_id}. Skipping.")
            pass # Ignore items with missing or invalid score data

    # Calculate total and average scores for filtered items
    total_post_score = sum(post_scores)
    total_comment_score = sum(comment_scores)
    avg_post_score = (total_post_score / len(post_scores)) if post_scores else 0
    avg_comment_score = (total_comment_score / len(comment_scores)) if comment_scores else 0

    # Get total karma from 'about' data if available
    total_link_karma, total_comment_karma_about, total_karma = "N/A", "N/A", "N/A"
    if about_data and isinstance(about_data, dict):
        logging.debug("         Using fetched 'about' data for karma.")
        # Safely retrieve karma values, default to "N/A"
        total_link_karma = about_data.get("link_karma", "N/A")
        total_comment_karma_about = about_data.get("comment_karma", "N/A")

        # Calculate combined karma if both link and comment karma are integers
        lk_int = total_link_karma if isinstance(total_link_karma, int) else None
        ck_int = total_comment_karma_about if isinstance(total_comment_karma_about, int) else None

        if lk_int is not None and ck_int is not None:
            total_karma = lk_int + ck_int
        elif lk_int is not None:
            total_karma = lk_int # If only link karma is int, use it
        elif ck_int is not None:
             total_karma = ck_int # If only comment karma is int, use it
        # If neither is int, total_karma remains "N/A"
    else:
        logging.debug("         'About' data unavailable or invalid for total karma.")

    return { "total_item_post_score": total_post_score,
             "total_item_comment_score": total_comment_score,
             "avg_item_post_score": f"{avg_post_score:.1f}", # Format to 1 decimal place
             "avg_item_comment_score": f"{avg_comment_score:.1f}", # Format to 1 decimal place
             "total_link_karma": total_link_karma, # From 'about' data
             "total_comment_karma": total_comment_karma_about, # From 'about' data
             "total_combined_karma": total_karma } # Sum of the two if available


def _calculate_temporal_stats(data):
    """
    Analyze the distribution of activity (posts and comments) over time
    based on creation timestamps. Breaks down activity by hour of day,
    day of the week, month, and year.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with activity counts grouped by time periods.
              Includes a total count of items successfully processed for temporal analysis.
    """
    logging.debug("      Calculating temporal stats (based on creation time of filtered items)...")
    # Counters for each time period
    hour_counter, weekday_counter, month_counter, year_counter = Counter(), Counter(), Counter(), Counter()
    # Map weekday index (0-6) to names
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    # Map month number (1-12) to padded string for consistent keys
    months_map = {i: f"{i:02d}" for i in range(1, 13)}

    items_processed = 0 # Count items with valid timestamps processed

    # Iterate through all filtered items (posts and comments)
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                # Get creation timestamp
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0: # Process only if timestamp is valid
                    # Convert timestamp to datetime object in UTC
                    dt = datetime.fromtimestamp(ts, timezone.utc)
                    # Increment counters based on the datetime object's components
                    hour_counter[dt.hour] += 1
                    weekday_counter[dt.weekday()] += 1 # weekday() returns 0 for Monday, 6 for Sunday
                    month_key = (dt.year, dt.month) # Use a tuple key for year-month
                    month_counter[month_key] += 1
                    year_counter[dt.year] += 1
                    items_processed += 1 # Count this item as successfully processed
                elif ts == 0:
                    logging.debug(f"      Skipping item {item_id} for temporal stats due to zero timestamp.")
            except Exception as e:
                logging.warning(f"      ⚠️ Error processing timestamp for temporal stats ({kind} {item_id}): {e}")
                continue # Continue processing other items

    # Format results for consistent keys and sorting
    # Ensure all 24 hours are represented, even if count is 0
    hours_sorted = {f"{hour:02d}": hour_counter.get(hour, 0) for hour in range(24)}
    # Ensure all 7 weekdays are represented
    weekdays_sorted = {days[i]: weekday_counter.get(i, 0) for i in range(7)}
    # Sort month keys chronologically (by year then month) and format
    months_activity = {f"{yr}-{months_map[mn]}": month_counter[key]
                       for key in sorted(month_counter.keys())
                       for yr, mn in [key]} # Use list comprehension trick to unpack tuple key
    # Sort year keys chronologically and format
    years_activity = {yr: year_counter[yr] for yr in sorted(year_counter.keys())}

    logging.debug(f"      Finished temporal stats calculation ({items_processed} items processed).")
    return { "activity_by_hour_utc": hours_sorted,
             "activity_by_weekday_utc": weekdays_sorted,
             "activity_by_month_utc": months_activity,
             "activity_by_year_utc": years_activity,
             "total_items_for_temporal": items_processed } # Report how many items were included


def _calculate_score_stats(data, top_n=5):
    """
    Calculate distribution statistics (min, max, avg, median, quartiles)
    for post and comment scores. Also identifies the top N and bottom N
    items based on score.

    Args:
        data (dict): A dictionary containing filtered Reddit items.
        top_n (int): The number of top/bottom items to list.

    Returns:
        dict: A dictionary with score distribution stats and lists of top/bottom items.
    """
    logging.debug(f"      Calculating score stats (distribution, top/bottom {top_n})...")
    # Lists to store (score, permalink, text_snippet) tuples
    post_details, comment_details = [], []

    # Collect score and details for posts
    for item_id, item in data.get("t3", {}).items():
        try:
            d = item.get("data", {});
            s = d.get("score");
            l = d.get("permalink");
            t = d.get("title", "[No Title]") # Get title, default if missing
            if isinstance(s, int) and isinstance(l, str) and l: # Ensure score is int and permalink is valid string
                post_details.append((s, l, t))
            elif s is not None or l is not None:
                logging.debug(f"         Skipping post {item_id} for score stats (invalid score type '{type(s) if s is not None else 'None'}' or missing permalink '{l}').")
        except Exception as e:
            logging.warning(f"      ⚠️ Error processing post {item_id} for score stats: {e}"); continue

    # Collect score and details for comments
    for item_id, item in data.get("t1", {}).items():
        try:
            d = item.get("data", {});
            s = d.get("score");
            l = d.get("permalink");
            b = d.get("body", "[No Body]") # Get body, default if missing
            if isinstance(s, int) and isinstance(l, str) and l: # Ensure score is int and permalink is valid string
                 # Create a short snippet of the comment body
                 snippet = b[:80].replace('\n',' ') + ("..." if len(b)>80 else "")
                 comment_details.append((s, l, snippet))
            elif s is not None or l is not None:
                logging.debug(f"         Skipping comment {item_id} for score stats (invalid score type '{type(s) if s is not None else 'None'}' or missing permalink '{l}').")
        except Exception as e:
            logging.warning(f"      ⚠️ Error processing comment {item_id} for score stats: {e}"); continue

    # Sort items by score (descending for top lists)
    post_details.sort(key=lambda x: x[0], reverse=True)
    comment_details.sort(key=lambda x: x[0], reverse=True)

    # Extract scores into separate lists for distribution calculations
    post_scores = [item[0] for item in post_details]
    comment_scores = [item[0] for item in comment_details]

    def get_score_distribution(scores):
        """Helper function to calculate distribution stats for a list of scores."""
        n = len(scores)
        # Initialize results with default "N/A" values
        dist = {"count": n, "min": "N/A", "max": "N/A", "average": "N/A",
                "median": "N/A", "q1": "N/A", "q3": "N/A"}

        if not scores:
            return dist # Return defaults if no scores

        # Calculate basic stats if scores exist
        scores.sort(); # Ensure scores are sorted for min/max/quantiles
        dist["min"] = scores[0];
        dist["max"] = scores[-1];
        dist["average"] = f"{(sum(scores) / n):.1f}" # Calculate and format average

        # Calculate median and quartiles using the statistics module
        try:
            dist["median"] = statistics.median(scores)
            # Quantiles require at least 4 data points for quartiles, or 2 for median splits
            if n >= 4:
                # statistics.quantiles returns [Q1, Q2 (median), Q3]
                quantiles = statistics.quantiles(scores, n=4);
                dist["q1"] = quantiles[0];
                dist["q3"] = quantiles[2]
            elif n > 1:
                # Simple split for Q1 and Q3 if less than 4 points but more than 1
                dist["q1"] = scores[0]; # Or scores[math.floor(n*0.25)] depending on method
                dist["q3"] = scores[-1] # Or scores[math.ceil(n*0.75)-1] depending on method
                                        # Using min/max for simplicity when n < 4
            else: # Only 1 data point
                dist["q1"] = scores[0];
                dist["q3"] = scores[0]

        # Handle potential errors from the statistics module
        except AttributeError:
             # Fallback for older Python versions that might not have statistics.quantiles
             logging.warning("      ⚠️ statistics.quantiles not available, using simple median/quartile calculation.")
             dist["median"] = statistics.median(scores)
             if n >= 4:
                 # Basic implementation of quartiles using indices
                 dist["q1"] = scores[max(0, math.ceil(n * 0.25) - 1)]
                 dist["q3"] = scores[min(n - 1, math.ceil(n * 0.75) - 1)]
             elif n > 1:
                 dist["q1"] = scores[0]; dist["q3"] = scores[-1]
             else:
                 dist["q1"] = scores[0]; dist["q3"] = scores[0]
        except Exception as e:
            # Catch any other potential errors during calculation
            logging.error(f"      ❌ Error calculating score distribution quantiles/median: {e}")
            dist["median"] = "Error"; dist["q1"] = "Error"; dist["q3"] = "Error"

        # Format numerical results to 1 decimal place
        for k in ["min", "max", "average", "median", "q1", "q3"]: # Include min/max for formatting consistency
            if isinstance(dist[k], (int, float)):
                 dist[k] = f"{dist[k]:.1f}"

        return dist

    # Ensure top_n is not negative
    safe_top_n = max(0, top_n)

    return { "post_score_distribution": get_score_distribution(post_scores),
             "comment_score_distribution": get_score_distribution(comment_scores),
             # Select top N items (already sorted descending)
             "top_posts": post_details[:safe_top_n],
             "top_comments": comment_details[:safe_top_n],
             # Select bottom N items (sort ascending first, then take top N, then reverse for presentation)
             # Or simply take the last N items from the descending list and reverse
             "bottom_posts": post_details[-(safe_top_n):][::-1] if safe_top_n > 0 else [],
             "bottom_comments": comment_details[-(safe_top_n):][::-1] if safe_top_n > 0 else [] }


def _calculate_award_stats(data):
    """
    Count the total number of awards received across all filtered posts
    and comments, and the number of unique items that received at least one award.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with total award counts and item counts.
    """
    logging.debug("      Calculating award stats...")
    total_awards, items_with_awards = 0, 0

    # Iterate through all filtered items (posts and comments)
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                # Safely get 'total_awards_received', default to 0 if missing
                awards = item.get("data", {}).get("total_awards_received", 0)
                # Check if the value is a positive integer
                if isinstance(awards, int) and awards > 0:
                    total_awards += awards;
                    items_with_awards += 1
                elif awards is not None and awards != 0:
                    # Log if award count is present but not a positive integer
                    logging.debug(f"         Item {item_id} has non-integer award count: {awards} ({type(awards)})")
            except Exception as e:
                logging.warning(f"      ⚠️ Error processing item {item_id} for award stats: {e}"); continue

    return { "total_awards_received": total_awards, "items_with_awards": items_with_awards }


def _calculate_flair_stats(data):
    """
    Count the occurrences of user flair (on comments) and link flair
    (on posts) within the filtered data, grouped by subreddit.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with flair counts per subreddit and total counts
              of items found with flair.
    """
    logging.debug("      Calculating flair stats...")
    user_flairs, post_flairs = Counter(), Counter();
    comments_with_user_flair, posts_with_link_flair = 0, 0
    processed_comments, processed_posts = 0, 0 # Track total items considered

    # Process comments for user flair
    for item_id, item in data.get("t1", {}).items():
        processed_comments += 1
        try:
            d = item.get("data", {});
            sub = d.get("subreddit");
            flair = d.get("author_flair_text")
            # Check if subreddit and non-empty flair text are present
            if sub and isinstance(sub, str) and flair and isinstance(flair, str) and flair.strip():
                # Store flair count associated with its subreddit
                user_flairs[f"{sub}: {flair.strip()}"] += 1;
                comments_with_user_flair += 1 # Count this comment as having flair
        except Exception as e:
            logging.warning(f"      ⚠️ Error processing comment {item_id} for user flair: {e}"); continue

    # Process posts for link flair
    for item_id, item in data.get("t3", {}).items():
        processed_posts += 1
        try:
            d = item.get("data", {});
            sub = d.get("subreddit");
            flair = d.get("link_flair_text")
            # Check if subreddit and non-empty flair text are present
            if sub and isinstance(sub, str) and flair and isinstance(flair, str) and flair.strip():
                # Store flair count associated with its subreddit
                post_flairs[f"{sub}: {flair.strip()}"] += 1;
                posts_with_link_flair += 1 # Count this post as having flair
        except Exception as e:
            logging.warning(f"      ⚠️ Error processing post {item_id} for link flair: {e}"); continue

    logging.debug(f"         Processed {processed_comments} comments ({comments_with_user_flair} with user flair), {processed_posts} posts ({posts_with_link_flair} with link flair).")
    return { "user_flairs_by_sub": dict(user_flairs.most_common()), # Convert Counter to dict
             "post_flairs_by_sub": dict(post_flairs.most_common()), # Convert Counter to dict
             "total_comments_with_user_flair": comments_with_user_flair,
             "total_posts_with_link_flair": posts_with_link_flair }


def _calculate_post_engagement(data):
    """
    Calculate the average number of comments received per post among the
    filtered posts. Also identifies the top N posts by comment count.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with average comments per post and a list of
              top commented posts.
    """
    logging.debug("      Calculating post engagement (num_comments)...")
    comment_counts = []; # List to store comment counts for averaging
    top_commented_posts = [] # List to store (count, permalink, title) for top list
    posts_analyzed = 0; # Counter for posts successfully analyzed
    total_posts_in_data = len(data.get("t3", {})) # Total posts available in input data

    # Iterate through posts in the filtered data
    for item_id, item in data.get("t3", {}).items():
        posts_analyzed += 1 # Increment counter for every post considered
        try:
            d = item.get("data", {});
            num_comments = d.get("num_comments");
            permalink = d.get("permalink");
            title = d.get("title", "[No Title]") # Get title, default if missing
            # Check if 'num_comments' is a valid integer and permalink exists
            if isinstance(num_comments, int) and num_comments >= 0 and isinstance(permalink, str) and permalink:
                 comment_counts.append(num_comments);
                 top_commented_posts.append((num_comments, permalink, title))
            elif num_comments is not None or permalink is not None:
                 logging.debug(f"         Post {item_id} skipped for engagement (invalid 'num_comments': {num_comments} ({type(num_comments)}) or missing permalink: {permalink})")
        except Exception as e:
            logging.warning(f"      ⚠️ Error processing post {item_id} for engagement stats: {e}"); continue

    # Handle case where no valid posts were found or analyzed
    if not comment_counts:
        logging.warning(f"      ⚠️ No valid posts found for comment engagement analysis (out of {total_posts_in_data} total posts in input).")
        return {"avg_comments_per_post": "0.0", "total_posts_analyzed_for_comments": 0, "top_commented_posts": []}

    # Calculate average comments per post
    avg_comments = sum(comment_counts) / len(comment_counts)

    # Sort posts by comment count (descending) for the top list
    top_commented_posts.sort(key=lambda x: x[0], reverse=True)

    return { "avg_comments_per_post": f"{avg_comments:.1f}", # Format to 1 decimal place
             "total_posts_analyzed_for_comments": len(comment_counts), # Count posts *successfully* analyzed
             "top_commented_posts": top_commented_posts[:5] } # Return top 5 posts


def _calculate_editing_stats(data):
    """
    Analyze how often posts and comments were edited by the user
    and calculate the average time delay between creation and the last edit.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with counts and percentages of edited items,
              and average edit delay in seconds and formatted string.
    """
    logging.debug("      Calculating editing stats...")
    posts_edited, comments_edited = 0, 0;
    total_posts = len(data.get("t3", {}));
    total_comments = len(data.get("t1", {}))
    edit_delays_s = []; # List to store edit delays in seconds
    items_processed = 0 # Total items considered for editing

    # Iterate through all filtered items (posts and comments)
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            items_processed +=1 # Count every item we check
            try:
                d = item.get("data", {});
                created_utc = d.get("created_utc");
                edited_ts_val = d.get("edited") # 'edited' can be False or a timestamp

                # Check if the 'edited' field indicates an edit occurred
                if edited_ts_val and str(edited_ts_val).lower() != 'false':
                    try:
                        # Attempt to convert timestamps to floats (epoch seconds)
                        edited_ts = float(edited_ts_val);
                        created_ts = float(created_utc)

                        # Calculate delay only if edited time is after creation time
                        if edited_ts > created_ts:
                            if kind == "t3":
                                posts_edited += 1
                            else: # kind == "t1"
                                comments_edited += 1
                            edit_delays_s.append(edited_ts - created_ts)

                    except (ValueError, TypeError, KeyError) as convert_err:
                        logging.debug(f"         Could not parse created/edited timestamp for edit stat on {kind} {item_id}: {convert_err}. Skipping edit delay calculation for this item.")
                        pass # Skip delay calculation for this item but still count it as edited if appropriate logic above was met

            except Exception as e:
                 logging.warning(f"      ⚠️ Error processing item {item_id} for editing stats: {e}"); pass # Continue processing other items

    # Calculate percentages of edited items
    edit_percent_posts = (posts_edited / total_posts * 100) if total_posts > 0 else 0
    edit_percent_comments = (comments_edited / total_comments * 100) if total_comments > 0 else 0

    # Calculate average edit delay
    avg_delay_s = (sum(edit_delays_s) / len(edit_delays_s)) if edit_delays_s else 0
    # Format the average delay into a human-readable string (e.g., "1 hour 30 minutes")
    avg_delay_str = _format_timedelta(avg_delay_s)

    return { "posts_edited_count": posts_edited,
             "comments_edited_count": comments_edited,
             "total_posts_analyzed_for_edits": total_posts, # Total posts *considered*
             "total_comments_analyzed_for_edits": total_comments, # Total comments *considered*
             "edit_percentage_posts": f"{edit_percent_posts:.1f}%", # Format percentage
             "edit_percentage_comments": f"{edit_percent_comments:.1f}%", # Format percentage
             "average_edit_delay_seconds": round(avg_delay_s, 1), # Round raw seconds
             "average_edit_delay_formatted": avg_delay_str }


def _calculate_sentiment_ratio(posts_csv_path, comments_csv_path):
    """
    Calculate the overall sentiment ratio (positive, negative, neutral)
    of the text content using VADER sentiment analysis. Reads content
    from the provided CSV file paths.

    Args:
        posts_csv_path (str): Path to the CSV file containing filtered post data.
        comments_csv_path (str): Path to the CSV file containing filtered comment data.

    Returns:
        dict: A dictionary with sentiment counts, the positive-to-negative ratio,
              average compound score, and status/reason if analysis was skipped.
    """
    logging.debug("      Calculating sentiment ratio (VADER, from filtered CSVs)...")
    global vader_available # Access the global flag

    # Check if VADER is available before proceeding
    if not vader_available:
        logging.warning(f"      {YELLOW}⚠️ Skipping sentiment ratio: VADER library not available.{RESET}")
        return {"sentiment_analysis_skipped": True, "reason": "VADER library not installed or import failed"}

    # Double check if the Analyzer class is available (should be if vader_available is True, but good practice)
    if not SentimentIntensityAnalyzer:
         logging.warning(f"{YELLOW}⚠️ Skipping sentiment ratio: VADER Analyzer class unavailable.{RESET}"); vader_available = False # Update flag
         return {"sentiment_analysis_skipped": True, "reason": "VADER Analyzer class unavailable"}

    # Initialize the VADER analyzer
    try:
        analyzer = SentimentIntensityAnalyzer()
        logging.debug("         VADER SentimentIntensityAnalyzer initialized for ratio.")
    except Exception as e:
        logging.error(f"    ❌ Failed to initialize VADER SentimentIntensityAnalyzer for ratio: {e}", exc_info=True);
        vader_available = False # Mark as unavailable if init fails
        return {"sentiment_analysis_skipped": True, "reason": f"VADER Analyzer initialization failed: {e}"}

    # Initialize sentiment counters and score list
    pos_count, neg_count, neu_count = 0, 0, 0;
    total_analyzed = 0;
    sentiment_scores = [] # Store compound scores for average

    # List of files and columns to process
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    # Check if any files are available
    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate sentiment ratio.")
        return {"sentiment_analysis_skipped": False, # Analysis attempted but found no data
                "positive_count": 0, "negative_count": 0, "neutral_count": 0,
                "total_items_sentiment_analyzed": 0, "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}

    # Process each specified CSV file
    for file_path, cols in files:
        logging.debug(f"         Analyzing sentiment ratio in {CYAN}{file_path}{RESET}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                    # Combine text from specified columns
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()

                    # Analyze sentiment only if text is non-empty and not a placeholder
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]', '[no title]'):
                        try:
                            # Get polarity scores from VADER
                            vs = analyzer.polarity_scores(full_text)
                            compound_score = vs['compound'] # Use the compound score

                            sentiment_scores.append(compound_score) # Add to list for average
                            total_analyzed += 1 # Count this item as analyzed

                            # Categorize based on standard VADER thresholds
                            if compound_score >= 0.05:
                                pos_count += 1
                            elif compound_score <= -0.05:
                                neg_count += 1
                            else:
                                neu_count += 1

                        except Exception as vader_err:
                            # Log errors specific to VADER analysis on a single item
                            logging.warning(f"{YELLOW}⚠️ VADER error processing item (row {i+1}) in {os.path.basename(file_path)}: {vader_err}{RESET}");
                            # Continue to the next item

        except Exception as e:
            # Log errors reading the CSV file itself
            logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for sentiment ratio: {e}")
            # Continue to the next file if possible

    # Calculate final ratio and average score
    if total_analyzed == 0:
        logging.warning("      ⚠️ No valid text items found in CSVs for sentiment ratio analysis.")
        return {"sentiment_analysis_skipped": False, # Analysis attempted but no text found
                "positive_count": 0, "negative_count": 0, "neutral_count": 0,
                "total_items_sentiment_analyzed": 0, "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}

    # Calculate positive-to-negative ratio
    pos_neg_ratio = f"{(pos_count / neg_count):.2f}:1" if neg_count > 0 else (f"{pos_count}:0" if pos_count > 0 else "N/A")

    # Calculate average compound score
    avg_compound = sum(sentiment_scores) / total_analyzed if total_analyzed > 0 else 0

    return { "sentiment_analysis_skipped": False, # Analysis was performed
             "positive_count": pos_count,
             "negative_count": neg_count,
             "neutral_count": neu_count,
             "total_items_sentiment_analyzed": total_analyzed,
             "pos_neg_ratio": pos_neg_ratio,
             "avg_compound_score": f"{avg_compound:.3f}" } # Format average compound score


def _calculate_age_vs_activity(about_data, temporal_stats):
    """
    Compares the account creation date (from 'about' data) to the overall
    activity level and trend (from temporal stats). Estimates average
    activity rates per year/month and identifies a general trend.

    Args:
        about_data (dict or None): Dictionary containing data from the user's
                                   'about' page, or None. Must include 'created_utc'.
        temporal_stats (dict or None): Dictionary containing temporal activity stats,
                                       especially 'total_items_for_temporal' and
                                       'activity_by_year_utc'.

    Returns:
        dict: A dictionary with account age, average activity rates, and an
              activity trend status. Returns default "N/A" values or specific
              reasons in 'activity_trend_status' if data is insufficient.
    """
    logging.debug("      Calculating account age vs activity...")
    # Initialize results with default values
    results = { "account_created_utc": None, "account_created_formatted": "N/A",
                "account_age_days": "N/A", "total_activity_items": 0,
                "average_activity_per_year": "N/A", "average_activity_per_month": "N/A",
                "activity_trend_status": "N/A" }

    # --- Check and process account creation data ---
    if not about_data or not isinstance(about_data, dict) or "created_utc" not in about_data:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity: Missing or invalid 'about_data'.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Account Info)";
        return results

    try:
        created_ts = float(about_data["created_utc"]);
        results["account_created_utc"] = created_ts
        # Format creation timestamp using the imported helper
        results["account_created_formatted"] = format_timestamp(created_ts) if format_timestamp else "N/A"

        # Calculate age in days
        now_ts = datetime.now(timezone.utc).timestamp();
        age_seconds = now_ts - created_ts
        if age_seconds < 0:
            logging.warning(f"      {YELLOW}⚠️ Account creation timestamp ({created_ts}) is in the future? Setting age to 0.{RESET}")
            age_days = 0
        else:
            age_days = age_seconds / 86400 # Seconds per day

        results["account_age_days"] = round(age_days, 1) # Round age to 1 decimal place

    except (ValueError, TypeError) as e:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity: Invalid 'created_utc' in about_data: {e}.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (Invalid Creation Time)";
        return results

    # --- Check and process temporal activity data ---
    if not temporal_stats or not isinstance(temporal_stats, dict) or "total_items_for_temporal" not in temporal_stats:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity trends: Missing or invalid 'temporal_stats'.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Temporal Stats)";
        return results

    total_items = temporal_stats.get("total_items_for_temporal", 0);
    results["total_activity_items"] = total_items

    activity_by_year = temporal_stats.get("activity_by_year_utc", {})

    if total_items == 0:
        logging.debug("         No activity items found in temporal stats for trend analysis.")
        results["activity_trend_status"] = "No Activity Found";
        return results # Analysis stops here if no activity

    # --- Calculate average activity rates ---
    if age_days > 0:
        age_years = age_days / 365.25; # Account for leap years approximately
        results["average_activity_per_year"] = f"{total_items / age_years:.1f}" if age_years > 0 else "N/A"
        age_months = age_days / (365.25 / 12); # Approximate number of months
        results["average_activity_per_month"] = f"{total_items / age_months:.1f}" if age_months > 0 else "N/A"
    else:
        logging.debug("         Account age is zero or negative, cannot calculate average rates.")

    # --- Determine activity trend ---
    # Requires activity data broken down by year
    if not activity_by_year:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity trends: Missing 'activity_by_year_utc' in temporal_stats.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Yearly Breakdown)";
        return results

    sorted_years = sorted(activity_by_year.keys())

    if len(sorted_years) < 2:
        # Need activity in at least two different years to determine a trend
        results["activity_trend_status"] = "Insufficient Data (Less than 2 years of activity)"
    else:
        # Split the activity span into two halves and compare rates
        first_year = sorted_years[0];
        last_year = sorted_years[-1]
        total_years_span = last_year - first_year + 1 # Total number of calendar years with activity

        if total_years_span < 2:
             # This check is redundant with the len(sorted_years) check but safer
             results["activity_trend_status"] = "Insufficient Data (Activity Span < 2 Years)"
        else:
            # Determine the midpoint year to split the data
            mid_point_year = first_year + total_years_span // 2

            activity_first_half = sum(count for year, count in activity_by_year.items() if year < mid_point_year)
            activity_second_half = sum(count for year, count in activity_by_year.items() if year >= mid_point_year)

            # Calculate the number of years covered by each half
            # This needs careful handling for fractional years or single years
            num_years_first = max(0, mid_point_year - first_year)
            num_years_second = max(0, last_year - mid_point_year + 1)

            # Calculate average activity rate per year for each half
            rate_first = (activity_first_half / num_years_first) if num_years_first > 0 else 0
            rate_second = (activity_second_half / num_years_second) if num_years_second > 0 else 0

            # Determine trend based on comparison (using a threshold to avoid minor fluctuations)
            if rate_second > rate_first * 1.2: # If second half rate is > 20% higher
                results["activity_trend_status"] = "Increasing"
            elif rate_first > rate_second * 1.2: # If first half rate is > 20% higher
                results["activity_trend_status"] = "Decreasing"
            elif rate_first == 0 and rate_second == 0:
                 results["activity_trend_status"] = "No Activity Found" # Should be caught earlier, but safety check
            else:
                # Otherwise, consider it stable or fluctuating within a narrow range
                results["activity_trend_status"] = "Stable / Fluctuating"

    logging.debug(f"         Age vs Activity calculated: Age={results['account_age_days']}d, Total Items={total_items}, Trend={results['activity_trend_status']}")
    return results


def _calculate_crosspost_stats(data):
    """
    Count the number of posts that were originally posted in another
    subreddit and then crossposted by the user. Identifies the most
    common source subreddits for these crossposts.

    Args:
        data (dict): A dictionary containing filtered Reddit items.
                     Specifically looks at 't3' (posts).

    Returns:
        dict: A dictionary with crosspost counts, percentage, and top
              source subreddits.
    """
    logging.debug("      Calculating crosspost stats...")
    crosspost_count, analyzed_posts = 0, 0;
    source_sub_counter = Counter() # Counter for source subreddits

    posts_data = data.get("t3", {}); # Get only post data
    total_posts = len(posts_data) # Total posts available in input data

    if total_posts == 0:
        logging.debug("         No posts found in filtered data to analyze for crossposts.")
        return { "total_posts_analyzed": 0, "crosspost_count": 0,
                 "crosspost_percentage": "0.0%", "source_subreddits": {} }

    # Iterate through posts
    for item_id, item in posts_data.items():
        analyzed_posts += 1 # Count post as analyzed
        try:
            item_data = item.get("data", {});
            # Crossposts have a 'crosspost_parent_list' key
            crosspost_parent_list = item_data.get("crosspost_parent_list")

            if isinstance(crosspost_parent_list, list) and len(crosspost_parent_list) > 0:
                crosspost_count += 1; # Increment crosspost count
                parent_data = crosspost_parent_list[0] # Get data for the original post (usually the first in the list)

                if isinstance(parent_data, dict):
                    source_sub = parent_data.get("subreddit")
                    if source_sub and isinstance(source_sub, str):
                        source_sub_counter[source_sub] += 1 # Count the source subreddit
                    else:
                        logging.debug(f"         Crosspost {item_id} parent data missing 'subreddit' key.")
                        source_sub_counter["_UnknownSource"] += 1 # Track unknown sources
                else:
                    logging.debug(f"         Crosspost {item_id} parent item is not a dictionary.")
                    source_sub_counter["_InvalidParentData"] += 1 # Track invalid parent data

        except Exception as e:
            logging.warning(f"      {YELLOW}⚠️ Error processing post {item_id} for crosspost stats: {e}{RESET}"); continue

    # Calculate percentage of posts that are crossposts
    crosspost_percentage = (crosspost_count / total_posts * 100) if total_posts > 0 else 0

    results = { "total_posts_analyzed": total_posts, # Total posts considered
                "crosspost_count": crosspost_count,
                "crosspost_percentage": f"{crosspost_percentage:.1f}%", # Format percentage
                "source_subreddits": dict(source_sub_counter.most_common(10)) } # Top 10 source subs

    logging.debug(f"         Crosspost stats calculated: {crosspost_count} out of {total_posts} posts are crossposts.")
    return results


def _calculate_removal_deletion_stats(data):
    """
    Identifies posts and comments that appear to have been removed by
    moderators ([removed]) or deleted by the user ([deleted]). This is
    based on the content/author fields.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with counts and percentages for removed and
              deleted posts and comments.
    """
    logging.debug("      Calculating removal/deletion stats...")
    posts_removed, posts_deleted = 0, 0
    comments_removed, comments_deleted = 0, 0

    posts_data = data.get("t3", {});
    comments_data = data.get("t1", {})
    total_posts = len(posts_data);
    total_comments = len(comments_data)
    analyzed_posts, analyzed_comments = 0, 0 # Track how many items we actually look at

    # Process posts
    for item_id, item in posts_data.items():
        analyzed_posts += 1
        try:
            item_data = item.get("data", {});
            author = item_data.get("author");
            selftext = item_data.get("selftext") # Post body

            # Check for typical removal/deletion indicators
            if author == "[deleted]":
                posts_deleted += 1
            # 'selftext' being '[removed]' often indicates mod removal for self-posts
            elif selftext == "[removed]":
                 posts_removed += 1
            # Note: A post with [removed] selftext might also have a [deleted] author
            # if the user deleted their account AFTER mod removal. We prioritize [deleted]
            # as user action, but this is an approximation.

        except Exception as e:
            logging.warning(f"      {YELLOW}⚠️ Error processing post {item_id} for removal/deletion stats: {e}{RESET}")

    # Process comments
    for item_id, item in comments_data.items():
        analyzed_comments += 1
        try:
            item_data = item.get("data", {});
            author = item_data.get("author");
            body = item_data.get("body") # Comment body

            # Check for typical removal/deletion indicators
            if author == "[deleted]":
                comments_deleted += 1
            # 'body' being '[removed]' often indicates mod removal
            elif body == "[removed]":
                comments_removed += 1
            # Sometimes the body is "[deleted]" but the author is not. This
            # usually means the comment was deleted by the user *before*
            # their account was deleted, or an API quirk. Treat as deleted.
            elif body == "[deleted]" and author != "[deleted]":
                 comments_deleted += 1
            # Note: A comment can be both [deleted] author and [removed] body
            # if mod removed it then user deleted account. Priority given to [deleted].

        except Exception as e:
            logging.warning(f"      {YELLOW}⚠️ Error processing comment {item_id} for removal/deletion stats: {e}{RESET}")

    # Calculate percentages
    posts_removed_perc = (posts_removed / total_posts * 100) if total_posts > 0 else 0
    posts_deleted_perc = (posts_deleted / total_posts * 100) if total_posts > 0 else 0
    comments_removed_perc = (comments_removed / total_comments * 100) if total_comments > 0 else 0
    comments_deleted_perc = (comments_deleted / total_comments * 100) if total_comments > 0 else 0

    results = { "total_posts_analyzed": total_posts, # Total posts considered
                "posts_content_removed": posts_removed,
                "posts_user_deleted": posts_deleted,
                "posts_content_removed_percentage": f"{posts_removed_perc:.1f}%", # Format percentage
                "posts_user_deleted_percentage": f"{posts_deleted_perc:.1f}%", # Format percentage

                "total_comments_analyzed": total_comments, # Total comments considered
                "comments_content_removed": comments_removed,
                "comments_user_deleted": comments_deleted,
                "comments_content_removed_percentage": f"{comments_removed_perc:.1f}%", # Format percentage
                "comments_user_deleted_percentage": f"{comments_deleted_perc:.1f}%", # Format percentage
               }
    logging.debug(f"         Removal/Deletion stats calculated: Posts Removed={posts_removed}, Deleted={posts_deleted}; Comments Removed={comments_removed}, Deleted={comments_deleted}")
    return results


def _calculate_subreddit_diversity(subreddit_activity_stats):
    """
    Calculate subreddit diversity using ecological indices (Simpson's and
    Normalized Shannon Entropy). Higher values indicate activity spread across
    more subreddits more evenly. Based on counts from _calculate_subreddit_activity.

    Args:
        subreddit_activity_stats (dict or None): Dictionary returned by
                                                _calculate_subreddit_activity,
                                                containing 'posts_per_subreddit'
                                                and 'comments_per_subreddit'.

    Returns:
        dict: A dictionary with diversity index values and the number of
              subreddits active in. Returns "N/A" if data is insufficient or calculation fails.
    """
    logging.debug("      Calculating subreddit diversity...")
    # Initialize results
    results = { "num_subreddits_active_in": 0, "simpson_diversity_index": "N/A", "normalized_shannon_entropy": "N/A" }

    # Check for required input data
    if not subreddit_activity_stats or not isinstance(subreddit_activity_stats, dict):
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate subreddit diversity: Missing or invalid 'subreddit_activity_stats'.{RESET}");
        return results

    # Get activity counts per subreddit
    posts_per_sub = subreddit_activity_stats.get('posts_per_subreddit', {})
    comments_per_sub = subreddit_activity_stats.get('comments_per_subreddit', {})

    # Ensure inputs are dict-like (Counter is fine)
    if not isinstance(posts_per_sub, (dict, Counter)): posts_per_sub = {}
    if not isinstance(comments_per_sub, (dict, Counter)): comments_per_sub = {}

    # Combine post and comment activity counts per subreddit
    combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub)

    num_subreddits = len(combined_activity);
    results["num_subreddits_active_in"] = num_subreddits
    total_items = sum(combined_activity.values()) # Total number of posts + comments

    if total_items == 0:
        logging.debug("         No subreddit activity found for diversity calculation.");
        results["simpson_diversity_index"] = 0.0; # Diversity is 0 if no activity
        results["normalized_shannon_entropy"] = 0.0;
        return results

    # Diversity indices are typically 0 when activity is in only 1 category
    if num_subreddits <= 1:
        logging.debug(f"         Activity only in {num_subreddits} subreddit(s), diversity index is 0.");
        results["simpson_diversity_index"] = 0.0;
        results["normalized_shannon_entropy"] = 0.0;
        return results

    # --- Calculate Simpson's Diversity Index (1 - Sum of squared proportions) ---
    # Proportion (pi) = count in subreddit / total items
    # Index = 1 - Sum(pi^2)
    try:
        sum_sq_proportions = sum([(count / total_items) ** 2 for count in combined_activity.values()])
        simpson_diversity = 1.0 - sum_sq_proportions;
        results["simpson_diversity_index"] = f"{simpson_diversity:.3f}" # Format to 3 decimal places
    except Exception as e:
        logging.error(f"      ❌ Error calculating Simpson's diversity index: {e}");
        results["simpson_diversity_index"] = "Error"

    # --- Calculate Normalized Shannon Entropy ---
    # Shannon Entropy (H) = - Sum(pi * log2(pi))
    # Normalized H = H / log2(Number of Subreddits)
    try:
        shannon_entropy = 0.0
        for count in combined_activity.values():
             if count > 0:
                 proportion = count / total_items;
                 shannon_entropy -= proportion * math.log2(proportion) # Use log base 2

        if num_subreddits > 1:
            max_entropy = math.log2(num_subreddits); # Maximum possible entropy for this number of categories
            # Normalize entropy by dividing by the max possible entropy
            normalized_shannon = shannon_entropy / max_entropy if max_entropy > 0 else 0
            results["normalized_shannon_entropy"] = f"{normalized_shannon:.3f}" # Format to 3 decimal places
        else:
            # If only one subreddit, entropy is 0
            results["normalized_shannon_entropy"] = 0.0
    except Exception as e:
        logging.error(f"      ❌ Error calculating Shannon entropy: {e}");
        results["normalized_shannon_entropy"] = "Error"

    logging.debug(f"         Subreddit diversity calculated: Simpson={results['simpson_diversity_index']}, Shannon={results['normalized_shannon_entropy']}, NumSubs={num_subreddits}")
    return results


def _calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=[2, 3], top_k=20):
    """
    Calculate the frequency of N-grams (sequences of N words) in the text
    content, excluding common stop words. Returns the top K most frequent
    N-grams for each specified N value. Reads content from the provided
    CSV file paths.

    Args:
        posts_csv_path (str): Path to the CSV file containing filtered post data.
        comments_csv_path (str): Path to the CSV file containing filtered comment data.
        n_values (list of int): A list of N values for which to calculate N-grams
                                 (e.g., [2] for bigrams, [2, 3] for bigrams and trigrams).
                                 Must be > 1.
        top_k (int): The number of top N-grams to return for each N value.

    Returns:
        dict: A dictionary where keys are N-gram types ('bigrams', 'trigrams', etc.)
              and values are dictionaries mapping the top K N-grams to their counts.
              Returns empty dictionary if no valid N values or no text processed.
    """
    # Filter n_values to include only valid integers greater than 1
    valid_n_values = [n for n in n_values if isinstance(n, int) and n > 1]

    if not valid_n_values:
        logging.warning(f"      {YELLOW}⚠️ No valid n values provided for n-gram calculation ({n_values}). Skipping.{RESET}");
        return {} # Return empty results if no valid N is requested

    logging.debug(f"      Calculating n-gram frequency (n={valid_n_values}, top {top_k}, from filtered CSVs)...")

    # Initialize a Counter for each valid N value
    ngram_counters = {n: Counter() for n in valid_n_values};
    files = []
    # List of files and the columns within them that contain text
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    # Check if any files are available
    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate n-gram frequency.")
        # Return a dictionary with keys for the requested n-grams, but empty counts
        return { {2: 'bigrams', 3: 'trigrams'}.get(n, f'{n}grams'): {} for n in valid_n_values }

    total_rows_processed = 0
    # Process each specified CSV file
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for n-grams...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    # Combine text from specified columns
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()

                    # Process text if it's not empty or a placeholder
                    if full_text and full_text.lower() not in ['[deleted]', '[removed]', '[no body]', '[no title]']:
                        # Clean text and remove stop words before generating n-grams
                        cleaned_words = clean_text(full_text, remove_stopwords=True)

                        # Generate n-grams for each requested N value
                        for n in valid_n_values:
                            # _generate_ngrams yields tuples of words
                            for ngram_tuple in _generate_ngrams(cleaned_words, n):
                                # Join words with spaces to form the n-gram string key
                                ngram_counters[n][" ".join(ngram_tuple)] += 1

                    total_rows_processed += 1 # Count all rows attempted
        except Exception as e:
            logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for n-gram freq: {e}")

    logging.debug(f"         Finished n-gram freq calculation from {total_rows_processed} total rows read.")

    # Format results: create a dictionary mapping descriptive names ('bigrams')
    # to the top K n-grams for each N.
    results = {}
    safe_top_k = max(0, top_k) # Ensure top_k is not negative
    for n in valid_n_values:
        # Determine a descriptive name for the N-gram type
        key_name = {2: 'bigrams', 3: 'trigrams'}.get(n, f'{n}grams')
        results[key_name] = dict(ngram_counters[n].most_common(safe_top_k)) # Get top K and convert to dict

    return results


def _calculate_activity_burstiness(data):
    """
    Measures the "burstiness" of activity by analyzing the time intervals
    between consecutive posts and comments. Calculates statistical measures
    (mean, median, standard deviation, min, max) of these time intervals.
    A higher standard deviation relative to the mean can indicate burstier activity.

    Args:
        data (dict): A dictionary containing filtered Reddit items.

    Returns:
        dict: A dictionary with statistics on the time intervals between items,
              in seconds and formatted strings. Returns "N/A" if insufficient data.
    """
    logging.debug("      Calculating activity burstiness...")
    # Initialize results with default "N/A" values
    results = { "mean_interval_s": "N/A", "mean_interval_formatted": "N/A",
                "median_interval_s": "N/A", "median_interval_formatted": "N/A",
                "stdev_interval_s": "N/A", "stdev_interval_formatted": "N/A",
                "min_interval_s": "N/A", "min_interval_formatted": "N/A",
                "max_interval_s": "N/A", "max_interval_formatted": "N/A",
                "num_intervals_analyzed": 0 }

    all_timestamps = [] # List to collect all creation timestamps

    # Collect timestamps from all filtered items (posts and comments)
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                # Get creation timestamp, ignoring edits
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0: # Only add valid, positive timestamps
                    all_timestamps.append(ts)
                elif ts == 0:
                    logging.debug(f"         Item {item_id} skipped for burstiness due to zero timestamp.")
            except Exception as e:
                logging.warning(f"      {YELLOW}⚠️ Error getting timestamp for burstiness ({kind} {item_id}): {e}{RESET}")

    # Need at least two timestamps to calculate an interval
    if len(all_timestamps) < 2:
        logging.debug(f"         Insufficient data points ({len(all_timestamps)}) for burstiness calculation (need at least 2).");
        return results # Return defaults if not enough data

    # Sort timestamps chronologically
    all_timestamps.sort();

    # Calculate the time difference (delta) between consecutive events
    deltas = [all_timestamps[i] - all_timestamps[i-1] for i in range(1, len(all_timestamps))]

    # Filter out any zero or negative deltas (shouldn't happen with valid, sorted timestamps, but as a safeguard)
    deltas = [d for d in deltas if d > 0]

    if not deltas:
        logging.debug("         No valid positive time intervals found after filtering.")
        return results # Return defaults if no valid intervals calculated

    results["num_intervals_analyzed"] = len(deltas) # Report how many intervals were calculated

    # Calculate statistical measures for the intervals
    try:
        # Mean (Average) interval
        mean_delta = statistics.mean(deltas);
        results["mean_interval_s"] = round(mean_delta, 1);
        results["mean_interval_formatted"] = _format_timedelta(mean_delta)

        # Median (Middle) interval
        median_delta = statistics.median(deltas);
        results["median_interval_s"] = round(median_delta, 1);
        results["median_interval_formatted"] = _format_timedelta(median_delta)

        # Standard Deviation (Measure of spread/variability). Requires at least 2 data points.
        if len(deltas) > 1:
            stdev_delta = statistics.stdev(deltas);
            results["stdev_interval_s"] = round(stdev_delta, 1);
            results["stdev_interval_formatted"] = _format_timedelta(stdev_delta)
        else:
            # Stdev is 0 if there's only one interval (or if all intervals were identical, though unlikely after filtering >0)
            results["stdev_interval_s"] = 0.0;
            results["stdev_interval_formatted"] = _format_timedelta(0.0) # Format 0 delay

        # Minimum and Maximum intervals
        min_delta = min(deltas);
        results["min_interval_s"] = round(min_delta, 1);
        results["min_interval_formatted"] = _format_timedelta(min_delta)

        max_delta = max(deltas);
        results["max_interval_s"] = round(max_delta, 1);
        results["max_interval_formatted"] = _format_timedelta(max_delta)

        logging.debug(f"         Activity burstiness calculated: Mean Interval={results['mean_interval_formatted']}, Stdev Interval={results['stdev_interval_formatted']}")

    except statistics.StatisticsError as stat_err:
         # This might happen for stdev if len(deltas) < 2, though checked above.
         logging.error(f"      ❌ Error calculating burstiness statistics: {stat_err}")
         # Set relevant fields to Error state if calculation failed
         # Check if mean was already set successfully, otherwise mark all as error
         if "mean_interval_s" not in results or results["mean_interval_s"] == "N/A":
             for key in results:
                 if "num_intervals_analyzed" not in key: results[key] = "Error"
         else: # If mean was ok, just mark the specific failed ones (like stdev)
             results["stdev_interval_s"] = "Error"; results["stdev_interval_formatted"] = "Error"

    except Exception as e:
        # Catch any other unexpected errors during calculation
        logging.error(f"      ❌ Unexpected error calculating burstiness statistics: {e}", exc_info=True)
        # Mark all relevant fields as error on unexpected exception
        for key in results:
             if "num_intervals_analyzed" not in key: results[key] = "Error"
        results["num_intervals_analyzed"] = len(deltas) # Keep the count if it was obtained

    return results

# --- NEW: Sentiment Arc Calculation ---
def _calculate_sentiment_arc(filtered_data, time_window='monthly'):
    """
    Calculates the average VADER sentiment compound score for posts and comments
    over defined time windows (e.g., monthly or yearly). Provides a time-series
    view of sentiment. Requires the vaderSentiment library.

    Args:
        filtered_data (dict): The dictionary containing filtered Reddit items.
                              This function reads text and creation timestamps
                              directly from this structure.
        time_window (str): The time window to aggregate sentiment over.
                           Accepted values: 'monthly', 'yearly'. Defaults to 'monthly'.

    Returns:
        dict: A dictionary containing the time-series sentiment data mapping
              window keys (e.g., "YYYY-MM", "YYYY") to average compound scores.
              Also includes analysis status and reason if skipped.
    """
    logging.debug(f"      Calculating sentiment arc ({time_window})...")
    global vader_available # Use the flag set during import

    # Initialize results dictionary
    results = {"sentiment_arc_data": {},
               "analysis_performed": False, # Flag indicating if analysis ran
               "reason": "", # Reason if skipped
               "window_type": time_window} # Store the requested window type

    # --- Check VADER dependency ---
    if not vader_available:
        results["reason"] = "VADER library not installed or import failed"
        logging.warning(f"      {YELLOW}⚠️ Skipping sentiment arc: VADER library not available.{RESET}")
        return results

    # Double check if the Analyzer class is available
    if not SentimentIntensityAnalyzer:
        results["reason"] = "VADER Analyzer class unavailable"
        if vader_available:
            logging.warning(f"{YELLOW}⚠️ Skipping sentiment arc: VADER lib import seemed ok, but class unavailable.{RESET}")
            vader_available = False # Update flag
        return results

    # Initialize the VADER analyzer
    try:
        analyzer = SentimentIntensityAnalyzer()
        logging.debug("         VADER SentimentIntensityAnalyzer initialized for arc.")
    except Exception as e:
        logging.error(f"    ❌ Failed to initialize VADER SentimentIntensityAnalyzer for arc: {e}", exc_info=True)
        vader_available = False # Mark as unavailable if init fails
        results["reason"] = f"VADER Analyzer initialization failed: {e}";
        return results

    # Use defaultdict to easily group scores by time window
    sentiment_by_window = defaultdict(list)
    items_processed = 0
    items_with_errors = 0

    # --- Process items from the filtered data dictionary ---
    # Iterate through both posts and comments
    for kind in ["t3", "t1"]:
        for item_id, item in filtered_data.get(kind, {}).items():
            try:
                item_data = item.get("data", {})
                # Use CREATION time for consistency with temporal stats and arcs
                ts = _get_timestamp(item_data, use_edited=False)
                if not (ts and ts > 0):
                    # Skip items with invalid or zero timestamps
                    # logging.debug(f"         Skipping item {item_id} for sentiment arc due to zero/invalid timestamp.")
                    continue

                # Extract text content based on item type
                text = ""
                if kind == "t1": # Comment
                    text = item_data.get('body', '')
                elif kind == "t3": # Post
                    title = item_data.get('title', '')
                    selftext = item_data.get('selftext', '')
                    # Combine title and selftext for post analysis
                    text = f"{title} {selftext}".strip()

                # Perform basic cleaning and skip empty/placeholder text
                text = text.replace('<br>', ' ').strip()
                if not text or text.lower() in ('[deleted]', '[removed]', '[no body]', '[no title]'):
                    # logging.debug(f"         Skipping item {item_id} for sentiment arc: empty or placeholder text.")
                    continue # Skip items with no meaningful text

                # Calculate sentiment using VADER
                vs = analyzer.polarity_scores(text)
                compound_score = vs['compound'] # We use the compound score for the overall sentiment

                # Determine the time window key based on the item's timestamp
                dt = datetime.fromtimestamp(ts, timezone.utc)
                if time_window == 'monthly':
                    window_key = dt.strftime('%Y-%m') # e.g., "2023-10"
                elif time_window == 'yearly':
                    window_key = dt.strftime('%Y') # e.g., "2023"
                else:
                    # Handle invalid time_window input, default to monthly
                    logging.warning(f"      ⚠️ Invalid time window '{time_window}' for sentiment arc, defaulting to 'monthly'.")
                    window_key = dt.strftime('%Y-%m')
                    time_window = 'monthly' # Update the window_type stored in results

                # Add the compound score to the list for this time window
                sentiment_by_window[window_key].append(compound_score)
                items_processed += 1

            except Exception as e:
                # Log any unexpected errors during item processing for sentiment
                logging.warning(f"      ⚠️ Error processing item {item_id} for sentiment arc: {e}")
                items_with_errors += 1
                continue # Continue processing other items

    # --- Aggregate scores and calculate average per window ---
    if items_processed == 0:
        # If no items had valid text and timestamps for analysis
        results["reason"] = "No valid items found in filtered data for analysis"
        logging.warning(f"      ⚠️ No items processed for sentiment arc (Errors: {items_with_errors}).")
        return results

    avg_sentiment_arc = {}
    # Sort the window keys chronologically (e.g., "2023-01", "2023-02", ...)
    sorted_windows = sorted(sentiment_by_window.keys())

    # Calculate the average score for each time window
    for window in sorted_windows:
        scores = sentiment_by_window[window]
        if scores: # This list should not be empty due to defaultdict usage, but safe check
            avg_score = sum(scores) / len(scores)
            avg_sentiment_arc[window] = round(avg_score, 3) # Store average score rounded to 3 decimal places
        # If a window somehow had no scores (e.g., filter resulted in no items for a month),
        # it won't appear in the final avg_sentiment_arc dict, which is desired.

    # --- Final results ---
    results["sentiment_arc_data"] = avg_sentiment_arc
    results["analysis_performed"] = True # Mark as successfully performed
    results["window_type"] = time_window # Store the actual window used in the output
    logging.debug(f"         Sentiment arc calculated for {len(avg_sentiment_arc)} windows ({items_processed} items processed, {items_with_errors} errors encountered).")

    return results


# --- NEW: Question Ratio Calculation ---
def _calculate_question_ratio(posts_csv_path, comments_csv_path):
    """
    Calculates the percentage of posts and comments containing at least one
    question mark character ('?'). Uses Pandas for efficient processing of CSV files.

    Args:
        posts_csv_path (str): Path to the CSV file containing filtered post data.
        comments_csv_path (str): Path to the CSV file containing filtered comment data.

    Returns:
        dict: A dictionary with counts of items containing questions, total items
              analyzed, the calculated ratio as a percentage, and analysis status/reason.
    """
    logging.debug("      Calculating question mark presence ratio...")
    global pandas_available, pd # Access the global flags and imported object

    # Initialize results dictionary with defaults
    results = { "total_items_analyzed": 0,
                "question_items": 0,
                "question_ratio": "N/A",
                "analysis_performed": False, # Flag indicating if analysis ran
                "reason": "" } # Reason if skipped

    # --- Check Pandas dependency ---
    if not pandas_available:
        results["reason"] = "Pandas library missing"
        logging.warning(f"      {YELLOW}⚠️ Skipping question ratio: Pandas library not available.{RESET}")
        return results

    # Double check if the pandas object is available (should be if pandas_available is True)
    if not pd:
         results["reason"] = "Pandas (pd) object is None"
         logging.warning(f"{YELLOW}⚠️ Skipping question ratio: Pandas (pd) unavailable after import check.{RESET}"); # pandas_available = False # Maybe set false if it gets here unexpectedly?
         return results

    def _contains_question_mark(text):
        """Helper function: Checks if the text contains a question mark character."""
        # Ensure input is a string and not empty after stripping whitespace
        if not isinstance(text, str) or not text.strip():
            return False
        # Simple check for the presence of '?' character
        return '?' in text

    total_items = 0 # Counter for all items processed
    question_items = 0 # Counter for items containing a question mark
    files_processed_count = 0 # Track how many CSV files were successfully read

    # --- Process Posts CSV using Pandas ---
    if posts_csv_path and os.path.exists(posts_csv_path):
        files_processed_count += 1
        logging.debug(f"         Processing {CYAN}{posts_csv_path}{RESET} for question mark ratio...")
        try:
            # Read only necessary columns ('title', 'selftext') to save memory
            # low_memory=False is recommended for potentially large CSVs
            # encoding='utf-8' is standard for Reddit data
            df_posts = pd.read_csv(posts_csv_path, usecols=['title', 'selftext'], low_memory=False, encoding='utf-8')

            # Combine 'title' and 'selftext' into a single string column for analysis.
            # Use .fillna('') to handle potential NaN values in either column, ensuring they are treated as empty strings.
            df_posts['full_text'] = df_posts['title'].fillna('').astype(str) + ' ' + df_posts['selftext'].fillna('').astype(str)

            # Apply the helper function to the combined text column.
            # This creates a boolean Series where True indicates the text contains '?'.
            question_flags = df_posts['full_text'].apply(_contains_question_mark)

            # Sum the boolean Series (True counts as 1, False as 0) to get the count of items with questions.
            question_items += question_flags.sum()

            # Add the number of rows in this DataFrame to the total items count.
            total_items += len(df_posts)

            del df_posts # Explicitly delete DataFrame to free up memory

        except FileNotFoundError:
            logging.warning(f"      ⚠️ Posts CSV not found at {CYAN}{posts_csv_path}{RESET} for question ratio.")
            # Continue to comments CSV if posts file is missing
        except Exception as e:
            # Catch any other potential errors during Pandas processing (e.g., parsing errors)
            logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv_path}{RESET} for question mark ratio: {e}")
            # Continue to comments CSV

    # --- Process Comments CSV using Pandas ---
    if comments_csv_path and os.path.exists(comments_csv_path):
        files_processed_count += 1
        logging.debug(f"         Processing {CYAN}{comments_csv_path}{RESET} for question mark ratio...")
        try:
            # Read only the 'body' column
            df_comments = pd.read_csv(comments_csv_path, usecols=['body'], low_memory=False, encoding='utf-8')

            # Ensure 'body' is treated as string and handle NaN values
            df_comments['body'] = df_comments['body'].fillna('').astype(str)

            # Apply the helper function to the 'body' column
            question_flags = df_comments['body'].apply(_contains_question_mark)

            # Sum the boolean Series to get the count of comments with questions
            question_items += question_flags.sum()

            # Add the number of rows to the total items count
            total_items += len(df_comments)

            del df_comments # Explicitly delete DataFrame

        except FileNotFoundError:
            logging.warning(f"      ⚠️ Comments CSV not found at {CYAN}{comments_csv_path}{RESET} for question ratio.")
            # Continue (no more files to process)
        except Exception as e:
            logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv_path}{RESET} for question mark ratio: {e}")
            # Continue

    # --- Final Calculations and Results ---
    if files_processed_count == 0:
        results["reason"] = "No valid CSV files found or provided"
        logging.warning("      ⚠️ No valid CSV files found for question mark ratio analysis.")
        return results # Return defaults if no files were read

    results["total_items_analyzed"] = total_items
    # Ensure question_items is an integer, as sum() on booleans results in float if empty or specific dtypes
    results["question_items"] = int(question_items)

    if total_items > 0:
        # Calculate ratio and format as a percentage with one decimal place
        ratio = (question_items / total_items)
        results["question_ratio"] = f"{ratio:.1%}"
    else:
        # If total_items is 0 (e.g., empty CSVs but files existed), ratio is 0% if no questions were found, otherwise N/A (shouldn't happen if total_items=0)
        results["question_ratio"] = "0.0%" if question_items == 0 else "N/A"
        logging.debug("         Total items analyzed for question ratio is 0.")


    results["analysis_performed"] = True # Mark as successfully performed

    logging.debug(f"         Question mark ratio calculated: {question_items} items out of {total_items} contain '?' ({results['question_ratio']}).")
    return results


# --- NEW: Mention Frequency Calculation ---
def _calculate_mention_frequency(posts_csv_path, comments_csv_path, top_n=20):
    """
    Calculates the frequency of user mentions (u/username) and subreddit
    mentions (r/subreddit) within the text content using regular expressions.
    Identifies the top N most mentioned users and subreddits. Uses Pandas
    for efficient processing of CSV files.

    Args:
        posts_csv_path (str): Path to the CSV file containing filtered post data.
        comments_csv_path (str): Path to the CSV file containing filtered comment data.
        top_n (int): The number of top mentions (users and subreddits) to return.

    Returns:
        dict: A dictionary with top N user/subreddit mentions, total mention
              instances, and analysis status/reason. Includes full counts of
              all unique mentions as 'all_user_mentions' and 'all_subreddit_mentions'.
    """
    logging.debug(f"      Calculating mention frequency (top {top_n})...")
    global pandas_available, pd # Access global flags and imported object

    # Initialize results dictionary
    results = { "top_user_mentions": {},
                "top_subreddit_mentions": {},
                "all_user_mentions": {}, # Include full counts for potential future use or debugging
                "all_subreddit_mentions": {},
                "total_user_mention_instances": 0, # Count total number of times *any* user was mentioned
                "total_subreddit_mention_instances": 0, # Count total number of times *any* subreddit was mentioned
                "analysis_performed": False, # Flag indicating if analysis ran
                "reason": "" } # Reason if skipped

    # --- Check Pandas dependency ---
    if not pandas_available:
        results["reason"] = "Pandas library missing"
        logging.warning(f"      {YELLOW}⚠️ Skipping mention frequency: Pandas library not available.{RESET}")
        return results
    if not pd: # Double check after availability flag
        results["reason"] = "Pandas (pd) object is None"
        logging.warning(f"{YELLOW}⚠️ Skipping mention frequency: Pandas (pd) unavailable after import check.{RESET}");
        return results

    # --- Define Regex patterns for mentions ---
    # user_pattern: Looks for 'u/' or 'U/' followed by 3-20 word characters (letters, numbers, _, -).
    # It uses lookbehind assertions `(?<=\s)`, `(?<=^)` etc. to ensure the mention is preceded
    # by a space, start of string, or common punctuation like '(' or '['.
    # It uses a negative lookahead `(?![A-Za-z0-9_-])` to ensure the mention is not followed by
    # characters that would be part of a longer username/word.
    # The username itself is captured in group 1: `([A-Za-z0-9_-]{3,20})`.
    user_pattern = re.compile(r'(?:(?<=\s)|(?<=^)|(?<=\()|(?<=\[))[uU]/([A-Za-z0-9_-]{3,20})(?![A-Za-z0-9_-])')

    # sub_pattern: Similar to user_pattern, but looks for 'r/' or 'R/' followed by 3-21 word characters (letters, numbers, _).
    # Reddit subreddit names have different valid characters and length limits compared to usernames.
    # The subreddit name is captured in group 1: `([A-Za-z0-9_]{3,21})`.
    sub_pattern = re.compile(r'(?:(?<=\s)|(?<=^)|(?<=\()|(?<=\[))[rR]/([A-Za-z0-9_]{3,21})(?![A-Za-z0-9_])')

    # Counters for mentions
    user_mentions = Counter()
    subreddit_mentions = Counter()
    files_processed_count = 0
    total_user_instances = 0 # Raw count of *all* 'u/' instances found
    total_subreddit_instances = 0 # Raw count of *all* 'r/' instances found

    def process_text_for_mentions(text_series, user_counter, sub_counter):
        """
        Helper function to apply regex patterns to a Pandas Series of text
        and update the mention counters.

        Args:
            text_series (pd.Series): The Pandas Series containing text data.
            user_counter (Counter): The Counter for user mentions.
            sub_counter (Counter): The Counter for subreddit mentions.
        """
        nonlocal total_user_instances, total_subreddit_instances # Allow modifying these outer variables

        # Skip if the Series is None or empty
        if text_series is None or text_series.empty:
            return

        # Ensure text is string type and handle potential NaNs by treating them as empty strings
        text_series = text_series.fillna('').astype(str)

        # Iterate through each string in the Series
        for text in text_series:
            if not text:
                continue # Skip processing if text is empty after fillna

            try:
                # Find all matches of the patterns in the text
                users = user_pattern.findall(text)
                subs = sub_pattern.findall(text)

                # Update counters with the found mentions, converting to lowercase
                # for case-insensitive counting (Reddit handles u/user and U/user same)
                user_counter.update(u.lower() for u in users)
                sub_counter.update(s.lower() for s in subs)

                # Count the total instances found
                total_user_instances += len(users)
                total_subreddit_instances += len(subs)

            except Exception as e:
                 # Log potential errors during regex matching on a specific text block
                 # Using debug level because it's less critical if one item fails
                 logging.debug(f"         ⚠️ Regex error processing text for mentions: {e}")
                 # Continue to the next item even if one fails

    # --- Process Posts CSV using Pandas ---
    if posts_csv_path and os.path.exists(posts_csv_path):
        files_processed_count += 1
        logging.debug(f"         Processing {CYAN}{posts_csv_path}{RESET} for mentions...")
        try:
            # Read only necessary text columns
            df_posts = pd.read_csv(posts_csv_path, usecols=['title', 'selftext'], low_memory=False, encoding='utf-8')

            # Process the 'title' column
            process_text_for_mentions(df_posts['title'], user_mentions, subreddit_mentions)
            # Process the 'selftext' column
            process_text_for_mentions(df_posts['selftext'], user_mentions, subreddit_mentions)

            del df_posts # Free up memory

        except FileNotFoundError:
            logging.warning(f"      ⚠️ Posts CSV not found at {CYAN}{posts_csv_path}{RESET} for mention frequency.")
            # Continue to comments CSV
        except Exception as e:
            logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv_path}{RESET} for mentions: {e}")
            # Continue

    # --- Process Comments CSV using Pandas ---
    if comments_csv_path and os.path.exists(comments_csv_path):
        files_processed_count += 1
        logging.debug(f"         Processing {CYAN}{comments_csv_path}{RESET} for mentions...")
        try:
            # Read only the 'body' column
            df_comments = pd.read_csv(comments_csv_path, usecols=['body'], low_memory=False, encoding='utf-8')

            # Process the 'body' column
            process_text_for_mentions(df_comments['body'], user_mentions, subreddit_mentions)

            del df_comments # Free up memory

        except FileNotFoundError:
            logging.warning(f"      ⚠️ Comments CSV not found at {CYAN}{comments_csv_path}{RESET} for mention frequency.")
            # Continue
        except Exception as e:
            logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv_path}{RESET} for mentions: {e}")
            # Continue

    # --- Final Results ---
    if files_processed_count == 0:
        results["reason"] = "No valid CSV files found or provided"
        logging.warning("      ⚠️ No valid CSV files found for mention frequency analysis.")
        return results # Return defaults if no files were read

    # Get the top N most common mentions
    safe_top_n = max(0, top_n) # Ensure top_n is not negative
    results["top_user_mentions"] = dict(user_mentions.most_common(safe_top_n))
    results["top_subreddit_mentions"] = dict(subreddit_mentions.most_common(safe_top_n))

    # Convert the full Counters to dictionaries to include all unique mentions
    results["all_user_mentions"] = dict(user_mentions)
    results["all_subreddit_mentions"] = dict(subreddit_mentions)

    # Store the total instances found
    results["total_user_mention_instances"] = total_user_instances
    results["total_subreddit_mention_instances"] = total_subreddit_instances

    results["analysis_performed"] = True # Mark as successfully performed

    logging.debug(f"         Mention frequency calculated: Found {len(user_mentions)} unique users mentioned ({total_user_instances} total instances), {len(subreddit_mentions)} unique subs mentioned ({total_subreddit_instances} total instances).")
    return results
