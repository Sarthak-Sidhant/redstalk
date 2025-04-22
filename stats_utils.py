import json
import csv
import logging
import os
import re
import time
import requests
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import math
import statistics # <-- NEW IMPORT for standard deviation

# --- NEW DEPENDENCY ---
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    vader_available = True
except ImportError:
    vader_available = False
    SentimentIntensityAnalyzer = None # Ensure it's None if import fails

# --- ANSI Codes (for logging ONLY) ---
# These should NOT be used in the report formatting functions (_format_report, _format_comparison_report)
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; RED = "\033[31m"; YELLOW = "\033[33m"

# Import helpers from reddit_utils
try:
    # Make sure format_timestamp and _fetch_user_about_data are imported
    from reddit_utils import get_modification_date, format_timestamp, _fetch_user_about_data
except ImportError:
    logging.critical("❌ Failed to import necessary functions from reddit_utils.py.")
    # Define dummy functions to prevent crashes later, though stats will be wrong
    def get_modification_date(entry): return 0
    def format_timestamp(ts): return "ERROR"
    def _fetch_user_about_data(user, cfg): return None


# --- Helper Functions & Constants ---
def _get_timestamp(item_data, use_edited=True):
    """Gets created or edited timestamp (UTC float) from item data."""
    if not isinstance(item_data, dict): return 0
    edited_ts = item_data.get("edited")
    created_utc = item_data.get("created_utc", 0)
    if use_edited and edited_ts and str(edited_ts).lower() != 'false':
        try:
            edited_ts_float = float(edited_ts)
            created_ts_float = float(created_utc) # Ensure created is float too
            if edited_ts_float > created_ts_float: return edited_ts_float
            else: return created_ts_float
        except (ValueError, TypeError): pass
    try: return float(created_utc)
    except (ValueError, TypeError): return 0

STOP_WORDS = set([
    # --- Full list ---
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
    'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers',
    'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
    'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
    'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if',
    'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between',
    'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out',
    'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why',
    'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
    'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't",
    'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
    "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't",
    'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't",
    'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't",
    'like', 'get', 'also', 'would', 'could', 'one', 'post', 'comment', 'people', 'subreddit', 'even', 'use',
    'go', 'make', 'see', 'know', 'think', 'time', 'really', 'say', 'well', 'thing', 'good', 'much', 'need',
    'want', 'look', 'way', 'user', 'reddit', 'www', 'https', 'http', 'com', 'org', 'net', 'edit', 'op',
    'deleted', 'removed', 'image', 'video', 'link', 'source', 'title', 'body', 'self', 'text', 'post', 'karma',
    'amp', 'gt', 'lt', "dont", "cant", "wont", "couldnt", "shouldnt", "wouldnt", "isnt", "arent", "bhai","bhi","hai","let","ha","nahi","thats","thi","ki","kya","koi","kuch","bhi","sab","sabhi","sabse","sabka","sare","saare","saaray","hi",
    'however', 'therefore', 'moreover', 'furthermore', 'besides', 'anyway', 'actually', 'basically', 'literally', 'totally',
    'simply', 'maybe', 'perhaps', 'likely', 'possibly', 'certainly', 'obviously', 'clearly', 'indeed', 'something', 'someone',
    'anything', 'anyone', 'everything', 'everyone', 'lot', 'little', 'many', 'few', 'several', 'various', 'enough', 'quite',
    'rather', 'such', 'account', 'share', 'commented', 'upvoted', 'downvoted', 'thread', 'page', 'today', 'yesterday',
    'tomorrow', 'now', 'later', 'soon', 'eventually', 'always', 'never', 'often', 'sometimes', 'rarely', 'put', 'take', 'give',
    'made', 'went', 'mera', 'meri', 'mere', 'tumhara', 'tumhari', 'tumhare', 'uska', 'uski', 'uske', 'humara', 'humari',
    'humare', 'inka', 'inki', 'inke', 'unka', 'unki', 'unke', 'aapka', 'aapki', 'aapke', 'toh', 'yaar', 'acha', 'ab', 'phir',
    'kyunki', 'lekin', 'magar', 'aur', 'bhi', 'tab', 'jab', 'agar', 'wahi', 'ussi', 'usse', 'ismein', 'usmein', 'yahan',
    'wahan', 'idhar', 'udhar', 'hona', 'karna', 'jana', 'aana', 'dena', 'lena', 'milna', 'dekhna', 'bolna', 'sunna', 'baat',
    'log', 'kaam', 'cheez', 'tarah', 'waqt', 'din', 'raat', 'shaam', 'subah', 'arre', 'oh', 'haan', 'nahin', 'bas', 'theek',
    # Added common markers to avoid counting them in frequency if clean_text is used carelessly elsewhere
    'username_mention', 'subreddit_mention',
])

def clean_text(text, remove_stopwords=True):
    """Improved text cleaning."""
    if not isinstance(text, str): return []
    text = text.lower()
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)
    text = re.sub(r'/?u/[\w_-]+', ' username_mention ', text)
    text = re.sub(r'/?r/[\w_-]+', ' subreddit_mention ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    words = text.split()
    if remove_stopwords:
        words = [word for word in words if word not in STOP_WORDS and len(word) > 1]
    else:
        words = [word for word in words if len(word) > 0]
    return words

def _format_timedelta(seconds):
    """Formats a duration in seconds into a human-readable string (s, m, h, d)."""
    if seconds < 0: return "N/A"
    if seconds < 60: return f"{seconds:.1f}s"
    elif seconds < 3600: return f"{seconds/60:.1f}m"
    elif seconds < 86400: return f"{seconds/3600:.1f}h"
    else: return f"{seconds/86400:.1f}d"


# --- Data Loading & Filtering ---

def _load_data_from_json(json_path):
    """Safely loads the full user data JSON."""
    logging.debug(f"      Attempting to load stats data source: {CYAN}{json_path}{RESET}")
    if not os.path.exists(json_path):
        logging.error(f"   ❌ Stats generation failed: JSON file not found at {CYAN}{json_path}{RESET}")
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as f: data = json.load(f)
        if not isinstance(data, dict): raise ValueError("JSON root is not a dictionary")
        if not isinstance(data.get("t1"), dict): data["t1"] = {}
        if not isinstance(data.get("t3"), dict): data["t3"] = {}
        logging.debug(f"      ✅ JSON data loaded successfully for stats.")
        return data
    except (json.JSONDecodeError, ValueError, Exception) as e:
        logging.error(f"   ❌ Stats generation failed: Error reading/parsing JSON file {CYAN}{json_path}{RESET}: {e}")
        return None

def _filter_data_by_date(data, date_filter):
    """Filters the loaded JSON data based on the date filter (using modification date)."""
    start_ts, end_ts = date_filter
    if start_ts <= 0 and end_ts == float('inf'): return data # No filter needed
    logging.debug(f"      Applying date filter (Mod Time: {start_ts} to {end_ts}) to loaded JSON data...")
    filtered_data = {"t1": {}, "t3": {}}; items_kept = 0; items_filtered = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            ts_mod = get_modification_date(item)
            if ts_mod == 0: items_filtered += 1; continue # Skip items with invalid dates
            if start_ts <= ts_mod < end_ts: filtered_data[kind][item_id] = item; items_kept += 1
            else: items_filtered += 1
    logging.info(f"      📊 Date filter applied to JSON: {items_kept} items kept, {items_filtered} items filtered out.")
    if items_kept == 0 and items_filtered > 0: logging.warning("      ⚠️ All items were filtered out by the specified date range.")
    return filtered_data

def _filter_data_by_subreddit(data, subreddit_filter):
    """Filters the loaded JSON data by subreddit (case-insensitive)."""
    if not subreddit_filter: return data # No filter needed
    sub_filter_lower = subreddit_filter.lower()
    logging.debug(f"      Applying subreddit filter (/r/{sub_filter_lower}) to loaded JSON data...")
    filtered_data = {"t1": {}, "t3": {}}; items_kept = 0; items_filtered = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                subreddit = item.get("data", {}).get("subreddit")
                if subreddit and isinstance(subreddit, str) and subreddit.lower() == sub_filter_lower:
                    filtered_data[kind][item_id] = item; items_kept += 1
                else: items_filtered += 1
            except Exception: items_filtered += 1 # Filter if error accessing data
    logging.info(f"      📊 Subreddit filter applied to JSON: {items_kept} items kept, {items_filtered} items filtered out.")
    if items_kept == 0 and items_filtered > 0: logging.warning(f"      ⚠️ All items were filtered out by the /r/{sub_filter_lower} filter.")
    return filtered_data

# --- Calculation Helpers ---
def _calculate_basic_counts(data):
    logging.debug("      Calculating basic counts...")
    return {"total_posts": len(data.get("t3", {})), "total_comments": len(data.get("t1", {}))}

def _calculate_time_range(data):
    logging.debug("      Calculating time range (based on creation time of filtered items)...")
    all_timestamps = [_get_timestamp(item.get("data",{}), use_edited=False) for kind in ["t3", "t1"] for item_id, item in data.get(kind, {}).items() if _get_timestamp(item.get("data",{}), use_edited=False) > 0]
    if not all_timestamps: return {"first_activity": None, "last_activity": None}
    min_ts, max_ts = min(all_timestamps), max(all_timestamps)
    return {"first_activity": format_timestamp(min_ts), "last_activity": format_timestamp(max_ts)}

def _calculate_subreddit_activity(data):
    logging.debug("      Calculating subreddit activity...")
    post_subs, comment_subs = Counter(), Counter(); posted_set, commented_set = set(), set()
    for item_id, item in data.get("t3", {}).items():
        try: subreddit = item.get("data", {}).get("subreddit")
        except AttributeError: subreddit = None
        if subreddit: post_subs[subreddit] += 1; posted_set.add(subreddit)
    for item_id, item in data.get("t1", {}).items():
        try: subreddit = item.get("data", {}).get("subreddit")
        except AttributeError: subreddit = None
        if subreddit: comment_subs[subreddit] += 1; commented_set.add(subreddit)
    all_active = sorted(list(posted_set.union(commented_set)), key=str.lower)
    return {"posts_per_subreddit": dict(post_subs), "comments_per_subreddit": dict(comment_subs), "unique_subs_posted": len(posted_set), "unique_subs_commented": len(commented_set), "all_active_subs": all_active}

def _calculate_text_stats(posts_csv_path, comments_csv_path):
    logging.debug("      Calculating text stats (from filtered CSVs)...")
    stats = {"total_post_words": 0, "total_comment_words": 0, "all_words": [], "num_posts_with_text": 0, "num_comments_with_text": 0}
    if posts_csv_path and os.path.exists(posts_csv_path):
        logging.debug(f"         Reading post text from {CYAN}{posts_csv_path}{RESET}")
        try:
            with open(posts_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    # Combine title and selftext, handle potential missing keys
                    title = row.get('title', '')
                    selftext = row.get('selftext', '').replace('<br>', ' ')
                    full_text = f"{title} {selftext}".strip()
                    if full_text and full_text != '[No Body]' and full_text != '[deleted]': # Avoid counting placeholder/deleted
                        words = clean_text(full_text, False); stats["total_post_words"] += len(words); stats["all_words"].extend(words); stats["num_posts_with_text"] += 1
        except Exception as e: logging.error(f"      ❌ Error reading posts CSV {CYAN}{posts_csv_path}{RESET} for stats: {e}")
    else: logging.debug(f"         Posts CSV for text stats not found or not provided: {CYAN}{posts_csv_path}{RESET}")

    if comments_csv_path and os.path.exists(comments_csv_path):
        logging.debug(f"         Reading comment text from {CYAN}{comments_csv_path}{RESET}")
        try:
            with open(comments_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    body = row.get('body', '').replace('<br>', ' ').strip()
                    if body and body != '[NO BODY]' and body != '[deleted]' and body != '[removed]': # Avoid counting placeholders/deleted/removed
                        words = clean_text(body, False); stats["total_comment_words"] += len(words); stats["all_words"].extend(words); stats["num_comments_with_text"] += 1
        except Exception as e: logging.error(f"      ❌ Error reading comments CSV {CYAN}{comments_csv_path}{RESET} for stats: {e}")
    else: logging.debug(f"         Comments CSV for text stats not found or not provided: {CYAN}{comments_csv_path}{RESET}")

    total_words = stats["total_post_words"] + stats["total_comment_words"]; unique_words = set(word for word in stats["all_words"] if len(word) > 1)
    lex_div = (len(unique_words) / total_words) if total_words > 0 else 0
    avg_p = (stats["total_post_words"] / stats["num_posts_with_text"]) if stats["num_posts_with_text"] > 0 else 0
    avg_c = (stats["total_comment_words"] / stats["num_comments_with_text"]) if stats["num_comments_with_text"] > 0 else 0
    return {"total_words": total_words, "total_post_words": stats["total_post_words"], "total_comment_words": stats["total_comment_words"], "total_unique_words": len(unique_words), "lexical_diversity": f"{lex_div:.3f}", "avg_post_word_length": f"{avg_p:.1f}", "avg_comment_word_length": f"{avg_c:.1f}"}


def _calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=50):
    logging.debug(f"      Calculating word frequency (top {top_n}, from filtered CSVs)...")
    word_counter = Counter(); files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))
    if not files: logging.warning("      ⚠️ No CSV files found to calculate word frequency."); return {"word_frequency": {}}
    total_rows_processed = 0
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for word frequency...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                     full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                     # Check if text is not just placeholders before cleaning
                     if full_text and full_text not in ['[deleted]', '[removed]', '[No Body]', '[NO BODY]']:
                        word_counter.update(clean_text(full_text, True))
                     total_rows_processed += 1
        except Exception as e: logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for word freq: {e}")
    logging.debug(f"         Finished word freq calculation from {total_rows_processed} total rows.")
    return {"word_frequency": dict(word_counter.most_common(top_n))}

def _calculate_post_types(data):
    logging.debug("      Calculating post types...")
    link_p, self_p, unknown_p = 0, 0, 0
    for item_id, item in data.get("t3", {}).items():
         try: is_self = item.get("data", {}).get("is_self");
         except Exception: unknown_p += 1; continue
         if is_self is True: self_p += 1
         elif is_self is False: link_p += 1
         else: unknown_p += 1
    if unknown_p > 0: logging.warning(f"      ⚠️ Found {unknown_p} posts with unknown type.")
    return { "link_posts": link_p, "self_posts": self_p }

def _calculate_engagement_stats(data, about_data):
    logging.debug("      Calculating engagement stats (item scores & overall karma)...")
    post_scores, comment_scores = [], []
    for item_id, item in data.get("t3", {}).items():
        try: score = item.get("data", {}).get("score"); post_scores.append(int(score))
        except (AttributeError, ValueError, TypeError, KeyError): pass
    for item_id, item in data.get("t1", {}).items():
        try: score = item.get("data", {}).get("score"); comment_scores.append(int(score))
        except (AttributeError, ValueError, TypeError, KeyError): pass
    total_post_score, total_comment_score = sum(post_scores), sum(comment_scores)
    avg_post_score = (total_post_score / len(post_scores)) if post_scores else 0
    avg_comment_score = (total_comment_score / len(comment_scores)) if comment_scores else 0
    total_link_karma, total_comment_karma_about, total_karma = "N/A", "N/A", "N/A"
    if about_data and isinstance(about_data, dict):
        logging.debug("         Using fetched 'about' data for karma.")
        total_link_karma = about_data.get("link_karma", "N/A"); total_comment_karma_about = about_data.get("comment_karma", "N/A")
        lk_int = total_link_karma if isinstance(total_link_karma, int) else None
        ck_int = total_comment_karma_about if isinstance(total_comment_karma_about, int) else None
        if lk_int is not None and ck_int is not None: total_karma = lk_int + ck_int
        elif lk_int is not None: total_karma = lk_int
        elif ck_int is not None: total_karma = ck_int
    else: logging.debug("         'About' data unavailable or invalid for total karma.")
    return { "total_item_post_score": total_post_score, "total_item_comment_score": total_comment_score,
             "avg_item_post_score": f"{avg_post_score:.1f}", "avg_item_comment_score": f"{avg_comment_score:.1f}",
             "total_link_karma": total_link_karma, "total_comment_karma": total_comment_karma_about,
             "total_combined_karma": total_karma, }

def _calculate_temporal_stats(data):
    logging.debug("      Calculating temporal stats (based on creation time of filtered items)...")
    hour_counter, weekday_counter, month_counter, year_counter = Counter(), Counter(), Counter(), Counter()
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months_map = {i: f"{i:02d}" for i in range(1, 13)}
    items_processed = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                ts = _get_timestamp(item.get("data",{}), use_edited=False) # Use creation time
                if ts > 0:
                    dt = datetime.fromtimestamp(ts, timezone.utc)
                    hour_counter[dt.hour] += 1; weekday_counter[dt.weekday()] += 1
                    month_key = (dt.year, dt.month); month_counter[month_key] += 1
                    year_counter[dt.year] += 1; items_processed += 1
            except Exception as e: logging.warning(f"      ⚠️ Error processing timestamp for temporal stats ({kind} {item_id}): {e}")
    hours_sorted = {f"{hour:02d}": hour_counter.get(hour, 0) for hour in range(24)}
    weekdays_sorted = {days[i]: weekday_counter.get(i, 0) for i in range(7)}
    months_sorted_keys = sorted(month_counter.keys())
    months_activity = {f"{yr}-{months_map[mn]}": month_counter[key] for key in months_sorted_keys for yr, mn in [key]}
    years_sorted_keys = sorted(year_counter.keys())
    years_activity = {yr: year_counter[yr] for yr in years_sorted_keys}
    logging.debug(f"      Finished temporal stats calculation ({items_processed} items).")
    return { "activity_by_hour_utc": hours_sorted, "activity_by_weekday_utc": weekdays_sorted,
             "activity_by_month_utc": months_activity, "activity_by_year_utc": years_activity,
             "total_items_for_temporal": items_processed }

def _calculate_score_stats(data, top_n=5):
    logging.debug(f"      Calculating score stats (distribution, top/bottom {top_n})...")
    post_details, comment_details = [], []
    for item_id, item in data.get("t3", {}).items():
        try: d = item.get("data", {}); s = d.get("score"); l = d.get("permalink"); t = d.get("title", "")
        except Exception: continue
        if isinstance(s, int) and l: post_details.append((s, l, t))
    for item_id, item in data.get("t1", {}).items():
        try: d = item.get("data", {}); s = d.get("score"); l = d.get("permalink"); b = d.get("body", "")
        except Exception: continue
        if isinstance(s, int) and l: comment_details.append((s, l, b[:80].replace('\n',' ') + ("..." if len(b)>80 else "")))
    post_details.sort(key=lambda x: x[0], reverse=True); comment_details.sort(key=lambda x: x[0], reverse=True)
    post_scores = [item[0] for item in post_details]; comment_scores = [item[0] for item in comment_details]
    def get_score_distribution(scores):
        n = len(scores);
        if not scores: return {"count": 0, "min": "N/A", "max": "N/A", "average": "N/A", "q1": "N/A", "median": "N/A", "q3": "N/A"}
        scores.sort(); dist = {"count": n, "min": scores[0], "max": scores[-1], "average": f"{(sum(scores) / n):.1f}"}
        if n == 1: dist["q1"], dist["median"], dist["q3"] = scores[0], scores[0], scores[0]
        elif n > 1: dist["q1"] = scores[max(0, math.ceil(n * 0.25) -1)]; dist["median"] = scores[math.ceil(n * 0.5) -1]; dist["q3"] = scores[min(n-1, math.ceil(n * 0.75) -1)] # Adjusted indices
        return dist
    return { "post_score_distribution": get_score_distribution(post_scores), "comment_score_distribution": get_score_distribution(comment_scores),
             "top_posts": post_details[:top_n], "bottom_posts": post_details[-(top_n):][::-1] if top_n > 0 else [],
             "top_comments": comment_details[:top_n], "bottom_comments": comment_details[-(top_n):][::-1] if top_n > 0 else [] }

def _calculate_award_stats(data):
    logging.debug("      Calculating award stats...")
    total_awards, items_with_awards = 0, 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try: awards = item.get("data", {}).get("total_awards_received", 0)
            except Exception: continue
            if isinstance(awards, int) and awards > 0: total_awards += awards; items_with_awards += 1
    return { "total_awards_received": total_awards, "items_with_awards": items_with_awards }

def _calculate_flair_stats(data):
    logging.debug("      Calculating flair stats...")
    user_flairs = Counter(); post_flairs = Counter(); comments_with_user_flair = 0; posts_with_link_flair = 0
    processed_comments = 0; processed_posts = 0
    for item_id, item in data.get("t1", {}).items():
        processed_comments += 1
        try: d = item.get("data", {}); sub = d.get("subreddit"); flair = d.get("author_flair_text")
        except Exception: continue
        if sub and flair: user_flairs[f"{sub}: {flair}"] += 1; comments_with_user_flair += 1
    for item_id, item in data.get("t3", {}).items():
        processed_posts += 1
        try: d = item.get("data", {}); sub = d.get("subreddit"); flair = d.get("link_flair_text")
        except Exception: continue
        if sub and flair: post_flairs[f"{sub}: {flair}"] += 1; posts_with_link_flair += 1
    logging.debug(f"         Processed {processed_comments} comments ({comments_with_user_flair} user flair), {processed_posts} posts ({posts_with_link_flair} link flair).")
    return { "user_flairs_by_sub": dict(user_flairs.most_common()), "post_flairs_by_sub": dict(post_flairs.most_common()),
             "total_comments_with_user_flair": comments_with_user_flair, "total_posts_with_link_flair": posts_with_link_flair, }

def _calculate_post_engagement(data):
    logging.debug("      Calculating post engagement (num_comments)...")
    comment_counts = []; posts_analyzed = 0; top_commented_posts = []
    for item_id, item in data.get("t3", {}).items():
        posts_analyzed += 1
        try: d = item.get("data", {}); num_comments = d.get("num_comments"); permalink = d.get("permalink"); title = d.get("title", "[No Title]")
        except Exception: continue
        if isinstance(num_comments, int) and num_comments >= 0 and permalink is not None:
             comment_counts.append(num_comments); top_commented_posts.append((num_comments, permalink, title))
        elif permalink is not None: logging.debug(f"         Post {item_id} has invalid 'num_comments': {num_comments}")
    if not comment_counts: return {"avg_comments_per_post": "0.0", "total_posts_analyzed_for_comments": posts_analyzed, "top_commented_posts": []}
    avg_comments = sum(comment_counts) / len(comment_counts); top_commented_posts.sort(key=lambda x: x[0], reverse=True)
    return { "avg_comments_per_post": f"{avg_comments:.1f}", "total_posts_analyzed_for_comments": len(comment_counts),
             "top_commented_posts": top_commented_posts[:5] }

def _calculate_editing_stats(data):
    logging.debug("      Calculating editing stats...")
    posts_edited = 0; comments_edited = 0; total_posts = len(data.get("t3", {})); total_comments = len(data.get("t1", {}))
    edit_delays_s = []; items_processed = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            items_processed +=1
            try:
                d = item.get("data", {}); created_utc = d.get("created_utc"); edited_ts_val = d.get("edited")
                if edited_ts_val and str(edited_ts_val).lower() != 'false':
                    try:
                        edited_ts = float(edited_ts_val); created_ts = float(created_utc)
                        if edited_ts > created_ts:
                            if kind == "t3": posts_edited += 1
                            else: comments_edited += 1
                            edit_delays_s.append(edited_ts - created_ts)
                    except (ValueError, TypeError): pass
            except Exception: pass
    edit_percent_posts = (posts_edited / total_posts * 100) if total_posts > 0 else 0
    edit_percent_comments = (comments_edited / total_comments * 100) if total_comments > 0 else 0
    avg_delay_s = (sum(edit_delays_s) / len(edit_delays_s)) if edit_delays_s else 0
    avg_delay_str = _format_timedelta(avg_delay_s) # Use helper

    return { "posts_edited_count": posts_edited, "comments_edited_count": comments_edited, "total_posts_analyzed_for_edits": total_posts,
             "total_comments_analyzed_for_edits": total_comments, "edit_percentage_posts": f"{edit_percent_posts:.1f}%",
             "edit_percentage_comments": f"{edit_percent_comments:.1f}%", "average_edit_delay_seconds": round(avg_delay_s, 1),
             "average_edit_delay_formatted": avg_delay_str }

def _calculate_sentiment_ratio(posts_csv_path, comments_csv_path):
    logging.debug("      Calculating sentiment ratio (VADER, from filtered CSVs)...")
    global vader_available
    if not vader_available: return {"sentiment_analysis_skipped": True, "reason": "Library not installed or failed to initialize"}
    if not SentimentIntensityAnalyzer: # Check if class loaded
         if vader_available: logging.warning(" VADER lib import seemed ok, but class unavailable. Skipping."); vader_available = False
         return {"sentiment_analysis_skipped": True, "reason": "Library failed initialization"}
    try:
        analyzer = SentimentIntensityAnalyzer()
    except Exception as e:
        logging.error(f"    ❌ Failed to initialize VADER SentimentIntensityAnalyzer: {e}", exc_info=True)
        vader_available = False # Prevent further attempts
        return {"sentiment_analysis_skipped": True, "reason": "VADER Analyzer initialization failed"}

    pos_count, neg_count, neu_count = 0, 0, 0; total_analyzed = 0; sentiment_scores = []
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))
    if not files: logging.warning("      ⚠️ No CSV files found to calculate sentiment."); return {"positive_count": 0, "negative_count": 0, "neutral_count": 0, "total_items_sentiment_analyzed": 0, "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}
    for file_path, cols in files:
        logging.debug(f"         Analyzing sentiment in {CYAN}{file_path}{RESET}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    if full_text and full_text not in ['[No Body]', '[NO BODY]', '[deleted]', '[removed]', '[NO TITLE]']:
                        try: vs = analyzer.polarity_scores(full_text); sentiment_scores.append(vs['compound'])
                        except Exception as vader_err: logging.warning(f" VADER error row {i+1}: {vader_err}"); continue
                        if vs['compound'] >= 0.05: pos_count += 1
                        elif vs['compound'] <= -0.05: neg_count += 1
                        else: neu_count += 1
                        total_analyzed += 1
        except Exception as e: logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for sentiment: {e}")
    if total_analyzed == 0: return {"positive_count": 0, "negative_count": 0, "neutral_count": 0, "total_items_sentiment_analyzed": 0, "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}
    pos_neg_ratio = f"{(pos_count / neg_count):.2f}:1" if neg_count > 0 else (f"{pos_count}:0" if pos_count > 0 else "N/A")
    avg_compound = sum(sentiment_scores) / total_analyzed if total_analyzed > 0 else 0
    return { "sentiment_analysis_skipped": False, "positive_count": pos_count, "negative_count": neg_count, "neutral_count": neu_count,
             "total_items_sentiment_analyzed": total_analyzed, "pos_neg_ratio": pos_neg_ratio, "avg_compound_score": f"{avg_compound:.3f}" }

# --- NEW Calculation Helpers ---

def _calculate_age_vs_activity(about_data, temporal_stats):
    """Analyzes activity trends relative to account age."""
    logging.debug("      Calculating account age vs activity...")
    results = {
        "account_created_utc": None,
        "account_created_formatted": "N/A",
        "account_age_days": "N/A",
        "total_activity_items": 0,
        "average_activity_per_year": "N/A",
        "average_activity_per_month": "N/A",
        "activity_trend_status": "N/A" # e.g., Increasing, Decreasing, Stable, Insufficient Data
    }

    if not about_data or not isinstance(about_data, dict) or "created_utc" not in about_data:
        logging.warning("      ⚠️ Cannot calculate age vs activity: Missing or invalid 'about_data'.")
        return results

    try:
        created_ts = float(about_data["created_utc"])
        results["account_created_utc"] = created_ts
        results["account_created_formatted"] = format_timestamp(created_ts)
        age_seconds = time.time() - created_ts
        age_days = age_seconds / 86400
        results["account_age_days"] = round(age_days, 1)
    except (ValueError, TypeError):
        logging.warning("      ⚠️ Cannot calculate age vs activity: Invalid 'created_utc' in about_data.")
        return results # Return early if age can't be determined

    if age_days <= 0:
        logging.warning("      ⚠️ Cannot calculate age vs activity: Account age is zero or negative.")
        return results

    if not temporal_stats or not isinstance(temporal_stats, dict) or not temporal_stats.get("total_items_for_temporal"):
        logging.warning("      ⚠️ Cannot calculate age vs activity: Missing or empty 'temporal_stats'.")
        results["activity_trend_status"] = "Insufficient Data (No Temporal)"
        return results

    total_items = temporal_stats["total_items_for_temporal"]
    results["total_activity_items"] = total_items
    activity_by_year = temporal_stats.get("activity_by_year_utc", {})

    if not activity_by_year:
        logging.warning("      ⚠️ Cannot calculate age vs activity trends: Missing 'activity_by_year_utc'.")
        results["activity_trend_status"] = "Insufficient Data (No Yearly Breakdown)"
        return results

    # Calculate averages
    num_years_with_activity = len(activity_by_year)
    # Use actual age in years for average calculation for better accuracy over longer periods
    age_years = age_days / 365.25
    if age_years > 0:
        results["average_activity_per_year"] = f"{total_items / age_years:.1f}"
    age_months = age_days / (365.25 / 12)
    if age_months > 0:
        results["average_activity_per_month"] = f"{total_items / age_months:.1f}"


    # Simple Trend Analysis (comparing first vs last period with activity)
    sorted_years = sorted(activity_by_year.keys())
    if len(sorted_years) < 2:
        results["activity_trend_status"] = "Insufficient Data (Less than 2 years)"
    else:
        first_year = sorted_years[0]
        last_year = sorted_years[-1]
        first_year_activity = activity_by_year[first_year]
        last_year_activity = activity_by_year[last_year]

        # Compare activity in the first active year vs last active year (simple indicator)
        # A more robust trend would analyze more points or use regression
        if last_year_activity > first_year_activity * 1.2: # Arbitrary threshold for 'increasing'
            results["activity_trend_status"] = "Increasing"
        elif first_year_activity > last_year_activity * 1.2: # Arbitrary threshold for 'decreasing'
             results["activity_trend_status"] = "Decreasing"
        else:
             results["activity_trend_status"] = "Stable / Fluctuating"

        # You could add more sophisticated analysis here, e.g., comparing halves, regression slope

    logging.debug(f"         Age vs Activity calculated: Age={results['account_age_days']}d, Trend={results['activity_trend_status']}")
    return results

def _calculate_crosspost_stats(data):
    """Calculates statistics about crossposting activity."""
    logging.debug("      Calculating crosspost stats...")
    crosspost_count = 0
    source_sub_counter = Counter()
    analyzed_posts = 0

    posts_data = data.get("t3", {})
    total_posts = len(posts_data)

    for item_id, item in posts_data.items():
        analyzed_posts += 1
        try:
            item_data = item.get("data", {})
            crosspost_parent_list = item_data.get("crosspost_parent_list")

            # Check if it's a crosspost by looking for the list
            if isinstance(crosspost_parent_list, list) and len(crosspost_parent_list) > 0:
                crosspost_count += 1
                # Attempt to get the source subreddit from the parent data
                parent_data = crosspost_parent_list[0] # Usually only one parent
                if isinstance(parent_data, dict):
                    source_sub = parent_data.get("subreddit")
                    if source_sub:
                        source_sub_counter[source_sub] += 1
                    else:
                         # Log if parent data exists but subreddit doesn't (unlikely)
                         logging.debug(f"         Crosspost {item_id} parent data missing 'subreddit' key.")
                         source_sub_counter["_UnknownSource"] += 1
                else:
                    # Log if parent list item isn't a dictionary (unexpected structure)
                    logging.debug(f"         Crosspost {item_id} parent item is not a dictionary.")
                    source_sub_counter["_InvalidParentData"] += 1

        except Exception as e:
            logging.warning(f"      ⚠️ Error processing post {item_id} for crosspost stats: {e}")
            continue # Skip to next post on error

    crosspost_percentage = (crosspost_count / total_posts * 100) if total_posts > 0 else 0

    results = {
        "total_posts_analyzed": total_posts,
        "crosspost_count": crosspost_count,
        "crosspost_percentage": f"{crosspost_percentage:.1f}%",
        "source_subreddits": dict(source_sub_counter.most_common(10)) # Top 10 sources
    }
    logging.debug(f"         Crosspost stats calculated: {crosspost_count}/{total_posts} crossposts.")
    return results

def _calculate_removal_deletion_stats(data):
    """Estimates the ratio of removed/deleted content."""
    logging.debug("      Calculating removal/deletion stats...")
    posts_removed = 0       # Mod/Admin/Automod/Spam removal (selftext)
    posts_deleted = 0       # User deleted (author)
    comments_removed = 0    # Mod/Admin/Automod/Spam removal (body)
    comments_deleted = 0    # User deleted (author or body placeholder)

    posts_data = data.get("t3", {})
    comments_data = data.get("t1", {})
    total_posts = len(posts_data)
    total_comments = len(comments_data)

    # Analyze Posts
    for item_id, item in posts_data.items():
        try:
            item_data = item.get("data", {})
            author = item_data.get("author")
            selftext = item_data.get("selftext")

            # More reliable: check author first for user deletion
            if author == "[deleted]":
                posts_deleted += 1
            # Check selftext for removal only if not already marked as user-deleted
            # (though a mod could remove a user-deleted post, count only once)
            elif selftext == "[removed]":
                posts_removed += 1
            # Consider edge case: post deleted by user AND removed by mod?
            # Current logic prioritizes user deletion count.

        except Exception as e:
            logging.warning(f"      ⚠️ Error processing post {item_id} for removal/deletion stats: {e}")

    # Analyze Comments
    for item_id, item in comments_data.items():
        try:
            item_data = item.get("data", {})
            author = item_data.get("author")
            body = item_data.get("body")

            # Check author first for user deletion
            if author == "[deleted]":
                comments_deleted += 1
            # Check body for mod removal OR sometimes user deletion placeholder
            elif body == "[removed]":
                comments_removed += 1
            elif body == "[deleted]":
                 # Count body='[deleted]' as user deleted if author wasn't already '[deleted]'
                 # This handles cases where only the body text changes but author remains (rare?)
                 if author != "[deleted]":
                     comments_deleted += 1
                 # else: Already counted via author, avoid double counting

        except Exception as e:
            logging.warning(f"      ⚠️ Error processing comment {item_id} for removal/deletion stats: {e}")


    # Calculate percentages
    posts_removed_perc = (posts_removed / total_posts * 100) if total_posts > 0 else 0
    posts_deleted_perc = (posts_deleted / total_posts * 100) if total_posts > 0 else 0
    comments_removed_perc = (comments_removed / total_comments * 100) if total_comments > 0 else 0
    comments_deleted_perc = (comments_deleted / total_comments * 100) if total_comments > 0 else 0

    results = {
        "total_posts_analyzed": total_posts,
        "posts_content_removed": posts_removed, # Explicitly naming content removal
        "posts_user_deleted": posts_deleted,
        "posts_content_removed_percentage": f"{posts_removed_perc:.1f}%",
        "posts_user_deleted_percentage": f"{posts_deleted_perc:.1f}%",
        "total_comments_analyzed": total_comments,
        "comments_content_removed": comments_removed,
        "comments_user_deleted": comments_deleted,
        "comments_content_removed_percentage": f"{comments_removed_perc:.1f}%",
        "comments_user_deleted_percentage": f"{comments_deleted_perc:.1f}%",
    }
    logging.debug(f"         Removal/Deletion stats calculated: P_rem={posts_removed}, P_del={posts_deleted}, C_rem={comments_removed}, C_del={comments_deleted}")
    return results


# --- NEW STATS FUNCTIONS ---

def _calculate_subreddit_diversity(subreddit_activity_stats):
    """Calculates subreddit diversity using Simpson's Index."""
    logging.debug("      Calculating subreddit diversity...")
    results = {
        "num_subreddits_active_in": 0,
        "simpson_diversity_index": "N/A", # Range 0 (low diversity) to 1 (high diversity)
        "normalized_shannon_entropy": "N/A" # Optional: Range 0 to 1
    }

    if not subreddit_activity_stats or not isinstance(subreddit_activity_stats, dict):
        logging.warning("      ⚠️ Cannot calculate subreddit diversity: Missing or invalid 'subreddit_activity_stats'.")
        return results

    posts_per_sub = subreddit_activity_stats.get('posts_per_subreddit', {})
    comments_per_sub = subreddit_activity_stats.get('comments_per_subreddit', {})
    combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub)

    num_subreddits = len(combined_activity)
    results["num_subreddits_active_in"] = num_subreddits
    total_items = sum(combined_activity.values())

    if total_items == 0 or num_subreddits <= 1:
        # If only 0 or 1 subreddit, diversity is minimal (index is 0)
        results["simpson_diversity_index"] = 0.0
        results["normalized_shannon_entropy"] = 0.0
        if num_subreddits == 0: logging.debug("         No subreddit activity found for diversity calculation.")
        else: logging.debug(f"         Activity only in 1 subreddit, diversity index is 0.")
        return results

    try:
        # Simpson's Index (D) = sum of squares of proportions
        sum_sq_proportions = sum([(count / total_items) ** 2 for count in combined_activity.values()])
        # Simpson's Diversity Index (1 - D)
        simpson_diversity = 1.0 - sum_sq_proportions
        results["simpson_diversity_index"] = f"{simpson_diversity:.3f}"

        # Optional: Normalized Shannon Entropy
        shannon_entropy = 0.0
        for count in combined_activity.values():
             if count > 0: # Avoid log(0)
                 proportion = count / total_items
                 shannon_entropy -= proportion * math.log2(proportion)

        # Normalize by max entropy (log2 of number of categories)
        if num_subreddits > 1: # Avoid log2(1) = 0 division error
            max_entropy = math.log2(num_subreddits)
            normalized_shannon = shannon_entropy / max_entropy if max_entropy > 0 else 0
            results["normalized_shannon_entropy"] = f"{normalized_shannon:.3f}"
        else: # Handles the num_subreddits=1 case explicitly
             results["normalized_shannon_entropy"] = 0.0


        logging.debug(f"         Subreddit diversity calculated: Simpson={results['simpson_diversity_index']}, Shannon={results['normalized_shannon_entropy']}, NumSubs={num_subreddits}")

    except Exception as e:
        logging.error(f"      ❌ Error calculating diversity indices: {e}")
        # Keep results as "N/A" in case of error

    return results

def _generate_ngrams(words, n):
    """Helper to generate n-grams from a list of words."""
    # Simple implementation without padding
    if len(words) < n:
        return
    for i in range(len(words) - n + 1):
        yield tuple(words[i:i+n])

def _calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=[2, 3], top_k=20):
    """Calculates frequency of n-grams (bigrams, trigrams) from text."""
    logging.debug(f"      Calculating n-gram frequency (n={n_values}, top {top_k}, from filtered CSVs)...")
    ngram_counters = {n: Counter() for n in n_values}
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate n-gram frequency.")
        return {f'{n}grams': {} for n in n_values}

    total_rows_processed = 0
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for n-grams...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    if full_text and full_text not in ['[deleted]', '[removed]', '[No Body]', '[NO BODY]']:
                        # Clean text *with* stopword removal before generating n-grams
                        # (Change remove_stopwords=False for different behavior)
                        cleaned_words = clean_text(full_text, remove_stopwords=True)
                        for n in n_values:
                            for ngram_tuple in _generate_ngrams(cleaned_words, n):
                                # Store n-gram as space-separated string for easy display
                                ngram_counters[n][" ".join(ngram_tuple)] += 1
                    total_rows_processed += 1
        except Exception as e:
            logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for n-gram freq: {e}")

    logging.debug(f"         Finished n-gram freq calculation from {total_rows_processed} total rows.")

    # Prepare results
    results = {}
    for n in n_values:
        # Define key based on n (e.g., 'bigrams', 'trigrams')
        key_name = {2: 'bigrams', 3: 'trigrams'}.get(n, f'{n}grams')
        results[key_name] = dict(ngram_counters[n].most_common(top_k))

    return results


def _calculate_activity_burstiness(data):
    """Analyzes timing between consecutive activities."""
    logging.debug("      Calculating activity burstiness...")
    results = {
        "mean_interval_s": "N/A",
        "mean_interval_formatted": "N/A",
        "median_interval_s": "N/A",
        "median_interval_formatted": "N/A",
        "stdev_interval_s": "N/A",
        "stdev_interval_formatted": "N/A",
        "min_interval_s": "N/A",
        "min_interval_formatted": "N/A",
        "max_interval_s": "N/A",
        "max_interval_formatted": "N/A",
        "num_intervals_analyzed": 0
    }

    all_timestamps = []
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                # Use CREATION time for burstiness analysis
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0:
                    all_timestamps.append(ts)
            except Exception as e:
                 logging.warning(f"      ⚠️ Error getting timestamp for burstiness ({kind} {item_id}): {e}")

    if len(all_timestamps) < 2:
        logging.debug("         Insufficient data points (< 2) for burstiness calculation.")
        return results # Not enough data to calculate intervals

    all_timestamps.sort() # Sort chronologically
    deltas = [all_timestamps[i] - all_timestamps[i-1] for i in range(1, len(all_timestamps))]

    # Filter out potential zero deltas if timestamps were identical (unlikely but possible)
    deltas = [d for d in deltas if d > 0]

    if not deltas:
         logging.debug("         No valid time intervals found after filtering.")
         return results

    results["num_intervals_analyzed"] = len(deltas)

    try:
        mean_delta = statistics.mean(deltas)
        results["mean_interval_s"] = round(mean_delta, 1)
        results["mean_interval_formatted"] = _format_timedelta(mean_delta)

        median_delta = statistics.median(deltas)
        results["median_interval_s"] = round(median_delta, 1)
        results["median_interval_formatted"] = _format_timedelta(median_delta)

        # Standard deviation requires at least 2 intervals
        if len(deltas) > 1:
            stdev_delta = statistics.stdev(deltas)
            results["stdev_interval_s"] = round(stdev_delta, 1)
            results["stdev_interval_formatted"] = _format_timedelta(stdev_delta)
        else:
             results["stdev_interval_s"] = 0.0
             results["stdev_interval_formatted"] = _format_timedelta(0.0)


        min_delta = min(deltas)
        results["min_interval_s"] = round(min_delta, 1)
        results["min_interval_formatted"] = _format_timedelta(min_delta)

        max_delta = max(deltas)
        results["max_interval_s"] = round(max_delta, 1)
        results["max_interval_formatted"] = _format_timedelta(max_delta)

        logging.debug(f"         Activity burstiness calculated: Mean={results['mean_interval_formatted']}, Stdev={results['stdev_interval_formatted']}")

    except Exception as e:
        logging.error(f"      ❌ Error calculating burstiness statistics: {e}")
        # Reset potentially partially calculated values to N/A
        for key in results:
            if "num_intervals" not in key: results[key] = "N/A"
        results["num_intervals_analyzed"] = len(deltas) # Keep interval count if possible

    return results

# --- Comparison Calculation Helpers ---
def _calculate_subreddit_overlap(subs1, subs2):
    set1 = set(subs1); set2 = set(subs2)
    intersection = set1.intersection(set2); union = set1.union(set2)
    jaccard = len(intersection) / len(union) if union else 0
    return { "shared_subreddits": sorted(list(intersection), key=str.lower), "num_shared": len(intersection), "jaccard_index": f"{jaccard:.3f}" }

def _compare_word_frequency(freq1, freq2, top_n=20):
    # Ensure freq1 and freq2 are dictionaries
    if not isinstance(freq1, dict): freq1 = {}
    if not isinstance(freq2, dict): freq2 = {}
    top_words1 = set(dict(sorted(freq1.items(), key=lambda item: item[1], reverse=True)[:top_n]).keys())
    top_words2 = set(dict(sorted(freq2.items(), key=lambda item: item[1], reverse=True)[:top_n]).keys())
    intersection = top_words1.intersection(top_words2); union = top_words1.union(top_words2)
    jaccard = len(intersection) / len(union) if union else 0
    return { "top_n_compared": top_n, "shared_top_words": sorted(list(intersection)), "num_shared_top_words": len(intersection), "jaccard_index": f"{jaccard:.3f}" }


# --- Report Formatting ---

def _format_report(stats_data, username):
    """Formats the single-user statistics into a plain Markdown report."""
    # ***** NO ANSI CODES SHOULD BE USED IN THIS FUNCTION *****
    logging.debug(f"      Formatting stats report for /u/{username}...")
    report = f"# Reddit User Statistics Report for /u/{username}\n\n"
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S'); report += f"*Report generated: {dt_now}*\n\n"
    if stats_data.get("_filter_info"):
        f_info = stats_data["_filter_info"]
        filter_desc = f"Includes activity from {f_info['start']} to {f_info['end']}"
        if f_info.get('subreddit'): filter_desc += f" in {f_info['subreddit']}"
        report += f"***Data Filter Applied:** {filter_desc} (UTC, based on modification time)*\n\n"

    # --- Section I: Overall Summary ---
    report += "## I. Overall Activity Summary\n"; report += "| Statistic             | Value         |\n"; report += "|-----------------------|---------------|\n"
    report += f"| Total Posts Analyzed  | {stats_data.get('basic_counts', {}).get('total_posts', 'N/A')} |\n"
    report += f"| Total Comments Analyzed | {stats_data.get('basic_counts', {}).get('total_comments', 'N/A')} |\n"
    report += f"| First Activity (Created) | {stats_data.get('time_range', {}).get('first_activity', 'N/A')} |\n"
    report += f"| Last Activity (Created)  | {stats_data.get('time_range', {}).get('last_activity', 'N/A')} |\n"
    # Account Age Info (Now part of age_activity_analysis)
    age_analysis = stats_data.get("age_activity_analysis", {})
    report += f"| Account Created (UTC) | {age_analysis.get('account_created_formatted', 'N/A')} |\n"
    report += f"| Account Age           | {age_analysis.get('account_age_days', 'N/A')} days |\n"
    # Karma Info
    report += f"| Total Link Karma      | **{stats_data.get('engagement', {}).get('total_link_karma', 'N/A')}** |\n" # Use MD bold
    report += f"| Total Comment Karma   | **{stats_data.get('engagement', {}).get('total_comment_karma', 'N/A')}** |\n"
    report += f"| **Total Combined Karma**| **{stats_data.get('engagement', {}).get('total_combined_karma', 'N/A')}** |\n"; report += "\n"

    # --- Section II: Content & Style Analysis ---
    report += "## II. Content & Style Analysis\n"; report += "**Text & Post Types:**\n"; report += "| Statistic                 | Value         |\n"; report += "|---------------------------|---------------|\n"
    report += f"| Total Word Count          | {stats_data.get('text_stats', {}).get('total_words', 'N/A')} |\n"; report += f"|   *(Posts)*             | {stats_data.get('text_stats', {}).get('total_post_words', 'N/A')} |\n"; report += f"|   *(Comments)*          | {stats_data.get('text_stats', {}).get('total_comment_words', 'N/A')} |\n"
    report += f"| Total Unique Words (>1 char) | {stats_data.get('text_stats', {}).get('total_unique_words', 'N/A')} |\n"; report += f"| Lexical Diversity         | {stats_data.get('text_stats', {}).get('lexical_diversity', 'N/A')} |\n"
    report += f"| Avg. Words per Post     | {stats_data.get('text_stats', {}).get('avg_post_word_length', 'N/A')} |\n"; report += f"| Avg. Words per Comment  | {stats_data.get('text_stats', {}).get('avg_comment_word_length', 'N/A')} |\n"
    report += f"| Link Posts              | {stats_data.get('post_types', {}).get('link_posts', 'N/A')} |\n"; report += f"| Self Posts              | {stats_data.get('post_types', {}).get('self_posts', 'N/A')} |\n"; report += "\n"

    # Crossposting Info
    xp_stats = stats_data.get('crosspost_stats', {})
    report += "**Crossposting:**\n"
    report += f"* Crossposts Made: {xp_stats.get('crosspost_count', 'N/A')} / {xp_stats.get('total_posts_analyzed','N/A')} posts ({xp_stats.get('crosspost_percentage', 'N/A')})\n"
    if xp_stats.get('source_subreddits'):
        report += "* Top Source Subreddits for Crossposts:\n"
        for sub, count in xp_stats['source_subreddits'].items():
            report += f"  * /r/{sub} ({count})\n"
    else:
        report += "* No crossposts found or analyzed within filter.\n"
    report += "\n"

    # Editing Habits
    edit_stats = stats_data.get("editing_stats", {}); report += "**Editing Habits:**\n"; total_p_edit = edit_stats.get('total_posts_analyzed_for_edits', 0); total_c_edit = edit_stats.get('total_comments_analyzed_for_edits', 0)
    report += f"* Posts Edited: {edit_stats.get('posts_edited_count', 'N/A')} / {total_p_edit} ({edit_stats.get('edit_percentage_posts', 'N/A')})\n"; report += f"* Comments Edited: {edit_stats.get('comments_edited_count', 'N/A')} / {total_c_edit} ({edit_stats.get('edit_percentage_comments', 'N/A')})\n"
    report += f"* Average Edit Delay: {edit_stats.get('average_edit_delay_formatted', 'N/A')}\n"; report += "\n"

    # Removal/Deletion Info
    rd_stats = stats_data.get('removal_deletion_stats', {})
    report += "**Content Removal/Deletion Estimate:**\n"
    report += f"* Posts Removed (by Mod/etc.): {rd_stats.get('posts_content_removed', 'N/A')} ({rd_stats.get('posts_content_removed_percentage', 'N/A')})\n"
    report += f"* Posts Deleted (by User): {rd_stats.get('posts_user_deleted', 'N/A')} ({rd_stats.get('posts_user_deleted_percentage', 'N/A')})\n"
    report += f"* Comments Removed (by Mod/etc.): {rd_stats.get('comments_content_removed', 'N/A')} ({rd_stats.get('comments_content_removed_percentage', 'N/A')})\n"
    report += f"* Comments Deleted (by User): {rd_stats.get('comments_user_deleted', 'N/A')} ({rd_stats.get('comments_user_deleted_percentage', 'N/A')})\n"
    report += "* *Note: Based on common markers like `[removed]`/`[deleted]`. Interpretation requires caution.*\n\n"

    # Sentiment Analysis
    sentiment = stats_data.get("sentiment_ratio", {});
    if sentiment.get("sentiment_analysis_skipped"): report += f"**Sentiment Analysis (VADER):** *Skipped ({sentiment.get('reason', 'Unknown')})*\n\n"
    else:
        report += "**Sentiment Analysis (VADER):**\n"
        report += f"* Item Counts (Pos/Neg/Neu): {sentiment.get('positive_count','N/A')} / {sentiment.get('negative_count','N/A')} / {sentiment.get('neutral_count','N/A')} (Total: {sentiment.get('total_items_sentiment_analyzed', 'N/A')})\n"
        report += f"* Positive-to-Negative Ratio: {sentiment.get('pos_neg_ratio', 'N/A')}\n"; avg_score_str = sentiment.get('avg_compound_score', 'N/A')
        report += f"* Average Compound Score: {avg_score_str} (Range: -1 to +1)\n"; report += "\n" # No color

    # --- Section III: Engagement & Recognition ---
    report += "## III. Engagement & Recognition\n"; report += "**Item Scores (Sum & Average):**\n"; report += "| Statistic               | Value         |\n"; report += "|-------------------------|---------------|\n"
    report += f"| Sum of Post Scores      | {stats_data.get('engagement', {}).get('total_item_post_score', 'N/A')} |\n"; report += f"| Sum of Comment Scores   | {stats_data.get('engagement', {}).get('total_item_comment_score', 'N/A')} |\n"
    report += f"| Avg. Post Score         | {stats_data.get('engagement', {}).get('avg_item_post_score', 'N/A')} |\n"; report += f"| Avg. Comment Score      | {stats_data.get('engagement', {}).get('avg_item_comment_score', 'N/A')} |\n"
    report += f"*Note: Based on items within filtered date range.*\n\n"
    dist_posts = stats_data.get("score_stats", {}).get("post_score_distribution", {}); dist_comments = stats_data.get("score_stats", {}).get("comment_score_distribution", {})
    if dist_posts.get("count", 0) > 0: report += f"**Post Score Distribution:** Count={dist_posts['count']}, Min={dist_posts.get('min','N/A')}, Q1={dist_posts.get('q1','N/A')}, Med={dist_posts.get('median','N/A')}, Q3={dist_posts.get('q3','N/A')}, Max={dist_posts.get('max','N/A')}, Avg={dist_posts.get('average','N/A')}\n"
    if dist_comments.get("count", 0) > 0: report += f"**Comment Score Distribution:** Count={dist_comments['count']}, Min={dist_comments.get('min','N/A')}, Q1={dist_comments.get('q1','N/A')}, Med={dist_comments.get('median','N/A')}, Q3={dist_comments.get('q3','N/A')}, Max={dist_comments.get('max','N/A')}, Avg={dist_comments.get('average','N/A')}\n"; report += "\n"
    top_n_disp = len(stats_data.get("score_stats", {}).get("top_posts", [])); report += f"**Top {top_n_disp} Scored Posts:**\n"
    for score, link, title in stats_data.get("score_stats", {}).get("top_posts", []): report += f"* `+{score}`: [{title[:60]}...](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("top_posts"): report += "* *(None within filter)*\n"
    report += f"**Top {top_n_disp} Scored Comments:**\n"
    for score, link, snippet in stats_data.get("score_stats", {}).get("top_comments", []): report += f"* `+{score}`: [{snippet}](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("top_comments"): report += "* *(None within filter)*\n"
    report += f"**Lowest {top_n_disp} Scored Posts:**\n"
    for score, link, title in stats_data.get("score_stats", {}).get("bottom_posts", []): report += f"* `{score}`: [{title[:60]}...](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("bottom_posts"): report += "* *(None within filter)*\n"
    report += f"**Lowest {top_n_disp} Scored Comments:**\n"
    for score, link, snippet in stats_data.get("score_stats", {}).get("bottom_comments", []): report += f"* `{score}`: [{snippet}](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("bottom_comments"): report += "* *(None within filter)*\n"; report += "\n"
    report += "**Awards Received:**\n"; report += f"* 🏆 Total Awards: {stats_data.get('award_stats', {}).get('total_awards_received', 'N/A')}\n"; report += f"* ✨ Items Awarded: {stats_data.get('award_stats', {}).get('items_with_awards', 'N/A')}\n"; report += "\n"
    post_engage = stats_data.get("post_engagement", {}); report += "**Post Engagement:**\n"; report += f"* Average Comments Received per Post: {post_engage.get('avg_comments_per_post', 'N/A')}\n"
    if post_engage.get("top_commented_posts"): report += f"* Top {len(post_engage['top_commented_posts'])} Most Commented Posts:\n";
    for num_comments, link, title in post_engage["top_commented_posts"]: report += f"  * `{num_comments} comments`: [{title[:60]}...](https://reddit.com{link})\n"; report += "\n"

    # --- Section IV: Subreddit Activity & Flair ---
    report += "## IV. Subreddit Activity & Flair\n"; posts_per_sub = stats_data.get('subreddit_activity', {}).get('posts_per_subreddit', {}); comments_per_sub = stats_data.get('subreddit_activity', {}).get('comments_per_subreddit', {}); all_subs = stats_data.get('subreddit_activity', {}).get('all_active_subs', [])
    sub_diversity = stats_data.get('subreddit_diversity', {}) # <-- Get diversity stats

    if not all_subs: report += "No subreddit activity found within filter.\n\n"
    else:
        report += f"Active in **{len(all_subs)}** unique subreddits ({stats_data.get('subreddit_activity', {}).get('unique_subs_posted', 0)} posted in, {stats_data.get('subreddit_activity', {}).get('unique_subs_commented', 0)} commented in).\n"
        # Add diversity score here
        report += f"* **Subreddit Diversity (Simpson Index): {sub_diversity.get('simpson_diversity_index', 'N/A')}** (0=Low, 1=High)\n\n"
        # Optional: Add Shannon Entropy if desired
        # report += f"* Subreddit Diversity (Norm. Shannon Entropy): {sub_diversity.get('normalized_shannon_entropy', 'N/A')} (0=Low, 1=High)\n\n"

        report += "| Subreddit         | Posts | Comments | Total |\n"; report += "|-------------------|-------|----------|-------|\n"
        combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub); sorted_subs_by_activity = sorted(all_subs, key=lambda sub: combined_activity.get(sub, 0), reverse=True)
        for sub in sorted_subs_by_activity: p_count = posts_per_sub.get(sub, 0); c_count = comments_per_sub.get(sub, 0); t_count = combined_activity.get(sub, 0); report += f"| /r/{sub:<15} | {p_count:<5} | {c_count:<8} | **{t_count:<5}** |\n"; report += "\n" # Use MD bold
        top_n_subs = 10
        if combined_activity: report += f"**Top {min(top_n_subs, len(combined_activity))} Most Active Subreddits (Posts + Comments):**\n";
        for i, (sub, count) in enumerate(combined_activity.most_common(top_n_subs)): report += f"* {i+1}. /r/{sub} ({count})\n"; report += "\n"

    flair_stats = stats_data.get("flair_stats", {}); report += "**Flair Usage:**\n"; user_flairs = flair_stats.get("user_flairs_by_sub", {}); post_flairs = flair_stats.get("post_flairs_by_sub", {})
    if user_flairs:
        report += f"* User Flairs Used ({flair_stats.get('total_comments_with_user_flair', 0)} instances):\n"; sorted_user_flairs = sorted(user_flairs.items(), key=lambda item: item[1], reverse=True)
        for flair_combo, count in sorted_user_flairs[:10]: sub, flair_text = flair_combo.split(': ', 1); report += f"  * `/r/{sub}`: `{flair_text}` ({count})\n"
        if len(sorted_user_flairs) > 10: report += f"  * ... and {len(sorted_user_flairs)-10} more\n"
    else: report += "* No user flairs found/analyzed within filter.\n"
    if post_flairs:
        report += f"* Post Flairs Used ({flair_stats.get('total_posts_with_link_flair', 0)} instances):\n"; sorted_post_flairs = sorted(post_flairs.items(), key=lambda item: item[1], reverse=True)
        for flair_combo, count in sorted_post_flairs[:10]: sub, flair_text = flair_combo.split(': ', 1); report += f"  * `/r/{sub}`: `{flair_text}` ({count})\n"
        if len(sorted_post_flairs) > 10: report += f"  * ... and {len(sorted_post_flairs)-10} more\n"
    else: report += "* No post flairs found/analyzed within filter.\n"; report += "\n"

    # --- Section V: Temporal Activity Patterns (UTC) ---
    report += "## V. Temporal Activity Patterns (UTC)\n"; temporal_data = stats_data.get('temporal_stats', {}); total_temporal_items = temporal_data.get("total_items_for_temporal", 0)
    if not total_temporal_items: report += "No temporal data available within filter.\n\n"
    else:
        report += f"*(Based on creation time of {total_temporal_items} items within filter)*\n\n";
        # Add Age vs Activity Trend info here
        age_analysis = stats_data.get("age_activity_analysis", {})
        report += "**Account Age vs Activity Trend:**\n"
        report += f"* Average Activity Rate (per year): {age_analysis.get('average_activity_per_year', 'N/A')}\n"
        report += f"* Average Activity Rate (per month): {age_analysis.get('average_activity_per_month', 'N/A')}\n"
        report += f"* Overall Trend Estimate: **{age_analysis.get('activity_trend_status', 'N/A')}**\n\n" # MD Bold

        # Add Burstiness Info here
        burst_stats = stats_data.get('activity_burstiness', {})
        report += "**Activity Timing (Burstiness):**\n"
        report += f"* Number of Intervals Analyzed: {burst_stats.get('num_intervals_analyzed', 'N/A')}\n"
        report += f"* Mean Interval Between Activities: {burst_stats.get('mean_interval_formatted', 'N/A')}\n"
        report += f"* Median Interval Between Activities: {burst_stats.get('median_interval_formatted', 'N/A')}\n"
        report += f"* Interval Standard Deviation: **{burst_stats.get('stdev_interval_formatted', 'N/A')}**\n"
        report += f"* Min/Max Interval: {burst_stats.get('min_interval_formatted', 'N/A')} / {burst_stats.get('max_interval_formatted', 'N/A')}\n"
        report += "* *(Higher StDev indicates more 'bursty' activity vs. regular intervals)*\n\n"


        activity_hour = temporal_data.get('activity_by_hour_utc', {}); activity_wday = temporal_data.get('activity_by_weekday_utc', {}); activity_month = temporal_data.get('activity_by_month_utc', {}); activity_year = temporal_data.get('activity_by_year_utc', {})
        if activity_hour: report += "**Activity by Hour of Day (00-23 UTC):**\n```\n"; max_val = max(activity_hour.values()) if activity_hour else 0; scale = 50 / max_val if max_val > 0 else 0;
        for hour in range(24): hour_str = f"{hour:02d}"; count = activity_hour.get(hour_str, 0); bar = '#' * int(count * scale); report += f"{hour_str}: {bar:<50} ({count})\n"; report += "```\n" # Plain bar
        if activity_wday: report += "**Activity by Day of Week (UTC):**\n```\n"; max_val = max(activity_wday.values()) if activity_wday else 0; scale = 50 / max_val if max_val > 0 else 0; days_ordered = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
        for day in days_ordered: count = activity_wday.get(day, 0); bar = '#' * int(count * scale); report += f"{day:<9}: {bar:<50} ({count})\n"; report += "```\n"
        if activity_month: report += "**Activity by Month (YYYY-MM UTC):**\n"; report += "| Month   | Count |\n|---------|-------|\n"; sorted_months = sorted(activity_month.items()); # Sort chronologically
        for month_key, count in sorted_months: report += f"| {month_key} | {count:<5} |\n"; report += "\n"
        if activity_year: report += "**Activity by Year (UTC):**\n"; report += "| Year | Count |\n|------|-------|\n"; sorted_years = sorted(activity_year.items()); # Sort chronologically
        for year, count in sorted_years: report += f"| {year} | {count:<5} |\n"; report += "\n"

    # --- Section VI: Word & Phrase Frequency --- <--- Renamed Section
    report += "## VI. Word & Phrase Frequency\n"
    word_freq = stats_data.get('word_frequency', {}).get('word_frequency', {})
    ngram_freq = stats_data.get('ngram_frequency', {})

    if not word_freq and not ngram_freq:
        report += "No word or phrase frequency data available within filter or from CSVs.\n"
    else:
        if word_freq:
            top_n_words = len(word_freq)
            report += f"**Top {top_n_words} Most Frequent Words:**\n";
            report += "*(Cleaned, stop words removed, from filtered data)*\n\n"
            report += "| Word             | Count |\n|------------------|-------|\n"
            for word, count in word_freq.items(): report += f"| {word:<16} | {count:<5} |\n"; report += "\n"
        else:
             report += "**Word Frequency:** *N/A*\n\n"

        # Add N-grams
        for n_key, n_data in ngram_freq.items():
             if n_data:
                 n = {'bigrams': 2, 'trigrams': 3}.get(n_key, '?')
                 top_n_phrases = len(n_data)
                 report += f"**Top {top_n_phrases} Most Frequent {n_key.capitalize()} ({n}-word phrases):**\n"
                 report += "*(Cleaned, stop words removed)*\n\n"
                 report += "| Phrase                   | Count |\n|--------------------------|-------|\n"
                 # Adjust column width dynamically? For now, fixed.
                 max_phrase_len = max(len(phrase) for phrase in n_data.keys()) if n_data else 24
                 col_width = max(24, max_phrase_len)
                 report += f"| {'-'*col_width}|-------|\n"
                 for phrase, count in n_data.items():
                     report += f"| {phrase:<{col_width}} | {count:<5} |\n"
                 report += "\n"
             else:
                  report += f"**{n_key.capitalize()}:** *N/A*\n\n"

    return report


def _format_comparison_report(stats1, stats2, user1, user2, comparison_results):
    """Formats the comparison statistics into a plain Markdown report."""
    # ***** NO ANSI CODES SHOULD BE USED IN THIS FUNCTION *****
    logging.debug(f"      Formatting comparison report for /u/{user1} vs /u/{user2}...")
    report = f"# Reddit User Comparison Report: /u/{user1} vs /u/{user2}\n\n"
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S'); report += f"*Report generated: {dt_now}*\n\n"
    report += f"*Note: Statistics generally based on full available history unless filtering was applied *before* comparison generation.*\n\n" # Adjusted note slightly

    # --- Section I: Basic Stats Comparison ---
    report += "## I. Overall Activity Comparison\n"
    report += "| Statistic          | /u/{:<18} | /u/{:<18} |\n".format(user1, user2)
    report += "|--------------------|-----------------------|-----------------------|\n"
    report += "| Total Posts        | {:<21} | {:<21} |\n".format(stats1.get('basic_counts', {}).get('total_posts', 'N/A'), stats2.get('basic_counts', {}).get('total_posts', 'N/A'))
    report += "| Total Comments     | {:<21} | {:<21} |\n".format(stats1.get('basic_counts', {}).get('total_comments', 'N/A'), stats2.get('basic_counts', {}).get('total_comments', 'N/A'))
    report += "| First Activity     | {:<21} | {:<21} |\n".format(stats1.get('time_range', {}).get('first_activity', 'N/A'), stats2.get('time_range', {}).get('first_activity', 'N/A'))
    report += "| Last Activity      | {:<21} | {:<21} |\n".format(stats1.get('time_range', {}).get('last_activity', 'N/A'), stats2.get('time_range', {}).get('last_activity', 'N/A'))
    # Use age from age_activity_analysis if available
    age1_str = stats1.get('age_activity_analysis', {}).get('account_age_days', 'N/A')
    age2_str = stats2.get('age_activity_analysis', {}).get('account_age_days', 'N/A')
    if age1_str != 'N/A': age1_str = f"{age1_str} days"
    if age2_str != 'N/A': age2_str = f"{age2_str} days"

    report += "| Account Age        | {:<21} | {:<21} |\n".format(age1_str, age2_str)
    report += "| Link Karma         | **{:<19}** | **{:<19}** |\n".format(stats1.get('engagement', {}).get('total_link_karma', 'N/A'), stats2.get('engagement', {}).get('total_link_karma', 'N/A')) # MD Bold
    report += "| Comment Karma      | **{:<19}** | **{:<19}** |\n".format(stats1.get('engagement', {}).get('total_comment_karma', 'N/A'), stats2.get('engagement', {}).get('total_comment_karma', 'N/A'))
    report += "| Combined Karma     | **{:<19}** | **{:<19}** |\n".format(stats1.get('engagement', {}).get('total_combined_karma', 'N/A'), stats2.get('engagement', {}).get('total_combined_karma', 'N/A'))
    report += "| Avg Post Score     | {:<21} | {:<21} |\n".format(stats1.get('engagement', {}).get('avg_item_post_score', 'N/A'), stats2.get('engagement', {}).get('avg_item_post_score', 'N/A'))
    report += "| Avg Comment Score  | {:<21} | {:<21} |\n".format(stats1.get('engagement', {}).get('avg_item_comment_score', 'N/A'), stats2.get('engagement', {}).get('avg_item_comment_score', 'N/A'))
    report += "\n"

    # --- Section II: Subreddit Activity Comparison ---
    report += "## II. Subreddit Activity Comparison\n"
    sub_overlap = comparison_results.get("subreddit_overlap", {})
    div1 = stats1.get('subreddit_diversity', {})
    div2 = stats2.get('subreddit_diversity', {})
    report += f"* **Shared Subreddits:** {sub_overlap.get('num_shared', 0)}\n"
    report += f"* **Jaccard Index (Similarity):** {sub_overlap.get('jaccard_index', 'N/A')}\n\n"
    report += "| Statistic                     | /u/{:<18} | /u/{:<18} |\n".format(user1, user2)
    report += "|-------------------------------|-----------------------|-----------------------|\n"
    report += "| Unique Subs Active            | {:<21} | {:<21} |\n".format(len(stats1.get('subreddit_activity', {}).get('all_active_subs', [])), len(stats2.get('subreddit_activity', {}).get('all_active_subs', [])))
    report += "| Unique Subs Posted            | {:<21} | {:<21} |\n".format(stats1.get('subreddit_activity', {}).get('unique_subs_posted', 'N/A'), stats2.get('subreddit_activity', {}).get('unique_subs_posted', 'N/A'))
    report += "| Unique Subs Commented         | {:<21} | {:<21} |\n".format(stats1.get('subreddit_activity', {}).get('unique_subs_commented', 'N/A'), stats2.get('subreddit_activity', {}).get('unique_subs_commented', 'N/A'))
    report += "| **Simpson Diversity Index**   | **{:<19}** | **{:<19}** |\n".format(div1.get('simpson_diversity_index', 'N/A'), div2.get('simpson_diversity_index', 'N/A')) # MD Bold
    # Optional: Add Shannon Entropy Comparison
    # report += "| Norm. Shannon Entropy         | {:<21} | {:<21} |\n".format(div1.get('normalized_shannon_entropy', 'N/A'), div2.get('normalized_shannon_entropy', 'N/A'))
    report += "\n"
    report += "* *(Diversity Index: 0=Low, 1=High)*\n\n"

    if sub_overlap.get("shared_subreddits"):
        report += "**Shared Subreddits List:**\n"; max_shared_display = 20; shared_list = sub_overlap["shared_subreddits"]
        for i, sub in enumerate(shared_list):
            if i >= max_shared_display: report += f"* ... and {len(shared_list)-max_shared_display} more\n"; break
            report += f"* /r/{sub}\n"
        report += "\n"
    report += f"**Top 5 Subreddits for /u/{user1}:**\n"; comb1 = Counter(stats1.get('subreddit_activity', {}).get('posts_per_subreddit', {})) + Counter(stats1.get('subreddit_activity', {}).get('comments_per_subreddit', {}))
    for i, (sub, count) in enumerate(comb1.most_common(5)): report += f"* {i+1}. /r/{sub} ({count})\n"
    if not comb1: report += "* *(None)*\n"
    report += f"\n**Top 5 Subreddits for /u/{user2}:**\n"; comb2 = Counter(stats2.get('subreddit_activity', {}).get('posts_per_subreddit', {})) + Counter(stats2.get('subreddit_activity', {}).get('comments_per_subreddit', {}))
    for i, (sub, count) in enumerate(comb2.most_common(5)): report += f"* {i+1}. /r/{sub} ({count})\n"
    if not comb2: report += "* *(None)*\n"; report += "\n"

    # --- Section III: Word & Phrase Frequency Comparison --- <-- RENUMBERED
    report += "## III. Word & Phrase Frequency Comparison\n";
    word_comp = comparison_results.get("word_frequency_comparison", {}); top_n_words = word_comp.get("top_n_compared", "N/A")
    report += f"**Single Words:**\n"
    report += f"* Comparison based on Top {top_n_words} frequent words (stop words removed).\n"
    report += f"* Shared Top Words: {word_comp.get('num_shared_top_words', 'N/A')}\n"
    report += f"* Jaccard Index (Word Similarity): {word_comp.get('jaccard_index', 'N/A')}\n\n"
    if word_comp.get("shared_top_words"):
        report += "*Shared Top Words List:*\n"; shared_word_list = word_comp["shared_top_words"]
        report += "`" + "`, `".join(shared_word_list) + "`\n\n" # Use backticks
    report += f"*Top 5 Words for /u/{user1}:*\n"; freq1_list = list(stats1.get('word_frequency', {}).get('word_frequency', {}).items())
    for i, (word, count) in enumerate(freq1_list[:5]): report += f"  * {i+1}. `{word}` ({count})\n"
    if not freq1_list: report += "  * *(None)*\n"
    report += f"\n*Top 5 Words for /u/{user2}:*\n"; freq2_list = list(stats2.get('word_frequency', {}).get('word_frequency', {}).items())
    for i, (word, count) in enumerate(freq2_list[:5]): report += f"  * {i+1}. `{word}` ({count})\n"
    if not freq2_list: report += "  * *(None)*\n"; report += "\n"

    # N-gram Comparison (Simple Listing)
    report += f"**Common Phrases (N-grams):**\n"
    ngrams1 = stats1.get('ngram_frequency', {})
    ngrams2 = stats2.get('ngram_frequency', {})
    # Compare Bigrams
    report += f"*Top 5 Bigrams for /u/{user1}:*\n"
    bigrams1 = list(ngrams1.get('bigrams', {}).items())
    for i, (phrase, count) in enumerate(bigrams1[:5]): report += f"  * {i+1}. `{phrase}` ({count})\n"
    if not bigrams1: report += "  * *(None)*\n"
    report += f"\n*Top 5 Bigrams for /u/{user2}:*\n"
    bigrams2 = list(ngrams2.get('bigrams', {}).items())
    for i, (phrase, count) in enumerate(bigrams2[:5]): report += f"  * {i+1}. `{phrase}` ({count})\n"
    if not bigrams2: report += "  * *(None)*\n"; report += "\n"
    # Compare Trigrams (optional, can add more Ns if calculated)
    report += f"*Top 5 Trigrams for /u/{user1}:*\n"
    trigrams1 = list(ngrams1.get('trigrams', {}).items())
    for i, (phrase, count) in enumerate(trigrams1[:5]): report += f"  * {i+1}. `{phrase}` ({count})\n"
    if not trigrams1: report += "  * *(None)*\n"
    report += f"\n*Top 5 Trigrams for /u/{user2}:*\n"
    trigrams2 = list(ngrams2.get('trigrams', {}).items())
    for i, (phrase, count) in enumerate(trigrams2[:5]): report += f"  * {i+1}. `{phrase}` ({count})\n"
    if not trigrams2: report += "  * *(None)*\n"; report += "\n"


    # --- Section IV: Sentiment Comparison ---
    sent1 = stats1.get("sentiment_ratio", {}); sent2 = stats2.get("sentiment_ratio", {})
    if sent1.get("sentiment_analysis_skipped") or sent2.get("sentiment_analysis_skipped"):
         report += "## IV. Sentiment Comparison (VADER)\n"; report += "*Sentiment analysis skipped for one or both users.*\n\n"
    else:
         report += "## IV. Sentiment Comparison (VADER)\n"; report += "| Metric                      | /u/{:<18} | /u/{:<18} |\n".format(user1, user2); report += "|-----------------------------|-----------------------|-----------------------|\n"
         report += "| Avg. Compound Score         | {:<21} | {:<21} |\n".format(sent1.get('avg_compound_score', 'N/A'), sent2.get('avg_compound_score', 'N/A'))
         report += "| Positive:Negative Ratio     | {:<21} | {:<21} |\n".format(sent1.get('pos_neg_ratio', 'N/A'), sent2.get('pos_neg_ratio', 'N/A'))
         total1 = sent1.get('total_items_sentiment_analyzed', 1); total2 = sent2.get('total_items_sentiment_analyzed', 1)
         pos_perc1 = (sent1.get('positive_count',0)*100/total1) if total1 > 0 else 0 # Check division by zero
         pos_perc2 = (sent2.get('positive_count',0)*100/total2) if total2 > 0 else 0
         neg_perc1 = (sent1.get('negative_count',0)*100/total1) if total1 > 0 else 0
         neg_perc2 = (sent2.get('negative_count',0)*100/total2) if total2 > 0 else 0
         report += "| Positive Items (%)        | {:<21.1f} | {:<21.1f} |\n".format(pos_perc1, pos_perc2)
         report += "| Negative Items (%)        | {:<21.1f} | {:<21.1f} |\n".format(neg_perc1, neg_perc2)
         report += "\n"

    # --- Section V: Content Style Comparison ---
    report += "## V. Content Style Comparison\n"
    report += "| Statistic                     | /u/{:<18} | /u/{:<18} |\n".format(user1, user2)
    report += "|-------------------------------|-----------------------|-----------------------|\n"
    # Crossposting
    xp1 = stats1.get('crosspost_stats', {}); xp2 = stats2.get('crosspost_stats', {})
    report += "| Crosspost Percentage          | {:<21} | {:<21} |\n".format(xp1.get('crosspost_percentage', 'N/A'), xp2.get('crosspost_percentage', 'N/A'))
    # Editing
    ed1 = stats1.get('editing_stats', {}); ed2 = stats2.get('editing_stats', {})
    report += "| Post Edit Percentage          | {:<21} | {:<21} |\n".format(ed1.get('edit_percentage_posts', 'N/A'), ed2.get('edit_percentage_posts', 'N/A'))
    report += "| Comment Edit Percentage       | {:<21} | {:<21} |\n".format(ed1.get('edit_percentage_comments', 'N/A'), ed2.get('edit_percentage_comments', 'N/A'))
    report += "| Avg. Edit Delay               | {:<21} | {:<21} |\n".format(ed1.get('average_edit_delay_formatted', 'N/A'), ed2.get('average_edit_delay_formatted', 'N/A'))
    # Removal/Deletion
    rd1 = stats1.get('removal_deletion_stats', {}); rd2 = stats2.get('removal_deletion_stats', {})
    report += "| Removed Post Ratio (%)      | {:<21} | {:<21} |\n".format(rd1.get('posts_content_removed_percentage', 'N/A'), rd2.get('posts_content_removed_percentage', 'N/A'))
    report += "| Deleted Post Ratio (%)      | {:<21} | {:<21} |\n".format(rd1.get('posts_user_deleted_percentage', 'N/A'), rd2.get('posts_user_deleted_percentage', 'N/A'))
    report += "| Removed Comment Ratio (%)   | {:<21} | {:<21} |\n".format(rd1.get('comments_content_removed_percentage', 'N/A'), rd2.get('comments_content_removed_percentage', 'N/A'))
    report += "| Deleted Comment Ratio (%)   | {:<21} | {:<21} |\n".format(rd1.get('comments_user_deleted_percentage', 'N/A'), rd2.get('comments_user_deleted_percentage', 'N/A'))
    report += "\n"
    report += "* *Note: Removal/Deletion stats are estimates based on common markers.*\n\n"

    # --- Section VI: Temporal Pattern Comparison --- <-- NEW SECTION
    report += "## VI. Temporal Pattern Comparison\n"
    b1 = stats1.get('activity_burstiness', {})
    b2 = stats2.get('activity_burstiness', {})
    report += "| Statistic                     | /u/{:<18} | /u/{:<18} |\n".format(user1, user2)
    report += "|-------------------------------|-----------------------|-----------------------|\n"
    report += "| Mean Interval Between Activity| {:<21} | {:<21} |\n".format(b1.get('mean_interval_formatted', 'N/A'), b2.get('mean_interval_formatted', 'N/A'))
    report += "| Median Interval Between Activity| {:<21} | {:<21} |\n".format(b1.get('median_interval_formatted', 'N/A'), b2.get('median_interval_formatted', 'N/A'))
    report += "| **StDev Interval (Burstiness)** | **{:<19}** | **{:<19}** |\n".format(b1.get('stdev_interval_formatted', 'N/A'), b2.get('stdev_interval_formatted', 'N/A')) # MD Bold
    report += "\n"
    report += "* *(Higher StDev indicates more 'bursty' activity vs. regular intervals)*\n\n"


    return report

# --- Main Stats Function ---
def generate_stats_report(json_path, about_data, posts_csv_path, comments_csv_path, username,
                          output_path, stats_json_path, date_filter, subreddit_filter,
                          top_n_words=50, top_n_items=5,
                          # N-gram specific parameters
                          ngram_n_values=[2, 3], ngram_top_k=20,
                          write_md_report=True, write_json_report=True): # Control output flags
    """
    Calculates stats (with filters), saves MD/JSON reports.
    Returns tuple: (success_boolean, stats_results_dict or None)
    """
    start_time = time.time()
    # Log start only if actually generating report
    if write_md_report or write_json_report:
        logging.info(f"   📊 Generating statistics report for /u/{BOLD}{username}{RESET}...")
        if subreddit_filter: logging.info(f"      Applying filter: /r/{subreddit_filter}")

    full_data = _load_data_from_json(json_path)
    if full_data is None: return False, None

    # --- Apply Filters ---
    filtered_data_date = _filter_data_by_date(full_data, date_filter)
    filtered_data = _filter_data_by_subreddit(filtered_data_date, subreddit_filter)

    if not filtered_data.get("t1") and not filtered_data.get("t3"):
         if write_md_report or write_json_report: # Log only if generating report
             logging.warning(f"   ⚠️ No data remains for /u/{username} after applying filters. Skipping stats calculation.")
         return False, {} # Return success=False, empty dict

    if about_data is None and (write_md_report or write_json_report):
        logging.warning("      ⚠️ User 'about' data not available (required for Account Age analysis).")

    stats_results = {}
    calculation_errors = []
    try:
        if write_md_report or write_json_report: logging.info("   ⚙️ Calculating statistics...")
        # --- Run all calculations (Order matters for dependencies) ---
        stats_results["basic_counts"] = _calculate_basic_counts(filtered_data)
        stats_results["time_range"] = _calculate_time_range(filtered_data)
        stats_results["subreddit_activity"] = _calculate_subreddit_activity(filtered_data) # Needed for Diversity
        stats_results["text_stats"] = _calculate_text_stats(posts_csv_path, comments_csv_path) # Reads CSVs
        stats_results["word_frequency"] = _calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=top_n_words) # Reads CSVs
        stats_results["engagement"] = _calculate_engagement_stats(filtered_data, about_data)
        stats_results["post_types"] = _calculate_post_types(filtered_data)
        stats_results["temporal_stats"] = _calculate_temporal_stats(filtered_data) # Needed for Age/Activity
        stats_results["score_stats"] = _calculate_score_stats(filtered_data, top_n=top_n_items)
        stats_results["award_stats"] = _calculate_award_stats(filtered_data)
        stats_results["flair_stats"] = _calculate_flair_stats(filtered_data)
        stats_results["post_engagement"] = _calculate_post_engagement(filtered_data)
        stats_results["editing_stats"] = _calculate_editing_stats(filtered_data)
        stats_results["sentiment_ratio"] = _calculate_sentiment_ratio(posts_csv_path, comments_csv_path) # Reads CSVs

        # --- NEW Calculations (Round 2) ---
        stats_results['age_activity_analysis'] = _calculate_age_vs_activity(about_data, stats_results.get('temporal_stats'))
        stats_results['crosspost_stats'] = _calculate_crosspost_stats(filtered_data)
        stats_results['removal_deletion_stats'] = _calculate_removal_deletion_stats(filtered_data)

        # --- NEW Calculations (Round 3) ---
        stats_results['subreddit_diversity'] = _calculate_subreddit_diversity(stats_results.get('subreddit_activity')) # Depends on subreddit_activity
        stats_results['ngram_frequency'] = _calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=ngram_n_values, top_k=ngram_top_k) # Reads CSVs
        stats_results['activity_burstiness'] = _calculate_activity_burstiness(filtered_data) # Uses timestamps from filtered data

        # Add internal info needed for formatting (only if writing MD report)
        if write_md_report:
            start_f, end_f = date_filter
            stats_results["_filter_info"] = {
                "start": datetime.fromtimestamp(start_f, timezone.utc).strftime('%Y-%m-%d') if start_f > 0 else 'Beginning',
                "end": datetime.fromtimestamp(end_f - 1, timezone.utc).strftime('%Y-%m-%d') if end_f != float('inf') else 'End',
                "subreddit": f"/r/{subreddit_filter}" if subreddit_filter else None
            } if start_f > 0 or end_f != float('inf') or subreddit_filter else None

        if write_md_report or write_json_report: logging.info("   ✅ All statistics calculated.")

    except Exception as e:
        logging.error(f"   ❌ Error during statistics calculation phase: {e}", exc_info=True)
        calculation_errors.append(str(e))

    # --- Format Report ---
    report_content = None; formatting_success = False
    if write_md_report:
        logging.info("   ✍️ Formatting statistics report...")
        try:
            report_content = _format_report(stats_results, username)
            formatting_success = True
            logging.debug("      Report formatting complete.")
        except Exception as e:
            logging.error(f"   ❌ Error during report formatting: {e}", exc_info=True)
            calculation_errors.append(f"Report formatting error: {e}")

    # --- Save Markdown Report ---
    md_saved = False
    if write_md_report and formatting_success and report_content and output_path:
        logging.info(f"   💾 Saving statistics report to {CYAN}{output_path}{RESET}...")
        try:
            os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f: f.write(report_content)
            md_saved = True
            logging.info(f"   ✅ Markdown report saved successfully.")
        except Exception as e:
            logging.error(f"   ❌ Error saving statistics report to {CYAN}{output_path}{RESET}: {e}", exc_info=True)
            calculation_errors.append(f"MD Save Error: {e}")
    elif write_md_report: # Log why it wasn't saved if requested
         if not output_path: logging.error("   ❌ Skipping MD report save: No output path provided.")
         elif not formatting_success: logging.error(f"   ❌ Skipping MD report save due to formatting errors.")
         else: logging.error(f"   ❌ Skipping MD report save due to missing report content.")


    # --- Save JSON Stats Data ---
    json_saved = False
    if write_json_report and stats_json_path:
        logging.info(f"   💾 Saving calculated stats data to {CYAN}{stats_json_path}{RESET}...")
        clean_stats_results = {k: v for k, v in stats_results.items() if not k.startswith('_')}
        if calculation_errors: clean_stats_results["_calculation_errors"] = calculation_errors
        try:
            os.makedirs(os.path.dirname(stats_json_path) or '.', exist_ok=True)
            with open(stats_json_path, "w", encoding="utf-8") as f_json: json.dump(clean_stats_results, f_json, indent=2)
            json_saved = True
            logging.info(f"   ✅ JSON stats data saved successfully.")
        except Exception as e:
            logging.error(f"   ❌ Error saving JSON stats data to {CYAN}{stats_json_path}{RESET}: {e}", exc_info=True)
            # Still consider the overall operation potentially successful if calculation was ok
            # but allow the caller to check the boolean flags if needed.

    elapsed_time = time.time() - start_time
    # Log duration only if reports were generated
    if write_md_report or write_json_report:
        logging.info(f"   ⏱️ Statistics generation finished in {elapsed_time:.2f}s.")

    # Final success depends on calculations AND MD saving (if requested) AND JSON saving (if requested)
    final_success = not calculation_errors \
                    and (md_saved if write_md_report and output_path else True) \
                    and (json_saved if write_json_report and stats_json_path else True)

    return final_success, stats_results

# --- Comparison Report Generation Function ---
def generate_comparison_report(stats1, stats2, user1, user2, output_path):
    """Generates and saves a comparison report based on two pre-calculated stats dicts."""
    logging.info(f"   👥 Generating comparison report for /u/{user1} vs /u/{user2}...")
    start_time = time.time()
    if not stats1 or not isinstance(stats1, dict):
        logging.error(f"   ❌ Cannot generate comparison report: Invalid or missing stats data for user {user1}.")
        return False
    if not stats2 or not isinstance(stats2, dict):
        logging.error(f"   ❌ Cannot generate comparison report: Invalid or missing stats data for user {user2}.")
        return False

    comparison_results = {}
    report_content = ""
    try:
        logging.info("      Calculating comparison metrics...")
        # Subreddit Overlap
        subs1 = stats1.get('subreddit_activity', {}).get('all_active_subs', [])
        subs2 = stats2.get('subreddit_activity', {}).get('all_active_subs', [])
        comparison_results["subreddit_overlap"] = _calculate_subreddit_overlap(subs1, subs2)

        # Word Frequency Overlap
        freq1 = stats1.get('word_frequency', {}).get('word_frequency', {})
        freq2 = stats2.get('word_frequency', {}).get('word_frequency', {})
        # Use a larger N for comparison than typically displayed to get better overlap measure
        comparison_results["word_frequency_comparison"] = _compare_word_frequency(freq1, freq2, top_n=150)

        # N-gram comparison is handled directly in formatting by listing top N

        logging.info("   ✍️ Formatting comparison report...")
        report_content = _format_comparison_report(stats1, stats2, user1, user2, comparison_results)

        logging.info(f"   💾 Saving comparison report to {CYAN}{output_path}{RESET}...")
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f: f.write(report_content)

        elapsed_time = time.time() - start_time
        logging.info(f"   ✅ Comparison report saved successfully ({elapsed_time:.2f}s).")
        return True

    except Exception as e:
        logging.error(f"   ❌ Error during comparison report generation: {e}", exc_info=True)
        return False