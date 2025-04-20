# stats_utils.py
import json
import csv
import logging
import os
import re
import time                 # Added for timing stats generation
import requests             # Added for fetching about.json
from collections import Counter, defaultdict
from datetime import datetime, timezone
import math

# --- Helper Functions & Constants ---

# Added requests dependency, ensure it's handled if used standalone
try:
    import requests
except ImportError:
    logging.warning("The 'requests' library is needed for fetching user 'about' data in stats_utils.")
    requests = None # Set to None if not available

def _get_timestamp(item_data, use_edited=True):
    # ... (implementation remains the same) ...
    if not isinstance(item_data, dict): return 0
    edited_ts = item_data.get("edited")
    if use_edited and edited_ts and edited_ts is not False:
        try: return float(edited_ts)
        except (ValueError, TypeError): pass
    return float(item_data.get("created_utc", 0))

STOP_WORDS = set([
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
    # Domain specific additions
    'like', 'get', 'also', 'would', 'could', 'one', 'post', 'comment', 'people', 'subreddit', 'even', 'use',
    'go', 'make', 'see', 'know', 'think', 'time', 'really', 'say', 'well', 'thing', 'good', 'much', 'need',
    'want', 'look', 'way', 'user', 'reddit', 'www', 'https', 'http', 'com', 'org', 'net', 'edit', 'op',
    'deleted', 'removed', 'image', 'video', 'link', 'source', 'title', 'body', 'self', 'text', 'post', 'karma',
    'amp', 'gt', 'lt', # Common reddit/html artifacts
])

def clean_text(text, remove_stopwords=True):
    """Improved text cleaning."""
    if not isinstance(text, str): return []
    text = text.lower()
    # Remove URLs more robustly
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)
    # Remove /u/ and /r/ links specifically
    text = re.sub(r'/?u/[\w_-]+', ' username_mention ', text) # Replace with placeholder
    text = re.sub(r'/?r/[\w_-]+', ' subreddit_mention ', text) # Replace with placeholder
    # Remove punctuation and numbers, keep only words and spaces
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    words = text.split()
    if remove_stopwords:
        words = [word for word in words if word not in STOP_WORDS and len(word) > 1]
    else:
        words = [word for word in words if len(word) > 0] # Just remove empty strings
    return words

# --- Data Loading ---

def _load_data_from_json(json_path):
    """Safely loads the full user data JSON."""
    # ... (implementation remains the same) ...
    if not os.path.exists(json_path):
        logging.error(f"Stats generation failed: JSON file not found at {json_path}")
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as f: data = json.load(f)
        if not isinstance(data, dict): raise ValueError("JSON root is not a dictionary")
        if "t1" not in data: data["t1"] = {} # Ensure keys exist
        if "t3" not in data: data["t3"] = {}
        return data
    except (json.JSONDecodeError, ValueError, Exception) as e:
        logging.error(f"Stats generation failed: Error reading/parsing JSON file {json_path}: {e}")
        return None

def _fetch_user_about_data(username, config):
    """Fetches data from the user's 'about.json' endpoint."""
    if not requests: # Check if requests library was imported
        logging.error("Cannot fetch 'about' data: 'requests' library not installed.")
        return None

    url = f"https://www.reddit.com/user/{username}/about.json"
    headers = {"User-Agent": config.get('user_agent', 'Python:RedditStatsUtil:v1.0')}
    logging.debug(f"Fetching user about data from: {url}")
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        about_json = response.json()
        if isinstance(about_json, dict) and about_json.get("kind") == "t2" and "data" in about_json:
             logging.info(f"Successfully fetched 'about' data for /u/{username}.")
             return about_json["data"]
        else:
             logging.warning(f"Unexpected structure in 'about.json' response for /u/{username}.")
             return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching 'about.json' for /u/{username}: {e}")
        if hasattr(e, 'response') and e.response is not None:
             logging.error(f"HTTP Status Code: {e.response.status_code}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from 'about.json' for /u/{username}.")
        return None

# --- Calculation Helpers ---
# ... (keep _calculate_basic_counts, _calculate_subreddit_activity, _calculate_text_stats,
#      _calculate_word_frequency, _calculate_post_types) ...
def _calculate_basic_counts(data): return {"total_posts": len(data.get("t3", {})), "total_comments": len(data.get("t1", {}))}
def _calculate_time_range(data):
    all_timestamps = [_get_timestamp(item["data"], use_edited=False) for kind in ["t3", "t1"] for item_id, item in data.get(kind, {}).items() if isinstance(item, dict) and "data" in item and _get_timestamp(item["data"], use_edited=False) > 0]
    if not all_timestamps: return {"first_activity": None, "last_activity": None}
    min_ts, max_ts = min(all_timestamps), max(all_timestamps)
    dt_format = '%Y-%m-%d %H:%M:%S UTC'
    try: first_dt = datetime.fromtimestamp(min_ts, timezone.utc).strftime(dt_format)
    except Exception: first_dt = "Error"
    try: last_dt = datetime.fromtimestamp(max_ts, timezone.utc).strftime(dt_format)
    except Exception: last_dt = "Error"
    return {"first_activity": first_dt, "last_activity": last_dt}
def _calculate_subreddit_activity(data):
    post_subs, comment_subs = Counter(), Counter()
    posted_set, commented_set = set(), set()
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
    stats = {"total_post_words": 0, "total_comment_words": 0, "all_words": [], "num_posts_with_text": 0, "num_comments_with_text": 0}
    if posts_csv_path and os.path.exists(posts_csv_path):
        try:
            with open(posts_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    full_text = f"{row.get('title', '')} {row.get('selftext', '').replace('<br>', ' ')}".strip()
                    if full_text: words = clean_text(full_text, False); stats["total_post_words"] += len(words); stats["all_words"].extend(words); stats["num_posts_with_text"] += 1
        except Exception as e: logging.error(f"Error reading posts CSV stats {posts_csv_path}: {e}")
    if comments_csv_path and os.path.exists(comments_csv_path):
        try:
            with open(comments_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    body = row.get('body', '').replace('<br>', ' ').strip()
                    if body: words = clean_text(body, False); stats["total_comment_words"] += len(words); stats["all_words"].extend(words); stats["num_comments_with_text"] += 1
        except Exception as e: logging.error(f"Error reading comments CSV stats {comments_csv_path}: {e}")
    total_words = stats["total_post_words"] + stats["total_comment_words"]; unique_words = set(stats["all_words"])
    lex_div = (len(unique_words) / total_words) if total_words > 0 else 0
    avg_p = (stats["total_post_words"] / stats["num_posts_with_text"]) if stats["num_posts_with_text"] > 0 else 0
    avg_c = (stats["total_comment_words"] / stats["num_comments_with_text"]) if stats["num_comments_with_text"] > 0 else 0
    return {"total_words": total_words, "total_post_words": stats["total_post_words"], "total_comment_words": stats["total_comment_words"], "total_unique_words": len(unique_words), "lexical_diversity": f"{lex_div:.3f}", "avg_post_word_length": f"{avg_p:.1f}", "avg_comment_word_length": f"{avg_c:.1f}"}
def _calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=50):
    word_counter = Counter()
    files = [(p, ['title', 'selftext']) for p in [posts_csv_path] if p and os.path.exists(p)] + [(c, ['body']) for c in [comments_csv_path] if c and os.path.exists(c)]
    for file_path, cols in files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for row in reader: full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip(); word_counter.update(clean_text(full_text, True))
        except Exception as e: logging.error(f"Error reading {file_path} for word freq: {e}")
    return {"word_frequency": dict(word_counter.most_common(top_n))}
def _calculate_post_types(data):
    link_p, self_p = 0, 0
    for item_id, item in data.get("t3", {}).items():
         try:
            if item.get("data", {}).get("is_self", False): self_p += 1
            else: link_p += 1
         except Exception: pass
    return { "link_posts": link_p, "self_posts": self_p }


# --- Updated & New Calculation Helpers ---

def _calculate_engagement_stats(data, about_data): # Takes about_data now
    """Calculates total/average item scores and gets total karma."""
    post_scores, comment_scores = [], []
    # Item scores (as before)
    for item_id, item in data.get("t3", {}).items():
        try: score = item.get("data", {}).get("score"); post_scores.append(int(score))
        except (AttributeError, ValueError, TypeError): pass
    for item_id, item in data.get("t1", {}).items():
        try: score = item.get("data", {}).get("score"); comment_scores.append(int(score))
        except (AttributeError, ValueError, TypeError): pass

    total_post_score, total_comment_score = sum(post_scores), sum(comment_scores)
    avg_post_score = (total_post_score / len(post_scores)) if post_scores else 0
    avg_comment_score = (total_comment_score / len(comment_scores)) if comment_scores else 0

    # Overall Karma from about_data
    total_link_karma = "N/A"
    total_comment_karma_about = "N/A" # Renamed to avoid confusion with sum of item scores
    total_karma = "N/A"
    if about_data:
        total_link_karma = about_data.get("link_karma", "N/A")
        total_comment_karma_about = about_data.get("comment_karma", "N/A")
        if isinstance(total_link_karma, int) and isinstance(total_comment_karma_about, int):
            total_karma = total_link_karma + total_comment_karma_about
        elif isinstance(total_link_karma, int): # Handle case where only one is available
             total_karma = total_link_karma
        elif isinstance(total_comment_karma_about, int):
             total_karma = total_comment_karma_about


    return {
        # Item scores
        "total_item_post_score": total_post_score,
        "total_item_comment_score": total_comment_score,
        "avg_item_post_score": f"{avg_post_score:.1f}",
        "avg_item_comment_score": f"{avg_comment_score:.1f}",
        # Overall Karma (might be different from sum of item scores)
        "total_link_karma": total_link_karma,
        "total_comment_karma": total_comment_karma_about,
        "total_combined_karma": total_karma,
    }


def _calculate_temporal_stats(data):
    """Calculates activity by hour, weekday, month, and year (UTC)."""
    hour_counter = Counter() # 0-23
    weekday_counter = Counter() # 0-6 (Mon-Sun)
    month_counter = Counter() # (YYYY, MM) tuple
    year_counter = Counter() # YYYY

    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months_map = {i: f"{i:02d}" for i in range(1, 13)} # For formatting month keys "01", "02" ... "12"

    items_processed = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                 if isinstance(item, dict) and "data" in item:
                    # Use creation time for temporal patterns
                    ts = _get_timestamp(item["data"], use_edited=False)
                    if ts > 0:
                        dt = datetime.fromtimestamp(ts, timezone.utc)
                        hour_counter[dt.hour] += 1
                        weekday_counter[dt.weekday()] += 1 # Monday is 0
                        month_key = (dt.year, dt.month)
                        month_counter[month_key] += 1
                        year_counter[dt.year] += 1
                        items_processed += 1
            except Exception as e:
                logging.warning(f"Error processing timestamp for {kind} {item_id}: {e}")

    # Prepare sorted results
    hours_sorted = {f"{hour:02d}": hour_counter.get(hour, 0) for hour in range(24)}
    weekdays_sorted = {days[i]: weekday_counter.get(i, 0) for i in range(7)}

    # Sort months chronologically
    months_sorted_keys = sorted(month_counter.keys())
    months_activity = {f"{yr}-{months_map[mn]}": month_counter[key] for key in months_sorted_keys for yr, mn in [key]}

    # Sort years
    years_sorted_keys = sorted(year_counter.keys())
    years_activity = {yr: year_counter[yr] for yr in years_sorted_keys}

    return {
        "activity_by_hour_utc": hours_sorted,
        "activity_by_weekday_utc": weekdays_sorted,
        "activity_by_month_utc": months_activity,
        "activity_by_year_utc": years_activity,
        "total_items_for_temporal": items_processed
    }

def _calculate_score_stats(data, top_n=5):
    """Calculates score distribution and identifies top/bottom items."""
    post_details = [] # Store (score, permalink, title)
    comment_details = [] # Store (score, permalink, body_snippet)

    # Extract details along with scores
    for item_id, item in data.get("t3", {}).items():
        try:
            d = item.get("data", {})
            s = d.get("score"); l = d.get("permalink"); t = d.get("title", "")
            if isinstance(s, int) and l: post_details.append((s, l, t))
        except Exception: pass
    for item_id, item in data.get("t1", {}).items():
        try:
            d = item.get("data", {})
            s = d.get("score"); l = d.get("permalink"); b = d.get("body", "")
            if isinstance(s, int) and l: comment_details.append((s, l, b[:80].replace('\n',' ') + "..."))
        except Exception: pass

    # Sort details by score
    post_details.sort(key=lambda x: x[0], reverse=True)
    comment_details.sort(key=lambda x: x[0], reverse=True)

    # Get just the scores for distribution calculation
    post_scores = [item[0] for item in post_details]
    comment_scores = [item[0] for item in comment_details]

    def get_score_distribution(scores):
        n = len(scores)
        if not scores: return {"count": 0}
        scores.sort() # Ensure sorted for quartiles
        dist = {
            "count": n,
            "min": scores[0],
            "max": scores[-1],
            "average": f"{(sum(scores) / n):.1f}"
        }
        if n >= 4:
            dist["q1"] = scores[n // 4]
            dist["median"] = scores[n // 2]
            dist["q3"] = scores[3 * n // 4]
        elif n > 0: # Handle small lists
            dist["median"] = scores[n // 2]
            dist["q1"] = scores[0]
            dist["q3"] = scores[-1]
        return dist

    # Return top/bottom N items and distributions
    return {
        "post_score_distribution": get_score_distribution(post_scores),
        "comment_score_distribution": get_score_distribution(comment_scores),
        "top_posts": post_details[:top_n],
        "bottom_posts": post_details[-(top_n):][::-1], # Lowest score first
        "top_comments": comment_details[:top_n],
        "bottom_comments": comment_details[-(top_n):][::-1], # Lowest score first
    }


def _calculate_award_stats(data):
    """Calculates total awards received."""
    total_awards = 0
    items_with_awards = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                 awards = item.get("data", {}).get("total_awards_received", 0)
                 if isinstance(awards, int) and awards > 0:
                     total_awards += awards
                     items_with_awards += 1
            except Exception: pass
    return {
        "total_awards_received": total_awards,
        "items_with_awards": items_with_awards,
    }


# --- Report Formatting ---

def _format_report(stats_data, username):
    """Formats the calculated statistics into a Markdown report."""
    report = f"# Reddit User Statistics Report for /u/{username}\n\n"
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    report += f"*Report generated: {dt_now}*\n\n"

    # --- Section I: Overall Summary ---
    report += "## I. Overall Activity Summary\n"
    report += "| Statistic             | Value         |\n"
    report += "|-----------------------|---------------|\n"
    report += f"| Total Posts Analyzed  | {stats_data.get('basic_counts', {}).get('total_posts', 'N/A')} |\n"
    report += f"| Total Comments Analyzed | {stats_data.get('basic_counts', {}).get('total_comments', 'N/A')} |\n"
    report += f"| First Activity        | {stats_data.get('time_range', {}).get('first_activity', 'N/A')} |\n"
    report += f"| Last Activity         | {stats_data.get('time_range', {}).get('last_activity', 'N/A')} |\n"
    report += f"| Total Link Karma      | {stats_data.get('engagement', {}).get('total_link_karma', 'N/A')} |\n"
    report += f"| Total Comment Karma   | {stats_data.get('engagement', {}).get('total_comment_karma', 'N/A')} |\n"
    report += f"| **Total Combined Karma**| **{stats_data.get('engagement', {}).get('total_combined_karma', 'N/A')}** |\n"
    report += "\n"

    # --- Section II: Content Analysis ---
    report += "## II. Content Analysis\n"
    report += "| Statistic                 | Value         |\n"
    report += "|---------------------------|---------------|\n"
    report += f"| Total Word Count          | {stats_data.get('text_stats', {}).get('total_words', 'N/A')} |\n"
    report += f"|   _(Posts)_             | {stats_data.get('text_stats', {}).get('total_post_words', 'N/A')} |\n"
    report += f"|   _(Comments)_          | {stats_data.get('text_stats', {}).get('total_comment_words', 'N/A')} |\n"
    report += f"| Total Unique Words        | {stats_data.get('text_stats', {}).get('total_unique_words', 'N/A')} |\n"
    report += f"| Lexical Diversity         | {stats_data.get('text_stats', {}).get('lexical_diversity', 'N/A')} |\n"
    report += f"| Avg. Words per Post     | {stats_data.get('text_stats', {}).get('avg_post_word_length', 'N/A')} |\n"
    report += f"| Avg. Words per Comment  | {stats_data.get('text_stats', {}).get('avg_comment_word_length', 'N/A')} |\n"
    report += f"| Link Posts              | {stats_data.get('post_types', {}).get('link_posts', 'N/A')} |\n"
    report += f"| Self Posts              | {stats_data.get('post_types', {}).get('self_posts', 'N/A')} |\n"
    report += "\n"

    # --- Section III: Engagement & Recognition ---
    report += "## III. Engagement & Recognition\n"
    # Item Scores
    report += "**Item Scores (Sum & Average):**\n"
    report += "| Statistic               | Value         |\n"
    report += "|-------------------------|---------------|\n"
    report += f"| Sum of Post Scores      | {stats_data.get('engagement', {}).get('total_item_post_score', 'N/A')} |\n"
    report += f"| Sum of Comment Scores   | {stats_data.get('engagement', {}).get('total_item_comment_score', 'N/A')} |\n"
    report += f"| Avg. Post Score         | {stats_data.get('engagement', {}).get('avg_item_post_score', 'N/A')} |\n"
    report += f"| Avg. Comment Score      | {stats_data.get('engagement', {}).get('avg_item_comment_score', 'N/A')} |\n"
    report += "*Note: Sum/Avg of item scores may differ from total user karma due to timing and voting mechanics.*\n\n"

    # Score distribution
    dist_posts = stats_data.get("score_stats", {}).get("post_score_distribution", {})
    dist_comments = stats_data.get("score_stats", {}).get("comment_score_distribution", {})
    if dist_posts.get("count", 0) > 0:
        report += "**Post Score Distribution:** "
        report += f"Count={dist_posts['count']}, Min={dist_posts['min']}, Q1={dist_posts.get('q1','N/A')}, Med={dist_posts.get('median','N/A')}, Q3={dist_posts.get('q3','N/A')}, Max={dist_posts['max']}, Avg={dist_posts['average']}\n"
    if dist_comments.get("count", 0) > 0:
        report += "**Comment Score Distribution:** "
        report += f"Count={dist_comments['count']}, Min={dist_comments['min']}, Q1={dist_comments.get('q1','N/A')}, Med={dist_comments.get('median','N/A')}, Q3={dist_comments.get('q3','N/A')}, Max={dist_comments['max']}, Avg={dist_comments['average']}\n"
    report += "\n"

    # Top/Bottom Items
    top_n_disp = len(stats_data.get("score_stats", {}).get("top_posts", [])) # Get actual N used
    report += f"**Top {top_n_disp} Scored Posts:**\n"
    for score, link, title in stats_data.get("score_stats", {}).get("top_posts", []): report += f"* `+{score}`: [{title[:60]}...](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("top_posts"): report += "* (No posts found or scored)\n"

    report += f"**Top {top_n_disp} Scored Comments:**\n"
    for score, link, snippet in stats_data.get("score_stats", {}).get("top_comments", []): report += f"* `+{score}`: [{snippet}](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("top_comments"): report += "* (No comments found or scored)\n"

    report += f"**Lowest {top_n_disp} Scored Posts:** (May include deleted/removed context)\n"
    for score, link, title in stats_data.get("score_stats", {}).get("bottom_posts", []): report += f"* `{score}`: [{title[:60]}...](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("bottom_posts"): report += "* (No posts found or scored)\n"

    report += f"**Lowest {top_n_disp} Scored Comments:** (May include deleted/removed context)\n"
    for score, link, snippet in stats_data.get("score_stats", {}).get("bottom_comments", []): report += f"* `{score}`: [{snippet}](https://reddit.com{link})\n"
    if not stats_data.get("score_stats", {}).get("bottom_comments"): report += "* (No comments found or scored)\n"
    report += "\n"

    # Awards
    report += "**Awards Received:**\n"
    report += f"* Total Awards: {stats_data.get('award_stats', {}).get('total_awards_received', 'N/A')}\n"
    report += f"* Items Awarded: {stats_data.get('award_stats', {}).get('items_with_awards', 'N/A')}\n"
    report += "\n"

    # --- Section IV: Subreddit Activity ---
    report += "## IV. Subreddit Activity\n"
    # ... (Subreddit table and top list - keep as before, maybe add Total column) ...
    posts_per_sub = stats_data.get('subreddit_activity', {}).get('posts_per_subreddit', {})
    comments_per_sub = stats_data.get('subreddit_activity', {}).get('comments_per_subreddit', {})
    all_subs = stats_data.get('subreddit_activity', {}).get('all_active_subs', [])
    if not all_subs: report += "No subreddit activity found.\n\n"
    else:
        report += f"Active in **{len(all_subs)}** unique subreddits ({stats_data.get('subreddit_activity', {}).get('unique_subs_posted', 0)} posted in, {stats_data.get('subreddit_activity', {}).get('unique_subs_commented', 0)} commented in).\n\n"
        report += "| Subreddit         | Posts | Comments | Total |\n"
        report += "|-------------------|-------|----------|-------|\n"
        combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub)
        for sub in all_subs: # Already sorted
            p_count = posts_per_sub.get(sub, 0); c_count = comments_per_sub.get(sub, 0); t_count = combined_activity.get(sub, 0)
            report += f"| /r/{sub:<15} | {p_count:<5} | {c_count:<8} | {t_count:<5} |\n"
        report += "\n"
        top_n_subs = 10
        if combined_activity:
             report += f"**Top {top_n_subs} Most Active Subreddits (Posts + Comments):**\n"
             for sub, count in combined_activity.most_common(top_n_subs): report += f"* /r/{sub} ({count})\n"
             report += "\n"

    # --- Section V: Temporal Activity Patterns (UTC) ---
    report += "## V. Temporal Activity Patterns (UTC)\n"
    temporal_data = stats_data.get('temporal_stats', {})
    if not temporal_data.get("total_items_for_temporal"):
         report += "No temporal data available.\n\n"
    else:
        activity_hour = temporal_data.get('activity_by_hour_utc', {})
        activity_wday = temporal_data.get('activity_by_weekday_utc', {})
        activity_month = temporal_data.get('activity_by_month_utc', {})
        activity_year = temporal_data.get('activity_by_year_utc', {})

        # Hour of Day
        if activity_hour:
            report += "**Activity by Hour of Day (00-23 UTC):**\n```\n"
            max_val = max(activity_hour.values()) if activity_hour else 0; scale = 50 / max_val if max_val > 0 else 0
            for hour in range(24): hour_str = f"{hour:02d}"; count = activity_hour.get(hour_str, 0); bar = '#' * int(count * scale); report += f"{hour_str}: {bar:<50} ({count})\n"
            report += "```\n"

        # Day of Week
        if activity_wday:
            report += "**Activity by Day of Week (UTC):**\n```\n"
            max_val = max(activity_wday.values()) if activity_wday else 0; scale = 50 / max_val if max_val > 0 else 0
            days_ordered = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            for day in days_ordered: count = activity_wday.get(day, 0); bar = '#' * int(count * scale); report += f"{day:<9}: {bar:<50} ({count})\n"
            report += "```\n"

        # Month
        if activity_month:
             report += "**Activity by Month (YYYY-MM UTC):**\n"
             # Just list counts for now, true heatmap needs external tools
             report += "| Month   | Count |\n|---------|-------|\n"
             for month_key, count in activity_month.items():
                 report += f"| {month_key} | {count:<5} |\n"
             report += "\n"

        # Year
        if activity_year:
             report += "**Activity by Year (UTC):**\n"
             report += "| Year | Count |\n|------|-------|\n"
             for year, count in activity_year.items():
                 report += f"| {year} | {count:<5} |\n"
             report += "\n"

    # --- Section VI: Word Frequency ---
    report += "## VI. Word Frequency\n"
    word_freq = stats_data.get('word_frequency', {}).get('word_frequency', {})
    if not word_freq: report += "No word frequency data available.\n"
    else:
        top_n_words = len(word_freq)
        report += f"Top {top_n_words} most frequent words (cleaned, stop words removed):\n\n"
        report += "| Word             | Count |\n|------------------|-------|\n"
        for word, count in word_freq.items(): report += f"| {word:<16} | {count:<5} |\n"
        report += "\n"

    return report


# --- Main Function ---

def generate_stats_report(json_path, about_data, posts_csv_path, comments_csv_path, username, output_path, top_n_words=50, top_n_items=5):
    """
    Calculates various statistics from user data and saves a Markdown report.
    Now requires about_data for total karma.
    """
    logging.info(f"Starting statistics generation for /u/{username}...")
    start_time = time.time()

    full_data = _load_data_from_json(json_path)
    if full_data is None: return False

    # about_data is now fetched in main script and passed in
    if about_data is None:
         logging.warning("User 'about' data not provided or failed to fetch. Total karma stats will be unavailable.")

    stats_results = {}
    try:
        # Calculate all stats
        stats_results["basic_counts"] = _calculate_basic_counts(full_data)
        stats_results["time_range"] = _calculate_time_range(full_data)
        stats_results["subreddit_activity"] = _calculate_subreddit_activity(full_data)
        stats_results["text_stats"] = _calculate_text_stats(posts_csv_path, comments_csv_path)
        stats_results["word_frequency"] = _calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=top_n_words)
        stats_results["engagement"] = _calculate_engagement_stats(full_data, about_data) # Pass about_data
        stats_results["post_types"] = _calculate_post_types(full_data)
        stats_results["temporal_stats"] = _calculate_temporal_stats(full_data)
        stats_results["score_stats"] = _calculate_score_stats(full_data, top_n=top_n_items)
        stats_results["award_stats"] = _calculate_award_stats(full_data)

    except Exception as e:
        logging.error(f"Error during statistics calculation: {e}", exc_info=True)
        return False

    logging.debug("Formatting statistics report...")
    try:
        report_content = _format_report(stats_results, username)
    except Exception as e:
        logging.error(f"Error during report formatting: {e}", exc_info=True)
        return False

    logging.debug(f"Saving statistics report to {output_path}...")
    try:
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f: f.write(report_content)
        elapsed_time = time.time() - start_time
        logging.info(f"✅ Statistics report saved successfully to {output_path} ({elapsed_time:.2f}s)")
        return True
    except Exception as e:
        logging.error(f"❌ Error saving statistics report: {e}", exc_info=True)
        return False