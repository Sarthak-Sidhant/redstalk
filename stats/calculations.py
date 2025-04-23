import logging
import os
import csv
import time
import math
import statistics
from collections import Counter
from datetime import datetime, timezone

# --- Import from sibling module ---
from .core_utils import clean_text, _get_timestamp, _format_timedelta, _generate_ngrams, STOP_WORDS, CYAN, RESET, BOLD, YELLOW
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; RED = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; BLUE = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"
# --- Import from modules OUTSIDE the 'stats' package ---
# This assumes your project root is in Python's path when run
try:
    from reddit_utils import format_timestamp
except ImportError:
    logging.critical(f"{BOLD}{RED}❌ Critical Error: Failed to import 'format_timestamp' from reddit_utils.py needed by calculations.{RESET}")
    # Define a dummy function to avoid immediate crashes, but reports will be broken.
    def format_timestamp(ts): return "TIMESTAMP_ERROR"

# --- VADER Dependency ---
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    vader_available = True
    logging.debug("VADER SentimentIntensityAnalyzer imported successfully.")
except ImportError:
    vader_available = False
    SentimentIntensityAnalyzer = None # Ensure it's None if import fails
    logging.warning(f"{YELLOW}⚠️ VADER sentiment library not found. Sentiment analysis will be skipped.{RESET}")


# --- Calculation Helpers ---
# Note: Removed data loading/filtering functions - they belong in single_report.py

def _calculate_basic_counts(data):
    logging.debug("      Calculating basic counts...")
    return {"total_posts": len(data.get("t3", {})), "total_comments": len(data.get("t1", {}))}

def _calculate_time_range(data):
    logging.debug("      Calculating time range (based on creation time of filtered items)...")
    all_timestamps = [_get_timestamp(item.get("data",{}), use_edited=False)
                      for kind in ["t3", "t1"]
                      for item_id, item in data.get(kind, {}).items()
                      if _get_timestamp(item.get("data",{}), use_edited=False) > 0]
    if not all_timestamps:
        return {"first_activity": None, "last_activity": None, "first_activity_ts": 0, "last_activity_ts": 0}
    min_ts, max_ts = min(all_timestamps), max(all_timestamps)
    return {"first_activity": format_timestamp(min_ts), "last_activity": format_timestamp(max_ts),
            "first_activity_ts": min_ts, "last_activity_ts": max_ts} # Also return raw timestamps if needed

def _calculate_subreddit_activity(data):
    logging.debug("      Calculating subreddit activity...")
    post_subs, comment_subs = Counter(), Counter()
    posted_set, commented_set = set(), set()
    for item_id, item in data.get("t3", {}).items():
        try:
            subreddit = item.get("data", {}).get("subreddit")
            if subreddit and isinstance(subreddit, str):
                post_subs[subreddit] += 1
                posted_set.add(subreddit)
        except AttributeError: # Handle cases where 'item' isn't a dict or 'data' is missing
            logging.warning(f"      ⚠️ Could not process post {item_id} for subreddit activity (invalid structure).")
            subreddit = None
    for item_id, item in data.get("t1", {}).items():
        try:
            subreddit = item.get("data", {}).get("subreddit")
            if subreddit and isinstance(subreddit, str):
                comment_subs[subreddit] += 1
                commented_set.add(subreddit)
        except AttributeError:
            logging.warning(f"      ⚠️ Could not process comment {item_id} for subreddit activity (invalid structure).")
            subreddit = None
    all_active = sorted(list(posted_set.union(commented_set)), key=str.lower)
    return {"posts_per_subreddit": dict(post_subs),
            "comments_per_subreddit": dict(comment_subs),
            "unique_subs_posted": len(posted_set),
            "unique_subs_commented": len(commented_set),
            "all_active_subs": all_active}


def _calculate_text_stats(posts_csv_path, comments_csv_path):
    logging.debug("      Calculating text stats (from filtered CSVs)...")
    stats = {"total_post_words": 0, "total_comment_words": 0, "all_words": [], "num_posts_with_text": 0, "num_comments_with_text": 0}
    valid_csv_found = False

    if posts_csv_path and os.path.exists(posts_csv_path):
        valid_csv_found = True
        logging.debug(f"         Reading post text from {CYAN}{posts_csv_path}{RESET}")
        try:
            with open(posts_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    # Combine title and selftext, handle potential missing keys
                    title = row.get('title', '')
                    selftext = row.get('selftext', '').replace('<br>', ' ') # Replace breaks
                    full_text = f"{title} {selftext}".strip()
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]'): # Avoid counting placeholders
                        words = clean_text(full_text, False); # Count all words, no stopword removal here
                        stats["total_post_words"] += len(words);
                        # Only extend all_words if needed for unique count later
                        # stats["all_words"].extend(words); # Moved unique calc below
                        stats["num_posts_with_text"] += 1
        except Exception as e: logging.error(f"      ❌ Error reading posts CSV {CYAN}{posts_csv_path}{RESET} for stats: {e}")
    else: logging.debug(f"         Posts CSV for text stats not found or not provided: {CYAN}{posts_csv_path}{RESET}")

    if comments_csv_path and os.path.exists(comments_csv_path):
        valid_csv_found = True
        logging.debug(f"         Reading comment text from {CYAN}{comments_csv_path}{RESET}")
        try:
            with open(comments_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    body = row.get('body', '').replace('<br>', ' ').strip() # Replace breaks
                    if body and body.lower() not in ('[no body]', '[deleted]', '[removed]'): # Avoid counting placeholders
                        words = clean_text(body, False); # Count all words
                        stats["total_comment_words"] += len(words);
                        # stats["all_words"].extend(words); # Moved unique calc below
                        stats["num_comments_with_text"] += 1
        except Exception as e: logging.error(f"      ❌ Error reading comments CSV {CYAN}{comments_csv_path}{RESET} for stats: {e}")
    else: logging.debug(f"         Comments CSV for text stats not found or not provided: {CYAN}{comments_csv_path}{RESET}")

    if not valid_csv_found:
        logging.warning("      ⚠️ No valid CSV files found for text stats calculation.")
        return {"total_words": 0, "total_post_words": 0, "total_comment_words": 0,
                "total_unique_words": 0, "lexical_diversity": "N/A",
                "avg_post_word_length": "N/A", "avg_comment_word_length": "N/A"}

    # --- Calculate unique words separately for efficiency ---
    # This requires reading files again, or storing all words (memory intensive)
    # Let's re-read for unique count to save memory
    unique_words_set = set()
    files_for_unique = []
    if posts_csv_path and os.path.exists(posts_csv_path): files_for_unique.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files_for_unique.append((comments_csv_path, ['body']))

    for file_path, cols in files_for_unique:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]'):
                         # Clean text *without* stopword removal for unique count of *all* words
                         words = clean_text(full_text, False)
                         # Filter for words > 1 char before adding to set
                         unique_words_set.update(word for word in words if len(word) > 1)
        except Exception as e:
            logging.error(f"      ❌ Error during unique word count read for {CYAN}{file_path}{RESET}: {e}")


    total_words = stats["total_post_words"] + stats["total_comment_words"];
    total_unique_words = len(unique_words_set)
    lex_div = (total_unique_words / total_words) if total_words > 0 else 0
    avg_p = (stats["total_post_words"] / stats["num_posts_with_text"]) if stats["num_posts_with_text"] > 0 else 0
    avg_c = (stats["total_comment_words"] / stats["num_comments_with_text"]) if stats["num_comments_with_text"] > 0 else 0

    return {"total_words": total_words,
            "total_post_words": stats["total_post_words"],
            "total_comment_words": stats["total_comment_words"],
            "total_unique_words": total_unique_words,
            "lexical_diversity": f"{lex_div:.3f}",
            "avg_post_word_length": f"{avg_p:.1f}",
            "avg_comment_word_length": f"{avg_c:.1f}"}


def _calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=50):
    logging.debug(f"      Calculating word frequency (top {top_n}, from filtered CSVs)...")
    word_counter = Counter()
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate word frequency.")
        return {"word_frequency": {}} # Return structure expected by report

    total_rows_processed = 0
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for word frequency...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                     full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                     # Check if text is not just placeholders before cleaning
                     if full_text and full_text.lower() not in ['[deleted]', '[removed]', '[no body]']:
                        # Clean text WITH stopword removal for frequency count
                        word_counter.update(clean_text(full_text, True))
                     total_rows_processed += 1
        except Exception as e: logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for word freq: {e}")
    logging.debug(f"         Finished word freq calculation from {total_rows_processed} total rows.")
    return {"word_frequency": dict(word_counter.most_common(top_n))}


def _calculate_post_types(data):
    logging.debug("      Calculating post types...")
    link_p, self_p, unknown_p = 0, 0, 0
    for item_id, item in data.get("t3", {}).items():
         try:
             is_self = item.get("data", {}).get("is_self")
             if is_self is True:
                 self_p += 1
             elif is_self is False:
                 link_p += 1
             else: # Handles None or other unexpected values
                 unknown_p += 1
         except Exception as e:
             logging.warning(f"      ⚠️ Error processing post {item_id} for type: {e}")
             unknown_p += 1
             continue
    if unknown_p > 0: logging.warning(f"      ⚠️ Found {unknown_p} posts with unknown or error in 'is_self' field.")
    return { "link_posts": link_p, "self_posts": self_p }


def _calculate_engagement_stats(data, about_data):
    logging.debug("      Calculating engagement stats (item scores & overall karma)...")
    post_scores, comment_scores = [], []
    for item_id, item in data.get("t3", {}).items():
        try:
            score = item.get("data", {}).get("score")
            # Ensure score is an integer before appending
            if score is not None:
                post_scores.append(int(score))
        except (AttributeError, ValueError, TypeError, KeyError):
             logging.debug(f"         Could not parse score for post {item_id}.")
             pass
    for item_id, item in data.get("t1", {}).items():
        try:
            score = item.get("data", {}).get("score")
            if score is not None:
                comment_scores.append(int(score))
        except (AttributeError, ValueError, TypeError, KeyError):
             logging.debug(f"         Could not parse score for comment {item_id}.")
             pass

    total_post_score, total_comment_score = sum(post_scores), sum(comment_scores)
    avg_post_score = (total_post_score / len(post_scores)) if post_scores else 0
    avg_comment_score = (total_comment_score / len(comment_scores)) if comment_scores else 0

    # Get Karma from 'about_data' if available
    total_link_karma, total_comment_karma_about, total_karma = "N/A", "N/A", "N/A"
    if about_data and isinstance(about_data, dict):
        logging.debug("         Using fetched 'about' data for karma.")
        total_link_karma = about_data.get("link_karma", "N/A")
        total_comment_karma_about = about_data.get("comment_karma", "N/A")

        # Ensure they are numbers for addition, fallback to N/A if not
        lk_int = int(total_link_karma) if isinstance(total_link_karma, int) else None
        ck_int = int(total_comment_karma_about) if isinstance(total_comment_karma_about, int) else None

        if lk_int is not None and ck_int is not None:
            total_karma = lk_int + ck_int
        elif lk_int is not None: # Only link karma is valid number
            total_karma = lk_int
        elif ck_int is not None: # Only comment karma is valid number
            total_karma = ck_int
        # If neither is a number, total_karma remains "N/A"

    else:
        logging.debug("         'About' data unavailable or invalid for total karma.")

    return { "total_item_post_score": total_post_score,
             "total_item_comment_score": total_comment_score,
             "avg_item_post_score": f"{avg_post_score:.1f}",
             "avg_item_comment_score": f"{avg_comment_score:.1f}",
             "total_link_karma": total_link_karma, # From about_data
             "total_comment_karma": total_comment_karma_about, # From about_data
             "total_combined_karma": total_karma, # Calculated from about_data
            }


def _calculate_temporal_stats(data):
    logging.debug("      Calculating temporal stats (based on creation time of filtered items)...")
    hour_counter, weekday_counter, month_counter, year_counter = Counter(), Counter(), Counter(), Counter()
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months_map = {i: f"{i:02d}" for i in range(1, 13)}
    items_processed = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                # Use creation time for temporal analysis
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0:
                    dt = datetime.fromtimestamp(ts, timezone.utc)
                    hour_counter[dt.hour] += 1
                    weekday_counter[dt.weekday()] += 1 # Monday is 0, Sunday is 6
                    month_key = (dt.year, dt.month)
                    month_counter[month_key] += 1
                    year_counter[dt.year] += 1
                    items_processed += 1
                elif ts == 0:
                    logging.debug(f"      Skipping item {item_id} for temporal stats due to zero timestamp.")
            except Exception as e:
                logging.warning(f"      ⚠️ Error processing timestamp for temporal stats ({kind} {item_id}): {e}")

    # Ensure all hours/days are present, even if count is 0
    hours_sorted = {f"{hour:02d}": hour_counter.get(hour, 0) for hour in range(24)}
    weekdays_sorted = {days[i]: weekday_counter.get(i, 0) for i in range(7)}

    # Format month keys as "YYYY-MM" and sort chronologically
    months_activity = {f"{yr}-{months_map[mn]}": month_counter[key] for key in sorted(month_counter.keys()) for yr, mn in [key]}
    # Sort years numerically
    years_activity = {yr: year_counter[yr] for yr in sorted(year_counter.keys())}

    logging.debug(f"      Finished temporal stats calculation ({items_processed} items).")
    return { "activity_by_hour_utc": hours_sorted,
             "activity_by_weekday_utc": weekdays_sorted,
             "activity_by_month_utc": months_activity,
             "activity_by_year_utc": years_activity,
             "total_items_for_temporal": items_processed }


def _calculate_score_stats(data, top_n=5):
    logging.debug(f"      Calculating score stats (distribution, top/bottom {top_n})...")
    post_details, comment_details = [], []
    for item_id, item in data.get("t3", {}).items():
        try:
            d = item.get("data", {})
            s = d.get("score")
            l = d.get("permalink")
            t = d.get("title", "[No Title]")
            # Ensure score is int and link exists
            if isinstance(s, int) and isinstance(l, str) and l:
                 post_details.append((s, l, t))
            elif s is not None or l is not None:
                 logging.debug(f"         Skipping post {item_id} for score stats (invalid score type '{type(s)}' or missing permalink '{l}').")
        except Exception as e:
             logging.warning(f"      ⚠️ Error processing post {item_id} for score stats: {e}")
             continue
    for item_id, item in data.get("t1", {}).items():
        try:
            d = item.get("data", {})
            s = d.get("score")
            l = d.get("permalink")
            b = d.get("body", "[No Body]")
            # Ensure score is int and link exists
            if isinstance(s, int) and isinstance(l, str) and l:
                 # Create snippet for comments
                 snippet = b[:80].replace('\n',' ') + ("..." if len(b)>80 else "")
                 comment_details.append((s, l, snippet))
            elif s is not None or l is not None:
                 logging.debug(f"         Skipping comment {item_id} for score stats (invalid score type '{type(s)}' or missing permalink '{l}').")
        except Exception as e:
             logging.warning(f"      ⚠️ Error processing comment {item_id} for score stats: {e}")
             continue

    post_details.sort(key=lambda x: x[0], reverse=True) # Sort by score descending
    comment_details.sort(key=lambda x: x[0], reverse=True)

    post_scores = [item[0] for item in post_details]
    comment_scores = [item[0] for item in comment_details]

    def get_score_distribution(scores):
        n = len(scores)
        dist = {"count": n, "min": "N/A", "max": "N/A", "average": "N/A", "median": "N/A", "q1": "N/A", "q3": "N/A"}
        if not scores: return dist
        scores.sort() # Sort numerically for quantiles
        dist["min"] = scores[0]; dist["max"] = scores[-1]
        dist["average"] = f"{(sum(scores) / n):.1f}"
        # Use statistics.quantiles for more robust calculation if available and needed
        try:
            dist["median"] = statistics.median(scores)
            if n >= 4: # Need at least 4 points for distinct quartiles
                 quantiles = statistics.quantiles(scores, n=4) # Returns [Q1, Q2, Q3]
                 dist["q1"] = quantiles[0]
                 dist["q3"] = quantiles[2]
            elif n > 1: # Handle 2 or 3 points
                 dist["q1"] = scores[0] # Or potentially interpolate
                 dist["q3"] = scores[-1] # Or potentially interpolate
            else: # n == 1
                 dist["q1"] = scores[0]; dist["q3"] = scores[0]

        except AttributeError: # statistics.quantiles might not be available in older pythons
             logging.warning("      ⚠️ statistics.quantiles not available, using simple median calculation.")
             dist["median"] = statistics.median(scores)
             # Simple percentile calculation as fallback
             if n >= 4:
                 dist["q1"] = scores[max(0, math.ceil(n * 0.25) - 1)]
                 dist["q3"] = scores[min(n - 1, math.ceil(n * 0.75) - 1)]
             elif n > 1:
                 dist["q1"] = scores[0]; dist["q3"] = scores[-1]
             else:
                 dist["q1"] = scores[0]; dist["q3"] = scores[0]
        except Exception as e:
            logging.error(f"      ❌ Error calculating score distribution quantiles: {e}")
            dist["median"] = "Error"; dist["q1"] = "Error"; dist["q3"] = "Error"

        # Ensure quantiles are formatted nicely if they are numbers
        for k in ["q1", "median", "q3"]:
            if isinstance(dist[k], (int, float)):
                dist[k] = f"{dist[k]:.1f}" # Format to 1 decimal place

        return dist

    # Handle potential case where top_n is 0 or negative
    safe_top_n = max(0, top_n)

    return { "post_score_distribution": get_score_distribution(post_scores),
             "comment_score_distribution": get_score_distribution(comment_scores),
             "top_posts": post_details[:safe_top_n],
             "bottom_posts": post_details[-(safe_top_n):][::-1] if safe_top_n > 0 else [], # Get last N, then reverse
             "top_comments": comment_details[:safe_top_n],
             "bottom_comments": comment_details[-(safe_top_n):][::-1] if safe_top_n > 0 else []
            }


def _calculate_award_stats(data):
    logging.debug("      Calculating award stats...")
    total_awards, items_with_awards = 0, 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                awards = item.get("data", {}).get("total_awards_received", 0)
                # Ensure awards is an int before adding
                if isinstance(awards, int) and awards > 0:
                     total_awards += awards
                     items_with_awards += 1
                elif awards is not None and awards != 0:
                     logging.debug(f"         Item {item_id} has non-integer award count: {awards}")
            except Exception as e:
                logging.warning(f"      ⚠️ Error processing item {item_id} for award stats: {e}")
                continue
    return { "total_awards_received": total_awards, "items_with_awards": items_with_awards }


def _calculate_flair_stats(data):
    logging.debug("      Calculating flair stats...")
    user_flairs = Counter() # Key: "Subreddit: Flair Text"
    post_flairs = Counter() # Key: "Subreddit: Flair Text"
    comments_with_user_flair = 0
    posts_with_link_flair = 0
    processed_comments = 0
    processed_posts = 0

    for item_id, item in data.get("t1", {}).items():
        processed_comments += 1
        try:
            d = item.get("data", {})
            sub = d.get("subreddit")
            flair = d.get("author_flair_text")
            # Ensure flair is a non-empty string
            if sub and flair and isinstance(flair, str) and flair.strip():
                user_flairs[f"{sub}: {flair.strip()}"] += 1
                comments_with_user_flair += 1
        except Exception as e:
            logging.warning(f"      ⚠️ Error processing comment {item_id} for user flair: {e}")
            continue

    for item_id, item in data.get("t3", {}).items():
        processed_posts += 1
        try:
            d = item.get("data", {})
            sub = d.get("subreddit")
            flair = d.get("link_flair_text")
            # Ensure flair is a non-empty string
            if sub and flair and isinstance(flair, str) and flair.strip():
                post_flairs[f"{sub}: {flair.strip()}"] += 1
                posts_with_link_flair += 1
        except Exception as e:
            logging.warning(f"      ⚠️ Error processing post {item_id} for link flair: {e}")
            continue

    logging.debug(f"         Processed {processed_comments} comments ({comments_with_user_flair} user flair), {processed_posts} posts ({posts_with_link_flair} link flair).")
    return { "user_flairs_by_sub": dict(user_flairs.most_common()), # Return sorted by frequency
             "post_flairs_by_sub": dict(post_flairs.most_common()),
             "total_comments_with_user_flair": comments_with_user_flair,
             "total_posts_with_link_flair": posts_with_link_flair,
             }


def _calculate_post_engagement(data):
    logging.debug("      Calculating post engagement (num_comments)...")
    comment_counts = []
    top_commented_posts = [] # Store tuples: (num_comments, permalink, title)
    posts_analyzed = 0
    total_posts_in_data = len(data.get("t3", {}))

    for item_id, item in data.get("t3", {}).items():
        posts_analyzed += 1
        try:
            d = item.get("data", {})
            num_comments = d.get("num_comments")
            permalink = d.get("permalink")
            title = d.get("title", "[No Title]")

            # Ensure num_comments is a non-negative integer and permalink exists
            if isinstance(num_comments, int) and num_comments >= 0 and isinstance(permalink, str) and permalink:
                 comment_counts.append(num_comments)
                 top_commented_posts.append((num_comments, permalink, title))
            elif num_comments is not None or permalink is not None:
                 logging.debug(f"         Post {item_id} skipped for engagement (invalid 'num_comments': {num_comments} or missing permalink: {permalink})")

        except Exception as e:
            logging.warning(f"      ⚠️ Error processing post {item_id} for engagement stats: {e}")
            continue

    # Check if any posts were successfully analyzed
    if not comment_counts:
        logging.warning(f"      ⚠️ No valid posts found for comment engagement analysis (out of {total_posts_in_data} total posts).")
        return {"avg_comments_per_post": "0.0",
                "total_posts_analyzed_for_comments": 0, # Explicitly 0 if none were valid
                "top_commented_posts": []}

    avg_comments = sum(comment_counts) / len(comment_counts)
    top_commented_posts.sort(key=lambda x: x[0], reverse=True) # Sort by num_comments descending

    return { "avg_comments_per_post": f"{avg_comments:.1f}",
             "total_posts_analyzed_for_comments": len(comment_counts), # Number of posts *used* in average
             "top_commented_posts": top_commented_posts[:5] # Return top 5
            }


def _calculate_editing_stats(data):
    logging.debug("      Calculating editing stats...")
    posts_edited = 0; comments_edited = 0
    total_posts = len(data.get("t3", {}))
    total_comments = len(data.get("t1", {}))
    edit_delays_s = []
    items_processed = 0

    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            items_processed +=1
            try:
                d = item.get("data", {})
                created_utc = d.get("created_utc")
                edited_ts_val = d.get("edited")

                # Check if 'edited' field indicates an edit (not None, not False, not 0)
                if edited_ts_val and str(edited_ts_val).lower() != 'false':
                    try:
                        # Convert both to float for comparison
                        edited_ts = float(edited_ts_val)
                        created_ts = float(created_utc)

                        # Ensure edit time is strictly after creation time
                        if edited_ts > created_ts:
                            if kind == "t3": posts_edited += 1
                            else: comments_edited += 1
                            edit_delays_s.append(edited_ts - created_ts)
                        # else: edited time is same or before created time, not a valid edit for delay calc

                    except (ValueError, TypeError, KeyError) as convert_err:
                         logging.debug(f"         Could not parse created/edited timestamp for edit stat on {kind} {item_id}: {convert_err}")
                         pass # Skip this item for edit stats if timestamps invalid
            except Exception as e:
                 logging.warning(f"      ⚠️ Error processing item {item_id} for editing stats: {e}")
                 pass # Skip this item

    edit_percent_posts = (posts_edited / total_posts * 100) if total_posts > 0 else 0
    edit_percent_comments = (comments_edited / total_comments * 100) if total_comments > 0 else 0
    avg_delay_s = (sum(edit_delays_s) / len(edit_delays_s)) if edit_delays_s else 0
    avg_delay_str = _format_timedelta(avg_delay_s) # Use helper from core_utils

    return { "posts_edited_count": posts_edited,
             "comments_edited_count": comments_edited,
             "total_posts_analyzed_for_edits": total_posts,
             "total_comments_analyzed_for_edits": total_comments,
             "edit_percentage_posts": f"{edit_percent_posts:.1f}%",
             "edit_percentage_comments": f"{edit_percent_comments:.1f}%",
             "average_edit_delay_seconds": round(avg_delay_s, 1),
             "average_edit_delay_formatted": avg_delay_str }


def _calculate_sentiment_ratio(posts_csv_path, comments_csv_path):
    logging.debug("      Calculating sentiment ratio (VADER, from filtered CSVs)...")
    global vader_available # Check the flag set during import
    if not vader_available:
        return {"sentiment_analysis_skipped": True, "reason": "VADER library not installed or import failed"}

    # Double check if class itself is usable (might fail initialization)
    if not SentimentIntensityAnalyzer:
         if vader_available: # Should not happen if flag is False, but check anyway
             logging.warning(f"{YELLOW} VADER lib import seemed ok, but class unavailable. Skipping.{RESET}")
             vader_available = False # Correct the flag
         return {"sentiment_analysis_skipped": True, "reason": "VADER Analyzer class unavailable"}

    try:
        # Initialize VADER analyzer *once* per function call
        analyzer = SentimentIntensityAnalyzer()
        logging.debug("         VADER SentimentIntensityAnalyzer initialized.")
    except Exception as e:
        logging.error(f"    ❌ Failed to initialize VADER SentimentIntensityAnalyzer: {e}", exc_info=True)
        vader_available = False # Prevent further attempts in this run
        return {"sentiment_analysis_skipped": True, "reason": "VADER Analyzer initialization failed"}

    pos_count, neg_count, neu_count = 0, 0, 0
    total_analyzed = 0
    sentiment_scores = [] # Store compound scores for average calculation
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate sentiment.")
        return {"sentiment_analysis_skipped": False, # Analysis wasn't skipped due to VADER, just no data
                "positive_count": 0, "negative_count": 0, "neutral_count": 0,
                "total_items_sentiment_analyzed": 0,
                "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}

    for file_path, cols in files:
        logging.debug(f"         Analyzing sentiment in {CYAN}{file_path}{RESET}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                    # Combine text, handle potential missing columns gracefully
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    # Ensure there's actual text and it's not a placeholder
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]', '[no title]'):
                        try:
                            vs = analyzer.polarity_scores(full_text)
                            sentiment_scores.append(vs['compound']) # Store score
                            # Classify based on standard VADER compound thresholds
                            if vs['compound'] >= 0.05: pos_count += 1
                            elif vs['compound'] <= -0.05: neg_count += 1
                            else: neu_count += 1
                            total_analyzed += 1
                        except Exception as vader_err:
                            logging.warning(f"{YELLOW} VADER error processing row {i+1} in {os.path.basename(file_path)}: {vader_err}{RESET}")
                            continue # Skip this row if VADER fails on it
        except Exception as e:
            logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for sentiment analysis: {e}")
            # Continue to next file if possible

    if total_analyzed == 0:
        logging.warning("      ⚠️ No valid text items found in CSVs for sentiment analysis.")
        return {"sentiment_analysis_skipped": False,
                "positive_count": 0, "negative_count": 0, "neutral_count": 0,
                "total_items_sentiment_analyzed": 0,
                "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}

    # Calculate ratio and average score
    pos_neg_ratio = f"{(pos_count / neg_count):.2f}:1" if neg_count > 0 else (f"{pos_count}:0" if pos_count > 0 else "N/A")
    avg_compound = sum(sentiment_scores) / total_analyzed if total_analyzed > 0 else 0

    return { "sentiment_analysis_skipped": False,
             "positive_count": pos_count, "negative_count": neg_count, "neutral_count": neu_count,
             "total_items_sentiment_analyzed": total_analyzed,
             "pos_neg_ratio": pos_neg_ratio,
             "avg_compound_score": f"{avg_compound:.3f}" }


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
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity: Missing or invalid 'about_data'.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Account Info)"
        return results

    try:
        created_ts = float(about_data["created_utc"])
        results["account_created_utc"] = created_ts
        results["account_created_formatted"] = format_timestamp(created_ts) # Uses imported/dummy function
        # Use current UTC time for age calculation
        now_ts = datetime.now(timezone.utc).timestamp()
        age_seconds = now_ts - created_ts
        if age_seconds < 0: # Account created in the future? Clock skew?
             logging.warning(f"      {YELLOW}⚠️ Account creation timestamp ({created_ts}) is in the future? Setting age to 0.{RESET}")
             age_days = 0
        else:
             age_days = age_seconds / 86400 # 60*60*24
        results["account_age_days"] = round(age_days, 1)
    except (ValueError, TypeError) as e:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity: Invalid 'created_utc' in about_data: {e}.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (Invalid Creation Time)"
        return results # Return early if age can't be determined

    # --- Activity Analysis (Requires Temporal Stats) ---

    if not temporal_stats or not isinstance(temporal_stats, dict) or "total_items_for_temporal" not in temporal_stats:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity trends: Missing or invalid 'temporal_stats'.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Temporal Stats)"
        return results # Cannot proceed without temporal data

    total_items = temporal_stats["total_items_for_temporal"]
    results["total_activity_items"] = total_items
    activity_by_year = temporal_stats.get("activity_by_year_utc", {})

    if total_items == 0:
        logging.debug("         No activity items found in temporal stats for trend analysis.")
        results["activity_trend_status"] = "No Activity Found"
        return results # No activity to analyze

    # Calculate averages using calculated age_days
    if age_days > 0:
        age_years = age_days / 365.25
        results["average_activity_per_year"] = f"{total_items / age_years:.1f}" if age_years > 0 else "N/A"
        age_months = age_days / (365.25 / 12)
        results["average_activity_per_month"] = f"{total_items / age_months:.1f}" if age_months > 0 else "N/A"
    else:
        logging.debug("         Account age is zero or negative, cannot calculate average rates.")

    # Simple Trend Analysis (requires yearly breakdown)
    if not activity_by_year:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity trends: Missing 'activity_by_year_utc' in temporal_stats.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Yearly Breakdown)"
        return results

    sorted_years = sorted(activity_by_year.keys())
    if len(sorted_years) < 2:
        # Only one year of activity or less
        results["activity_trend_status"] = "Insufficient Data (Less than 2 years of activity)"
    else:
        first_year = sorted_years[0]
        last_year = sorted_years[-1]

        # We need the time span between the first and last year for a better comparison
        # Consider the activity rate rather than absolute numbers if years are partial
        # For simplicity here, just compare first full year vs last full year if possible

        # Let's try comparing the first half of the activity period vs the second half
        total_years_span = last_year - first_year + 1
        if total_years_span < 2: # Should be caught above, but double-check
             results["activity_trend_status"] = "Insufficient Data (Activity Span < 2 Years)"
        else:
            mid_point_year = first_year + total_years_span // 2
            activity_first_half = sum(count for year, count in activity_by_year.items() if year < mid_point_year)
            activity_second_half = sum(count for year, count in activity_by_year.items() if year >= mid_point_year)
            num_years_first = mid_point_year - first_year
            num_years_second = last_year - mid_point_year + 1

            # Avoid division by zero if halves somehow have 0 years (shouldn't happen with span >= 2)
            rate_first = (activity_first_half / num_years_first) if num_years_first > 0 else 0
            rate_second = (activity_second_half / num_years_second) if num_years_second > 0 else 0

            # Compare rates with a threshold (e.g., 20% difference)
            if rate_second > rate_first * 1.2:
                results["activity_trend_status"] = "Increasing"
            elif rate_first > rate_second * 1.2:
                 results["activity_trend_status"] = "Decreasing"
            elif rate_first == 0 and rate_second == 0: # Handle no activity in either half
                 results["activity_trend_status"] = "No Activity Found"
            else:
                 results["activity_trend_status"] = "Stable / Fluctuating"

    logging.debug(f"         Age vs Activity calculated: Age={results['account_age_days']}d, Trend={results['activity_trend_status']}")
    return results


def _calculate_crosspost_stats(data):
    """Calculates statistics about crossposting activity."""
    logging.debug("      Calculating crosspost stats...")
    crosspost_count = 0
    source_sub_counter = Counter()
    analyzed_posts = 0

    posts_data = data.get("t3", {})
    total_posts = len(posts_data) # Total posts within the *filtered* data

    if total_posts == 0:
         logging.debug("         No posts found in filtered data to analyze for crossposts.")
         return { "total_posts_analyzed": 0, "crosspost_count": 0,
                  "crosspost_percentage": "0.0%", "source_subreddits": {} }

    for item_id, item in posts_data.items():
        analyzed_posts += 1
        try:
            item_data = item.get("data", {})
            # The key indicating a crosspost is 'crosspost_parent_list'
            crosspost_parent_list = item_data.get("crosspost_parent_list")

            # Check if it's a non-empty list (presence indicates crosspost)
            if isinstance(crosspost_parent_list, list) and len(crosspost_parent_list) > 0:
                crosspost_count += 1
                # The parent data is usually the first item in the list
                parent_data = crosspost_parent_list[0]
                if isinstance(parent_data, dict):
                    source_sub = parent_data.get("subreddit")
                    if source_sub and isinstance(source_sub, str):
                        source_sub_counter[source_sub] += 1
                    else:
                         logging.debug(f"         Crosspost {item_id} parent data missing 'subreddit' key.")
                         source_sub_counter["_UnknownSource"] += 1
                else:
                    logging.debug(f"         Crosspost {item_id} parent item is not a dictionary.")
                    source_sub_counter["_InvalidParentData"] += 1
            # else: Not a crosspost (key missing or list empty)

        except Exception as e:
            logging.warning(f"      {YELLOW}⚠️ Error processing post {item_id} for crosspost stats: {e}{RESET}")
            continue # Skip to next post on error

    # Calculate percentage based on total posts *analyzed* (should be same as total_posts here)
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
    """Estimates the ratio of removed/deleted content based on author/body markers."""
    logging.debug("      Calculating removal/deletion stats...")
    posts_removed = 0       # Likely Mod/Admin removal (selftext = '[removed]')
    posts_deleted = 0       # Likely User deletion (author = '[deleted]')
    comments_removed = 0    # Likely Mod/Admin removal (body = '[removed]')
    comments_deleted = 0    # Likely User deletion (author = '[deleted]' or body = '[deleted]')

    posts_data = data.get("t3", {})
    comments_data = data.get("t1", {})
    total_posts = len(posts_data)
    total_comments = len(comments_data)
    analyzed_posts, analyzed_comments = 0, 0

    # Analyze Posts
    for item_id, item in posts_data.items():
        analyzed_posts += 1
        try:
            item_data = item.get("data", {})
            author = item_data.get("author")
            selftext = item_data.get("selftext")

            # Prioritize checking author for user deletion
            if author == "[deleted]":
                posts_deleted += 1
            # Only check selftext if author isn't '[deleted]'
            elif selftext == "[removed]":
                posts_removed += 1
            # Else: Neither marker found

        except Exception as e:
            logging.warning(f"      {YELLOW}⚠️ Error processing post {item_id} for removal/deletion stats: {e}{RESET}")

    # Analyze Comments
    for item_id, item in comments_data.items():
        analyzed_comments += 1
        try:
            item_data = item.get("data", {})
            author = item_data.get("author")
            body = item_data.get("body")

            # Check author first for user deletion
            if author == "[deleted]":
                comments_deleted += 1
            # Check body for removal or potential user deletion placeholder
            elif body == "[removed]":
                comments_removed += 1
            elif body == "[deleted]":
                 # Count body='[deleted]' as user deleted ONLY if author isn't already '[deleted]'
                 # This catches cases where only the body is replaced, but avoids double-counting.
                 if author != "[deleted]":
                     comments_deleted += 1
                 # else: Already counted via author = '[deleted]'

        except Exception as e:
            logging.warning(f"      {YELLOW}⚠️ Error processing comment {item_id} for removal/deletion stats: {e}{RESET}")

    # Calculate percentages based on totals
    posts_removed_perc = (posts_removed / total_posts * 100) if total_posts > 0 else 0
    posts_deleted_perc = (posts_deleted / total_posts * 100) if total_posts > 0 else 0
    comments_removed_perc = (comments_removed / total_comments * 100) if total_comments > 0 else 0
    comments_deleted_perc = (comments_deleted / total_comments * 100) if total_comments > 0 else 0

    results = {
        "total_posts_analyzed": total_posts,
        "posts_content_removed": posts_removed, # Renamed for clarity
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


def _calculate_subreddit_diversity(subreddit_activity_stats):
    """Calculates subreddit diversity using Simpson's Index and optionally Shannon Entropy."""
    logging.debug("      Calculating subreddit diversity...")
    results = {
        "num_subreddits_active_in": 0,
        "simpson_diversity_index": "N/A", # Range 0 (low diversity) to 1 (high diversity)
        "normalized_shannon_entropy": "N/A" # Optional: Range 0 to 1
    }

    # Check if input stats are valid
    if not subreddit_activity_stats or not isinstance(subreddit_activity_stats, dict):
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate subreddit diversity: Missing or invalid 'subreddit_activity_stats'.{RESET}")
        return results

    # Combine post and comment activity per subreddit
    posts_per_sub = subreddit_activity_stats.get('posts_per_subreddit', {})
    comments_per_sub = subreddit_activity_stats.get('comments_per_subreddit', {})
    # Ensure they are counters or dicts before combining
    if not isinstance(posts_per_sub, (dict, Counter)): posts_per_sub = {}
    if not isinstance(comments_per_sub, (dict, Counter)): comments_per_sub = {}
    combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub)

    num_subreddits = len(combined_activity)
    results["num_subreddits_active_in"] = num_subreddits
    total_items = sum(combined_activity.values())

    # Handle edge cases: no activity or activity in only one subreddit
    if total_items == 0:
        logging.debug("         No subreddit activity found for diversity calculation.")
        results["simpson_diversity_index"] = 0.0
        results["normalized_shannon_entropy"] = 0.0
        return results
    if num_subreddits <= 1:
        logging.debug(f"         Activity only in {num_subreddits} subreddit(s), diversity index is 0.")
        results["simpson_diversity_index"] = 0.0
        results["normalized_shannon_entropy"] = 0.0
        return results

    # Calculate Simpson's Index
    try:
        # Simpson's Dominance Index (D) = sum of squares of proportions (p_i)^2
        sum_sq_proportions = sum([(count / total_items) ** 2 for count in combined_activity.values()])
        # Simpson's Diversity Index (1 - D)
        simpson_diversity = 1.0 - sum_sq_proportions
        results["simpson_diversity_index"] = f"{simpson_diversity:.3f}"
    except Exception as e:
        logging.error(f"      ❌ Error calculating Simpson's diversity index: {e}")
        results["simpson_diversity_index"] = "Error" # Mark as error

    # Calculate Normalized Shannon Entropy (Optional, but useful)
    try:
        shannon_entropy = 0.0
        for count in combined_activity.values():
             if count > 0: # Avoid log(0) error
                 proportion = count / total_items
                 shannon_entropy -= proportion * math.log2(proportion)

        # Normalize by max entropy (log2 of number of categories/subreddits)
        # Max entropy is log2(num_subreddits)
        if num_subreddits > 1: # Avoid log2(1) = 0 which leads to division by zero
            max_entropy = math.log2(num_subreddits)
            normalized_shannon = shannon_entropy / max_entropy if max_entropy > 0 else 0
            results["normalized_shannon_entropy"] = f"{normalized_shannon:.3f}"
        else:
             # Should have been caught earlier, but handle defensively
             results["normalized_shannon_entropy"] = 0.0

    except Exception as e:
        logging.error(f"      ❌ Error calculating Shannon entropy: {e}")
        results["normalized_shannon_entropy"] = "Error" # Mark as error

    logging.debug(f"         Subreddit diversity calculated: Simpson={results['simpson_diversity_index']}, Shannon={results['normalized_shannon_entropy']}, NumSubs={num_subreddits}")
    return results


def _calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=[2, 3], top_k=20):
    """Calculates frequency of n-grams (bigrams, trigrams, etc.) from text."""
    # Ensure n_values is a list/tuple of integers > 1
    valid_n_values = [n for n in n_values if isinstance(n, int) and n > 1]
    if not valid_n_values:
        logging.warning(f"      {YELLOW}⚠️ No valid n values (must be integers > 1) provided for n-gram calculation. Skipping.{RESET}")
        return {}
    logging.debug(f"      Calculating n-gram frequency (n={valid_n_values}, top {top_k}, from filtered CSVs)...")

    # Initialize counters for each valid n
    ngram_counters = {n: Counter() for n in valid_n_values}
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate n-gram frequency.")
        # Return expected structure with empty dicts for each n
        return { {2: 'bigrams', 3: 'trigrams'}.get(n, f'{n}grams'): {} for n in valid_n_values }

    total_rows_processed = 0
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for n-grams...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    # Process if text exists and isn't a common placeholder
                    if full_text and full_text.lower() not in ['[deleted]', '[removed]', '[no body]']:
                        # Clean text *with* stopword removal for more meaningful phrases
                        cleaned_words = clean_text(full_text, remove_stopwords=True)
                        # Generate n-grams for each requested n
                        for n in valid_n_values:
                            # Use the helper from core_utils
                            for ngram_tuple in _generate_ngrams(cleaned_words, n):
                                # Join the tuple into a space-separated string for the counter key
                                ngram_counters[n][" ".join(ngram_tuple)] += 1
                    total_rows_processed += 1
        except Exception as e:
            logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for n-gram freq: {e}")

    logging.debug(f"         Finished n-gram freq calculation from {total_rows_processed} total rows.")

    # Prepare results, using standard names (bigrams, trigrams)
    results = {}
    for n in valid_n_values:
        key_name = {2: 'bigrams', 3: 'trigrams'}.get(n, f'{n}grams') # Default key if n > 3
        # Get the top_k most common n-grams for this n
        results[key_name] = dict(ngram_counters[n].most_common(top_k))

    return results


def _calculate_activity_burstiness(data):
    """Analyzes timing between consecutive activities using creation timestamps."""
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
                # Use CREATION time (_get_timestamp with use_edited=False)
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0: # Ensure timestamp is valid and positive
                    all_timestamps.append(ts)
                elif ts == 0:
                     logging.debug(f"         Item {item_id} skipped for burstiness due to zero timestamp.")
            except Exception as e:
                 logging.warning(f"      {YELLOW}⚠️ Error getting timestamp for burstiness ({kind} {item_id}): {e}{RESET}")

    # Need at least two timestamps to calculate an interval
    if len(all_timestamps) < 2:
        logging.debug(f"         Insufficient data points ({len(all_timestamps)}) for burstiness calculation.")
        return results # Not enough data

    all_timestamps.sort() # Sort chronologically

    # Calculate time differences (deltas) between consecutive timestamps
    deltas = [all_timestamps[i] - all_timestamps[i-1] for i in range(1, len(all_timestamps))]

    # Filter out zero or negative deltas (e.g., duplicate timestamps, clock issues)
    deltas = [d for d in deltas if d > 0]

    if not deltas:
         logging.debug("         No valid positive time intervals found after filtering.")
         return results # No valid intervals

    results["num_intervals_analyzed"] = len(deltas)

    try:
        # Calculate statistics using the 'statistics' module
        mean_delta = statistics.mean(deltas)
        results["mean_interval_s"] = round(mean_delta, 1)
        results["mean_interval_formatted"] = _format_timedelta(mean_delta) # Use helper

        median_delta = statistics.median(deltas)
        results["median_interval_s"] = round(median_delta, 1)
        results["median_interval_formatted"] = _format_timedelta(median_delta)

        # Standard deviation requires at least 2 data points (intervals)
        if len(deltas) > 1:
            stdev_delta = statistics.stdev(deltas)
            results["stdev_interval_s"] = round(stdev_delta, 1)
            results["stdev_interval_formatted"] = _format_timedelta(stdev_delta)
        else:
             # Stdev is 0 if there's only one interval
             results["stdev_interval_s"] = 0.0
             results["stdev_interval_formatted"] = _format_timedelta(0.0)

        # Min and Max intervals
        min_delta = min(deltas)
        results["min_interval_s"] = round(min_delta, 1)
        results["min_interval_formatted"] = _format_timedelta(min_delta)

        max_delta = max(deltas)
        results["max_interval_s"] = round(max_delta, 1)
        results["max_interval_formatted"] = _format_timedelta(max_delta)

        logging.debug(f"         Activity burstiness calculated: Mean={results['mean_interval_formatted']}, Stdev={results['stdev_interval_formatted']}")

    except statistics.StatisticsError as stat_err:
         logging.error(f"      ❌ Error calculating burstiness statistics (likely insufficient data for stdev): {stat_err}")
         # Keep calculated values if possible, mark others N/A
         if "mean_interval_s" not in results or results["mean_interval_s"] == "N/A": # Check if mean failed too
             for key in results:
                 if "num_intervals" not in key: results[key] = "N/A"
         results["stdev_interval_s"] = "Error"
         results["stdev_interval_formatted"] = "Error"
    except Exception as e:
        logging.error(f"      ❌ Unexpected error calculating burstiness statistics: {e}", exc_info=True)
        # Reset potentially partially calculated values to N/A
        for key in results:
            if "num_intervals" not in key: results[key] = "N/A"
        results["num_intervals_analyzed"] = len(deltas) # Keep interval count if possible

    return results