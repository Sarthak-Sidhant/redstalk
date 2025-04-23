# stats/calculations.py
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
from .core_utils import clean_text, _get_timestamp, _format_timedelta, _generate_ngrams, STOP_WORDS, CYAN, RESET, BOLD, YELLOW, RED, GREEN # Added GREEN

# --- Import from modules OUTSIDE the 'stats' package ---
try:
    from reddit_utils import format_timestamp
except ImportError:
    logging.critical(f"{BOLD}{RED}❌ Critical Error: Failed to import 'format_timestamp' from reddit_utils.py needed by calculations.{RESET}")
    def format_timestamp(ts): return "TIMESTAMP_ERROR"

# --- VADER Dependency ---
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    vader_available = True
    logging.debug("VADER SentimentIntensityAnalyzer imported successfully.")
except ImportError:
    vader_available = False
    SentimentIntensityAnalyzer = None # Ensure it's None if import fails
    logging.warning(f"{YELLOW}⚠️ VADER sentiment library not found. Sentiment analysis (ratio, arc) will be skipped.{RESET}")

# --- Pandas Dependency ---
try:
    import pandas as pd
    pandas_available = True
    logging.debug("Pandas library imported successfully.")
except ImportError:
    pandas_available = False
    pd = None # Ensure pd is None if import fails
    logging.warning(f"{YELLOW}⚠️ Pandas library not found. Calculations requiring CSV reading via Pandas (Question Ratio, Mention Frequency) will be skipped.{RESET}")
    logging.warning(f"{YELLOW}   To enable, run: pip install pandas{RESET}")


# --- Calculation Helpers ---
# Note: Removed data loading/filtering functions - they belong in single_report.py

def _calculate_basic_counts(data):
    # (Function unchanged from original)
    logging.debug("      Calculating basic counts...")
    return {"total_posts": len(data.get("t3", {})), "total_comments": len(data.get("t1", {}))}

def _calculate_time_range(data):
    # (Function unchanged from original)
    logging.debug("      Calculating time range (based on creation time of filtered items)...")
    all_timestamps = [_get_timestamp(item.get("data",{}), use_edited=False)
                      for kind in ["t3", "t1"]
                      for item_id, item in data.get(kind, {}).items()
                      if _get_timestamp(item.get("data",{}), use_edited=False) > 0]
    if not all_timestamps:
        return {"first_activity": None, "last_activity": None, "first_activity_ts": 0, "last_activity_ts": 0}
    min_ts, max_ts = min(all_timestamps), max(all_timestamps)
    return {"first_activity": format_timestamp(min_ts), "last_activity": format_timestamp(max_ts),
            "first_activity_ts": min_ts, "last_activity_ts": max_ts}

def _calculate_subreddit_activity(data):
    # (Function unchanged from original)
    logging.debug("      Calculating subreddit activity...")
    post_subs, comment_subs = Counter(), Counter()
    posted_set, commented_set = set(), set()
    for item_id, item in data.get("t3", {}).items():
        try:
            subreddit = item.get("data", {}).get("subreddit")
            if subreddit and isinstance(subreddit, str):
                post_subs[subreddit] += 1
                posted_set.add(subreddit)
        except AttributeError:
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
    # (Function largely unchanged from original, but ensures CSV is still used here)
    logging.debug("      Calculating text stats (from filtered CSVs)...")
    stats = {"total_post_words": 0, "total_comment_words": 0, "num_posts_with_text": 0, "num_comments_with_text": 0} # Removed all_words
    valid_csv_found = False

    if posts_csv_path and os.path.exists(posts_csv_path):
        valid_csv_found = True
        logging.debug(f"         Reading post text from {CYAN}{posts_csv_path}{RESET}")
        try:
            with open(posts_csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    title = row.get('title', '')
                    selftext = row.get('selftext', '').replace('<br>', ' ')
                    full_text = f"{title} {selftext}".strip()
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]'):
                        words = clean_text(full_text, False);
                        stats["total_post_words"] += len(words);
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
                    body = row.get('body', '').replace('<br>', ' ').strip()
                    if body and body.lower() not in ('[no body]', '[deleted]', '[removed]'):
                        words = clean_text(body, False);
                        stats["total_comment_words"] += len(words);
                        stats["num_comments_with_text"] += 1
        except Exception as e: logging.error(f"      ❌ Error reading comments CSV {CYAN}{comments_csv_path}{RESET} for stats: {e}")
    else: logging.debug(f"         Comments CSV for text stats not found or not provided: {CYAN}{comments_csv_path}{RESET}")

    if not valid_csv_found:
        logging.warning("      ⚠️ No valid CSV files found for text stats calculation.")
        return {"total_words": 0, "total_post_words": 0, "total_comment_words": 0,
                "total_unique_words": 0, "lexical_diversity": "N/A",
                "avg_post_word_length": "N/A", "avg_comment_word_length": "N/A"}

    # --- Calculate unique words separately ---
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
                         words = clean_text(full_text, False)
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
    # (Function unchanged from original, uses CSV module)
    logging.debug(f"      Calculating word frequency (top {top_n}, from filtered CSVs)...")
    word_counter = Counter()
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))

    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate word frequency.")
        return {"word_frequency": {}}

    total_rows_processed = 0
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for word frequency...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                     full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                     if full_text and full_text.lower() not in ['[deleted]', '[removed]', '[no body]']:
                        word_counter.update(clean_text(full_text, True))
                     total_rows_processed += 1
        except Exception as e: logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for word freq: {e}")
    logging.debug(f"         Finished word freq calculation from {total_rows_processed} total rows.")
    return {"word_frequency": dict(word_counter.most_common(top_n))}


def _calculate_post_types(data):
    # (Function unchanged from original)
    logging.debug("      Calculating post types...")
    link_p, self_p, unknown_p = 0, 0, 0
    for item_id, item in data.get("t3", {}).items():
         try:
             is_self = item.get("data", {}).get("is_self")
             if is_self is True: self_p += 1
             elif is_self is False: link_p += 1
             else: unknown_p += 1
         except Exception as e:
             logging.warning(f"      ⚠️ Error processing post {item_id} for type: {e}")
             unknown_p += 1; continue
    if unknown_p > 0: logging.warning(f"      ⚠️ Found {unknown_p} posts with unknown or error in 'is_self' field.")
    return { "link_posts": link_p, "self_posts": self_p }


def _calculate_engagement_stats(data, about_data):
    # (Function unchanged from original)
    logging.debug("      Calculating engagement stats (item scores & overall karma)...")
    post_scores, comment_scores = [], []
    for item_id, item in data.get("t3", {}).items():
        try:
            score = item.get("data", {}).get("score")
            if score is not None: post_scores.append(int(score))
        except (AttributeError, ValueError, TypeError, KeyError): logging.debug(f"         Could not parse score for post {item_id}."); pass
    for item_id, item in data.get("t1", {}).items():
        try:
            score = item.get("data", {}).get("score")
            if score is not None: comment_scores.append(int(score))
        except (AttributeError, ValueError, TypeError, KeyError): logging.debug(f"         Could not parse score for comment {item_id}."); pass
    total_post_score, total_comment_score = sum(post_scores), sum(comment_scores)
    avg_post_score = (total_post_score / len(post_scores)) if post_scores else 0
    avg_comment_score = (total_comment_score / len(comment_scores)) if comment_scores else 0
    total_link_karma, total_comment_karma_about, total_karma = "N/A", "N/A", "N/A"
    if about_data and isinstance(about_data, dict):
        logging.debug("         Using fetched 'about' data for karma.")
        total_link_karma = about_data.get("link_karma", "N/A")
        total_comment_karma_about = about_data.get("comment_karma", "N/A")
        lk_int = int(total_link_karma) if isinstance(total_link_karma, int) else None
        ck_int = int(total_comment_karma_about) if isinstance(total_comment_karma_about, int) else None
        if lk_int is not None and ck_int is not None: total_karma = lk_int + ck_int
        elif lk_int is not None: total_karma = lk_int
        elif ck_int is not None: total_karma = ck_int
    else: logging.debug("         'About' data unavailable or invalid for total karma.")
    return { "total_item_post_score": total_post_score, "total_item_comment_score": total_comment_score,
             "avg_item_post_score": f"{avg_post_score:.1f}", "avg_item_comment_score": f"{avg_comment_score:.1f}",
             "total_link_karma": total_link_karma, "total_comment_karma": total_comment_karma_about,
             "total_combined_karma": total_karma }


def _calculate_temporal_stats(data):
    # (Function unchanged from original)
    logging.debug("      Calculating temporal stats (based on creation time of filtered items)...")
    hour_counter, weekday_counter, month_counter, year_counter = Counter(), Counter(), Counter(), Counter()
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    months_map = {i: f"{i:02d}" for i in range(1, 13)}
    items_processed = 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0:
                    dt = datetime.fromtimestamp(ts, timezone.utc)
                    hour_counter[dt.hour] += 1
                    weekday_counter[dt.weekday()] += 1
                    month_key = (dt.year, dt.month)
                    month_counter[month_key] += 1
                    year_counter[dt.year] += 1
                    items_processed += 1
                elif ts == 0: logging.debug(f"      Skipping item {item_id} for temporal stats due to zero timestamp.")
            except Exception as e: logging.warning(f"      ⚠️ Error processing timestamp for temporal stats ({kind} {item_id}): {e}")
    hours_sorted = {f"{hour:02d}": hour_counter.get(hour, 0) for hour in range(24)}
    weekdays_sorted = {days[i]: weekday_counter.get(i, 0) for i in range(7)}
    months_activity = {f"{yr}-{months_map[mn]}": month_counter[key] for key in sorted(month_counter.keys()) for yr, mn in [key]}
    years_activity = {yr: year_counter[yr] for yr in sorted(year_counter.keys())}
    logging.debug(f"      Finished temporal stats calculation ({items_processed} items).")
    return { "activity_by_hour_utc": hours_sorted, "activity_by_weekday_utc": weekdays_sorted,
             "activity_by_month_utc": months_activity, "activity_by_year_utc": years_activity,
             "total_items_for_temporal": items_processed }


def _calculate_score_stats(data, top_n=5):
    # (Function unchanged from original)
    logging.debug(f"      Calculating score stats (distribution, top/bottom {top_n})...")
    post_details, comment_details = [], []
    for item_id, item in data.get("t3", {}).items():
        try:
            d = item.get("data", {}); s = d.get("score"); l = d.get("permalink"); t = d.get("title", "[No Title]")
            if isinstance(s, int) and isinstance(l, str) and l: post_details.append((s, l, t))
            elif s is not None or l is not None: logging.debug(f"         Skipping post {item_id} for score stats (invalid score type '{type(s)}' or missing permalink '{l}').")
        except Exception as e: logging.warning(f"      ⚠️ Error processing post {item_id} for score stats: {e}"); continue
    for item_id, item in data.get("t1", {}).items():
        try:
            d = item.get("data", {}); s = d.get("score"); l = d.get("permalink"); b = d.get("body", "[No Body]")
            if isinstance(s, int) and isinstance(l, str) and l:
                 snippet = b[:80].replace('\n',' ') + ("..." if len(b)>80 else "")
                 comment_details.append((s, l, snippet))
            elif s is not None or l is not None: logging.debug(f"         Skipping comment {item_id} for score stats (invalid score type '{type(s)}' or missing permalink '{l}').")
        except Exception as e: logging.warning(f"      ⚠️ Error processing comment {item_id} for score stats: {e}"); continue
    post_details.sort(key=lambda x: x[0], reverse=True); comment_details.sort(key=lambda x: x[0], reverse=True)
    post_scores = [item[0] for item in post_details]; comment_scores = [item[0] for item in comment_details]
    def get_score_distribution(scores):
        n = len(scores); dist = {"count": n, "min": "N/A", "max": "N/A", "average": "N/A", "median": "N/A", "q1": "N/A", "q3": "N/A"}
        if not scores: return dist
        scores.sort(); dist["min"] = scores[0]; dist["max"] = scores[-1]; dist["average"] = f"{(sum(scores) / n):.1f}"
        try:
            dist["median"] = statistics.median(scores)
            if n >= 4: quantiles = statistics.quantiles(scores, n=4); dist["q1"] = quantiles[0]; dist["q3"] = quantiles[2]
            elif n > 1: dist["q1"] = scores[0]; dist["q3"] = scores[-1]
            else: dist["q1"] = scores[0]; dist["q3"] = scores[0]
        except AttributeError:
             logging.warning("      ⚠️ statistics.quantiles not available, using simple median calculation."); dist["median"] = statistics.median(scores)
             if n >= 4: dist["q1"] = scores[max(0, math.ceil(n * 0.25) - 1)]; dist["q3"] = scores[min(n - 1, math.ceil(n * 0.75) - 1)]
             elif n > 1: dist["q1"] = scores[0]; dist["q3"] = scores[-1]
             else: dist["q1"] = scores[0]; dist["q3"] = scores[0]
        except Exception as e: logging.error(f"      ❌ Error calculating score distribution quantiles: {e}"); dist["median"] = "Error"; dist["q1"] = "Error"; dist["q3"] = "Error"
        for k in ["q1", "median", "q3"]:
            if isinstance(dist[k], (int, float)): dist[k] = f"{dist[k]:.1f}"
        return dist
    safe_top_n = max(0, top_n)
    return { "post_score_distribution": get_score_distribution(post_scores), "comment_score_distribution": get_score_distribution(comment_scores),
             "top_posts": post_details[:safe_top_n], "bottom_posts": post_details[-(safe_top_n):][::-1] if safe_top_n > 0 else [],
             "top_comments": comment_details[:safe_top_n], "bottom_comments": comment_details[-(safe_top_n):][::-1] if safe_top_n > 0 else [] }


def _calculate_award_stats(data):
    # (Function unchanged from original)
    logging.debug("      Calculating award stats...")
    total_awards, items_with_awards = 0, 0
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                awards = item.get("data", {}).get("total_awards_received", 0)
                if isinstance(awards, int) and awards > 0: total_awards += awards; items_with_awards += 1
                elif awards is not None and awards != 0: logging.debug(f"         Item {item_id} has non-integer award count: {awards}")
            except Exception as e: logging.warning(f"      ⚠️ Error processing item {item_id} for award stats: {e}"); continue
    return { "total_awards_received": total_awards, "items_with_awards": items_with_awards }


def _calculate_flair_stats(data):
    # (Function unchanged from original)
    logging.debug("      Calculating flair stats...")
    user_flairs, post_flairs = Counter(), Counter(); comments_with_user_flair, posts_with_link_flair = 0, 0
    processed_comments, processed_posts = 0, 0
    for item_id, item in data.get("t1", {}).items():
        processed_comments += 1
        try:
            d = item.get("data", {}); sub = d.get("subreddit"); flair = d.get("author_flair_text")
            if sub and flair and isinstance(flair, str) and flair.strip(): user_flairs[f"{sub}: {flair.strip()}"] += 1; comments_with_user_flair += 1
        except Exception as e: logging.warning(f"      ⚠️ Error processing comment {item_id} for user flair: {e}"); continue
    for item_id, item in data.get("t3", {}).items():
        processed_posts += 1
        try:
            d = item.get("data", {}); sub = d.get("subreddit"); flair = d.get("link_flair_text")
            if sub and flair and isinstance(flair, str) and flair.strip(): post_flairs[f"{sub}: {flair.strip()}"] += 1; posts_with_link_flair += 1
        except Exception as e: logging.warning(f"      ⚠️ Error processing post {item_id} for link flair: {e}"); continue
    logging.debug(f"         Processed {processed_comments} comments ({comments_with_user_flair} user flair), {processed_posts} posts ({posts_with_link_flair} link flair).")
    return { "user_flairs_by_sub": dict(user_flairs.most_common()), "post_flairs_by_sub": dict(post_flairs.most_common()),
             "total_comments_with_user_flair": comments_with_user_flair, "total_posts_with_link_flair": posts_with_link_flair }


def _calculate_post_engagement(data):
    # (Function unchanged from original)
    logging.debug("      Calculating post engagement (num_comments)...")
    comment_counts = []; top_commented_posts = []
    posts_analyzed = 0; total_posts_in_data = len(data.get("t3", {}))
    for item_id, item in data.get("t3", {}).items():
        posts_analyzed += 1
        try:
            d = item.get("data", {}); num_comments = d.get("num_comments"); permalink = d.get("permalink"); title = d.get("title", "[No Title]")
            if isinstance(num_comments, int) and num_comments >= 0 and isinstance(permalink, str) and permalink:
                 comment_counts.append(num_comments); top_commented_posts.append((num_comments, permalink, title))
            elif num_comments is not None or permalink is not None: logging.debug(f"         Post {item_id} skipped for engagement (invalid 'num_comments': {num_comments} or missing permalink: {permalink})")
        except Exception as e: logging.warning(f"      ⚠️ Error processing post {item_id} for engagement stats: {e}"); continue
    if not comment_counts:
        logging.warning(f"      ⚠️ No valid posts found for comment engagement analysis (out of {total_posts_in_data} total posts).")
        return {"avg_comments_per_post": "0.0", "total_posts_analyzed_for_comments": 0, "top_commented_posts": []}
    avg_comments = sum(comment_counts) / len(comment_counts)
    top_commented_posts.sort(key=lambda x: x[0], reverse=True)
    return { "avg_comments_per_post": f"{avg_comments:.1f}", "total_posts_analyzed_for_comments": len(comment_counts), "top_commented_posts": top_commented_posts[:5] }


def _calculate_editing_stats(data):
    # (Function unchanged from original)
    logging.debug("      Calculating editing stats...")
    posts_edited, comments_edited = 0, 0; total_posts = len(data.get("t3", {})); total_comments = len(data.get("t1", {}))
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
                    except (ValueError, TypeError, KeyError) as convert_err: logging.debug(f"         Could not parse created/edited timestamp for edit stat on {kind} {item_id}: {convert_err}"); pass
            except Exception as e: logging.warning(f"      ⚠️ Error processing item {item_id} for editing stats: {e}"); pass
    edit_percent_posts = (posts_edited / total_posts * 100) if total_posts > 0 else 0
    edit_percent_comments = (comments_edited / total_comments * 100) if total_comments > 0 else 0
    avg_delay_s = (sum(edit_delays_s) / len(edit_delays_s)) if edit_delays_s else 0
    avg_delay_str = _format_timedelta(avg_delay_s)
    return { "posts_edited_count": posts_edited, "comments_edited_count": comments_edited, "total_posts_analyzed_for_edits": total_posts,
             "total_comments_analyzed_for_edits": total_comments, "edit_percentage_posts": f"{edit_percent_posts:.1f}%", "edit_percentage_comments": f"{edit_percent_comments:.1f}%",
             "average_edit_delay_seconds": round(avg_delay_s, 1), "average_edit_delay_formatted": avg_delay_str }


def _calculate_sentiment_ratio(posts_csv_path, comments_csv_path):
    # (Function unchanged from original)
    logging.debug("      Calculating sentiment ratio (VADER, from filtered CSVs)...")
    global vader_available
    if not vader_available: return {"sentiment_analysis_skipped": True, "reason": "VADER library not installed or import failed"}
    if not SentimentIntensityAnalyzer:
         if vader_available: logging.warning(f"{YELLOW} VADER lib import seemed ok, but class unavailable. Skipping.{RESET}"); vader_available = False
         return {"sentiment_analysis_skipped": True, "reason": "VADER Analyzer class unavailable"}
    try:
        analyzer = SentimentIntensityAnalyzer()
        logging.debug("         VADER SentimentIntensityAnalyzer initialized for ratio.")
    except Exception as e:
        logging.error(f"    ❌ Failed to initialize VADER SentimentIntensityAnalyzer: {e}", exc_info=True); vader_available = False
        return {"sentiment_analysis_skipped": True, "reason": "VADER Analyzer initialization failed"}
    pos_count, neg_count, neu_count = 0, 0, 0; total_analyzed = 0; sentiment_scores = []
    files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))
    if not files:
        logging.warning("      ⚠️ No CSV files found to calculate sentiment ratio.")
        return {"sentiment_analysis_skipped": False, "positive_count": 0, "negative_count": 0, "neutral_count": 0,
                "total_items_sentiment_analyzed": 0, "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}
    for file_path, cols in files:
        logging.debug(f"         Analyzing sentiment ratio in {CYAN}{file_path}{RESET}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f);
                for i, row in enumerate(reader):
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    if full_text and full_text.lower() not in ('[no body]', '[deleted]', '[removed]', '[no title]'):
                        try:
                            vs = analyzer.polarity_scores(full_text)
                            sentiment_scores.append(vs['compound'])
                            if vs['compound'] >= 0.05: pos_count += 1
                            elif vs['compound'] <= -0.05: neg_count += 1
                            else: neu_count += 1
                            total_analyzed += 1
                        except Exception as vader_err: logging.warning(f"{YELLOW} VADER error processing row {i+1} in {os.path.basename(file_path)}: {vader_err}{RESET}"); continue
        except Exception as e: logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for sentiment ratio: {e}")
    if total_analyzed == 0:
        logging.warning("      ⚠️ No valid text items found in CSVs for sentiment ratio.")
        return {"sentiment_analysis_skipped": False, "positive_count": 0, "negative_count": 0, "neutral_count": 0,
                "total_items_sentiment_analyzed": 0, "pos_neg_ratio": "N/A", "avg_compound_score": "N/A"}
    pos_neg_ratio = f"{(pos_count / neg_count):.2f}:1" if neg_count > 0 else (f"{pos_count}:0" if pos_count > 0 else "N/A")
    avg_compound = sum(sentiment_scores) / total_analyzed if total_analyzed > 0 else 0
    return { "sentiment_analysis_skipped": False, "positive_count": pos_count, "negative_count": neg_count, "neutral_count": neu_count,
             "total_items_sentiment_analyzed": total_analyzed, "pos_neg_ratio": pos_neg_ratio, "avg_compound_score": f"{avg_compound:.3f}" }


def _calculate_age_vs_activity(about_data, temporal_stats):
    # (Function unchanged from original)
    logging.debug("      Calculating account age vs activity...")
    results = { "account_created_utc": None, "account_created_formatted": "N/A", "account_age_days": "N/A",
                "total_activity_items": 0, "average_activity_per_year": "N/A", "average_activity_per_month": "N/A",
                "activity_trend_status": "N/A" }
    if not about_data or not isinstance(about_data, dict) or "created_utc" not in about_data:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity: Missing or invalid 'about_data'.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Account Info)"; return results
    try:
        created_ts = float(about_data["created_utc"]); results["account_created_utc"] = created_ts
        results["account_created_formatted"] = format_timestamp(created_ts)
        now_ts = datetime.now(timezone.utc).timestamp(); age_seconds = now_ts - created_ts
        if age_seconds < 0: logging.warning(f"      {YELLOW}⚠️ Account creation timestamp ({created_ts}) is in the future? Setting age to 0.{RESET}"); age_days = 0
        else: age_days = age_seconds / 86400
        results["account_age_days"] = round(age_days, 1)
    except (ValueError, TypeError) as e:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity: Invalid 'created_utc' in about_data: {e}.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (Invalid Creation Time)"; return results
    if not temporal_stats or not isinstance(temporal_stats, dict) or "total_items_for_temporal" not in temporal_stats:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity trends: Missing or invalid 'temporal_stats'.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Temporal Stats)"; return results
    total_items = temporal_stats["total_items_for_temporal"]; results["total_activity_items"] = total_items
    activity_by_year = temporal_stats.get("activity_by_year_utc", {})
    if total_items == 0: logging.debug("         No activity items found in temporal stats for trend analysis."); results["activity_trend_status"] = "No Activity Found"; return results
    if age_days > 0:
        age_years = age_days / 365.25; results["average_activity_per_year"] = f"{total_items / age_years:.1f}" if age_years > 0 else "N/A"
        age_months = age_days / (365.25 / 12); results["average_activity_per_month"] = f"{total_items / age_months:.1f}" if age_months > 0 else "N/A"
    else: logging.debug("         Account age is zero or negative, cannot calculate average rates.")
    if not activity_by_year:
        logging.warning(f"      {YELLOW}⚠️ Cannot calculate age vs activity trends: Missing 'activity_by_year_utc' in temporal_stats.{RESET}")
        results["activity_trend_status"] = "Insufficient Data (No Yearly Breakdown)"; return results
    sorted_years = sorted(activity_by_year.keys())
    if len(sorted_years) < 2: results["activity_trend_status"] = "Insufficient Data (Less than 2 years of activity)"
    else:
        first_year = sorted_years[0]; last_year = sorted_years[-1]
        total_years_span = last_year - first_year + 1
        if total_years_span < 2: results["activity_trend_status"] = "Insufficient Data (Activity Span < 2 Years)"
        else:
            mid_point_year = first_year + total_years_span // 2
            activity_first_half = sum(count for year, count in activity_by_year.items() if year < mid_point_year)
            activity_second_half = sum(count for year, count in activity_by_year.items() if year >= mid_point_year)
            num_years_first = mid_point_year - first_year; num_years_second = last_year - mid_point_year + 1
            rate_first = (activity_first_half / num_years_first) if num_years_first > 0 else 0
            rate_second = (activity_second_half / num_years_second) if num_years_second > 0 else 0
            if rate_second > rate_first * 1.2: results["activity_trend_status"] = "Increasing"
            elif rate_first > rate_second * 1.2: results["activity_trend_status"] = "Decreasing"
            elif rate_first == 0 and rate_second == 0: results["activity_trend_status"] = "No Activity Found"
            else: results["activity_trend_status"] = "Stable / Fluctuating"
    logging.debug(f"         Age vs Activity calculated: Age={results['account_age_days']}d, Trend={results['activity_trend_status']}")
    return results


def _calculate_crosspost_stats(data):
    # (Function unchanged from original)
    logging.debug("      Calculating crosspost stats...")
    crosspost_count, analyzed_posts = 0, 0; source_sub_counter = Counter()
    posts_data = data.get("t3", {}); total_posts = len(posts_data)
    if total_posts == 0: logging.debug("         No posts found in filtered data to analyze for crossposts."); return { "total_posts_analyzed": 0, "crosspost_count": 0, "crosspost_percentage": "0.0%", "source_subreddits": {} }
    for item_id, item in posts_data.items():
        analyzed_posts += 1
        try:
            item_data = item.get("data", {}); crosspost_parent_list = item_data.get("crosspost_parent_list")
            if isinstance(crosspost_parent_list, list) and len(crosspost_parent_list) > 0:
                crosspost_count += 1; parent_data = crosspost_parent_list[0]
                if isinstance(parent_data, dict):
                    source_sub = parent_data.get("subreddit")
                    if source_sub and isinstance(source_sub, str): source_sub_counter[source_sub] += 1
                    else: logging.debug(f"         Crosspost {item_id} parent data missing 'subreddit' key."); source_sub_counter["_UnknownSource"] += 1
                else: logging.debug(f"         Crosspost {item_id} parent item is not a dictionary."); source_sub_counter["_InvalidParentData"] += 1
        except Exception as e: logging.warning(f"      {YELLOW}⚠️ Error processing post {item_id} for crosspost stats: {e}{RESET}"); continue
    crosspost_percentage = (crosspost_count / total_posts * 100) if total_posts > 0 else 0
    results = { "total_posts_analyzed": total_posts, "crosspost_count": crosspost_count, "crosspost_percentage": f"{crosspost_percentage:.1f}%", "source_subreddits": dict(source_sub_counter.most_common(10)) }
    logging.debug(f"         Crosspost stats calculated: {crosspost_count}/{total_posts} crossposts.")
    return results


def _calculate_removal_deletion_stats(data):
    # (Function unchanged from original)
    logging.debug("      Calculating removal/deletion stats...")
    posts_removed, posts_deleted, comments_removed, comments_deleted = 0, 0, 0, 0
    posts_data = data.get("t3", {}); comments_data = data.get("t1", {})
    total_posts = len(posts_data); total_comments = len(comments_data)
    analyzed_posts, analyzed_comments = 0, 0
    for item_id, item in posts_data.items():
        analyzed_posts += 1
        try:
            item_data = item.get("data", {}); author = item_data.get("author"); selftext = item_data.get("selftext")
            if author == "[deleted]": posts_deleted += 1
            elif selftext == "[removed]": posts_removed += 1
        except Exception as e: logging.warning(f"      {YELLOW}⚠️ Error processing post {item_id} for removal/deletion stats: {e}{RESET}")
    for item_id, item in comments_data.items():
        analyzed_comments += 1
        try:
            item_data = item.get("data", {}); author = item_data.get("author"); body = item_data.get("body")
            if author == "[deleted]": comments_deleted += 1
            elif body == "[removed]": comments_removed += 1
            elif body == "[deleted]" and author != "[deleted]": comments_deleted += 1
        except Exception as e: logging.warning(f"      {YELLOW}⚠️ Error processing comment {item_id} for removal/deletion stats: {e}{RESET}")
    posts_removed_perc = (posts_removed / total_posts * 100) if total_posts > 0 else 0
    posts_deleted_perc = (posts_deleted / total_posts * 100) if total_posts > 0 else 0
    comments_removed_perc = (comments_removed / total_comments * 100) if total_comments > 0 else 0
    comments_deleted_perc = (comments_deleted / total_comments * 100) if total_comments > 0 else 0
    results = { "total_posts_analyzed": total_posts, "posts_content_removed": posts_removed, "posts_user_deleted": posts_deleted,
                "posts_content_removed_percentage": f"{posts_removed_perc:.1f}%", "posts_user_deleted_percentage": f"{posts_deleted_perc:.1f}%",
                "total_comments_analyzed": total_comments, "comments_content_removed": comments_removed, "comments_user_deleted": comments_deleted,
                "comments_content_removed_percentage": f"{comments_removed_perc:.1f}%", "comments_user_deleted_percentage": f"{comments_deleted_perc:.1f}%", }
    logging.debug(f"         Removal/Deletion stats calculated: P_rem={posts_removed}, P_del={posts_deleted}, C_rem={comments_removed}, C_del={comments_deleted}")
    return results


def _calculate_subreddit_diversity(subreddit_activity_stats):
    # (Function unchanged from original)
    logging.debug("      Calculating subreddit diversity...")
    results = { "num_subreddits_active_in": 0, "simpson_diversity_index": "N/A", "normalized_shannon_entropy": "N/A" }
    if not subreddit_activity_stats or not isinstance(subreddit_activity_stats, dict): logging.warning(f"      {YELLOW}⚠️ Cannot calculate subreddit diversity: Missing or invalid 'subreddit_activity_stats'.{RESET}"); return results
    posts_per_sub = subreddit_activity_stats.get('posts_per_subreddit', {}); comments_per_sub = subreddit_activity_stats.get('comments_per_subreddit', {})
    if not isinstance(posts_per_sub, (dict, Counter)): posts_per_sub = {}
    if not isinstance(comments_per_sub, (dict, Counter)): comments_per_sub = {}
    combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub)
    num_subreddits = len(combined_activity); results["num_subreddits_active_in"] = num_subreddits
    total_items = sum(combined_activity.values())
    if total_items == 0: logging.debug("         No subreddit activity found for diversity calculation."); results["simpson_diversity_index"] = 0.0; results["normalized_shannon_entropy"] = 0.0; return results
    if num_subreddits <= 1: logging.debug(f"         Activity only in {num_subreddits} subreddit(s), diversity index is 0."); results["simpson_diversity_index"] = 0.0; results["normalized_shannon_entropy"] = 0.0; return results
    try:
        sum_sq_proportions = sum([(count / total_items) ** 2 for count in combined_activity.values()])
        simpson_diversity = 1.0 - sum_sq_proportions; results["simpson_diversity_index"] = f"{simpson_diversity:.3f}"
    except Exception as e: logging.error(f"      ❌ Error calculating Simpson's diversity index: {e}"); results["simpson_diversity_index"] = "Error"
    try:
        shannon_entropy = 0.0
        for count in combined_activity.values():
             if count > 0: proportion = count / total_items; shannon_entropy -= proportion * math.log2(proportion)
        if num_subreddits > 1:
            max_entropy = math.log2(num_subreddits); normalized_shannon = shannon_entropy / max_entropy if max_entropy > 0 else 0
            results["normalized_shannon_entropy"] = f"{normalized_shannon:.3f}"
        else: results["normalized_shannon_entropy"] = 0.0
    except Exception as e: logging.error(f"      ❌ Error calculating Shannon entropy: {e}"); results["normalized_shannon_entropy"] = "Error"
    logging.debug(f"         Subreddit diversity calculated: Simpson={results['simpson_diversity_index']}, Shannon={results['normalized_shannon_entropy']}, NumSubs={num_subreddits}")
    return results


def _calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=[2, 3], top_k=20):
    # (Function unchanged from original, uses CSV module)
    valid_n_values = [n for n in n_values if isinstance(n, int) and n > 1]
    if not valid_n_values: logging.warning(f"      {YELLOW}⚠️ No valid n values provided for n-gram calculation. Skipping.{RESET}"); return {}
    logging.debug(f"      Calculating n-gram frequency (n={valid_n_values}, top {top_k}, from filtered CSVs)...")
    ngram_counters = {n: Counter() for n in valid_n_values}; files = []
    if posts_csv_path and os.path.exists(posts_csv_path): files.append((posts_csv_path, ['title', 'selftext']))
    if comments_csv_path and os.path.exists(comments_csv_path): files.append((comments_csv_path, ['body']))
    if not files: logging.warning("      ⚠️ No CSV files found to calculate n-gram frequency."); return { {2: 'bigrams', 3: 'trigrams'}.get(n, f'{n}grams'): {} for n in valid_n_values }
    total_rows_processed = 0
    for file_path, cols in files:
        logging.debug(f"         Processing {CYAN}{file_path}{RESET} for n-grams...")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    full_text = " ".join(row.get(col, '').replace('<br>', ' ') for col in cols).strip()
                    if full_text and full_text.lower() not in ['[deleted]', '[removed]', '[no body]']:
                        cleaned_words = clean_text(full_text, remove_stopwords=True)
                        for n in valid_n_values:
                            for ngram_tuple in _generate_ngrams(cleaned_words, n):
                                ngram_counters[n][" ".join(ngram_tuple)] += 1
                    total_rows_processed += 1
        except Exception as e: logging.error(f"      ❌ Error reading {CYAN}{file_path}{RESET} for n-gram freq: {e}")
    logging.debug(f"         Finished n-gram freq calculation from {total_rows_processed} total rows.")
    results = {}
    for n in valid_n_values:
        key_name = {2: 'bigrams', 3: 'trigrams'}.get(n, f'{n}grams')
        results[key_name] = dict(ngram_counters[n].most_common(top_k))
    return results


def _calculate_activity_burstiness(data):
    # (Function unchanged from original)
    logging.debug("      Calculating activity burstiness...")
    results = { "mean_interval_s": "N/A", "mean_interval_formatted": "N/A", "median_interval_s": "N/A", "median_interval_formatted": "N/A",
                "stdev_interval_s": "N/A", "stdev_interval_formatted": "N/A", "min_interval_s": "N/A", "min_interval_formatted": "N/A",
                "max_interval_s": "N/A", "max_interval_formatted": "N/A", "num_intervals_analyzed": 0 }
    all_timestamps = []
    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            try:
                ts = _get_timestamp(item.get("data",{}), use_edited=False)
                if ts and ts > 0: all_timestamps.append(ts)
                elif ts == 0: logging.debug(f"         Item {item_id} skipped for burstiness due to zero timestamp.")
            except Exception as e: logging.warning(f"      {YELLOW}⚠️ Error getting timestamp for burstiness ({kind} {item_id}): {e}{RESET}")
    if len(all_timestamps) < 2: logging.debug(f"         Insufficient data points ({len(all_timestamps)}) for burstiness calculation."); return results
    all_timestamps.sort(); deltas = [all_timestamps[i] - all_timestamps[i-1] for i in range(1, len(all_timestamps))]
    deltas = [d for d in deltas if d > 0]
    if not deltas: logging.debug("         No valid positive time intervals found after filtering."); return results
    results["num_intervals_analyzed"] = len(deltas)
    try:
        mean_delta = statistics.mean(deltas); results["mean_interval_s"] = round(mean_delta, 1); results["mean_interval_formatted"] = _format_timedelta(mean_delta)
        median_delta = statistics.median(deltas); results["median_interval_s"] = round(median_delta, 1); results["median_interval_formatted"] = _format_timedelta(median_delta)
        if len(deltas) > 1:
            stdev_delta = statistics.stdev(deltas); results["stdev_interval_s"] = round(stdev_delta, 1); results["stdev_interval_formatted"] = _format_timedelta(stdev_delta)
        else: results["stdev_interval_s"] = 0.0; results["stdev_interval_formatted"] = _format_timedelta(0.0)
        min_delta = min(deltas); results["min_interval_s"] = round(min_delta, 1); results["min_interval_formatted"] = _format_timedelta(min_delta)
        max_delta = max(deltas); results["max_interval_s"] = round(max_delta, 1); results["max_interval_formatted"] = _format_timedelta(max_delta)
        logging.debug(f"         Activity burstiness calculated: Mean={results['mean_interval_formatted']}, Stdev={results['stdev_interval_formatted']}")
    except statistics.StatisticsError as stat_err:
         logging.error(f"      ❌ Error calculating burstiness statistics (likely insufficient data for stdev): {stat_err}")
         if "mean_interval_s" not in results or results["mean_interval_s"] == "N/A":
             for key in results:
                 if "num_intervals" not in key: results[key] = "N/A"
         results["stdev_interval_s"] = "Error"; results["stdev_interval_formatted"] = "Error"
    except Exception as e:
        logging.error(f"      ❌ Unexpected error calculating burstiness statistics: {e}", exc_info=True)
        for key in results:
            if "num_intervals" not in key: results[key] = "N/A"
        results["num_intervals_analyzed"] = len(deltas)
    return results

# --- NEW: Sentiment Arc ---
def _calculate_sentiment_arc(filtered_data, time_window='monthly'):
    """Calculates the average sentiment score over time windows."""
    logging.debug(f"      Calculating sentiment arc ({time_window})...")
    global vader_available # Use the flag set during import
    results = {"sentiment_arc_data": {}, "analysis_performed": False, "reason": "", "window_type": time_window}

    if not vader_available:
        results["reason"] = "VADER library not installed or import failed"
        return results
    # Check if the class itself is available (it might be None if import failed)
    if not SentimentIntensityAnalyzer:
        results["reason"] = "VADER Analyzer class unavailable"
        # Update flag just in case it was True before but class is None
        if vader_available:
            logging.warning(f"{YELLOW} VADER lib import seemed ok, but class unavailable for arc. Skipping.{RESET}")
            vader_available = False
        return results

    try:
        analyzer = SentimentIntensityAnalyzer()
        logging.debug("         VADER SentimentIntensityAnalyzer initialized for arc.")
    except Exception as e:
        logging.error(f"    ❌ Failed to initialize VADER SentimentIntensityAnalyzer for arc: {e}", exc_info=True)
        # Mark VADER as unavailable for the rest of this run if init fails
        vader_available = False
        results["reason"] = "VADER Analyzer initialization failed"; return results

    sentiment_by_window = defaultdict(list)
    items_processed = 0
    items_with_errors = 0

    for kind in ["t3", "t1"]:
        for item_id, item in filtered_data.get(kind, {}).items():
            try:
                item_data = item.get("data", {})
                # Use CREATION time for arc consistency with temporal stats
                ts = _get_timestamp(item_data, use_edited=False)
                if not (ts and ts > 0): continue # Skip items with invalid timestamp

                text = ""
                if kind == "t1": # Comment
                    text = item_data.get('body', '')
                elif kind == "t3": # Post
                    title = item_data.get('title', '')
                    selftext = item_data.get('selftext', '')
                    text = f"{title} {selftext}".strip()

                text = text.replace('<br>', ' ').strip() # Basic cleaning
                if not text or text.lower() in ('[deleted]', '[removed]', '[no body]', '[no title]'):
                    continue # Skip empty or placeholder text

                # Calculate sentiment
                vs = analyzer.polarity_scores(text)
                compound_score = vs['compound']

                # Determine time window key
                dt = datetime.fromtimestamp(ts, timezone.utc)
                if time_window == 'monthly':
                    window_key = dt.strftime('%Y-%m')
                elif time_window == 'yearly':
                    window_key = dt.strftime('%Y')
                else: # Default to monthly if invalid window specified
                    logging.warning(f"      ⚠️ Invalid time window '{time_window}' for sentiment arc, defaulting to 'monthly'.")
                    window_key = dt.strftime('%Y-%m')
                    time_window = 'monthly' # Correct the type for the results dict

                sentiment_by_window[window_key].append(compound_score)
                items_processed += 1

            except Exception as e:
                logging.warning(f"      ⚠️ Error processing item {item_id} for sentiment arc: {e}")
                items_with_errors += 1
                continue

    if items_processed == 0:
        results["reason"] = "No valid items found in filtered data for analysis"
        logging.warning(f"      ⚠️ No items processed for sentiment arc (Errors: {items_with_errors}).")
        return results

    # Calculate average score per window
    avg_sentiment_arc = {}
    # Sort windows chronologically before calculating average
    sorted_windows = sorted(sentiment_by_window.keys())
    for window in sorted_windows:
        scores = sentiment_by_window[window]
        if scores:
            avg_score = sum(scores) / len(scores)
            avg_sentiment_arc[window] = round(avg_score, 3) # Store avg score rounded
        # No need for else, defaultdict ensures window key exists even if list is empty (which shouldn't happen here)

    results["sentiment_arc_data"] = avg_sentiment_arc
    results["analysis_performed"] = True
    results["window_type"] = time_window # Store the actual window used
    logging.debug(f"         Sentiment arc calculated for {len(avg_sentiment_arc)} windows ({items_processed} items, {items_with_errors} errors).")
    return results


def _calculate_question_ratio(posts_csv_path, comments_csv_path):
    """Calculates the ratio of posts/comments containing a question mark character ('?')."""
    logging.debug("      Calculating question mark presence ratio...")
    global pandas_available, pd # Use flags and imported objects
    results = { "total_items_analyzed": 0, "question_items": 0, "question_ratio": "N/A",
                "analysis_performed": False, "reason": "" }

    # Check dependencies first
    if not pandas_available:
        results["reason"] = "Pandas library missing"
        return results

    # Double check if the actual pandas object is available
    if not pd:
         results["reason"] = "Pandas (pd) unavailable"
         return results

    def _contains_question_mark(text):
        """Checks if the text contains a question mark character."""
        # Check if input is a valid, non-empty string
        if not isinstance(text, str) or not text.strip():
            return False
        # Simple check for the presence of '?'
        return '?' in text

    total_items = 0
    question_items = 0
    files_processed = 0

    # Process Posts CSV
    if posts_csv_path and os.path.exists(posts_csv_path):
        files_processed += 1
        logging.debug(f"         Processing {CYAN}{posts_csv_path}{RESET} for question mark ratio...")
        try:
            # Use pandas read_csv
            df_posts = pd.read_csv(posts_csv_path, usecols=['title', 'selftext'], low_memory=False, encoding='utf-8')
            # Combine title and selftext safely, handling potential NaN values
            df_posts['full_text'] = df_posts['title'].fillna('') + ' ' + df_posts['selftext'].fillna('')
            # Apply the simplified question mark check row-wise
            question_flags = df_posts['full_text'].apply(_contains_question_mark)
            question_items += question_flags.sum()
            total_items += len(df_posts)
            del df_posts # Free up memory
        except FileNotFoundError:
            logging.warning(f"      ⚠️ Posts CSV not found: {CYAN}{posts_csv_path}{RESET}")
        except Exception as e:
            # Log specific pandas errors if helpful, otherwise general error
            logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv_path}{RESET} for question mark ratio: {e}")
            # Optionally add more specific error handling for pandas read errors (e.g., CParserError)

    # Process Comments CSV
    if comments_csv_path and os.path.exists(comments_csv_path):
        files_processed += 1
        logging.debug(f"         Processing {CYAN}{comments_csv_path}{RESET} for question mark ratio...")
        try:
            # Use pandas read_csv
            df_comments = pd.read_csv(comments_csv_path, usecols=['body'], low_memory=False, encoding='utf-8')
            # Ensure 'body' is treated as string and handle NaN
            df_comments['body'] = df_comments['body'].fillna('').astype(str)
            # Apply the simplified question mark check row-wise
            question_flags = df_comments['body'].apply(_contains_question_mark)
            question_items += question_flags.sum()
            total_items += len(df_comments)
            del df_comments # Free up memory
        except FileNotFoundError:
            logging.warning(f"      ⚠️ Comments CSV not found: {CYAN}{comments_csv_path}{RESET}")
        except Exception as e:
            logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv_path}{RESET} for question mark ratio: {e}")

    if files_processed == 0:
        results["reason"] = "No valid CSV files found or provided"
        logging.warning("      ⚠️ No valid CSV files found for question mark ratio analysis.")
        return results

    results["total_items_analyzed"] = total_items
    results["question_items"] = int(question_items) # Ensure it's an int
    if total_items > 0:
        ratio = (question_items / total_items)
        results["question_ratio"] = f"{ratio:.1%}" # Format as percentage with one decimal
    else:
        # If no items were analyzed (e.g., empty CSVs), ratio is 0% if questions=0, else N/A
        results["question_ratio"] = "0.0%" if question_items == 0 else "N/A"

    results["analysis_performed"] = True
    logging.debug(f"         Question mark ratio calculated: {question_items}/{total_items} ({results['question_ratio']}).")
    return results

# --- NEW: Mention Frequency ---
def _calculate_mention_frequency(posts_csv_path, comments_csv_path, top_n=20):
    """Calculates the frequency of user and subreddit mentions using regex."""
    logging.debug(f"      Calculating mention frequency (top {top_n})...")
    global pandas_available, pd # Use flags and imported objects
    results = { "top_user_mentions": {}, "top_subreddit_mentions": {},
                "all_user_mentions": {}, "all_subreddit_mentions": {},
                "total_user_mention_instances": 0, "total_subreddit_mention_instances": 0,
                "analysis_performed": False, "reason": "" }

    if not pandas_available:
        results["reason"] = "Pandas library missing"
        return results
    if not pd: # Double check after availability flag
        results["reason"] = "Pandas (pd) object is None"
        return results

    # Refined Regex patterns (handle boundaries, typical length constraints)
    # Looks for u/ followed by 3-20 word chars (letters, numbers, _-), preceded by space/start/paren/bracket, not followed by word chars
    user_pattern = re.compile(r'(?:(?<=\s)|(?<=^)|(?<=\()|(?<=\[))[uU]/([A-Za-z0-9_-]{3,20})(?![A-Za-z0-9_-])')
    # Looks for r/ followed by 3-21 word chars (letters, numbers, _), preceded by space/start/paren/bracket, not followed by word chars
    sub_pattern = re.compile(r'(?:(?<=\s)|(?<=^)|(?<=\()|(?<=\[))[rR]/([A-Za-z0-9_]{3,21})(?![A-Za-z0-9_])')

    user_mentions = Counter()
    subreddit_mentions = Counter()
    files_processed = 0
    total_user_instances = 0
    total_subreddit_instances = 0

    def process_text_for_mentions(text_series, user_counter, sub_counter):
        """Helper to process a pandas Series of text for mentions."""
        nonlocal total_user_instances, total_subreddit_instances
        if text_series is None or text_series.empty:
            return
        # Ensure text is string and handle potential NaNs
        text_series = text_series.fillna('').astype(str)
        for text in text_series:
            if not text: continue # Skip empty strings efficiently
            try:
                users = user_pattern.findall(text)
                subs = sub_pattern.findall(text)
                # Update counters with lowercase versions
                user_counter.update(u.lower() for u in users)
                sub_counter.update(s.lower() for s in subs)
                total_user_instances += len(users)
                total_subreddit_instances += len(subs)
            except Exception as e:
                 # Log regex errors less frequently
                 logging.debug(f"         ⚠️ Regex error processing text for mentions: {e}")

    # Process Posts CSV
    if posts_csv_path and os.path.exists(posts_csv_path):
        files_processed += 1
        logging.debug(f"         Processing {CYAN}{posts_csv_path}{RESET} for mentions...")
        try:
            df_posts = pd.read_csv(posts_csv_path, usecols=['title', 'selftext'], low_memory=False, encoding='utf-8')
            process_text_for_mentions(df_posts['title'], user_mentions, subreddit_mentions)
            process_text_for_mentions(df_posts['selftext'], user_mentions, subreddit_mentions)
            del df_posts # Free memory
        except FileNotFoundError:
            logging.warning(f"      ⚠️ Posts CSV not found: {CYAN}{posts_csv_path}{RESET}")
        except Exception as e:
            logging.error(f"      ❌ Error processing posts CSV {CYAN}{posts_csv_path}{RESET} for mentions: {e}")

    # Process Comments CSV
    if comments_csv_path and os.path.exists(comments_csv_path):
        files_processed += 1
        logging.debug(f"         Processing {CYAN}{comments_csv_path}{RESET} for mentions...")
        try:
            df_comments = pd.read_csv(comments_csv_path, usecols=['body'], low_memory=False, encoding='utf-8')
            process_text_for_mentions(df_comments['body'], user_mentions, subreddit_mentions)
            del df_comments # Free memory
        except FileNotFoundError:
            logging.warning(f"      ⚠️ Comments CSV not found: {CYAN}{comments_csv_path}{RESET}")
        except Exception as e:
            logging.error(f"      ❌ Error processing comments CSV {CYAN}{comments_csv_path}{RESET} for mentions: {e}")

    if files_processed == 0:
        results["reason"] = "No valid CSV files found or provided"
        logging.warning("      ⚠️ No valid CSV files found for mention frequency analysis.")
        return results

    # Store results
    results["top_user_mentions"] = dict(user_mentions.most_common(top_n))
    results["top_subreddit_mentions"] = dict(subreddit_mentions.most_common(top_n))
    # Convert full counters to dicts for JSON serialization
    results["all_user_mentions"] = dict(user_mentions)
    results["all_subreddit_mentions"] = dict(subreddit_mentions)
    results["total_user_mention_instances"] = total_user_instances
    results["total_subreddit_mention_instances"] = total_subreddit_instances
    results["analysis_performed"] = True

    logging.debug(f"         Mention frequency calculated: Found {len(user_mentions)} unique users ({total_user_instances} instances), {len(subreddit_mentions)} unique subs ({total_subreddit_instances} instances).")
    return results