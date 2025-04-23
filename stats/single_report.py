# stats/single_report.py
import json
import csv
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple

# --- Import from sibling modules within the 'stats' package ---
from . import calculations as calc # Use alias for brevity
from .reporting import _format_report
from .core_utils import _get_timestamp, _format_timedelta, CYAN, RESET, BOLD, YELLOW, RED, GREEN # Added RED, GREEN

# --- Import from modules OUTSIDE the 'stats' package ---
try:
    from reddit_utils import get_modification_date, format_timestamp, _fetch_user_about_data
    reddit_utils_available = True
except ImportError:
    logging.critical(f"{BOLD}{RED}❌ Critical Error: Failed to import required functions from reddit_utils.py! Stats generation may fail or be inaccurate.{RESET}")
    reddit_utils_available = False
    # Define dummy functions to prevent crashes, though stats will be compromised
    def get_modification_date(entry): logging.error("Dummy get_modification_date called!"); return 0
    def format_timestamp(ts): logging.error("Dummy format_timestamp called!"); return "TIMESTAMP_ERROR"
    def _fetch_user_about_data(user, cfg): logging.error("Dummy _fetch_user_about_data called!"); return None

# --- Check Optional Dependencies needed by new calculations ---
# (These checks also happen in calculations.py, but good to have visibility here too)
# Note: We don't import the actual libraries here, just check availability for logging
# The actual import happens within the calculation functions themselves.
try:
    import pandas
    pandas_available_check = True
except ImportError:
    pandas_available_check = False
    logging.warning(f"{YELLOW}⚠️ Pandas library not found. Stats requiring pandas (Question Ratio, Mention Frequency) will be skipped.{RESET}")

try:
    import vaderSentiment
    vader_available_check = True
except ImportError:
    vader_available_check = False
    logging.warning(f"{YELLOW}⚠️ VADER sentiment library not found. Sentiment stats (Ratio, Arc) will be skipped.{RESET}")

try:
    import nltk
    # Quick check for punkt without triggering download here
    try:
        nltk.data.find('tokenizers/punkt')
        nltk_punkt_available = True
    except LookupError:
        nltk_punkt_available = False
        logging.warning(f"{YELLOW}⚠️ NLTK 'punkt' data not found. Question Ratio stats might fail or be skipped.{RESET}")
        logging.warning(f"{YELLOW}   Run: python -m nltk.downloader punkt{RESET}")
    nltk_available_check = True
except ImportError:
    nltk_available_check = False
    nltk_punkt_available = False # Can't have punkt if nltk isn't installed
    logging.warning(f"{YELLOW}⚠️ NLTK library not found. Question Ratio stats will be skipped.{RESET}")
    logging.warning(f"{YELLOW}   Install and download data: pip install nltk && python -m nltk.downloader punkt{RESET}")


# --- Data Loading & Filtering Functions ---

def _load_data_from_json(json_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """Safely loads the full user data JSON."""
    # (Function unchanged from original)
    logging.debug(f"      Attempting to load stats data source: {CYAN}{json_path}{RESET}")
    if not json_path or not os.path.exists(json_path):
        logging.error(f"   ❌ Stats generation failed: JSON file not found or path invalid: {CYAN}{json_path}{RESET}")
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as f: data = json.load(f)
        if not isinstance(data, dict): raise ValueError("JSON root is not a dictionary")
        # Ensure top-level keys 't1' and 't3' exist as dicts, even if empty
        if not isinstance(data.get("t1"), dict): data["t1"] = {}
        if not isinstance(data.get("t3"), dict): data["t3"] = {}
        logging.debug(f"      ✅ JSON data loaded successfully for stats ({len(data.get('t1',{}))} comments, {len(data.get('t3',{}))} posts).")
        return data
    except (json.JSONDecodeError, ValueError) as e: logging.error(f"   ❌ Stats generation failed: Error parsing JSON file {CYAN}{json_path}{RESET}: {e}"); return None
    except Exception as e: logging.error(f"   ❌ Stats generation failed: Unexpected error reading JSON file {CYAN}{json_path}{RESET}: {e}", exc_info=True); return None


def _apply_filters_to_data(
    data: Dict[str, Any],
    date_filter: Tuple[float, float],
    focus_subreddits: Optional[List[str]],
    ignore_subreddits: Optional[List[str]]
) -> Dict[str, Any]:
    """Filters the loaded JSON data based on date and subreddit filters."""
    # (Function unchanged from original)
    if not data: return {"t1": {}, "t3": {}}
    start_ts, end_ts = date_filter
    date_filter_active = start_ts > 0 or end_ts != float('inf')
    focus_filter_active = focus_subreddits is not None
    ignore_filter_active = ignore_subreddits is not None
    any_filter_active = date_filter_active or focus_filter_active or ignore_filter_active
    if not any_filter_active: logging.debug("      No filters applied to loaded JSON data."); return data

    logging.debug("      Applying filters to loaded JSON data...")
    filter_log_parts = []
    if date_filter_active:
        start_str = datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d') if start_ts > 0 else 'Beginning'
        end_str = datetime.fromtimestamp(end_ts - 1, timezone.utc).strftime('%Y-%m-%d') if end_ts != float('inf') else 'End'
        filter_log_parts.append(f"Date: {start_str} to {end_str}")
    if focus_filter_active: filter_log_parts.append(f"Focus: {focus_subreddits}")
    if ignore_filter_active: filter_log_parts.append(f"Ignore: {ignore_subreddits}")
    logging.info(f"      Applying JSON Filters: {'; '.join(filter_log_parts)}")

    focus_lower_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_lower_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None
    filtered_data = {"t1": {}, "t3": {}}; items_kept = 0; items_filtered_date = 0; items_filtered_sub = 0; items_error = 0

    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            item_data = item.get("data", {})
            if not item_data: items_error += 1; continue
            # --- Date Filtering ---
            if date_filter_active:
                if not reddit_utils_available: logging.error("   ❌ Cannot filter by date: reddit_utils not available."); pass
                else:
                    try:
                        ts_mod = get_modification_date(item)
                        if ts_mod == 0: items_error += 1; continue # Treat 0 as invalid/unfilterable date
                        if not (start_ts <= ts_mod < end_ts): items_filtered_date += 1; continue # Filter out by date
                    except Exception as e: logging.warning(f"      ⚠️ Error getting modification date for {kind} {item_id}: {e}. Filtering out."); items_error += 1; continue
            # --- Subreddit Filtering ---
            if focus_filter_active or ignore_filter_active:
                try:
                    subreddit = item_data.get("subreddit")
                    if not subreddit or not isinstance(subreddit, str): items_filtered_sub += 1; continue # Filter out if no sub info
                    item_subreddit_lower = subreddit.lower()
                    focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                    ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)
                    if not (focus_match and ignore_match): items_filtered_sub += 1; continue # Filter out by sub rules
                except Exception as e: logging.warning(f"      ⚠️ Error accessing subreddit for {kind} {item_id}: {e}. Filtering out."); items_error += 1; continue
            # --- Keep Item ---
            filtered_data[kind][item_id] = item
            items_kept += 1

    total_filtered = items_filtered_date + items_filtered_sub + items_error
    logging.info(f"      📊 JSON Filters Applied: {items_kept} items kept.")
    if total_filtered > 0:
         filter_details = []
         if items_filtered_date > 0: filter_details.append(f"{items_filtered_date} by date")
         if items_filtered_sub > 0: filter_details.append(f"{items_filtered_sub} by subreddit rules")
         if items_error > 0: filter_details.append(f"{items_error} due to errors")
         logging.info(f"         (Filtered Out: {', '.join(filter_details)})")
    if items_kept == 0 and total_filtered > 0: logging.warning(f"      {YELLOW}⚠️ All items were filtered out by the specified filters or due to errors.{RESET}")
    return filtered_data


# --- Main Stats Generation Function ---
def generate_stats_report(
    json_path: Optional[str],
    about_data: Optional[Dict[str, Any]],
    posts_csv_path: Optional[str],
    comments_csv_path: Optional[str],
    username: str,
    output_path: Optional[str],
    stats_json_path: Optional[str],
    date_filter: Tuple[float, float] = (0, float('inf')),
    focus_subreddits: Optional[List[str]] = None,
    ignore_subreddits: Optional[List[str]] = None,
    top_n_words: int = 50,
    top_n_items: int = 5,
    ngram_n_values: List[int] = [2, 3],
    ngram_top_k: int = 20,
    mention_top_n: int = 20, # Added parameter for mention frequency
    sentiment_arc_window: str = 'monthly', # Added parameter for sentiment arc window ('monthly' or 'yearly')
    write_md_report: bool = True,
    write_json_report: bool = True
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Calculates stats (with filters), saves MD/JSON reports.
    Returns tuple: (success_boolean, stats_results_dict or None)
    """
    start_time = time.time()
    generate_output = write_md_report or write_json_report
    filter_applied = date_filter != (0, float('inf')) or focus_subreddits is not None or ignore_subreddits is not None

    if generate_output: logging.info(f"   📊 Generating statistics report for /u/{BOLD}{username}{RESET}...")

    # --- Load Data ---
    full_data = _load_data_from_json(json_path)
    if full_data is None: return False, None # Loading failed

    # --- Apply Filters ---
    # Pass the filter arguments
    filtered_data = _apply_filters_to_data(
        full_data,
        date_filter=date_filter,
        focus_subreddits=focus_subreddits,
        ignore_subreddits=ignore_subreddits
    )

    # Check if any data remains *after* filtering
    if not filtered_data.get("t1") and not filtered_data.get("t3"):
         if generate_output: logging.warning(f"   {YELLOW}⚠️ No data remains for /u/{username} after applying filters. Skipping stats calculation.{RESET}")
         # Return True, but indicate emptiness in the results dict
         return True, {"_filter_applied": filter_applied, "_no_data_after_filter": True}

    # Check for 'about_data' needed for some calcs
    if about_data is None and generate_output:
        logging.warning(f"      {YELLOW}⚠️ User 'about' data not provided or fetch failed. Account Age/Karma stats might be incomplete.{RESET}")

    # --- Calculate Statistics ---
    stats_results = {}
    calculation_errors = []
    try:
        if generate_output: logging.info("   ⚙️ Calculating statistics...")
        # Use the filtered_data for calculations that rely on JSON structure
        # Use filtered CSV paths for calculations that rely on CSVs
        # (Note: We assume the CSVs passed here correspond to the filtered data)

        # --- Existing Calculations (No changes needed here) ---
        stats_results["basic_counts"] = calc._calculate_basic_counts(filtered_data)
        stats_results["time_range"] = calc._calculate_time_range(filtered_data)
        stats_results["subreddit_activity"] = calc._calculate_subreddit_activity(filtered_data)
        stats_results["text_stats"] = calc._calculate_text_stats(posts_csv_path, comments_csv_path)
        stats_results["word_frequency"] = calc._calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=top_n_words)
        stats_results["engagement"] = calc._calculate_engagement_stats(filtered_data, about_data)
        stats_results["post_types"] = calc._calculate_post_types(filtered_data)
        stats_results["temporal_stats"] = calc._calculate_temporal_stats(filtered_data)
        stats_results["score_stats"] = calc._calculate_score_stats(filtered_data, top_n=top_n_items)
        stats_results["award_stats"] = calc._calculate_award_stats(filtered_data)
        stats_results["flair_stats"] = calc._calculate_flair_stats(filtered_data)
        stats_results["post_engagement"] = calc._calculate_post_engagement(filtered_data)
        stats_results["editing_stats"] = calc._calculate_editing_stats(filtered_data)
        stats_results["sentiment_ratio"] = calc._calculate_sentiment_ratio(posts_csv_path, comments_csv_path)
        stats_results['age_activity_analysis'] = calc._calculate_age_vs_activity(about_data, stats_results.get('temporal_stats'))
        stats_results['crosspost_stats'] = calc._calculate_crosspost_stats(filtered_data)
        stats_results['removal_deletion_stats'] = calc._calculate_removal_deletion_stats(filtered_data)
        stats_results['subreddit_diversity'] = calc._calculate_subreddit_diversity(stats_results.get('subreddit_activity'))
        stats_results['ngram_frequency'] = calc._calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=ngram_n_values, top_k=ngram_top_k)
        stats_results['activity_burstiness'] = calc._calculate_activity_burstiness(filtered_data)

        # --- NEW Calculations ---
        # Sentiment Arc (Uses filtered JSON data in memory)
        stats_results['sentiment_arc'] = calc._calculate_sentiment_arc(filtered_data, time_window=sentiment_arc_window)
        # Question Ratio (Uses filtered CSV paths passed to this function)
        stats_results['question_ratio_stats'] = calc._calculate_question_ratio(posts_csv_path, comments_csv_path)
        # Mention Frequency (Uses filtered CSV paths passed to this function)
        stats_results['mention_stats'] = calc._calculate_mention_frequency(posts_csv_path, comments_csv_path, top_n=mention_top_n)

        # --- Add filter info for report formatting ---
        if write_md_report and filter_applied:
            start_f, end_f = date_filter
            start_str = datetime.fromtimestamp(start_f, timezone.utc).strftime('%Y-%m-%d') if start_f > 0 else None #'Beginning of Data'
            end_str = datetime.fromtimestamp(end_f - 1, timezone.utc).strftime('%Y-%m-%d') if end_f != float('inf') else None #'End of Data'
            # Only include date range if it was actually active
            date_info = {"start": start_str, "end": end_str} if date_filter != (0, float('inf')) else {}

            stats_results["_filter_info"] = {
                **date_info, # Merge date info if present
                "focus_subreddits": focus_subreddits, # Store list or None
                "ignore_subreddits": ignore_subreddits  # Store list or None
            }
        stats_results["_filter_applied"] = filter_applied # Add flag for comparison report check

        if generate_output: logging.info(f"   {GREEN}✅ All statistics calculated.{RESET}")

    except Exception as e:
        logging.error(f"   {RED}❌ Error during statistics calculation phase: {e}{RESET}", exc_info=True)
        calculation_errors.append(f"Calculation Error: {e}")

    # --- Format Report ---
    report_content = None
    formatting_success = False
    if write_md_report:
        logging.info("   ✍️ Formatting statistics report...")
        try:
            # Pass focus/ignore lists explicitly to _format_report
            report_content = _format_report(
                stats_data=stats_results,
                username=username,
                focus_subreddits=focus_subreddits, # Pass the list used for filtering
                ignore_subreddits=ignore_subreddits # Pass the list used for filtering
            )
            formatting_success = True
            logging.debug("      Report formatting complete.")
        except Exception as e:
            logging.error(f"   {RED}❌ Error during report formatting: {e}{RESET}", exc_info=True)
            calculation_errors.append(f"Report Formatting Error: {e}")

    # --- Save Markdown Report ---
    md_saved = False
    if write_md_report and formatting_success and report_content and output_path:
        logging.info(f"   💾 Saving statistics report to {CYAN}{output_path}{RESET}...")
        try:
            os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True) # Ensure dir exists
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(report_content)
            md_saved = True
            logging.info(f"   {GREEN}✅ Markdown report saved successfully.{RESET}")
        except IOError as e:
            logging.error(f"   {RED}❌ Error saving statistics report to {CYAN}{output_path}{RESET}: {e}{RESET}", exc_info=True)
            calculation_errors.append(f"MD Save IO Error: {e}")
        except Exception as e:
             logging.error(f"   {RED}❌ Unexpected error saving statistics report to {CYAN}{output_path}{RESET}: {e}{RESET}", exc_info=True)
             calculation_errors.append(f"MD Save Unexpected Error: {e}")
    elif write_md_report: # Log why it wasn't saved if requested
         if not output_path: logging.error(f"   {RED}❌ Skipping MD report save: No output path provided.{RESET}")
         elif not formatting_success: logging.error(f"   {RED}❌ Skipping MD report save due to formatting errors.{RESET}")
         elif not report_content: logging.error(f"   {RED}❌ Skipping MD report save due to missing report content.{RESET}")

    # --- Save JSON Stats Data ---
    json_saved = False
    if write_json_report and stats_json_path:
        logging.info(f"   💾 Saving calculated stats data to {CYAN}{stats_json_path}{RESET}...")
        # Create a clean copy for JSON, removing internal keys like _filter_info
        clean_stats_results = {k: v for k, v in stats_results.items() if not k.startswith('_')}
        # Add errors to the JSON output if any occurred
        if calculation_errors:
            clean_stats_results["_calculation_errors"] = calculation_errors
        try:
            os.makedirs(os.path.dirname(stats_json_path) or '.', exist_ok=True) # Ensure dir exists
            with open(stats_json_path, "w", encoding="utf-8") as f_json:
                # Use default=str to handle potential non-serializable types gracefully
                # (e.g., if a Counter object accidentally slipped through)
                json.dump(clean_stats_results, f_json, indent=2, default=str)
            json_saved = True
            logging.info(f"   {GREEN}✅ JSON stats data saved successfully.{RESET}")
        except TypeError as e:
             logging.error(f"   {RED}❌ Error saving JSON stats data (non-serializable type?) to {CYAN}{stats_json_path}{RESET}: {e}{RESET}", exc_info=True)
             calculation_errors.append(f"JSON Save Type Error: {e}")
        except IOError as e:
            logging.error(f"   {RED}❌ Error saving JSON stats data to {CYAN}{stats_json_path}{RESET}: {e}{RESET}", exc_info=True)
            calculation_errors.append(f"JSON Save IO Error: {e}")
        except Exception as e:
             logging.error(f"   {RED}❌ Unexpected error saving JSON stats data to {CYAN}{stats_json_path}{RESET}: {e}{RESET}", exc_info=True)
             calculation_errors.append(f"JSON Save Unexpected Error: {e}")

    elapsed_time = time.time() - start_time
    if generate_output:
        logging.info(f"   ⏱️ Statistics generation finished in {elapsed_time:.2f}s.")

    # Define overall success based on whether errors occurred during calculation/formatting/saving
    final_success = not calculation_errors
    # Additionally, if specific outputs were requested but failed, mark as unsuccessful
    if write_md_report and output_path and not md_saved: final_success = False
    if write_json_report and stats_json_path and not json_saved: final_success = False

    # Return success flag and the full results dict (incl. internal keys if generated)
    # Keep the results dict even on failure for potential debugging.
    return final_success, stats_results
