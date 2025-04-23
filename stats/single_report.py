import json
import csv
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple # Added typing imports

# --- Import from sibling modules within the 'stats' package ---
from . import calculations as calc # Use alias for brevity
from .reporting import _format_report
from .core_utils import _get_timestamp, _format_timedelta, CYAN, RESET, BOLD, YELLOW # Import needed core utils directly

RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; RED = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"; BLUE = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"
# --- Import from modules OUTSIDE the 'stats' package ---
# This assumes your project root is in Python's path when run
try:
    # These are needed for filtering and potentially fetching 'about' data if not provided
    from reddit_utils import get_modification_date, format_timestamp, _fetch_user_about_data
    reddit_utils_available = True
except ImportError:
    logging.critical(f"{BOLD}{RED}❌ Critical Error: Failed to import required functions from reddit_utils.py! Stats generation may fail or be inaccurate.{RESET}")
    reddit_utils_available = False
    # Define dummy functions to prevent crashes, though stats will be compromised
    def get_modification_date(entry): logging.error("Dummy get_modification_date called!"); return 0
    def format_timestamp(ts): logging.error("Dummy format_timestamp called!"); return "TIMESTAMP_ERROR"
    def _fetch_user_about_data(user, cfg): logging.error("Dummy _fetch_user_about_data called!"); return None


# --- Data Loading & Filtering Functions ---

def _load_data_from_json(json_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """Safely loads the full user data JSON."""
    logging.debug(f"      Attempting to load stats data source: {CYAN}{json_path}{RESET}")
    if not json_path or not os.path.exists(json_path):
        logging.error(f"   ❌ Stats generation failed: JSON file not found or path invalid: {CYAN}{json_path}{RESET}")
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Basic validation
        if not isinstance(data, dict):
            raise ValueError("JSON root is not a dictionary")
        # Ensure top-level keys 't1' and 't3' exist as dicts, even if empty
        if not isinstance(data.get("t1"), dict): data["t1"] = {}
        if not isinstance(data.get("t3"), dict): data["t3"] = {}
        logging.debug(f"      ✅ JSON data loaded successfully for stats ({len(data.get('t1',{}))} comments, {len(data.get('t3',{}))} posts).")
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"   ❌ Stats generation failed: Error parsing JSON file {CYAN}{json_path}{RESET}: {e}")
        return None
    except Exception as e:
        logging.error(f"   ❌ Stats generation failed: Unexpected error reading JSON file {CYAN}{json_path}{RESET}: {e}", exc_info=True)
        return None


def _apply_filters_to_data(
    data: Dict[str, Any],
    date_filter: Tuple[float, float],
    focus_subreddits: Optional[List[str]],
    ignore_subreddits: Optional[List[str]]
) -> Dict[str, Any]:
    """
    Filters the loaded JSON data based on date and subreddit filters.
    """
    if not data:
        return {"t1": {}, "t3": {}}

    start_ts, end_ts = date_filter
    date_filter_active = start_ts > 0 or end_ts != float('inf')
    focus_filter_active = focus_subreddits is not None
    ignore_filter_active = ignore_subreddits is not None
    any_filter_active = date_filter_active or focus_filter_active or ignore_filter_active

    if not any_filter_active:
        logging.debug("      No filters applied to loaded JSON data.")
        return data

    logging.debug("      Applying filters to loaded JSON data...")
    filter_log_parts = []
    if date_filter_active:
        start_str = datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d') if start_ts > 0 else 'Beginning'
        end_str = datetime.fromtimestamp(end_ts - 1, timezone.utc).strftime('%Y-%m-%d') if end_ts != float('inf') else 'End'
        filter_log_parts.append(f"Date: {start_str} to {end_str}")
    if focus_filter_active: filter_log_parts.append(f"Focus: {focus_subreddits}")
    if ignore_filter_active: filter_log_parts.append(f"Ignore: {ignore_subreddits}")
    logging.info(f"      Applying JSON Filters: {'; '.join(filter_log_parts)}")

    # Pre-process subreddit filters for efficiency
    focus_lower_set = {sub.lower() for sub in focus_subreddits} if focus_subreddits else None
    ignore_lower_set = {sub.lower() for sub in ignore_subreddits} if ignore_subreddits else None

    filtered_data = {"t1": {}, "t3": {}}
    items_kept = 0
    items_filtered_date = 0
    items_filtered_sub = 0
    items_error = 0

    for kind in ["t3", "t1"]:
        for item_id, item in data.get(kind, {}).items():
            item_data = item.get("data", {}) # Ensure data field exists
            if not item_data:
                items_error += 1; continue

            # --- Date Filtering ---
            if date_filter_active:
                if not reddit_utils_available:
                    logging.error("   ❌ Cannot filter by date: reddit_utils not available.")
                    # If utils unavailable, we have to decide: keep all or filter all?
                    # Let's keep it to avoid losing data unexpectedly, but log error.
                    pass # Keep item if date filtering fails due to missing utils
                else:
                    try:
                        ts_mod = get_modification_date(item)
                        if ts_mod == 0: # Treat 0 as invalid/unfilterable date
                            items_error += 1; continue # Skip item
                        if not (start_ts <= ts_mod < end_ts):
                            items_filtered_date += 1; continue # Filter out by date
                    except Exception as e:
                        logging.warning(f"      ⚠️ Error getting modification date for {kind} {item_id}: {e}. Filtering out.")
                        items_error += 1; continue # Filter out due to error

            # --- Subreddit Filtering (Focus/Ignore) ---
            if focus_filter_active or ignore_filter_active:
                try:
                    subreddit = item_data.get("subreddit")
                    if not subreddit or not isinstance(subreddit, str):
                        # If no subreddit info, can't apply sub filters reliably.
                        # Decide whether to keep or filter. Let's filter it out if sub filters are active.
                        items_filtered_sub += 1; continue

                    item_subreddit_lower = subreddit.lower()

                    # Focus check: MUST be in focus_lower_set if focus_lower_set is active
                    focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)
                    # Ignore check: MUST NOT be in ignore_lower_set if ignore_lower_set is active
                    ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)

                    if not (focus_match and ignore_match):
                        items_filtered_sub += 1; continue # Filter out by sub rules

                except Exception as e:
                    logging.warning(f"      ⚠️ Error accessing subreddit for {kind} {item_id}: {e}. Filtering out.")
                    items_error += 1; continue # Filter out due to error

            # --- Keep Item ---
            # If we reach here, the item passed all active filters
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

    if items_kept == 0 and total_filtered > 0:
        logging.warning(f"      {YELLOW}⚠️ All items were filtered out by the specified filters or due to errors.{RESET}")

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
    focus_subreddits: Optional[List[str]] = None, # MODIFIED
    ignore_subreddits: Optional[List[str]] = None, # NEW
    top_n_words: int = 50,
    top_n_items: int = 5,
    # N-gram specific parameters
    ngram_n_values: List[int] = [2, 3],
    ngram_top_k: int = 20,
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

    if generate_output:
        logging.info(f"   📊 Generating statistics report for /u/{BOLD}{username}{RESET}...")
        # Log filters being applied (now done within _apply_filters_to_data)

    # --- Load Data ---
    full_data = _load_data_from_json(json_path)
    if full_data is None:
        return False, None # Loading failed

    # --- Apply Filters ---
    # Pass the new filter arguments
    filtered_data = _apply_filters_to_data(
        full_data,
        date_filter=date_filter,
        focus_subreddits=focus_subreddits,
        ignore_subreddits=ignore_subreddits
    )

    # Check if any data remains *after* filtering
    if not filtered_data.get("t1") and not filtered_data.get("t3"):
         if generate_output: # Log only if generating report
             logging.warning(f"   {YELLOW}⚠️ No data remains for /u/{username} after applying filters. Skipping stats calculation.{RESET}")
         # Return True, but indicate emptiness
         return True, {"_filter_applied": filter_applied, "_no_data_after_filter": True}

    # Check for 'about_data' needed for some calcs
    if about_data is None and generate_output:
        logging.warning(f"      {YELLOW}⚠️ User 'about' data not provided or fetch failed. Account Age/Karma stats might be incomplete.{RESET}")

    # --- Calculate Statistics ---
    stats_results = {}
    calculation_errors = []
    try:
        if generate_output: logging.info("   ⚙️ Calculating statistics...")
        # Use the filtered_data for all calculations
        # Calls functions from the 'calculations' module using the 'calc' alias
        stats_results["basic_counts"] = calc._calculate_basic_counts(filtered_data)
        stats_results["time_range"] = calc._calculate_time_range(filtered_data) # Uses creation time
        stats_results["subreddit_activity"] = calc._calculate_subreddit_activity(filtered_data) # Needed for Diversity
        # Pass filtered CSV paths if they were generated based on filtered data
        stats_results["text_stats"] = calc._calculate_text_stats(posts_csv_path, comments_csv_path)
        stats_results["word_frequency"] = calc._calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=top_n_words)
        stats_results["engagement"] = calc._calculate_engagement_stats(filtered_data, about_data) # Uses about_data
        stats_results["post_types"] = calc._calculate_post_types(filtered_data)
        stats_results["temporal_stats"] = calc._calculate_temporal_stats(filtered_data) # Needed for Age/Activity & Burstiness
        stats_results["score_stats"] = calc._calculate_score_stats(filtered_data, top_n=top_n_items)
        stats_results["award_stats"] = calc._calculate_award_stats(filtered_data)
        stats_results["flair_stats"] = calc._calculate_flair_stats(filtered_data)
        stats_results["post_engagement"] = calc._calculate_post_engagement(filtered_data)
        stats_results["editing_stats"] = calc._calculate_editing_stats(filtered_data) # Uses created/edited times
        stats_results["sentiment_ratio"] = calc._calculate_sentiment_ratio(posts_csv_path, comments_csv_path) # Reads CSVs, uses VADER

        # --- NEW Calculations (depend on previous ones or about_data) ---
        stats_results['age_activity_analysis'] = calc._calculate_age_vs_activity(about_data, stats_results.get('temporal_stats'))
        stats_results['crosspost_stats'] = calc._calculate_crosspost_stats(filtered_data)
        stats_results['removal_deletion_stats'] = calc._calculate_removal_deletion_stats(filtered_data)
        stats_results['subreddit_diversity'] = calc._calculate_subreddit_diversity(stats_results.get('subreddit_activity'))
        stats_results['ngram_frequency'] = calc._calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=ngram_n_values, top_k=ngram_top_k) # Reads CSVs
        stats_results['activity_burstiness'] = calc._calculate_activity_burstiness(filtered_data) # Uses creation times

        # --- Add filter info for report formatting ---
        # MODIFIED: Store lists in filter info
        if write_md_report and filter_applied:
            start_f, end_f = date_filter
            start_str = datetime.fromtimestamp(start_f, timezone.utc).strftime('%Y-%m-%d') if start_f > 0 else 'Beginning of Data'
            end_str = datetime.fromtimestamp(end_f - 1, timezone.utc).strftime('%Y-%m-%d') if end_f != float('inf') else 'End of Data'

            stats_results["_filter_info"] = {
                "start": start_str if date_filter != (0, float('inf')) else None,
                "end": end_str if date_filter != (0, float('inf')) else None,
                "focus_subreddits": focus_subreddits, # Store list or None
                "ignore_subreddits": ignore_subreddits  # Store list or None
            }
        stats_results["_filter_applied"] = filter_applied # Add flag for comparison report check

        if generate_output: logging.info("   ✅ All statistics calculated.")

    except Exception as e:
        logging.error(f"   ❌ Error during statistics calculation phase: {e}", exc_info=True)
        calculation_errors.append(f"Calculation Error: {e}")

    # --- Format Report ---
    report_content = None
    formatting_success = False
    if write_md_report:
        logging.info("   ✍️ Formatting statistics report...")
        try:
            # MODIFIED: Pass focus/ignore lists to _format_report
            report_content = _format_report(
                stats_data=stats_results,
                username=username,
                focus_subreddits=focus_subreddits,
                ignore_subreddits=ignore_subreddits
            )
            formatting_success = True
            logging.debug("      Report formatting complete.")
        except Exception as e:
            logging.error(f"   ❌ Error during report formatting: {e}", exc_info=True)
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
            logging.info(f"   ✅ Markdown report saved successfully.")
        except IOError as e:
            logging.error(f"   ❌ Error saving statistics report to {CYAN}{output_path}{RESET}: {e}", exc_info=True)
            calculation_errors.append(f"MD Save IO Error: {e}")
        except Exception as e:
             logging.error(f"   ❌ Unexpected error saving statistics report to {CYAN}{output_path}{RESET}: {e}", exc_info=True)
             calculation_errors.append(f"MD Save Unexpected Error: {e}")

    elif write_md_report: # Log why it wasn't saved if requested
         if not output_path: logging.error(f"   ❌ Skipping MD report save: No output path provided.")
         elif not formatting_success: logging.error(f"   ❌ Skipping MD report save due to formatting errors.")
         elif not report_content: logging.error(f"   ❌ Skipping MD report save due to missing report content.")

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
                json.dump(clean_stats_results, f_json, indent=2)
            json_saved = True
            logging.info(f"   ✅ JSON stats data saved successfully.")
        except TypeError as e:
             logging.error(f"   ❌ Error saving JSON stats data (likely non-serializable data type) to {CYAN}{stats_json_path}{RESET}: {e}", exc_info=True)
             calculation_errors.append(f"JSON Save Type Error: {e}")
        except IOError as e:
            logging.error(f"   ❌ Error saving JSON stats data to {CYAN}{stats_json_path}{RESET}: {e}", exc_info=True)
            calculation_errors.append(f"JSON Save IO Error: {e}")
        except Exception as e:
             logging.error(f"   ❌ Unexpected error saving JSON stats data to {CYAN}{stats_json_path}{RESET}: {e}", exc_info=True)
             calculation_errors.append(f"JSON Save Unexpected Error: {e}")


    elapsed_time = time.time() - start_time
    if generate_output:
        logging.info(f"   ⏱️ Statistics generation finished in {elapsed_time:.2f}s.")

    # Define overall success based on whether errors occurred and if requested outputs were saved
    final_success = not calculation_errors # Success if no errors occurred during calc, format, or save
    # Tweak success based on whether requested outputs were actually generated
    if write_md_report and output_path and not md_saved: final_success = False
    if write_json_report and stats_json_path and not json_saved: final_success = False

    # Return success flag and the full results dict (incl. internal keys)
    # Return None for stats_results if final_success is False? No, keep results for debugging.
    return final_success, stats_results