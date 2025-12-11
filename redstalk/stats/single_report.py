# stats/single_report.py
"""
This module is responsible for orchestrating the generation of a
comprehensive statistical report for a single Reddit user or dataset.

It performs the following key steps:
1. Loads the user's full data from a JSON file.
2. Applies date and subreddit filters to the loaded data.
3. Calls various calculation functions (from the 'calculations' module)
   on the filtered data or associated CSV files to compute statistics.
4. Formats the calculated statistics into a human-readable Markdown report
   using a function from the 'reporting' module.
5. Optionally saves the Markdown report to a file.
6. Optionally saves the raw calculated statistics as a JSON file.

It handles optional external dependencies gracefully, logging warnings
and skipping calculations if necessary.
"""

import json # Used for loading the initial data and saving the stats JSON
import csv # Potentially used by calculations to read filtered CSVs (though calculations.py handles CSV reading internally now)
import logging # For logging progress, warnings, and errors
import os # For path manipulation and checking file existence
import time # For timing the report generation process
from datetime import datetime, timezone # For handling timestamps and date filters
from typing import List, Optional, Dict, Any, Tuple # Type hints for better code clarity

# --- Import from sibling modules within the 'stats' package ---
# 'calculations' module contains the logic for performing the statistical calculations.
from . import calculations as calc # Use alias 'calc' for brevity when calling functions
# 'reporting' module contains the logic for formatting the calculated stats into a report string.
from .reporting import _format_report
# 'core_utils' module contains shared helper functions like timestamp handling and color codes for logging.
from .core_utils import _get_timestamp, _format_timedelta, CYAN, RESET, BOLD, YELLOW, RED, GREEN # ANSI color codes for logging output

# --- Import from modules OUTSIDE the 'stats' package ---
# These are typically functions from the main data scraping/processing part
# of the project (e.g., in a 'reddit_utils.py' file in the root directory).
# get_modification_date: Used to determine the effective timestamp for filtering.
# format_timestamp: Used to format timestamps for human-readable output.
# _fetch_user_about_data: Used to get the user's 'about' data (karma, creation date).
# We use a try/except block here because this module relies heavily on these functions.
# If they fail to import, we define dummy functions and set a flag, ensuring
# the script doesn't crash immediately, though report generation will be severely limited.
try:
    from ..reddit_utils import get_modification_date, load_existing_data
    from ..data_utils import extract_csvs_from_json
    reddit_utils_available = True # Renamed from reddit_utils_available to indicate general utility availability
except ImportError:
    logging.critical(f"{BOLD}{RED}❌ Critical Error: Failed to import required functions from reddit_utils.py or data_utils.py! Stats generation may fail or be inaccurate.{RESET}")
    reddit_utils_available = False
    # Define dummy functions to prevent crashes if reddit_utils is missing
    def get_modification_date(entry):
        logging.error("Dummy get_modification_date called due to import failure!")
        return 0 # Indicate invalid/unfilterable date
    def format_timestamp(ts):
        logging.error("Dummy format_timestamp called due to import failure!")
        return "TIMESTAMP_ERROR" # Indicate formatting error
    def _fetch_user_about_data(user, cfg):
        logging.error("Dummy _fetch_user_about_data called due to import failure!")
        return None # Indicate about data is not available


# --- Check Optional Dependencies needed by specific calculations ---
# Some statistical calculations rely on optional third-party libraries.
# We check for their presence here and in the calculations.py module.
# This allows us to log warnings early and skip the relevant calculations
# gracefully if the dependencies are not installed.
# Note: We *do not* import the actual libraries here (`import pandas`, etc.).
# The actual imports and usage are confined to the calculations.py module.
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
# --- Data Loading & Filtering Functions ---
# These helper functions handle reading the input JSON data and applying filters.

def _load_data_from_json(json_path: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Safely loads the full user data JSON file generated by the scraping process.

    Args:
        json_path (Optional[str]): The path to the input JSON file containing user data.

    Returns:
        Optional[Dict[str, Any]]: The loaded data as a dictionary, or None if loading fails.
    """
    logging.debug(f"      Attempting to load stats data source: {CYAN}{json_path}{RESET}")

    # Validate input path
    if not json_path or not os.path.exists(json_path):
        logging.error(f"   {BOLD}{RED}❌ Stats generation failed: JSON file not found or path invalid: {CYAN}{json_path}{RESET}{RESET}")
        return None

    # Attempt to load the JSON file
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Basic validation: ensure the loaded data is a dictionary
        if not isinstance(data, dict):
            raise ValueError("JSON root is not a dictionary")

        # Ensure top-level keys for posts ('t3') and comments ('t1') exist
        # as dictionaries, even if they are empty in the source JSON.
        # This prevents subsequent loops from failing if a key is missing or has unexpected type.
        if not isinstance(data.get("t1"), dict): data["t1"] = {}
        if not isinstance(data.get("t3"), dict): data["t3"] = {}

        logging.debug(f"      ✅ JSON data loaded successfully for stats ({len(data.get('t1',{}))} comments, {len(data.get('t3',{}))} posts).")
        return data

    # Handle specific JSON decoding errors
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"   {BOLD}{RED}❌ Stats generation failed: Error parsing JSON file {CYAN}{json_path}{RESET}: {e}{RESET}")
        return None
    # Handle any other unexpected errors during file reading
    except Exception as e:
        logging.error(f"   {BOLD}{RED}❌ Stats generation failed: Unexpected error reading JSON file {CYAN}{json_path}{RESET}: {e}{RESET}", exc_info=True)
        return None


def _apply_filters_to_data(
    data: Dict[str, Any],
    date_filter: Tuple[float, float],
    focus_subreddits: Optional[List[str]],
    ignore_subreddits: Optional[List[str]]
) -> Dict[str, Any]:
    """
    Filters the loaded dictionary data based on date range and subreddit lists.

    Args:
        data (Dict[str, Any]): The dictionary containing loaded Reddit data.
        date_filter (Tuple[float, float]): A tuple (start_timestamp, end_timestamp).
                                           Items with a modification timestamp within
                                           [start_timestamp, end_timestamp) will be kept.
                                           Use 0 for start (beginning of data) and
                                           float('inf') for end (end of data).
        focus_subreddits (Optional[List[str]]): If not None, only keep items
                                                whose subreddit is in this list. Case-insensitive.
        ignore_subreddits (Optional[List[str]]): If not None, ignore items
                                                 whose subreddit is in this list. Case-insensitive.

    Returns:
        Dict[str, Any]: A new dictionary containing only the items that passed all filters.
                        Returns {"t1": {}, "t3": {}} if input data is None or empty after filtering.
    """
    if not data:
        logging.debug("      No data provided to filter. Returning empty filtered data.")
        return {"t1": {}, "t3": {}} # Return empty structure if no data was loaded

    # Unpack date filter timestamps
    start_ts, end_ts = date_filter

    # Determine if any filters are active
    date_filter_active = start_ts > 0 or end_ts != float('inf')
    focus_filter_active = focus_subreddits is not None and len(focus_subreddits) > 0
    ignore_filter_active = ignore_subreddits is not None and len(ignore_subreddits) > 0
    any_filter_active = date_filter_active or focus_filter_active or ignore_filter_active

    # If no filters are active, return the original data without copying
    if not any_filter_active:
        logging.debug("      No filters applied to loaded JSON data.");
        return data # Return original data if no filtering is needed

    logging.debug("      Applying filters to loaded JSON data...")
    filter_log_parts = [] # List to build a log message describing the filters

    # Add description of active filters to the log message parts
    if date_filter_active:
        # Format timestamps for logging
        start_str = datetime.fromtimestamp(start_ts, timezone.utc).strftime('%Y-%m-%d') if start_ts > 0 else 'Beginning'
        end_str = datetime.fromtimestamp(end_ts - 1, timezone.utc).strftime('%Y-%m-%d') if end_ts != float('inf') else 'End' # Subtract 1 second to be inclusive of the day before end_ts
        filter_log_parts.append(f"Date: {start_str} to {end_str} (based on modification time)")
    if focus_filter_active:
        filter_log_parts.append(f"Focus Subs: {focus_subreddits}")
    if ignore_filter_active:
        filter_log_parts.append(f"Ignore Subs: {ignore_subreddits}")

    # Log the applied filters
    logging.info(f"      Applying JSON Filters: {'; '.join(filter_log_parts)}")

    # Prepare sets of subreddits for efficient checking (case-insensitive)
    focus_lower_set = {sub.lower() for sub in focus_subreddits} if focus_filter_active else None
    ignore_lower_set = {sub.lower() for sub in ignore_subreddits} if ignore_filter_active else None

    # Initialize the dictionary to hold filtered data
    filtered_data = {"t1": {}, "t3": {}};
    items_kept = 0;
    items_filtered_date = 0;
    items_filtered_sub = 0;
    items_error = 0; # Count items skipped due to processing errors

    # Iterate through all items in the original data
    for kind in ["t3", "t1"]: # Process posts ('t3') and comments ('t1')
        # Iterate through items by their unique ID
        for item_id, item in data.get(kind, {}).items():
            item_data = item.get("data", {}) # Get the 'data' payload of the item
            if not item_data:
                items_error += 1;
                logging.debug(f"         Skipping item {item_id}: Missing 'data' field.")
                continue # Skip items with no data payload

            # --- Date Filtering Logic ---
            if date_filter_active:
                # Check if reddit_utils (specifically get_modification_date) is available
                if not reddit_utils_available:
                    logging.error("   ❌ Cannot filter by date: reddit_utils not available (checked once).")
                    # If reddit_utils is missing, we can't apply date filter correctly,
                    # but we shouldn't skip *all* items. We'll let the item pass
                    # the date filter step here, but a warning is logged.
                    pass # Proceed without date filtering this item if helper is missing
                else:
                    try:
                        # Get the modification timestamp (preferred for filtering to include edits)
                        ts_mod = get_modification_date(item)
                        if ts_mod == 0:
                             # If get_modification_date returns 0 (e.g., timestamp invalid), filter out
                             items_error += 1;
                             logging.debug(f"         Skipping item {item_id} for date filter: Invalid/Zero timestamp.")
                             continue
                        # Check if the modification timestamp is within the specified range
                        if not (start_ts <= ts_mod < end_ts):
                            items_filtered_date += 1;
                            # logging.debug(f"         Filtering out {kind} {item_id} by date (ts={ts_mod}).")
                            continue # Filter out by date range

                    except Exception as e:
                        # Catch any errors during date processing for a single item
                        logging.warning(f"      ⚠️ Error getting modification date for {kind} {item_id}: {e}. Filtering out item due to error.")
                        items_error += 1;
                        continue # Skip item due to error

            # --- Subreddit Filtering Logic ---
            if focus_filter_active or ignore_filter_active:
                try:
                    subreddit = item_data.get("subreddit")
                    # Filter out items if subreddit info is missing or not a string
                    if not subreddit or not isinstance(subreddit, str):
                        items_filtered_sub += 1;
                        # logging.debug(f"         Filtering out {kind} {item_id} by subreddit: Missing or invalid sub info.")
                        continue

                    item_subreddit_lower = subreddit.lower() # Get lowercase subreddit name

                    # Check against focus list (keep only if in focus list, or if no focus list)
                    focus_match = (focus_lower_set is None) or (item_subreddit_lower in focus_lower_set)

                    # Check against ignore list (keep only if NOT in ignore list, or if no ignore list)
                    ignore_match = (ignore_lower_set is None) or (item_subreddit_lower not in ignore_lower_set)

                    # An item is kept by subreddit filters ONLY if it matches the focus rule AND the ignore rule
                    if not (focus_match and ignore_match):
                        items_filtered_sub += 1;
                        # logging.debug(f"         Filtering out {kind} {item_id} by subreddit rules (sub={subreddit}).")
                        continue # Filter out by subreddit rules

                except Exception as e:
                    # Catch any errors during subreddit access/processing for a single item
                    logging.warning(f"      ⚠️ Error accessing subreddit for {kind} {item_id}: {e}. Filtering out item due to error.")
                    items_error += 1;
                    continue # Skip item due to error

            # --- If item passed all active filters ---
            # Add the item to the filtered data dictionary
            filtered_data[kind][item_id] = item
            items_kept += 1 # Increment count of items kept

    # Log summary of filtering results
    total_processed = len(data.get("t1",{})) + len(data.get("t3",{}))
    total_filtered = items_filtered_date + items_filtered_sub + items_error
    logging.info(f"      📊 JSON Filters Applied: {items_kept} items kept (out of {total_processed} total).")
    if total_filtered > 0:
         # Provide breakdown of why items were filtered out
         filter_details = []
         if items_filtered_date > 0: filter_details.append(f"{items_filtered_date} by date")
         if items_filtered_sub > 0: filter_details.append(f"{items_filtered_sub} by subreddit rules")
         if items_error > 0: filter_details.append(f"{items_error} due to errors")
         logging.info(f"         (Filtered Out Breakdown: {', '.join(filter_details)})")

    # Warn if filtering resulted in no data
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
    focus_subreddits: Optional[List[str]] = None,
    ignore_subreddits: Optional[List[str]] = None,
    top_n_words: int = 50,
    top_n_items: int = 5,
    ngram_n_values: List[int] = [2, 3],
    ngram_top_k: int = 20,
    mention_top_n: int = 20, # Added parameter: Number of top mentions to include in stats/report
    sentiment_arc_window: str = 'monthly', # Added parameter: Time window for sentiment arc ('monthly' or 'yearly')
    write_md_report: bool = True, # Option to enable/disable Markdown report generation
    write_json_report: bool = True # Option to enable/disable JSON stats data saving
) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Orchestrates the process of loading, filtering, calculating, formatting,
    and saving a single user's Reddit statistics report.

    Args:
        json_path (Optional[str]): Path to the input JSON file with raw user data.
        about_data (Optional[Dict[str, Any]]): User's 'about' data (karma, created),
                                               or None if not available.
        posts_csv_path (Optional[str]): Path to the CSV file containing *filtered* post data.
                                        Used by calculations that process text from CSV.
        comments_csv_path (Optional[str]): Path to the CSV file containing *filtered* comment data.
                                           Used by calculations that process text from CSV.
        username (str): The Reddit username to generate the report for.
        output_path (Optional[str]): Path to save the Markdown report file.
                                     Ignored if write_md_report is False.
        stats_json_path (Optional[str]): Path to save the raw calculated stats as JSON.
                                         Ignored if write_json_report is False.
        date_filter (Tuple[float, float]): Date filter as (start_ts, end_ts).
        focus_subreddits (Optional[List[str]]): List of subreddits to focus on.
        ignore_subreddits (Optional[List[str]]): List of subreddits to ignore.
        top_n_words (int): Number of top words for word frequency calculation.
        top_n_items (int): Number of top/bottom items for score stats.
        ngram_n_values (List[int]): List of N values for n-gram calculation (e.g., [2, 3]).
        ngram_top_k (int): Number of top k n-grams to find for each N.
        mention_top_n (int): Number of top mentioned users/subs to include in stats.
        sentiment_arc_window (str): Time window for sentiment arc ('monthly' or 'yearly').
        write_md_report (bool): Flag to control whether to generate and save the Markdown report.
        write_json_report (bool): Flag to control whether to save the calculated statistics as JSON.


    Returns:
        Tuple[bool, Optional[Dict[str, Any]]]: A tuple. The first element is True if the
                                               process completed without critical errors
                                               (even if no data was found or some optional
                                               calculations were skipped). The second element
                                               is the dictionary of calculated stats results,
                                               or None if initial data loading failed.
    """
    start_time = time.time() # Start timing the process
    # Determine if any output generation is actually requested
    generate_output = write_md_report or write_json_report
    # Determine if any filtering is active (used for logging and adding filter info to results)
    filter_applied = date_filter != (0, float('inf')) or focus_subreddits is not None or ignore_subreddits is not None

    if generate_output:
        logging.info(f"   📊 Generating statistics report for /u/{BOLD}{username}{RESET}...")

    # --- Step 1: Load Data from JSON ---
    full_data = _load_data_from_json(json_path)
    if full_data is None:
        # If initial JSON loading fails, we cannot proceed.
        logging.error("   ❌ Report generation aborted due to critical data loading failure.")
        return False, None # Return False for success and None for results

    # --- Step 2: Apply Filters ---
    # Call the filtering function, passing the loaded data and filter parameters.
    filtered_data = _apply_filters_to_data(
        full_data,
        date_filter=date_filter,
        focus_subreddits=focus_subreddits,
        ignore_subreddits=ignore_subreddits
    )

    # --- Check for Data After Filtering ---
    # If no data remains after filtering, indicate this and potentially stop further processing.
    if not filtered_data.get("t1") and not filtered_data.get("t3"):
         if generate_output:
             # Log a warning if output is expected but no data remains
             logging.warning(f"   {YELLOW}⚠️ No data remains for /u/{username} after applying filters. Skipping stats calculation.{RESET}")
         # Return True (process wasn't critically broken) but signal no data was found.
         # Include filter info for the report formatter to explain why there's no data.
         return True, {"_filter_applied": filter_applied, "_no_data_after_filter": True}

    # --- Check for About Data ---
    # About data is needed for some calculations but might not always be available.
    if about_data is None and generate_output:
        logging.warning(f"      {YELLOW}⚠️ User 'about' data not provided or fetch failed. Account Age/Karma stats might be incomplete.{RESET}")

    # --- Step 3: Calculate Statistics ---
    stats_results = {} # Dictionary to store all calculated statistics
    calculation_errors = [] # List to track any errors during calculation phase

    try:
        if generate_output:
             logging.info("   ⚙️ Calculating statistics...")

        # Perform each statistical calculation by calling functions from the 'calculations' module.
        # Pass either the filtered_data (in-memory dict) or the paths to the
        # filtered CSV files, depending on what each calculation function expects.
        # (Note: It's assumed the CSVs passed here were generated from data
        # that was filtered in the same way as the filtered_data dict).

        # Calculations using filtered_data dict (in-memory):
        stats_results["basic_counts"] = calc._calculate_basic_counts(filtered_data)
        stats_results["time_range"] = calc._calculate_time_range(filtered_data)
        stats_results["subreddit_activity"] = calc._calculate_subreddit_activity(filtered_data)
        # Engagement needs both filtered data (for item scores) and about_data (for total karma)
        stats_results["engagement"] = calc._calculate_engagement_stats(filtered_data, about_data)
        stats_results["post_types"] = calc._calculate_post_types(filtered_data)
        stats_results["temporal_stats"] = calc._calculate_temporal_stats(filtered_data)
        # Score stats need filtered data for scores and item details
        stats_results["score_stats"] = calc._calculate_score_stats(filtered_data, top_n=top_n_items)
        stats_results["award_stats"] = calc._calculate_award_stats(filtered_data)
        stats_results["flair_stats"] = calc._calculate_flair_stats(filtered_data)
        stats_results["post_engagement"] = calc._calculate_post_engagement(filtered_data)
        stats_results["editing_stats"] = calc._calculate_editing_stats(filtered_data)
        # Age vs Activity needs about_data and the temporal stats result
        stats_results['age_activity_analysis'] = calc._calculate_age_vs_activity(about_data, stats_results.get('temporal_stats'))
        stats_results['crosspost_stats'] = calc._calculate_crosspost_stats(filtered_data)
        stats_results['removal_deletion_stats'] = calc._calculate_removal_deletion_stats(filtered_data)
        # Subreddit diversity needs the subreddit activity result
        stats_results['subreddit_diversity'] = calc._calculate_subreddit_diversity(stats_results.get('subreddit_activity'))
        stats_results['activity_burstiness'] = calc._calculate_activity_burstiness(filtered_data)

        # Calculations using CSV file paths (expecting filtered data in CSVs):
        stats_results["text_stats"] = calc._calculate_text_stats(posts_csv_path, comments_csv_path)
        stats_results["word_frequency"] = calc._calculate_word_frequency(posts_csv_path, comments_csv_path, top_n=top_n_words)
        stats_results['ngram_frequency'] = calc._calculate_ngram_frequency(posts_csv_path, comments_csv_path, n_values=ngram_n_values, top_k=ngram_top_k)
        stats_results["sentiment_ratio"] = calc._calculate_sentiment_ratio(posts_csv_path, comments_csv_path)
        stats_results['reply_depth'] = calc._calculate_reply_depth(comments_csv_path)


        # --- NEW Calculations ---
        # Sentiment Arc: Uses filtered JSON data (in-memory dict) and the specified time window parameter.
        stats_results['sentiment_arc'] = calc._calculate_sentiment_arc(filtered_data, time_window=sentiment_arc_window)
        # Question Ratio: Uses filtered CSV paths.
        stats_results['question_ratio_stats'] = calc._calculate_question_ratio(posts_csv_path, comments_csv_path)
        # Mention Frequency: Uses filtered CSV paths and the specified top_n parameter.
        stats_results['mention_stats'] = calc._calculate_mention_frequency(posts_csv_path, comments_csv_path, top_n=mention_top_n)


        # --- Add Filter Info to Results for Reporting ---
        # Store information about the applied filters within the results dictionary
        # so the report formatter can describe the filter criteria.
        if write_md_report and filter_applied: # Only add if MD report is requested and filters were active
            # Format date filter timestamps for storage in _filter_info
            start_f, end_f = date_filter
            start_str = datetime.fromtimestamp(start_f, timezone.utc).strftime('%Y-%m-%d') if start_f > 0 else None #'Beginning of Data'
            end_str = datetime.fromtimestamp(end_f - 1, timezone.utc).strftime('%Y-%m-%d') if end_f != float('inf') else None #'End of Data'
            # Only include date info if the filter was actually date-based (not just default (0, inf))
            date_info_for_report = {"start": start_str, "end": end_str} if date_filter != (0, float('inf')) else {}

            stats_results["_filter_info"] = {
                **date_info_for_report, # Merge date info (if exists) into the dict
                "focus_subreddits": focus_subreddits, # Store the focus list (or None)
                "ignore_subreddits": ignore_subreddits  # Store the ignore list (or None)
            }
        stats_results["_filter_applied"] = filter_applied # Store a simple flag indicating if any filter was active


        if generate_output:
            # Log success if the calculation phase completed without raising exceptions
            logging.info(f"   {GREEN}✅ All statistics calculated.{RESET}")

    except Exception as e:
        # Catch any exceptions during the calculation phase.
        # Log the error and record it in the errors list.
        logging.error(f"   {RED}❌ Error during statistics calculation phase: {e}{RESET}", exc_info=True)
        calculation_errors.append(f"Calculation Error: {e}")
        # The process can potentially continue to saving, but calculation errors are critical for report accuracy.


    # --- Step 4: Format Markdown Report ---
    report_content = None # Variable to hold the formatted report string
    formatting_success = False # Flag to track if formatting was successful

    if write_md_report: # Only attempt formatting if Markdown report is requested
        logging.info("   ✍️ Formatting statistics report...")
        try:
            # Call the formatting function.
            # Pass the full stats_results dictionary.
            # Explicitly pass the original filter lists (focus/ignore) as the formatter
            # uses these directly to describe the filters in the report header.
            report_content = _format_report(
                stats_data=stats_results,
                username=username,
                focus_subreddits=focus_subreddits, # Pass the list used for filtering
                ignore_subreddits=ignore_subreddits # Pass the list used for filtering
            )
            formatting_success = True # Mark formatting as successful
            logging.debug("      Report formatting complete.")
        except Exception as e:
            # Catch any errors during the formatting process
            logging.error(f"   {RED}❌ Error during report formatting: {e}{RESET}", exc_info=True)
            calculation_errors.append(f"Report Formatting Error: {e}") # Add to errors list


    # --- Step 5: Save Markdown Report ---
    md_saved = False # Flag to track if MD report was successfully saved

    # Only attempt to save if requested, formatting was successful, content exists, and an output path is provided
    if write_md_report and formatting_success and report_content and output_path:
        logging.info(f"   💾 Saving statistics report to {CYAN}{output_path}{RESET}...")
        try:
            # Create the output directory if it doesn't exist.
            # os.path.dirname(output_path) extracts the directory part. '.' is used if output_path is just a filename.
            # exist_ok=True prevents an error if the directory already exists.
            os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

            # Open the output file in write mode ('w') with UTF-8 encoding.
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(report_content) # Write the formatted report string to the file

            md_saved = True # Mark as successfully saved
            logging.info(f"   {GREEN}✅ Markdown report saved successfully.{RESET}")

        # Handle specific IO errors (e.g., permissions, disk full)
        except IOError as e:
            logging.error(f"   {RED}❌ Error saving statistics report to {CYAN}{output_path}{RESET}: {e}{RESET}", exc_info=True)
            calculation_errors.append(f"MD Save IO Error: {e}") # Add to errors list
        # Handle any other unexpected errors during saving
        except Exception as e:
             logging.error(f"   {RED}❌ Unexpected error saving statistics report to {CYAN}{output_path}{RESET}: {e}{RESET}", exc_info=True)
             calculation_errors.append(f"MD Save Unexpected Error: {e}")

    # Log why the MD report was skipped if it was requested but not saved
    elif write_md_report:
         if not output_path: logging.error(f"   {RED}❌ Skipping MD report save: No output path provided.{RESET}")
         elif not formatting_success: logging.error(f"   {RED}❌ Skipping MD report save due to formatting errors.{RESET}")
         elif not report_content: logging.error(f"   {RED}❌ Skipping MD report save due to missing report content.{RESET}") # Should theoretically not happen if formatting was successful


    # --- Step 6: Save JSON Stats Data ---
    json_saved = False # Flag to track if JSON data was successfully saved

    # Only attempt to save if requested and a JSON path is provided
    if write_json_report and stats_json_path:
        logging.info(f"   💾 Saving calculated stats data to {CYAN}{stats_json_path}{RESET}...")

        # Create a copy of the stats results dictionary for JSON saving.
        # Remove any internal keys that start with '_' (like '_filter_info')
        # as these are primarily for internal processing/formatting, not raw stats.
        clean_stats_results = {k: v for k, v in stats_results.items() if not k.startswith('_')}

        # If any calculation or formatting errors occurred, add them to the JSON output
        # under a specific key so they are recorded in the saved data.
        if calculation_errors:
            clean_stats_results["_calculation_errors"] = calculation_errors

        try:
            # Ensure the output directory exists
            os.makedirs(os.path.dirname(stats_json_path) or '.', exist_ok=True)

            # Open the output file in write mode ('w') with UTF-8 encoding.
            with open(stats_json_path, "w", encoding="utf-8") as f_json:
                # Use json.dump to serialize the dictionary to JSON.
                # indent=2 makes the JSON output human-readable with indentation.
                # default=str handles potential non-JSON-serializable types by
                # converting them to their string representation. This helps prevent
                # the save operation from failing if a calculation function
                # returned a complex object type (like a Counter instance) inadvertently.
                json.dump(clean_stats_results, f_json, indent=2, default=str)

            json_saved = True # Mark as successfully saved
            logging.info(f"   {GREEN}✅ JSON stats data saved successfully.{RESET}")

        # Handle specific errors during JSON serialization (e.g., non-serializable objects not caught by default=str)
        except TypeError as e:
             logging.error(f"   {RED}❌ Error saving JSON stats data (non-serializable type?) to {CYAN}{stats_json_path}{RESET}: {e}{RESET}", exc_info=True)
             calculation_errors.append(f"JSON Save Type Error: {e}") # Add to errors list
        # Handle specific IO errors
        except IOError as e:
            logging.error(f"   {RED}❌ Error saving JSON stats data to {CYAN}{stats_json_path}{RESET}: {e}{RESET}", exc_info=True)
            calculation_errors.append(f"JSON Save IO Error: {e}") # Add to errors list
        # Handle any other unexpected errors
        except Exception as e:
             logging.error(f"   {RED}❌ Unexpected error saving JSON stats data to {CYAN}{stats_json_path}{RESET}: {e}{RESET}", exc_info=True)
             calculation_errors.append(f"JSON Save Unexpected Error: {e}")


    # --- Final Status and Return ---
    elapsed_time = time.time() - start_time # Calculate total elapsed time

    if generate_output: # Only log total time if some output was intended
        logging.info(f"   ⏱️ Statistics generation finished in {elapsed_time:.2f}s.")

    # Determine the overall success status. It's successful if no calculation errors
    # occurred, AND if any requested saves were successful.
    final_success = not calculation_errors # Start assuming success if no calc errors

    # If MD report was requested and saving failed, mark as unsuccessful
    if write_md_report and output_path and not md_saved:
        final_success = False
        logging.error(f"   {BOLD}{RED}❌ Single report generation failed: Markdown report not saved.{RESET}")

    # If JSON report was requested and saving failed, mark as unsuccessful
    if write_json_report and stats_json_path and not json_saved:
        final_success = False
        logging.error(f"   {BOLD}{RED}❌ Single report generation failed: JSON stats data not saved.{RESET}")

    # Return the final success flag and the calculated stats results dictionary.
    # The dictionary is returned even on failure for potential debugging purposes,
    # but might be incomplete if errors occurred during calculations.
    return final_success, stats_results