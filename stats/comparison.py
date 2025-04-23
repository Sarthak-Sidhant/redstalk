import logging
import os
import time
from collections import Counter
from datetime import datetime

# --- Import from sibling module ---
from .reporting import _format_comparison_report # For formatting the final report
from .core_utils import CYAN, RESET # For logging formatting


# --- Comparison Calculation Helpers ---

def _calculate_subreddit_overlap(subs1, subs2):
    """Calculates overlap between two lists/sets of subreddits."""
    logging.debug("         Calculating subreddit overlap...")
    # Ensure inputs are sets for efficient operations
    set1 = set(subs1) if subs1 else set()
    set2 = set(subs2) if subs2 else set()

    intersection = set1.intersection(set2)
    union = set1.union(set2)

    jaccard = len(intersection) / len(union) if union else 0.0 # Avoid division by zero

    return { "shared_subreddits": sorted(list(intersection), key=str.lower),
             "num_shared": len(intersection),
             "jaccard_index": f"{jaccard:.3f}" }

def _compare_word_frequency(freq1, freq2, top_n=20):
    """Compares the top N words from two frequency dictionaries."""
    logging.debug(f"         Comparing top {top_n} word frequencies...")
    # Ensure freq1 and freq2 are dictionaries
    if not isinstance(freq1, dict): freq1 = {}
    if not isinstance(freq2, dict): freq2 = {}

    # Get the actual sets of top N words (already sorted in calculation)
    # No need to sort again here if the input is already sorted like {'word': count}
    top_words1 = set(list(freq1.keys())[:top_n])
    top_words2 = set(list(freq2.keys())[:top_n])

    intersection = top_words1.intersection(top_words2)
    union = top_words1.union(top_words2)

    jaccard = len(intersection) / len(union) if union else 0.0 # Avoid division by zero

    return { "top_n_compared": top_n,
             "shared_top_words": sorted(list(intersection)), # Sort the shared list alphabetically
             "num_shared_top_words": len(intersection),
             "jaccard_index": f"{jaccard:.3f}" }


# --- Comparison Report Generation Function ---

def generate_comparison_report(stats1, stats2, user1, user2, output_path):
    """
    Generates and saves a comparison report based on two pre-calculated stats dicts.
    Returns True on success, False on failure.
    """
    logging.info(f"   👥 Generating comparison report for /u/{user1} vs /u/{user2}...")
    start_time = time.time()

    # --- Input Validation ---
    if not stats1 or not isinstance(stats1, dict):
        logging.error(f"   ❌ Cannot generate comparison report: Invalid or missing stats data for user {user1}.")
        return False
    if not stats2 or not isinstance(stats2, dict):
        logging.error(f"   ❌ Cannot generate comparison report: Invalid or missing stats data for user {user2}.")
        return False
    if not output_path:
         logging.error("   ❌ Cannot save comparison report: No output path provided.")
         return False

    comparison_results = {}
    report_content = ""
    success = False

    try:
        logging.info("      Calculating comparison metrics...")
        # --- Calculate Subreddit Overlap ---
        # Extract the list of all active subs from each stats dict
        subs1 = stats1.get('subreddit_activity', {}).get('all_active_subs', [])
        subs2 = stats2.get('subreddit_activity', {}).get('all_active_subs', [])
        comparison_results["subreddit_overlap"] = _calculate_subreddit_overlap(subs1, subs2)

        # --- Calculate Word Frequency Overlap ---
        # Extract the word frequency dict from each stats dict
        # Note: assumes 'word_frequency' key contains another 'word_frequency' dict
        freq1 = stats1.get('word_frequency', {}).get('word_frequency', {})
        freq2 = stats2.get('word_frequency', {}).get('word_frequency', {})
        # Use a larger N for comparison calculation for better overlap index
        # The formatting function will decide how many top words to *display*
        comparison_results["word_frequency_comparison"] = _compare_word_frequency(freq1, freq2, top_n=150)

        # --- N-gram comparison is handled directly in formatting ---
        # No separate calculation needed here, just pass stats1 & stats2 to formatter.

        logging.info("   ✍️ Formatting comparison report...")
        report_content = _format_comparison_report(stats1, stats2, user1, user2, comparison_results)

        logging.info(f"   💾 Saving comparison report to {CYAN}{output_path}{RESET}...")
        os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True) # Ensure directory exists
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report_content)

        elapsed_time = time.time() - start_time
        logging.info(f"   ✅ Comparison report saved successfully ({elapsed_time:.2f}s).")
        success = True

    except Exception as e:
        logging.error(f"   ❌ Error during comparison report generation for {user1} vs {user2}: {e}", exc_info=True)
        success = False # Ensure success is False on exception

    return success