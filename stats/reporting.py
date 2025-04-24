# stats/reporting.py
"""
This module contains functions responsible for formatting the pre-calculated
statistical results into human-readable reports. These reports are typically
generated in Markdown format, suitable for saving to a file.

Crucially, these functions focus solely on presenting the data and
SHOULD NOT include ANSI escape codes for coloring or styling the output,
as these codes are meant for terminal display and would appear as raw
characters in a file-based report.
"""

import logging
from datetime import datetime
from collections import Counter # Used for combining activity counts in report formatting
from typing import List, Optional, Dict, Any # Type hints for better code readability and maintainability


# --- NO Core Util Imports Likely Needed Here ---
# This module is about presentation, not core data processing like cleaning or timestamping.
# Although Counter is imported, it's used locally for combining data for display, not for core calculations.

# --- NO ANSI CODES HERE ---
# This is a crucial point reinforced here. The output of this module
# is intended to be clean text (like Markdown) that can be saved to a file,
# not terminal output.


def _format_report(
    stats_data: Dict[str, Any],
    username: str,
    focus_subreddits: Optional[List[str]] = None,
    ignore_subreddits: Optional[List[str]] = None
) -> str:
    """
    Formats the statistics for a single user or dataset into a plain Markdown report string.

    This function takes the comprehensive dictionary of calculated statistics,
    along with user information and details about any filters applied,
    and structures them into a well-organized report.

    Args:
        stats_data (Dict[str, Any]): A dictionary containing all the pre-calculated
                                     statistics (e.g., from calculations.py).
                                     Expected to have keys corresponding to the
                                     different calculation functions (e.g., 'basic_counts',
                                     'time_range', 'text_stats', etc.).
        username (str): The Reddit username for whom the report is being generated.
                        Used in the report title and context.
        focus_subreddits (Optional[List[str]]): List of subreddits the data was
                                                 filtered *to* include. Used for
                                                 mentioning filters in the report.
                                                 Defaults to None if no focus filter was used.
        ignore_subreddits (Optional[List[str]]): List of subreddits the data was
                                                  filtered *to* exclude. Used for
                                                  mentioning filters in the report.
                                                  Defaults to None if no ignore filter was used.

    Returns:
        str: A multi-line string formatted in Markdown, representing the user's
             statistics report.
    """
    # ***** NO ANSI CODES SHOULD BE USED IN THIS FUNCTION *****
    # This ensures the output file is clean Markdown.
    logging.debug(f"      Formatting stats report for /u/{username}...")

    # Start building the Markdown report string
    report = f"# Reddit User Statistics Report for /u/{username}\n\n"

    # Add a timestamp indicating when the report was generated
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S');
    report += f"*Report generated: {dt_now}*\n\n"

    # --- Filter Information Section ---
    # This block adds information about any filters that were applied when
    # generating the data, so the user understands the scope of the report.
    filter_info = stats_data.get("_filter_info", {}) # Get stored filter info dictionary
    # Check if any filter was applied. The _filter_applied flag is set by the data loading logic.
    filter_applied = stats_data.get("_filter_applied", False) or filter_info or focus_subreddits or ignore_subreddits

    if filter_applied:
        filter_desc_parts = [] # List to build the filter description sentence parts

        # Check and add date range filter info
        start_date = filter_info.get('start')
        end_date = filter_info.get('end')
        if start_date or end_date:
            start_str = start_date if start_date else "Beginning" # Use "Beginning" if start date wasn't specified
            end_str = end_date if end_date else "End" # Use "End" if end date wasn't specified
            filter_desc_parts.append(f"Date Range: {start_str} to {end_str}")

        # Check and add specific subreddit focus/ignore filter info from passed args
        # Note: Using the explicitly passed args (focus_subreddits, ignore_subreddits)
        # is generally more reliable than relying solely on the _filter_info dict
        # which might only contain the *type* of filter applied.
        if focus_subreddits:
            # Format the list of subreddits nicely
            focus_str = ', '.join(f"/r/{s}" for s in focus_subreddits)
            filter_desc_parts.append(f"Focused Subreddits: {focus_str}")
        if ignore_subreddits:
            ignore_str = ', '.join(f"/r/{s}" for s in ignore_subreddits)
            filter_desc_parts.append(f"Ignored Subreddits: {ignore_str}")

        # Add the formatted filter description to the report if any parts were added
        if filter_desc_parts:
             report += f"***Data Filters Applied:** {'; '.join(filter_desc_parts)} (UTC, based on creation time)*\n\n"
        elif filter_applied: # Fallback if filter_applied is True but no specific details were captured
             report += f"***Data Filter Applied:** Filter details not available in this context.*\n\n"

    # --- Handle Case: No data found after filtering ---
    # The data loading/filtering process sets this flag if no items match the criteria.
    if stats_data.get("_no_data_after_filter", False):
         report += "## Summary\n\n"
         report += "**No data found matching the specified filters.**\n"
         report += "*Report generation stopped.*\n"
         return report # Return the report string immediately, skipping subsequent sections.

    # --- Section I: Overall Activity Summary ---
    report += "## I. Overall Activity Summary\n"
    # Use Markdown table for key summary statistics
    report += "| Statistic             | Value         |\n"
    report += "|-----------------------|---------------|\n" # Separator line for Markdown table

    # Retrieve and format basic counts (posts and comments)
    report += f"| Total Posts Analyzed  | {stats_data.get('basic_counts', {}).get('total_posts', 'N/A')} |\n"
    report += f"| Total Comments Analyzed | {stats_data.get('basic_counts', {}).get('total_comments', 'N/A')} |\n"

    # Retrieve and format time range information
    time_range = stats_data.get('time_range', {}) # Get the time_range dictionary, default to empty dict
    report += f"| First Activity (Created) | {time_range.get('first_activity', 'N/A')} |\n" # Use .get() with default 'N/A'
    report += f"| Last Activity (Created)  | {time_range.get('last_activity', 'N/A')} |\n" # Use .get()

    # Retrieve and format account age information (from age_activity_analysis)
    age_analysis = stats_data.get("age_activity_analysis", {}) # Get age_analysis dict, default to empty dict
    report += f"| Account Created (UTC) | {age_analysis.get('account_created_formatted', 'N/A')} |\n" # Formatted creation time
    # Format age in days, adding " days" suffix unless "N/A"
    age_days_val = age_analysis.get('account_age_days', 'N/A')
    age_days_str = f"{age_days_val} days" if age_days_val != 'N/A' else 'N/A'
    report += f"| Account Age           | {age_days_str} |\n"

    # Retrieve and format total karma information (from engagement stats, which gets it from 'about')
    engagement = stats_data.get('engagement', {}) # Get engagement dict, default to empty dict
    report += f"| Total Link Karma      | **{engagement.get('total_link_karma', 'N/A')}** |\n" # Highlight total karma with bold
    report += f"| Total Comment Karma   | **{engagement.get('total_comment_karma', 'N/A')}** |\n"
    report += f"| **Total Combined Karma**| **{engagement.get('total_combined_karma', 'N/A')}** |\n"
    report += "\n" # Add a blank line for spacing in Markdown

    # --- Section II: Content & Style Analysis ---
    report += "## II. Content & Style Analysis\n"

    # Text Statistics Sub-section
    report += "**Text & Post Types:**\n"
    report += "| Statistic                 | Value         |\n"
    report += "|---------------------------|---------------|\n"
    text_stats = stats_data.get('text_stats', {}) # Get text_stats dict

    # Report total word counts
    report += f"| Total Word Count          | {text_stats.get('total_words', 'N/A')} |\n"
    report += f"|   *(Posts)*             | {text_stats.get('total_post_words', 'N/A')} |\n" # Sub-item formatting
    report += f"|   *(Comments)*          | {text_stats.get('total_comment_words', 'N/A')} |\n"

    # Report unique word count and lexical diversity
    report += f"| Total Unique Words (>1 char) | {text_stats.get('total_unique_words', 'N/A')} |\n"
    report += f"| Lexical Diversity         | {text_stats.get('lexical_diversity', 'N/A')} |\n" # Already formatted in calculation

    # Report average words per item type
    report += f"| Avg. Words per Post     | {text_stats.get('avg_post_word_length', 'N/A')} |\n" # Already formatted
    report += f"| Avg. Words per Comment  | {text_stats.get('avg_comment_word_length', 'N/A')} |\n"

    # --- Add Question Ratio Stats ---
    # This is a new addition to the report
    q_stats = stats_data.get("question_ratio_stats", {}) # Get question_ratio_stats dict

    # Check if the analysis was performed before trying to report its results
    if q_stats.get("analysis_performed"):
        q_items = q_stats.get('question_items', 'N/A')
        q_total = q_stats.get('total_items_analyzed', 'N/A')
        # Report the counts of items with questions and the total analyzed
        report += f"| Items containing Questions| {q_items} / {q_total} |\n"
        # Report the calculated ratio (already formatted as percentage)
        report += f"| Question Asking Ratio   | {q_stats.get('question_ratio', 'N/A')} |\n"
    else:
        # If analysis was skipped, report that and the reason
        reason = q_stats.get("reason", "Unknown reason")
        report += f"| Question Asking Ratio   | *Skipped ({reason})* |\n"
    # --- End Question Ratio Add ---

    # Report post types (link vs self posts)
    post_types = stats_data.get('post_types', {}) # Get post_types dict
    report += f"| Link Posts              | {post_types.get('link_posts', 'N/A')} |\n"
    report += f"| Self Posts              | {post_types.get('self_posts', 'N/A')} |\n"
    report += "\n"

    # Crossposting Sub-section
    xp_stats = stats_data.get('crosspost_stats', {}) # Get crosspost_stats dict
    report += "**Crossposting:**\n"
    total_xp_analyzed = xp_stats.get('total_posts_analyzed','N/A')
    # Report total crossposts count and percentage
    report += f"* Crossposts Made: {xp_stats.get('crosspost_count', 'N/A')} / {total_xp_analyzed} posts ({xp_stats.get('crosspost_percentage', 'N/A')})\n"
    source_subs = xp_stats.get('source_subreddits') # Get source_subreddits dict
    # Report top source subreddits if data exists
    if source_subs and isinstance(source_subs, dict) and source_subs:
        report += "* Top Source Subreddits for Crossposts:\n"
        # Iterate through the dictionary items (subreddit: count)
        for sub, count in source_subs.items():
             # Format special internal keys if they exist
             if sub.startswith("_"): sub_display = f"({sub[1:]})"
             else: sub_display = f"/r/{sub}"
             report += f"  * {sub_display} ({count})\n"
    elif total_xp_analyzed == 0:
         # Message if no posts were analyzed for crossposts
         report += "* No posts analyzed for crossposts.\n"
    else:
         # Message if posts were analyzed but none were crossposts
         report += "* No crossposts found or analyzed within filter.\n"
    report += "\n"

    # Editing Habits Sub-section
    edit_stats = stats_data.get("editing_stats", {}); # Get editing_stats dict
    report += "**Editing Habits:**\n"
    total_p_edit = edit_stats.get('total_posts_analyzed_for_edits', 0);
    total_c_edit = edit_stats.get('total_comments_analyzed_for_edits', 0)
    # Report counts and percentages of edited posts/comments
    report += f"* Posts Edited: {edit_stats.get('posts_edited_count', 'N/A')} / {total_p_edit} ({edit_stats.get('edit_percentage_posts', 'N/A')})\n"
    report += f"* Comments Edited: {edit_stats.get('comments_edited_count', 'N/A')} / {total_c_edit} ({edit_stats.get('edit_percentage_comments', 'N/A')})\n"
    # Report average edit delay in formatted string and seconds
    report += f"* Average Edit Delay: {edit_stats.get('average_edit_delay_formatted', 'N/A')} ({edit_stats.get('average_edit_delay_seconds', 'N/A')}s)\n"
    report += "\n"

    # Content Removal/Deletion Sub-section
    rd_stats = stats_data.get('removal_deletion_stats', {}) # Get removal_deletion_stats dict
    report += "**Content Removal/Deletion Estimate:**\n"
    # Report counts and percentages for removed/deleted posts/comments
    report += f"* Posts Removed (by Mod/etc.): {rd_stats.get('posts_content_removed', 'N/A')} ({rd_stats.get('posts_content_removed_percentage', 'N/A')})\n"
    report += f"* Posts Deleted (by User): {rd_stats.get('posts_user_deleted', 'N/A')} ({rd_stats.get('posts_user_deleted_percentage', 'N/A')})\n"
    report += f"* Comments Removed (by Mod/etc.): {rd_stats.get('comments_content_removed', 'N/A')} ({rd_stats.get('comments_content_removed_percentage', 'N/A')})\n"
    report += f"* Comments Deleted (by User): {rd_stats.get('comments_user_deleted', 'N/A')} ({rd_stats.get('comments_user_deleted_percentage', 'N/A')})\n"
    # Add a note about the estimation method
    report += "* *Note: Estimate based on `[removed]`/`[deleted]` markers. Accuracy may vary.*\n\n"

    # Sentiment Ratio Sub-section
    sentiment = stats_data.get("sentiment_ratio", {}); # Get sentiment_ratio dict
    # Check if sentiment analysis was skipped and report the reason
    if sentiment.get("sentiment_analysis_skipped"):
        report += f"**Sentiment Analysis (VADER):** *Skipped ({sentiment.get('reason', 'Unknown')})*\n\n"
    else:
        # If analysis was performed, report the results
        report += "**Sentiment Analysis (VADER):**\n"
        total_sent_items = sentiment.get('total_items_sentiment_analyzed', 0)
        pos_count = sentiment.get('positive_count','N/A')
        neg_count = sentiment.get('negative_count','N/A')
        neu_count = sentiment.get('neutral_count','N/A')
        # Report counts for each sentiment category
        report += f"* Item Counts (Pos/Neg/Neu): {pos_count} / {neg_count} / {neu_count} (Total: {total_sent_items})\n"
        # Report the positive-to-negative ratio
        report += f"* Positive-to-Negative Ratio: {sentiment.get('pos_neg_ratio', 'N/A')}\n"
        # Report the average compound score
        avg_score_str = sentiment.get('avg_compound_score', 'N/A') # Already formatted
        report += f"* Average Compound Score: {avg_score_str} (Range: -1 to +1)\n"
        report += "\n"

    # --- Section III: Engagement & Recognition ---
    report += "## III. Engagement & Recognition\n"

    # Item Scores (Sum & Average) Sub-section
    report += "**Item Scores (Sum & Average):**\n"
    report += "| Statistic               | Value         |\n"
    report += "|-------------------------|---------------|\n"
    # Report total item scores (sum of scores for items within the filter)
    report += f"| Sum of Post Scores      | {engagement.get('total_item_post_score', 'N/A')} |\n"
    report += f"| Sum of Comment Scores   | {engagement.get('total_item_comment_score', 'N/A')} |\n"
    # Report average item scores
    report += f"| Avg. Post Score         | {engagement.get('avg_item_post_score', 'N/A')} |\n" # Already formatted
    report += f"| Avg. Comment Score      | {engagement.get('avg_item_comment_score', 'N/A')} |\n" # Already formatted
    report += "*Note: Scores based on items within the filtered data.*\n\n" # Clarify scope

    # Score Distribution & Top/Bottom Items Sub-section
    score_stats = stats_data.get("score_stats", {}) # Get score_stats dict
    dist_posts = score_stats.get("post_score_distribution", {}); # Get post distribution dict
    dist_comments = score_stats.get("comment_score_distribution", {}) # Get comment distribution dict

    # Report post score distribution if data exists
    if dist_posts.get("count", 0) > 0:
        report += f"**Post Score Distribution:** Count={dist_posts['count']}, Min={dist_posts.get('min','N/A')}, Q1={dist_posts.get('q1','N/A')}, Med={dist_posts.get('median','N/A')}, Q3={dist_posts.get('q3','N/A')}, Max={dist_posts.get('max','N/A')}, Avg={dist_posts.get('average','N/A')}\n"
    # Report comment score distribution if data exists
    if dist_comments.get("count", 0) > 0:
        report += f"**Comment Score Distribution:** Count={dist_comments['count']}, Min={dist_comments.get('min','N/A')}, Q1={dist_comments.get('q1','N/A')}, Med={dist_comments.get('median','N/A')}, Q3={dist_comments.get('q3','N/A')}, Max={dist_comments.get('max','N/A')}, Avg={dist_comments.get('average','N/A')}\n"
        report += "\n" # Add extra line break after comment distribution

    # Report top/bottom scored items
    top_posts = score_stats.get("top_posts", []); bottom_posts = score_stats.get("bottom_posts", [])
    top_comments = score_stats.get("top_comments", []); bottom_comments = score_stats.get("bottom_comments", [])

    # Determine the effective 'top_n' based on how many items were actually returned
    top_n_disp = max(len(top_posts), len(top_comments), len(bottom_posts), len(bottom_comments))

    # Only include this sub-section if there are items to display in top/bottom lists
    if top_n_disp > 0 :
        report += f"**Top {top_n_disp} Scored Posts:**\n"
        if top_posts:
            # Iterate through top posts (score, permalink, title)
            for score, link, title in top_posts:
                # Format as a list item with score, truncated title, and link
                report += f"* `+{score}`: [{title[:60]}...](https://reddit.com{link})\n" # Truncate title to 60 chars
        else:
            report += "* *(None within filter)*\n" # Message if no top posts found within the filter

        report += f"**Top {top_n_disp} Scored Comments:**\n"
        if top_comments:
            # Iterate through top comments (score, permalink, snippet)
            for score, link, snippet in top_comments:
                 # Format as a list item with score, snippet, and link
                 report += f"* `+{score}`: [{snippet}](https://reddit.com{link})\n"
        else:
            report += "* *(None within filter)*\n" # Message if no top comments found

        report += f"**Lowest {top_n_disp} Scored Posts:**\n"
        if bottom_posts:
            # Iterate through bottom posts (score, permalink, title)
            for score, link, title in bottom_posts:
                 report += f"* `{score}`: [{title[:60]}...](https://reddit.com{link})\n" # Truncate title
        else:
            report += "* *(None within filter)*\n" # Message if no bottom posts found

        report += f"**Lowest {top_n_disp} Scored Comments:**\n"
        if bottom_comments:
            # Iterate through bottom comments (score, permalink, snippet)
            for score, link, snippet in bottom_comments:
                 report += f"* `{score}`: [{snippet}](https://reddit.com{link})\n"
        else:
            report += "* *(None within filter)*\n" # Message if no bottom comments found
        report += "\n"
    else:
        # Message if no items were available for top/bottom lists
        report += "**Top/Bottom Scored Items:** *N/A or Not Calculated*\n\n"


    # Awards Received Sub-section
    award_stats = stats_data.get('award_stats', {}) # Get award_stats dict
    report += "**Awards Received:**\n"
    # Report total awards count and count of items that received awards
    report += f"* 🏆 Total Awards: {award_stats.get('total_awards_received', 'N/A')}\n"
    report += f"* ✨ Items Awarded: {award_stats.get('items_with_awards', 'N/A')}\n"
    report += "\n"

    # Post Engagement Sub-section (comments per post)
    post_engage = stats_data.get("post_engagement", {}); # Get post_engagement dict
    report += "**Post Engagement:**\n"
    # Report average comments per post
    report += f"* Average Comments Received per Post: {post_engage.get('avg_comments_per_post', 'N/A')} (Analyzed: {post_engage.get('total_posts_analyzed_for_comments', 'N/A')} posts)\n"
    top_commented = post_engage.get("top_commented_posts") # Get list of top commented posts (count, link, title)

    # Report top commented posts if data exists
    if top_commented:
        report += f"* Top {len(top_commented)} Most Commented Posts:\n";
        for num_comments, link, title in top_commented:
             # Format as list item with comment count, truncated title, and link
             report += f"  * `{num_comments} comments`: [{title[:60]}...](https://reddit.com{link})\n"; # Truncate title
    else:
        report += "* Top Commented Posts: *(None within filter or not analyzed)*\n" # Message if no top commented posts found
    report += "\n"

    # --- Section IV: Subreddit Activity & Flair ---
    report += "## IV. Subreddit Activity & Flair\n";
    sub_activity = stats_data.get('subreddit_activity', {}) # Get subreddit_activity dict

    # Get activity counts and list of all unique subs
    posts_per_sub = sub_activity.get('posts_per_subreddit', {});
    comments_per_sub = sub_activity.get('comments_per_subreddit', {});
    all_subs = sub_activity.get('all_active_subs', []) # List of all unique subreddits

    # Get subreddit diversity stats
    sub_diversity = stats_data.get('subreddit_diversity', {}) # Get subreddit_diversity dict

    # Handle case with no subreddit activity found
    if not all_subs:
        report += "No subreddit activity found within filter.\n\n"
    else:
        num_all_subs = len(all_subs)
        # Report overall count of unique subreddits
        report += f"Active in **{num_all_subs}** unique subreddits ({sub_activity.get('unique_subs_posted', 0)} posted in, {sub_activity.get('unique_subs_commented', 0)} commented in).\n"
        # Report Subreddit Diversity Index (Simpson)
        report += f"* **Subreddit Diversity (Simpson Index): {sub_diversity.get('simpson_diversity_index', 'N/A')}** (0=Low, 1=High)\n"
        # Report Normalized Shannon Entropy if available and not an error
        if sub_diversity.get('normalized_shannon_entropy', 'N/A') not in ['N/A', 'Error']:
             report += f"* Subreddit Diversity (Norm. Shannon Entropy): {sub_diversity.get('normalized_shannon_entropy', 'N/A')} (0=Low, 1=High)\n"
        report += "\n"

        # Activity Distribution Table
        report += "**Activity Distribution:**\n"
        report += "| Subreddit         | Posts | Comments | Total |\n"
        report += "|-------------------|-------|----------|-------|\n"

        # Combine post and comment counts into a single Counter for total activity
        combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub);
        # Sort the list of all unique subreddits by their total activity (descending)
        sorted_subs_by_activity = sorted(all_subs, key=lambda sub: combined_activity.get(sub, 0), reverse=True)

        # Limit the number of subreddits displayed in the main table to keep the report concise
        display_sub_limit = 50 # Adjust this limit as needed

        # Iterate through the sorted list and add rows to the table
        for i, sub in enumerate(sorted_subs_by_activity):
            if i >= display_sub_limit:
                 # Add a row indicating truncation if there are more subs
                 report += f"| ...and {len(sorted_subs_by_activity)-display_sub_limit} more | ... | ... | ... |\n";
                 break # Stop adding rows

            # Get counts for the current subreddit
            p_count = posts_per_sub.get(sub, 0);
            c_count = comments_per_sub.get(sub, 0);
            t_count = combined_activity.get(sub, 0);

            # Add the table row, formatting columns for alignment
            report += f"| /r/{sub:<15} | {p_count:<5} | {c_count:<8} | **{t_count:<5}** |\n";

        report += "\n"

        # Top Subreddits List (Simplified view)
        top_n_subs = 10 # Number of top subs to list explicitly
        if combined_activity: # Ensure there was some activity to list
            report += f"**Top {min(top_n_subs, len(combined_activity))} Most Active Subreddits (Posts + Comments):**\n";
            # Use most_common() to get the top N directly from the Counter
            for i, (sub, count) in enumerate(combined_activity.most_common(top_n_subs)):
                 report += f"* {i+1}. /r/{sub} ({count})\n";
            report += "\n"

    # Flair Usage Sub-section
    flair_stats = stats_data.get("flair_stats", {}); # Get flair_stats dict
    report += "**Flair Usage:**\n";

    # Get flair dictionaries and total counts of items with flair
    user_flairs = flair_stats.get("user_flairs_by_sub", {});
    post_flairs = flair_stats.get("post_flairs_by_sub", {})
    user_flair_count = flair_stats.get('total_comments_with_user_flair', 0)
    post_flair_count = flair_stats.get('total_posts_with_link_flair', 0)

    # Report User Flairs if data exists
    if user_flairs:
        report += f"* User Flairs Used ({user_flair_count} instances):\n";
        display_count = 0 # Counter for limiting displayed items
        # user_flairs is already sorted by frequency
        for flair_combo, count in user_flairs.items():
            if display_count >= 10: # Limit to top 10 displayed flairs
                break
            try:
                # Split the "subreddit: flair" string if possible
                sub, flair_text = flair_combo.split(': ', 1);
                report += f"  * `/r/{sub}`: `{flair_text}` ({count})\n"; # Format as list item
                display_count += 1
            except ValueError:
                # Handle cases where the key might not be in the expected "sub: flair" format
                report += f"  * `{flair_combo}` ({count})\n";
                display_count += 1
        if len(user_flairs) > 10: # Indicate if more flairs exist than displayed
            report += f"  * ... and {len(user_flairs)-10} more\n"
    else:
        # Message if no user flairs were found or analyzed
        report += f"* User Flairs Used: {user_flair_count} instances found/analyzed.\n"

    # Report Post Flairs if data exists
    if post_flairs:
        report += f"* Post Flairs Used ({post_flair_count} instances):\n";
        display_count = 0 # Counter for limiting displayed items
        # post_flairs is already sorted by frequency
        for flair_combo, count in post_flairs.items():
             if display_count >= 10: # Limit to top 10 displayed flairs
                 break
             try:
                 # Split the "subreddit: flair" string
                 sub, flair_text = flair_combo.split(': ', 1);
                 report += f"  * `/r/{sub}`: `{flair_text}` ({count})\n";
                 display_count += 1
             except ValueError:
                 # Handle unexpected key format
                 report += f"  * `{flair_combo}` ({count})\n";
                 display_count += 1
        if len(post_flairs) > 10: # Indicate if more flairs exist than displayed
             report += f"  * ... and {len(post_flairs)-10} more\n"
    else:
        # Message if no post flairs were found or analyzed
        report += f"* Post Flairs Used: {post_flair_count} instances found/analyzed.\n";
    report += "\n"


    # --- Section V: Temporal Activity Patterns (UTC) ---
    report += "## V. Temporal Activity Patterns (UTC)\n";
    temporal_data = stats_data.get('temporal_stats', {}); # Get temporal_stats dict
    total_temporal_items = temporal_data.get("total_items_for_temporal", 0) # Total items used for temporal analysis

    # Handle case with no temporal data
    if not total_temporal_items or total_temporal_items == 0:
        report += "No temporal data available or calculated within filter.\n\n"
    else:
        # Indicate how many items were used for temporal analysis
        report += f"*(Based on creation time of {total_temporal_items} items within filter)*\n\n";

        # Account Age vs Activity Trend Sub-section
        age_analysis = stats_data.get("age_activity_analysis", {}) # Get age_analysis dict
        report += "**Account Age vs Activity Trend:**\n"
        # Report calculated average activity rates and overall trend estimate
        report += f"* Average Activity Rate (per year): {age_analysis.get('average_activity_per_year', 'N/A')}\n"
        report += f"* Average Activity Rate (per month): {age_analysis.get('average_activity_per_month', 'N/A')}\n"
        report += f"* Overall Trend Estimate: **{age_analysis.get('activity_trend_status', 'N/A')}**\n\n" # Highlight trend

        # Activity Timing (Burstiness) Sub-section
        burst_stats = stats_data.get('activity_burstiness', {}) # Get activity_burstiness dict
        # Report burstiness stats if data exists
        if burst_stats.get('num_intervals_analyzed', 0) > 0:
            report += "**Activity Timing (Burstiness):**\n"
            report += f"* Number of Intervals Analyzed: {burst_stats.get('num_intervals_analyzed', 'N/A')}\n"
            # Report interval statistics in formatted and raw seconds
            report += f"* Mean Interval Between Activities: {burst_stats.get('mean_interval_formatted', 'N/A')} ({burst_stats.get('mean_interval_s', 'N/A')}s)\n"
            report += f"* Median Interval Between Activities: {burst_stats.get('median_interval_formatted', 'N/A')} ({burst_stats.get('median_interval_s', 'N/A')}s)\n"
            report += f"* Interval Standard Deviation: **{burst_stats.get('stdev_interval_formatted', 'N/A')}** ({burst_stats.get('stdev_interval_s', 'N/A')}s)\n" # Highlight StDev
            report += f"* Min/Max Interval: {burst_stats.get('min_interval_formatted', 'N/A')} / {burst_stats.get('max_interval_formatted', 'N/A')}\n"
            # Add note explaining StDev significance for burstiness
            report += "* *(Higher StDev indicates more 'bursty' activity vs. regular intervals)*\n\n"
        else:
            report += "* Insufficient data for burstiness analysis.\n\n" # Message if not enough data

        # Detailed Activity Distributions (Hour, Weekday, Month, Year)
        activity_hour = temporal_data.get('activity_by_hour_utc', {});
        activity_wday = temporal_data.get('activity_by_weekday_utc', {});
        activity_month = temporal_data.get('activity_by_month_utc', {});
        activity_year = temporal_data.get('activity_by_year_utc', {})

        # Activity by Hour (UTC) - using a simple character bar chart in a code block
        if activity_hour:
            report += "**Activity by Hour of Day (00-23 UTC):**\n"
            report += "```\n"; # Start Markdown code block for fixed-width formatting
            # Calculate scaling factor for the bar chart visualization
            max_val_hr = max(activity_hour.values()) if activity_hour else 0;
            scale_hr = 50 / max_val_hr if max_val_hr > 0 else 0; # Scale bars to a max width of 50 characters
            # Iterate through hours 0-23
            for hour in range(24):
                hour_str = f"{hour:02d}"; # Format hour with leading zero
                count = activity_hour.get(hour_str, 0); # Get count, default to 0 if hour is missing
                bar = '#' * int(count * scale_hr); # Generate the bar using '#' characters
                report += f"{hour_str}: {bar:<50} ({count})\n"; # Format line with hour, bar, and count
            report += "```\n" # End Markdown code block

        # Activity by Day of Week (UTC) - using a simple character bar chart
        if activity_wday:
            report += "**Activity by Day of Week (UTC):**\n"
            report += "```\n"; # Start Markdown code block
            # Calculate scaling factor for the bar chart
            max_val_wd = max(activity_wday.values()) if activity_wday else 0;
            scale_wd = 50 / max_val_wd if max_val_wd > 0 else 0; # Scale bars to a max width of 50 characters
            days_ordered = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]; # Ensure consistent order
            # Iterate through days in order
            for day in days_ordered:
                count = activity_wday.get(day, 0); # Get count, default to 0
                bar = '#' * int(count * scale_wd); # Generate bar
                report += f"{day:<9}: {bar:<50} ({count})\n"; # Format line
            report += "```\n" # End Markdown code block

        # Activity by Month (YYYY-MM UTC) - using a Markdown table
        if activity_month:
            report += "**Activity by Month (YYYY-MM UTC):**\n";
            report += "| Month   | Count |\n" # Table header
            report += "|---------|-------|\n"; # Separator

            # Limit the number of months displayed if there are many
            display_month_limit = 60 # Adjust as needed
            month_items = list(activity_month.items()) # Get list of (month_key, count) tuples

            # Iterate through month items (already sorted chronologically by calculation function)
            for i, (month_key, count) in enumerate(month_items):
                 if i >= display_month_limit:
                     # Add a truncation row if limit reached
                     report += f"| ...and {len(month_items)-display_month_limit} more | ... |\n";
                     break # Stop adding rows
                 report += f"| {month_key} | {count:<5} |\n"; # Add table row, formatting count column
            report += "\n"

        # Activity by Year (UTC) - using a Markdown table
        if activity_year:
            report += "**Activity by Year (UTC):**\n";
            report += "| Year | Count |\n" # Table header
            report += "|------|-------|\n"; # Separator
            # Iterate through year items (already sorted chronologically by calculation function)
            for year, count in activity_year.items():
                report += f"| {year} | {count:<5} |\n"; # Add table row, formatting count column
            report += "\n"

    # --- Section VI: Word, Phrase & Mention Frequency --- [RENAMED SECTION]
    report += "## VI. Word, Phrase & Mention Frequency\n"

    # Get frequency data dictionaries
    word_freq_section = stats_data.get('word_frequency', {})
    word_freq = word_freq_section.get('word_frequency', {}) # Top words frequency dict
    ngram_freq = stats_data.get('ngram_frequency', {}) # N-gram frequency dict (contains 'bigrams', 'trigrams' etc.)
    mention_stats = stats_data.get('mention_stats', {}) # Mention frequency stats dict

    # Check if *any* data is available for this entire section
    no_freq_data = not word_freq and \
                   not any(ngram_freq.values()) and \
                   not mention_stats.get("analysis_performed") # Check if mention analysis even ran

    if no_freq_data:
        # Message if no data was found/calculated for this section
        report += "No word, phrase, or mention frequency data available (requires CSV processing and/or dependencies).\n\n"
    else:
        # --- Word Frequency Sub-section ---
        if word_freq: # Check if word frequency data exists
            top_n_words = len(word_freq) # Get the actual number of top words returned
            report += f"**Top {top_n_words} Most Frequent Words:**\n";
            report += "*(Cleaned, stop words removed, from filtered data)*\n\n";
            report += "| Word             | Count |\n" # Table header
            report += "|------------------|-------|\n"; # Separator
            # Iterate through word frequency items (already sorted by count)
            for word, count in word_freq.items():
                report += f"| {word:<16} | {count:<5} |\n"; # Add table row
            report += "\n"
        else:
            report += "**Word Frequency:** *N/A or Not Calculated*\n\n" # Message if no word frequency data

        # --- N-gram Frequency Sub-sections ---
        # Iterate through each type of n-gram (bigrams, trigrams, etc.) stored in ngram_freq
        for n_key, n_data in ngram_freq.items():
             if n_data: # Check if data exists for this n-gram type
                 # Determine the 'N' number from the key name (e.g., 'bigrams' -> 2)
                 n = {'bigrams': 2, 'trigrams': 3}.get(n_key, '?');
                 top_n_phrases = len(n_data) # Get the actual number of top phrases returned
                 report += f"**Top {top_n_phrases} Most Frequent {n_key.capitalize()} ({n}-word phrases):**\n";
                 report += "*(Cleaned, stop words removed)*\n\n";
                 report += "| Phrase                   | Count |\n" # Table header
                 report += "|--------------------------|-------|\n"; # Separator
                 max_phrase_len = 24 # Max length for the phrase column in the table to keep it readable

                 # Iterate through n-gram frequency items (already sorted by count)
                 for phrase, count in n_data.items():
                     # Truncate long phrases for display to fit within the table column width
                     display_phrase = phrase[:max_phrase_len] + ('...' if len(phrase) > max_phrase_len else '')
                     # Add table row, padding the truncated phrase and count columns
                     # Add 3 extra spaces to padding for the potential '...'
                     report += f"| {display_phrase:<{max_phrase_len+3}} | {count:<5} |\n"
                 report += "\n"
             else:
                 # Message if no data exists for this n-gram type
                 report += f"**{n_key.capitalize()}:** *N/A or Not Calculated*\n\n"

        # --- Add Mention Frequency Sub-section ---
        # This is a new addition to the report
        report += "**Mention Frequency:**\n"
        # Check if the mention frequency analysis was performed
        if mention_stats.get("analysis_performed"):
            # Get the top mention lists and total instance counts
            top_users = mention_stats.get("top_user_mentions", {})
            top_subs = mention_stats.get("top_subreddit_mentions", {})
            total_user_instances = mention_stats.get("total_user_mention_instances", 0)
            total_sub_instances = mention_stats.get("total_subreddit_mention_instances", 0)

            # Report total mention instances
            report += f"* Total User Mention Instances: {total_user_instances}\n"
            # Report top mentioned users if data exists
            if top_users:
                 report += f"* Top {len(top_users)} Mentioned Users (by frequency):\n"
                 # Iterate and format top user mentions as list items
                 for user, count in top_users.items():
                     report += f"  * u/{user} ({count})\n"
            else:
                 report += "* Top Mentioned Users: *(None found)*\n"

            # Report total subreddit mention instances
            report += f"* Total Subreddit Mention Instances: {total_sub_instances}\n"
            # Report top mentioned subreddits if data exists
            if top_subs:
                 report += f"* Top {len(top_subs)} Mentioned Subreddits (by frequency):\n"
                 # Iterate and format top subreddit mentions as list items
                 for sub, count in top_subs.items():
                     report += f"  * r/{sub} ({count})\n"
            else:
                 report += "* Top Mentioned Subreddits: *(None found)*\n"
            # Add a note about the method used
            report += "* *Note: Based on regex pattern matching `u/` and `r/`.*\n\n"
        else:
            # If analysis was skipped, report that and the reason
            reason = mention_stats.get("reason", "Unknown reason")
            report += f"* *Skipped ({reason})*\n\n"
        # --- End Mention Frequency Add ---


    # --- Section VII: Sentiment Trend Over Time (NEW SECTION) ---
    report += "## VII. Sentiment Trend Over Time (VADER)\n"
    sentiment_arc = stats_data.get('sentiment_arc', {}) # Get sentiment_arc dict

    # Check if the sentiment arc analysis was performed
    if sentiment_arc.get('analysis_performed'):
        arc_data = sentiment_arc.get('sentiment_arc_data', {}) # Get the time series data (dict of window: score)
        window_type = sentiment_arc.get('window_type', 'Time Period').capitalize() # Get the actual window type used (e.g., 'monthly', 'yearly') and capitalize for display

        if arc_data: # Check if there are any data points in the arc
            report += f"**Average Sentiment (Compound Score) by {window_type}:**\n"
            report += f"*(Range: -1 Negative to +1 Positive)*\n\n"

            # Determine the required width for the window column in the Markdown table
            # Adjust based on window type format (YYYY-MM is longer than YYYY)
            window_col_width = 12 if window_type == "Monthly" else 10 # Default to 10 if unknown

            # Create the Markdown table header
            report += f"| {window_type:<{window_col_width}} | Avg. Score |\n"
            # Create the separator line. Ensure it matches the width of the header.
            report += f"|{'-'*window_col_width}-|------------|\n"

            # Limit the number of windows displayed if the arc data is very long
            display_arc_limit = 60 # Adjust this limit as needed
            arc_items = list(arc_data.items()) # Get a list of (window_key, avg_score) tuples

            # Iterate through the arc data items (already sorted chronologically by window_key)
            for i, (window, avg_score) in enumerate(arc_items):
                if i >= display_arc_limit:
                     # Add a truncation row if the display limit is reached
                     report += f"| ...and {len(arc_items)-display_arc_limit} more | ...        |\n";
                     break # Stop adding rows

                # Add a table row.
                # Format the average score:
                # `^+` ensures the sign (+ or -) is always shown.
                # `11.3f` means a float, total width 11 characters (including sign, decimal point), 3 decimal places.
                # This formatting helps align scores visually.
                report += f"| {window:<{window_col_width}} | {avg_score:^+11.3f} |\n"

            report += "\n" # Add a blank line after the table

        else:
            report += "*No sentiment data points found to generate trend.*\n\n" # Message if arc data was empty
    else:
        # If sentiment arc analysis was skipped, report that and the reason
        reason = sentiment_arc.get("reason", "Unknown reason")
        report += f"*Analysis skipped ({reason})*\n\n"

    # --- Report Footer (Optional) ---
    # Add any closing remarks or notes here if needed.
    # report += "---\n\n*End of Report*\n"


    return report # Return the complete Markdown report string


# --- Comparison Report Formatting ---
# This function formats the comparison results between two user datasets.
# It takes the full stats for both users AND the specific comparison metrics
# calculated separately (like Jaccard indices).

def _format_comparison_report(stats1, stats2, user1, user2, comparison_results):
    """
    Formats the comparison statistics between two users/datasets into a
    plain Markdown report.

    This function takes the pre-calculated stats dictionaries for both
    entities and the results of specific comparison calculations (like overlap).
    It structures this data into a comparison report, often using tables
    to show metrics side-by-side.

    Args:
        stats1 (dict): The statistics dictionary for the first user/dataset.
        stats2 (dict): The statistics dictionary for the second user/dataset.
        user1 (str): The username or identifier for the first entity.
        user2 (str): The username or identifier for the second entity.
        comparison_results (dict): A dictionary containing the results of
                                   specific comparison calculations (e.g.,
                                   'subreddit_overlap', 'word_frequency_comparison').

    Returns:
        str: A multi-line string formatted in Markdown, representing the comparison report.
    """
    # ***** NO CHANGES NEEDED IN THIS FUNCTION for the new single-user features *****
    # This function already handles the comparison of the stats *sections* that
    # existed previously. The new single-user sections (Question Ratio, Mention
    # Frequency, Sentiment Arc) are not directly compared metric-by-metric in
    # this existing comparison logic. If comparison for these new stats were needed,
    # new comparison calculations would be added in comparison.py, and new
    # sections would be added to this formatting function.

    logging.debug(f"      Formatting comparison report for /u/{user1} vs /u/{user2}...")

    # Start building the Markdown report
    report = f"# Reddit User Comparison Report: /u/{user1} vs /u/{user2}\n\n"

    # Add report generation timestamp
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S');
    report += f"*Report generated: {dt_now}*\n\n"

    # Add a note about data filtering, if applicable for either user
    filter1_applied = stats1.get("_filter_applied", False);
    filter2_applied = stats2.get("_filter_applied", False)
    if filter1_applied or filter2_applied:
        report += f"*Note: Comparison values derived from base statistics generated for each user. Base statistics may have been filtered by date or subreddit.*\n\n"
    else:
        report += f"*Note: Statistics generally based on full available history for each user.*\n\n"


    # --- Section I: Basic Stats Comparison ---
    report += "## I. Overall Activity Comparison\n"

    # Determine appropriate column width for usernames in the Markdown table
    u1_len = len(user1);
    u2_len = len(user2);
    max_u_len = max(u1_len, u2_len, 18) # Ensure a minimum width (e.g., 18)

    # Define header and row formats using f-strings with dynamic padding based on max_u_len
    header_fmt = "| {:<18} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    # Row format: Statistic column (fixed width), then two value columns (dynamic width + padding)
    row_fmt = "| {:<18} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n" # +4 for padding around value
    # Bold row format (for karma): same as row_fmt but values are bolded within the padding
    bold_row_fmt = "| {:<18} | **{:<" + str(max_u_len+2) + "}** | **{:<" + str(max_u_len+2) + "}** |\n" # +2 for bold markers **

    # Add table header and separator
    report += header_fmt.format("Statistic", user1, user2);
    # Separator needs to match header structure and padding exactly
    report += "|--------------------|-" + "-"*(max_u_len+4) + "|-" + "-"*(max_u_len+4) + "|\n"

    # Populate table rows with basic stats for both users
    report += row_fmt.format("Total Posts", stats1.get('basic_counts', {}).get('total_posts', 'N/A'), stats2.get('basic_counts', {}).get('total_posts', 'N/A'))
    report += row_fmt.format("Total Comments", stats1.get('basic_counts', {}).get('total_comments', 'N/A'), stats2.get('basic_counts', {}).get('total_comments', 'N/A'))
    report += row_fmt.format("First Activity", stats1.get('time_range', {}).get('first_activity', 'N/A'), stats2.get('time_range', {}).get('first_activity', 'N/A'))
    report += row_fmt.format("Last Activity", stats1.get('time_range', {}).get('last_activity', 'N/A'), stats2.get('time_range', {}).get('last_activity', 'N/A'))

    # Format Account Age string
    age1_str = stats1.get('age_activity_analysis', {}).get('account_age_days', 'N/A');
    age2_str = stats2.get('age_activity_analysis', {}).get('account_age_days', 'N/A')
    if age1_str != 'N/A': age1_str = f"{age1_str} days"
    if age2_str != 'N/A': age2_str = f"{age2_str} days"
    report += row_fmt.format("Account Age", age1_str, age2_str)

    # Populate rows for karma, using bold formatting
    eng1 = stats1.get('engagement', {});
    eng2 = stats2.get('engagement', {})
    report += bold_row_fmt.format("Link Karma", eng1.get('total_link_karma', 'N/A'), eng2.get('total_link_karma', 'N/A'))
    report += bold_row_fmt.format("Comment Karma", eng1.get('total_comment_karma', 'N/A'), eng2.get('total_comment_karma', 'N/A'))
    report += bold_row_fmt.format("Combined Karma", eng1.get('total_combined_karma', 'N/A'), eng2.get('total_combined_karma', 'N/A'))

    # Populate rows for average item scores
    report += row_fmt.format("Avg Post Score", eng1.get('avg_item_post_score', 'N/A'), eng2.get('avg_item_post_score', 'N/A'))
    report += row_fmt.format("Avg Comment Score", eng1.get('avg_item_comment_score', 'N/A'), eng2.get('avg_item_comment_score', 'N/A'));
    report += "\n" # Add blank line


    # --- Section II: Subreddit Activity Comparison ---
    report += "## II. Subreddit Activity Comparison\n";
    # Get the subreddit overlap calculation results
    sub_overlap = comparison_results.get("subreddit_overlap", {})
    # Get subreddit diversity stats for each user
    div1 = stats1.get('subreddit_diversity', {});
    div2 = stats2.get('subreddit_diversity', {})

    # Report key overlap metrics (shared count, Jaccard index)
    report += f"* **Shared Subreddits:** {sub_overlap.get('num_shared', 0)}\n";
    report += f"* **Jaccard Index (Similarity):** {sub_overlap.get('jaccard_index', 'N/A')}\n\n"

    # Create table comparing other subreddit activity stats
    sub_header_fmt = "| {:<29} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    sub_row_fmt = "| {:<29} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
    sub_bold_row_fmt = "| {:<29} | **{:<" + str(max_u_len+2) + "}** | **{:<" + str(max_u_len+2) + "}** |\n"
    report += sub_header_fmt.format("Statistic", user1, user2);
    report += "|-------------------------------|-" + "-"*(max_u_len+4) + "|-" + "-"*(max_u_len+4) + "|\n"

    # Populate rows with subreddit activity counts and diversity index
    sub_act1 = stats1.get('subreddit_activity', {});
    sub_act2 = stats2.get('subreddit_activity', {})
    report += sub_row_fmt.format("Unique Subs Active", len(sub_act1.get('all_active_subs', [])), len(sub_act2.get('all_active_subs', [])))
    report += sub_row_fmt.format("Unique Subs Posted", sub_act1.get('unique_subs_posted', 'N/A'), sub_act2.get('unique_subs_posted', 'N/A'))
    report += sub_row_fmt.format("Unique Subs Commented", sub_act1.get('unique_subs_commented', 'N/A'), sub_act2.get('unique_subs_commented', 'N/A'))
    report += sub_bold_row_fmt.format("Simpson Diversity Index", div1.get('simpson_diversity_index', 'N/A'), div2.get('simpson_diversity_index', 'N/A'));
    report += "\n"
    report += "* *(Diversity Index: 0=Lowest, 1=Highest)*\n\n";

    # List the shared subreddits (up to a certain limit)
    shared_list = sub_overlap.get("shared_subreddits", [])
    if shared_list:
        report += "**Shared Subreddits List:**\n";
        max_shared_display = 20; # Limit the number of shared subreddits listed
        for i, sub in enumerate(shared_list):
            if i >= max_shared_display:
                report += f"* ... and {len(shared_list)-max_shared_display} more\n";
                break
            report += f"* /r/{sub}\n";
        report += "\n"

    # Helper function to get top N subreddits for listing
    def get_top_subs(stats, n=5):
        posts = stats.get('subreddit_activity', {}).get('posts_per_subreddit', {});
        comments = stats.get('subreddit_activity', {}).get('comments_per_subreddit', {});
        # Combine counts and get top N most common using Counter
        combined = Counter(posts) + Counter(comments);
        return combined.most_common(n)

    # List top 5 subreddits for each user
    top_subs1 = get_top_subs(stats1);
    report += f"**Top 5 Subreddits for /u/{user1}:**\n";
    if top_subs1:
        for i, (sub, count) in enumerate(top_subs1):
            report += f"* {i+1}. /r/{sub} ({count})\n"
    else:
        report += "* *(None)*\n"

    top_subs2 = get_top_subs(stats2);
    report += f"\n**Top 5 Subreddits for /u/{user2}:**\n";
    if top_subs2:
        for i, (sub, count) in enumerate(top_subs2):
            report += f"* {i+1}. /r/{sub} ({count})\n"
    else:
        report += "* *(None)*\n";
    report += "\n"

    # --- Section III: Word & Phrase Frequency Comparison ---
    report += "## III. Word & Phrase Frequency Comparison\n";
    # Get word frequency comparison results (calculated in comparison.py)
    word_comp = comparison_results.get("word_frequency_comparison", {});
    top_n_words_comp = word_comp.get("top_n_compared", "N/A") # The N value used for comparison calculation

    # Report word comparison metrics
    report += f"**Single Words:**\n";
    report += f"* Comparison based on Top {top_n_words_comp} frequent words (stop words removed).\n";
    report += f"* Shared Top {top_n_words_comp} Words: {word_comp.get('num_shared_top_words', 'N/A')}\n";
    report += f"* Jaccard Index (Word Similarity): {word_comp.get('jaccard_index', 'N/A')}\n\n"

    # List the shared top words (the ones in the intersection)
    shared_word_list = word_comp.get("shared_top_words", [])
    if shared_word_list:
        report += "*Shared Top Words List:*\n";
        # Format as an inline code block list for readability
        report += "`" + "`, `".join(shared_word_list) + "`\n\n"

    # Helper function to get top N words for listing
    def get_top_words(stats, n=5):
        freq_section = stats.get('word_frequency', {});
        word_freq = freq_section.get('word_frequency', {});
        # Get top N items as a list of (word, count) tuples
        return list(word_freq.items())[:n]

    # List top 5 words for each user
    top_words1 = get_top_words(stats1);
    report += f"*Top 5 Words for /u/{user1}:*\n";
    if top_words1:
        for i, (word, count) in enumerate(top_words1):
            report += f"  * {i+1}. `{word}` ({count})\n"
    else:
        report += "  * *(None or N/A)*\n"

    top_words2 = get_top_words(stats2);
    report += f"\n*Top 5 Words for /u/{user2}:*\n";
    if top_words2:
        for i, (word, count) in enumerate(top_words2):
            report += f"  * {i+1}. `{word}` ({count})\n"
    else:
        report += "  * *(None or N/A)*\n";
    report += "\n"

    # Common Phrases (N-grams) Sub-section
    report += f"**Common Phrases (N-grams):**\n";
    # Get n-gram frequency data for each user
    ngrams1 = stats1.get('ngram_frequency', {});
    ngrams2 = stats2.get('ngram_frequency', {})

    # List top 5 bigrams for each user
    bigrams1 = list(ngrams1.get('bigrams', {}).items())[:5];
    report += f"*Top 5 Bigrams for /u/{user1}:*\n"
    if bigrams1:
        for i, (phrase, count) in enumerate(bigrams1):
            report += f"  * {i+1}. `{phrase}` ({count})\n" # Format phrase as inline code
    else:
        report += "  * *(None or N/A)*\n"

    bigrams2 = list(ngrams2.get('bigrams', {}).items())[:5];
    report += f"\n*Top 5 Bigrams for /u/{user2}:*\n"
    if bigrams2:
        for i, (phrase, count) in enumerate(bigrams2):
            report += f"  * {i+1}. `{phrase}` ({count})\n"
    else:
        report += "  * *(None or N/A)*\n";
    report += "\n"

    # List top 5 trigrams for each user
    trigrams1 = list(ngrams1.get('trigrams', {}).items())[:5];
    report += f"*Top 5 Trigrams for /u/{user1}:*\n"
    if trigrams1:
        for i, (phrase, count) in enumerate(trigrams1):
            report += f"  * {i+1}. `{phrase}` ({count})\n"
    else:
        report += "  * *(None or N/A)*\n"

    trigrams2 = list(ngrams2.get('trigrams', {}).items())[:5];
    report += f"\n*Top 5 Trigrams for /u/{user2}:*\n"
    if trigrams2:
        for i, (phrase, count) in enumerate(trigrams2):
            report += f"  * {i+1}. `{phrase}` ({count})\n"
    else:
        report += "  * *(None or N/A)*\n";
    report += "\n"


    # --- Section IV: Sentiment Comparison ---
    # Note: This section compares the *overall* sentiment ratio stats, not the arc.
    sent1 = stats1.get("sentiment_ratio", {});
    sent2 = stats2.get("sentiment_ratio", {})
    # Check if sentiment analysis was skipped for either user
    skip1 = sent1.get("sentiment_analysis_skipped", True);
    skip2 = sent2.get("sentiment_analysis_skipped", True)

    report += "## IV. Sentiment Comparison (VADER)\n";

    if skip1 or skip2:
         # Report if analysis was skipped for one or both users
         reason1 = f"({sent1.get('reason', 'N/A')})" if skip1 else "(OK)";
         reason2 = f"({sent2.get('reason', 'N/A')})" if skip2 else "(OK)"
         report += f"*Sentiment analysis skipped or unavailable for: /u/{user1} {reason1}, /u/{user2} {reason2}.*\n\n"
    else:
         # If analysis was performed for both, create comparison table
         sent_header_fmt = "| {:<29} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
         sent_row_fmt = "| {:<29} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
         # Format for percentage rows (assuming float value)
         sent_float_row_fmt = "| {:<29} | {:<" + str(max_u_len+3) + ".1f} | {:<" + str(max_u_len+3) + ".1f} |\n" # .1f for 1 decimal place

         report += sent_header_fmt.format("Metric", user1, user2);
         report += "|-------------------------------|-" + "-"*(max_u_len+4) + "|-" + "-"*(max_u_len+4) + "|\n"

         # Report average compound score and pos/neg ratio
         report += sent_row_fmt.format("Avg. Compound Score", sent1.get('avg_compound_score', 'N/A'), sent2.get('avg_compound_score', 'N/A'))
         report += sent_row_fmt.format("Positive:Negative Ratio", sent1.get('pos_neg_ratio', 'N/A'), sent2.get('pos_neg_ratio', 'N/A'))

         # Calculate and report percentage of positive/negative items
         total1 = sent1.get('total_items_sentiment_analyzed', 0);
         total2 = sent2.get('total_items_sentiment_analyzed', 0)

         # Calculate percentages, handle division by zero
         pos_perc1 = (sent1.get('positive_count',0)*100/total1) if total1 > 0 else 0.0;
         pos_perc2 = (sent2.get('positive_count',0)*100/total2) if total2 > 0 else 0.0;
         neg_perc1 = (sent1.get('negative_count',0)*100/total1) if total1 > 0 else 0.0;
         neg_perc2 = (sent2.get('negative_count',0)*100/total2) if total2 > 0 else 0.0;

         # Report percentages using float formatting
         report += sent_float_row_fmt.format("Positive Items (%)", pos_perc1, pos_perc2)
         report += sent_float_row_fmt.format("Negative Items (%)", neg_perc1, neg_perc2);
         report += "\n"


    # --- Section V: Content Style Comparison ---
    report += "## V. Content Style Comparison\n";
    # Create table comparing content style stats
    style_header_fmt = "| {:<29} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    style_row_fmt = "| {:<29} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
    report += style_header_fmt.format("Statistic", user1, user2);
    report += "|-------------------------------|-" + "-"*(max_u_len+4) + "|-" + "-"*(max_u_len+4) + "|\n"

    # Report crosspost percentage
    xp1 = stats1.get('crosspost_stats', {});
    xp2 = stats2.get('crosspost_stats', {})
    report += style_row_fmt.format("Crosspost Percentage", xp1.get('crosspost_percentage', 'N/A'), xp2.get('crosspost_percentage', 'N/A'))

    # Report editing percentages and average delay
    ed1 = stats1.get('editing_stats', {});
    ed2 = stats2.get('editing_stats', {})
    report += style_row_fmt.format("Post Edit Percentage", ed1.get('edit_percentage_posts', 'N/A'), ed2.get('edit_percentage_posts', 'N/A'))
    report += style_row_fmt.format("Comment Edit Percentage", ed1.get('edit_percentage_comments', 'N/A'), ed2.get('edit_percentage_comments', 'N/A'))
    report += style_row_fmt.format("Avg. Edit Delay", ed1.get('average_edit_delay_formatted', 'N/A'), ed2.get('average_edit_delay_formatted', 'N/A'))

    # Report removal/deletion percentages
    rd1 = stats1.get('removal_deletion_stats', {});
    rd2 = stats2.get('removal_deletion_stats', {})
    report += style_row_fmt.format("Removed Post Ratio (%)", rd1.get('posts_content_removed_percentage', 'N/A'), rd2.get('posts_content_removed_percentage', 'N/A'))
    report += style_row_fmt.format("Deleted Post Ratio (%)", rd1.get('posts_user_deleted_percentage', 'N/A'), rd2.get('posts_user_deleted_percentage', 'N/A'))
    report += style_row_fmt.format("Removed Comment Ratio (%)", rd1.get('comments_content_removed_percentage', 'N/A'), rd2.get('comments_content_removed_percentage', 'N/A'))
    report += style_row_fmt.format("Deleted Comment Ratio (%)", rd1.get('comments_user_deleted_percentage', 'N/A'), rd2.get('comments_user_deleted_percentage', 'N/A'));
    report += "\n"
    report += "* *Note: Removal/Deletion stats are estimates based on common markers.*\n\n"

    # --- Section VI: Temporal Pattern Comparison ---
    report += "## VI. Temporal Pattern Comparison\n";
    # Get activity burstiness stats for each user
    b1 = stats1.get('activity_burstiness', {});
    b2 = stats2.get('activity_burstiness', {})

    # Create table comparing temporal stats like burstiness
    temp_header_fmt = "| {:<31} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    temp_row_fmt = "| {:<31} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
    temp_bold_row_fmt = "| {:<31} | **{:<" + str(max_u_len+2) + "}** | **{:<" + str(max_u_len+2) + "}** |\n"
    report += temp_header_fmt.format("Statistic", user1, user2);
    report += "|-----------------------------------|-" + "-"*(max_u_len+4) + "|-" + "-"*(max_u_len+4) + "|\n"

    # Check if burstiness data is available for each user before reporting
    burst1_ok = b1.get('num_intervals_analyzed', 0) > 0;
    burst2_ok = b2.get('num_intervals_analyzed', 0) > 0

    # Report mean and median intervals, using 'N/A' if data wasn't OK
    report += temp_row_fmt.format("Mean Interval Between Activity", b1.get('mean_interval_formatted', 'N/A') if burst1_ok else 'N/A', b2.get('mean_interval_formatted', 'N/A') if burst2_ok else 'N/A')
    report += temp_row_fmt.format("Median Interval Between Activity", b1.get('median_interval_formatted', 'N/A') if burst1_ok else 'N/A', b2.get('median_interval_formatted', 'N/A') if burst2_ok else 'N/A')
    # Report standard deviation (burstiness index), using 'N/A' if data wasn't OK and bolding
    report += temp_bold_row_fmt.format("StDev Interval (Burstiness)", b1.get('stdev_interval_formatted', 'N/A') if burst1_ok else 'N/A', b2.get('stdev_interval_formatted', 'N/A') if burst2_ok else 'N/A');
    report += "\n"
    report += "* *(Higher StDev indicates more 'bursty' activity vs. regular intervals)*\n\n"

    # Add notes if burstiness data was unavailable for either user
    if not burst1_ok: report += f"* *Burstiness data N/A for /u/{user1} (insufficient intervals)*\n"
    if not burst2_ok: report += f"* *Burstiness data N/A for /u/{user2} (insufficient intervals)*\n"
    if not burst1_ok or not burst2_ok: report += "\n" # Add blank line if any notes were added

    # --- Note on other temporal stats ---
    # Comparison of hour/weekday/month/year distribution is not automated here,
    # but a user could visually compare the graphs/tables in the single-user reports.

    # --- Note on new stats comparison ---
    # Comparison of Question Ratio, Mention Frequency, and Sentiment Arc
    # is not included in this comparison report function as currently designed.
    # If needed, specific comparison calculations and corresponding formatting
    # sections would need to be added.


    # --- Report Footer (Optional) ---
    # Add any closing remarks here if needed.
    # report += "---\n\n*End of Comparison Report*\n"


    return report # Return the complete Markdown comparison report string