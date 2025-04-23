import logging
from datetime import datetime
from collections import Counter
from typing import List, Optional, Dict, Any

# --- NO Core Util Imports Likely Needed Here ---
# --- NO ANSI CODES HERE --- This module generates the clean report text.


def _format_report(
    stats_data: Dict[str, Any],
    username: str,
    focus_subreddits: Optional[List[str]] = None,
    ignore_subreddits: Optional[List[str]] = None
) -> str:
    """Formats the single-user statistics into a plain Markdown report."""
    # ***** NO ANSI CODES SHOULD BE USED IN THIS FUNCTION *****
    logging.debug(f"      Formatting stats report for /u/{username}...")
    report = f"# Reddit User Statistics Report for /u/{username}\n\n"
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S'); report += f"*Report generated: {dt_now}*\n\n"

    # --- Filter Info (if present) ---
    filter_info = stats_data.get("_filter_info", {})
    filter_applied = stats_data.get("_filter_applied", False) or filter_info or focus_subreddits or ignore_subreddits

    if filter_applied:
        filter_desc_parts = []
        start_date = filter_info.get('start')
        end_date = filter_info.get('end')
        if start_date or end_date:
            start_str = start_date if start_date else "Beginning"
            end_str = end_date if end_date else "End"
            filter_desc_parts.append(f"Date Range: {start_str} to {end_str}")

        # Subreddit Filter Info (Use explicitly passed args from single_report)
        if focus_subreddits:
            focus_str = ', '.join(f"/r/{s}" for s in focus_subreddits)
            filter_desc_parts.append(f"Focused Subreddits: {focus_str}")
        if ignore_subreddits:
            ignore_str = ', '.join(f"/r/{s}" for s in ignore_subreddits)
            filter_desc_parts.append(f"Ignored Subreddits: {ignore_str}")

        if filter_desc_parts:
             report += f"***Data Filters Applied:** {'; '.join(filter_desc_parts)} (UTC, based on modification time)*\n\n"
        elif filter_applied: # If filter was applied but no details found (fallback)
             report += f"***Data Filter Applied:** Filter details not available in this context.*\n\n"

    # --- Handle Case: No data after filtering ---
    if stats_data.get("_no_data_after_filter", False):
         report += "## Summary\n\n"
         report += "**No data found matching the specified filters.**\n"
         report += "*Report generation stopped.*\n"
         return report # Return early

    # --- Section I: Overall Activity Summary ---
    # (Unchanged from original)
    report += "## I. Overall Activity Summary\n"; report += "| Statistic             | Value         |\n"; report += "|-----------------------|---------------|\n"
    report += f"| Total Posts Analyzed  | {stats_data.get('basic_counts', {}).get('total_posts', 'N/A')} |\n"
    report += f"| Total Comments Analyzed | {stats_data.get('basic_counts', {}).get('total_comments', 'N/A')} |\n"
    time_range = stats_data.get('time_range', {})
    report += f"| First Activity (Created) | {time_range.get('first_activity', 'N/A')} |\n"
    report += f"| Last Activity (Created)  | {time_range.get('last_activity', 'N/A')} |\n"
    age_analysis = stats_data.get("age_activity_analysis", {})
    report += f"| Account Created (UTC) | {age_analysis.get('account_created_formatted', 'N/A')} |\n"
    report += f"| Account Age           | {age_analysis.get('account_age_days', 'N/A')} days |\n"
    engagement = stats_data.get('engagement', {})
    report += f"| Total Link Karma      | **{engagement.get('total_link_karma', 'N/A')}** |\n"
    report += f"| Total Comment Karma   | **{engagement.get('total_comment_karma', 'N/A')}** |\n"
    report += f"| **Total Combined Karma**| **{engagement.get('total_combined_karma', 'N/A')}** |\n"; report += "\n"

    # --- Section II: Content & Style Analysis ---
    report += "## II. Content & Style Analysis\n"; report += "**Text & Post Types:**\n"; report += "| Statistic                 | Value         |\n"; report += "|---------------------------|---------------|\n"
    text_stats = stats_data.get('text_stats', {})
    report += f"| Total Word Count          | {text_stats.get('total_words', 'N/A')} |\n"; report += f"|   *(Posts)*             | {text_stats.get('total_post_words', 'N/A')} |\n"; report += f"|   *(Comments)*          | {text_stats.get('total_comment_words', 'N/A')} |\n"
    report += f"| Total Unique Words (>1 char) | {text_stats.get('total_unique_words', 'N/A')} |\n"; report += f"| Lexical Diversity         | {text_stats.get('lexical_diversity', 'N/A')} |\n"
    report += f"| Avg. Words per Post     | {text_stats.get('avg_post_word_length', 'N/A')} |\n"; report += f"| Avg. Words per Comment  | {text_stats.get('avg_comment_word_length', 'N/A')} |\n"
    # --- Add Question Ratio Stats ---
    q_stats = stats_data.get("question_ratio_stats", {})
    if q_stats.get("analysis_performed"):
        q_items = q_stats.get('question_items', 'N/A')
        q_total = q_stats.get('total_items_analyzed', 'N/A')
        report += f"| Items containing Questions| {q_items} / {q_total} |\n" # Show counts
        report += f"| Question Asking Ratio   | {q_stats.get('question_ratio', 'N/A')} |\n"
    else:
        reason = q_stats.get("reason", "Unknown")
        report += f"| Question Asking Ratio   | *Skipped ({reason})* |\n"
    # --- End Question Ratio Add ---
    post_types = stats_data.get('post_types', {})
    report += f"| Link Posts              | {post_types.get('link_posts', 'N/A')} |\n"; report += f"| Self Posts              | {post_types.get('self_posts', 'N/A')} |\n"; report += "\n"
    xp_stats = stats_data.get('crosspost_stats', {})
    # (Crosspost section unchanged from original)
    report += "**Crossposting:**\n"
    total_xp_analyzed = xp_stats.get('total_posts_analyzed','N/A')
    report += f"* Crossposts Made: {xp_stats.get('crosspost_count', 'N/A')} / {total_xp_analyzed} posts ({xp_stats.get('crosspost_percentage', 'N/A')})\n"
    source_subs = xp_stats.get('source_subreddits')
    if source_subs and isinstance(source_subs, dict) and source_subs:
        report += "* Top Source Subreddits for Crossposts:\n"
        for sub, count in source_subs.items():
             if sub.startswith("_"): sub_display = f"({sub[1:]})"
             else: sub_display = f"/r/{sub}"
             report += f"  * {sub_display} ({count})\n"
    elif total_xp_analyzed == 0: report += "* No posts analyzed for crossposts.\n"
    else: report += "* No crossposts found or analyzed within filter.\n"
    report += "\n"
    edit_stats = stats_data.get("editing_stats", {});
    # (Editing section unchanged from original)
    report += "**Editing Habits:**\n"; total_p_edit = edit_stats.get('total_posts_analyzed_for_edits', 0); total_c_edit = edit_stats.get('total_comments_analyzed_for_edits', 0)
    report += f"* Posts Edited: {edit_stats.get('posts_edited_count', 'N/A')} / {total_p_edit} ({edit_stats.get('edit_percentage_posts', 'N/A')})\n"; report += f"* Comments Edited: {edit_stats.get('comments_edited_count', 'N/A')} / {total_c_edit} ({edit_stats.get('edit_percentage_comments', 'N/A')})\n"
    report += f"* Average Edit Delay: {edit_stats.get('average_edit_delay_formatted', 'N/A')} ({edit_stats.get('average_edit_delay_seconds', 'N/A')}s)\n"; report += "\n"
    rd_stats = stats_data.get('removal_deletion_stats', {})
    # (Removal/Deletion section unchanged from original)
    report += "**Content Removal/Deletion Estimate:**\n"
    report += f"* Posts Removed (by Mod/etc.): {rd_stats.get('posts_content_removed', 'N/A')} ({rd_stats.get('posts_content_removed_percentage', 'N/A')})\n"
    report += f"* Posts Deleted (by User): {rd_stats.get('posts_user_deleted', 'N/A')} ({rd_stats.get('posts_user_deleted_percentage', 'N/A')})\n"
    report += f"* Comments Removed (by Mod/etc.): {rd_stats.get('comments_content_removed', 'N/A')} ({rd_stats.get('comments_content_removed_percentage', 'N/A')})\n"
    report += f"* Comments Deleted (by User): {rd_stats.get('comments_user_deleted', 'N/A')} ({rd_stats.get('comments_user_deleted_percentage', 'N/A')})\n"
    report += "* *Note: Estimate based on `[removed]`/`[deleted]` markers. Accuracy may vary.*\n\n"
    sentiment = stats_data.get("sentiment_ratio", {});
    # (Sentiment Ratio section unchanged from original)
    if sentiment.get("sentiment_analysis_skipped"): report += f"**Sentiment Analysis (VADER):** *Skipped ({sentiment.get('reason', 'Unknown')})*\n\n"
    else:
        report += "**Sentiment Analysis (VADER):**\n"
        total_sent_items = sentiment.get('total_items_sentiment_analyzed', 0)
        pos_count = sentiment.get('positive_count','N/A')
        neg_count = sentiment.get('negative_count','N/A')
        neu_count = sentiment.get('neutral_count','N/A')
        report += f"* Item Counts (Pos/Neg/Neu): {pos_count} / {neg_count} / {neu_count} (Total: {total_sent_items})\n"
        report += f"* Positive-to-Negative Ratio: {sentiment.get('pos_neg_ratio', 'N/A')}\n"; avg_score_str = sentiment.get('avg_compound_score', 'N/A')
        report += f"* Average Compound Score: {avg_score_str} (Range: -1 to +1)\n"; report += "\n"

    # --- Section III: Engagement & Recognition ---
    # (Unchanged from original)
    report += "## III. Engagement & Recognition\n"; report += "**Item Scores (Sum & Average):**\n"; report += "| Statistic               | Value         |\n"; report += "|-------------------------|---------------|\n"
    report += f"| Sum of Post Scores      | {engagement.get('total_item_post_score', 'N/A')} |\n"; report += f"| Sum of Comment Scores   | {engagement.get('total_item_comment_score', 'N/A')} |\n"
    report += f"| Avg. Post Score         | {engagement.get('avg_item_post_score', 'N/A')} |\n"; report += f"| Avg. Comment Score      | {engagement.get('avg_item_comment_score', 'N/A')} |\n"
    report += "*Note: Scores based on items within the filtered data.*\n\n"
    score_stats = stats_data.get("score_stats", {})
    dist_posts = score_stats.get("post_score_distribution", {}); dist_comments = score_stats.get("comment_score_distribution", {})
    if dist_posts.get("count", 0) > 0: report += f"**Post Score Distribution:** Count={dist_posts['count']}, Min={dist_posts.get('min','N/A')}, Q1={dist_posts.get('q1','N/A')}, Med={dist_posts.get('median','N/A')}, Q3={dist_posts.get('q3','N/A')}, Max={dist_posts.get('max','N/A')}, Avg={dist_posts.get('average','N/A')}\n"
    if dist_comments.get("count", 0) > 0: report += f"**Comment Score Distribution:** Count={dist_comments['count']}, Min={dist_comments.get('min','N/A')}, Q1={dist_comments.get('q1','N/A')}, Med={dist_comments.get('median','N/A')}, Q3={dist_comments.get('q3','N/A')}, Max={dist_comments.get('max','N/A')}, Avg={dist_comments.get('average','N/A')}\n"; report += "\n"
    top_posts = score_stats.get("top_posts", []); bottom_posts = score_stats.get("bottom_posts", [])
    top_comments = score_stats.get("top_comments", []); bottom_comments = score_stats.get("bottom_comments", [])
    top_n_disp = max(len(top_posts), len(top_comments), len(bottom_posts), len(bottom_comments))
    if top_n_disp > 0 :
        report += f"**Top {top_n_disp} Scored Posts:**\n"
        if top_posts:
            for score, link, title in top_posts: report += f"* `+{score}`: [{title[:60]}...](https://reddit.com{link})\n"
        else: report += "* *(None within filter)*\n"
        report += f"**Top {top_n_disp} Scored Comments:**\n"
        if top_comments:
            for score, link, snippet in top_comments: report += f"* `+{score}`: [{snippet}](https://reddit.com{link})\n"
        else: report += "* *(None within filter)*\n"
        report += f"**Lowest {top_n_disp} Scored Posts:**\n"
        if bottom_posts:
            for score, link, title in bottom_posts: report += f"* `{score}`: [{title[:60]}...](https://reddit.com{link})\n"
        else: report += "* *(None within filter)*\n"
        report += f"**Lowest {top_n_disp} Scored Comments:**\n"
        if bottom_comments:
            for score, link, snippet in bottom_comments: report += f"* `{score}`: [{snippet}](https://reddit.com{link})\n"
        else: report += "* *(None within filter)*\n";
        report += "\n"
    else: report += "**Top/Bottom Scored Items:** *N/A or Not Calculated*\n\n"
    award_stats = stats_data.get('award_stats', {})
    report += "**Awards Received:**\n"; report += f"* 🏆 Total Awards: {award_stats.get('total_awards_received', 'N/A')}\n"; report += f"* ✨ Items Awarded: {award_stats.get('items_with_awards', 'N/A')}\n"; report += "\n"
    post_engage = stats_data.get("post_engagement", {}); report += "**Post Engagement:**\n";
    report += f"* Average Comments Received per Post: {post_engage.get('avg_comments_per_post', 'N/A')} (Analyzed: {post_engage.get('total_posts_analyzed_for_comments', 'N/A')} posts)\n"
    top_commented = post_engage.get("top_commented_posts")
    if top_commented:
        report += f"* Top {len(top_commented)} Most Commented Posts:\n";
        for num_comments, link, title in top_commented:
             report += f"  * `{num_comments} comments`: [{title[:60]}...](https://reddit.com{link})\n";
    else: report += "* Top Commented Posts: *(None within filter or not analyzed)*\n"
    report += "\n"

    # --- Section IV: Subreddit Activity & Flair ---
    # (Unchanged from original)
    report += "## IV. Subreddit Activity & Flair\n";
    sub_activity = stats_data.get('subreddit_activity', {})
    posts_per_sub = sub_activity.get('posts_per_subreddit', {}); comments_per_sub = sub_activity.get('comments_per_subreddit', {}); all_subs = sub_activity.get('all_active_subs', [])
    sub_diversity = stats_data.get('subreddit_diversity', {})
    if not all_subs: report += "No subreddit activity found within filter.\n\n"
    else:
        num_all_subs = len(all_subs)
        report += f"Active in **{num_all_subs}** unique subreddits ({sub_activity.get('unique_subs_posted', 0)} posted in, {sub_activity.get('unique_subs_commented', 0)} commented in).\n"
        report += f"* **Subreddit Diversity (Simpson Index): {sub_diversity.get('simpson_diversity_index', 'N/A')}** (0=Low, 1=High)\n"
        if sub_diversity.get('normalized_shannon_entropy', 'N/A') not in ['N/A', 'Error']:
             report += f"* Subreddit Diversity (Norm. Shannon Entropy): {sub_diversity.get('normalized_shannon_entropy', 'N/A')} (0=Low, 1=High)\n"
        report += "\n"
        report += "**Activity Distribution:**\n"
        report += "| Subreddit         | Posts | Comments | Total |\n"; report += "|-------------------|-------|----------|-------|\n"
        combined_activity = Counter(posts_per_sub) + Counter(comments_per_sub);
        sorted_subs_by_activity = sorted(all_subs, key=lambda sub: combined_activity.get(sub, 0), reverse=True)
        # Limit displayed subs to avoid huge tables
        display_sub_limit = 50
        for i, sub in enumerate(sorted_subs_by_activity):
            if i >= display_sub_limit:
                 report += f"| ...and {len(sorted_subs_by_activity)-display_sub_limit} more | ... | ... | ... |\n"; break
            p_count = posts_per_sub.get(sub, 0); c_count = comments_per_sub.get(sub, 0); t_count = combined_activity.get(sub, 0);
            report += f"| /r/{sub:<15} | {p_count:<5} | {c_count:<8} | **{t_count:<5}** |\n";
        report += "\n"
        top_n_subs = 10
        if combined_activity:
            report += f"**Top {min(top_n_subs, len(combined_activity))} Most Active Subreddits (Posts + Comments):**\n";
            for i, (sub, count) in enumerate(combined_activity.most_common(top_n_subs)):
                 report += f"* {i+1}. /r/{sub} ({count})\n";
            report += "\n"
    flair_stats = stats_data.get("flair_stats", {}); report += "**Flair Usage:**\n"; user_flairs = flair_stats.get("user_flairs_by_sub", {}); post_flairs = flair_stats.get("post_flairs_by_sub", {})
    user_flair_count = flair_stats.get('total_comments_with_user_flair', 0)
    post_flair_count = flair_stats.get('total_posts_with_link_flair', 0)
    if user_flairs:
        report += f"* User Flairs Used ({user_flair_count} instances):\n";
        display_count = 0
        for flair_combo, count in user_flairs.items():
            if display_count >= 10: break
            try: sub, flair_text = flair_combo.split(': ', 1); report += f"  * `/r/{sub}`: `{flair_text}` ({count})\n"; display_count += 1
            except ValueError: report += f"  * `{flair_combo}` ({count})\n"; display_count += 1
        if len(user_flairs) > 10: report += f"  * ... and {len(user_flairs)-10} more\n"
    else: report += f"* User Flairs Used: {user_flair_count} instances found/analyzed.\n"
    if post_flairs:
        report += f"* Post Flairs Used ({post_flair_count} instances):\n";
        display_count = 0
        for flair_combo, count in post_flairs.items():
             if display_count >= 10: break
             try: sub, flair_text = flair_combo.split(': ', 1); report += f"  * `/r/{sub}`: `{flair_text}` ({count})\n"; display_count += 1
             except ValueError: report += f"  * `{flair_combo}` ({count})\n"; display_count += 1
        if len(post_flairs) > 10: report += f"  * ... and {len(post_flairs)-10} more\n"
    else: report += f"* Post Flairs Used: {post_flair_count} instances found/analyzed.\n";
    report += "\n"

    # --- Section V: Temporal Activity Patterns (UTC) ---
    # (Unchanged from original)
    report += "## V. Temporal Activity Patterns (UTC)\n";
    temporal_data = stats_data.get('temporal_stats', {});
    total_temporal_items = temporal_data.get("total_items_for_temporal", 0)
    if not total_temporal_items or total_temporal_items == 0: report += "No temporal data available or calculated within filter.\n\n"
    else:
        report += f"*(Based on creation time of {total_temporal_items} items within filter)*\n\n";
        age_analysis = stats_data.get("age_activity_analysis", {})
        report += "**Account Age vs Activity Trend:**\n"
        report += f"* Average Activity Rate (per year): {age_analysis.get('average_activity_per_year', 'N/A')}\n"
        report += f"* Average Activity Rate (per month): {age_analysis.get('average_activity_per_month', 'N/A')}\n"
        report += f"* Overall Trend Estimate: **{age_analysis.get('activity_trend_status', 'N/A')}**\n\n"
        burst_stats = stats_data.get('activity_burstiness', {})
        report += "**Activity Timing (Burstiness):**\n"
        if burst_stats.get('num_intervals_analyzed', 0) > 0:
            report += f"* Number of Intervals Analyzed: {burst_stats.get('num_intervals_analyzed', 'N/A')}\n"
            report += f"* Mean Interval Between Activities: {burst_stats.get('mean_interval_formatted', 'N/A')} ({burst_stats.get('mean_interval_s', 'N/A')}s)\n"
            report += f"* Median Interval Between Activities: {burst_stats.get('median_interval_formatted', 'N/A')} ({burst_stats.get('median_interval_s', 'N/A')}s)\n"
            report += f"* Interval Standard Deviation: **{burst_stats.get('stdev_interval_formatted', 'N/A')}** ({burst_stats.get('stdev_interval_s', 'N/A')}s)\n"
            report += f"* Min/Max Interval: {burst_stats.get('min_interval_formatted', 'N/A')} / {burst_stats.get('max_interval_formatted', 'N/A')}\n"
            report += "* *(Higher StDev indicates more 'bursty' activity vs. regular intervals)*\n\n"
        else: report += "* Insufficient data for burstiness analysis.\n\n"
        activity_hour = temporal_data.get('activity_by_hour_utc', {}); activity_wday = temporal_data.get('activity_by_weekday_utc', {}); activity_month = temporal_data.get('activity_by_month_utc', {}); activity_year = temporal_data.get('activity_by_year_utc', {})
        if activity_hour:
            report += "**Activity by Hour of Day (00-23 UTC):**\n```\n";
            max_val_hr = max(activity_hour.values()) if activity_hour else 0; scale_hr = 50 / max_val_hr if max_val_hr > 0 else 0;
            for hour in range(24): hour_str = f"{hour:02d}"; count = activity_hour.get(hour_str, 0); bar = '#' * int(count * scale_hr); report += f"{hour_str}: {bar:<50} ({count})\n";
            report += "```\n"
        if activity_wday:
            report += "**Activity by Day of Week (UTC):**\n```\n";
            max_val_wd = max(activity_wday.values()) if activity_wday else 0; scale_wd = 50 / max_val_wd if max_val_wd > 0 else 0;
            days_ordered = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"];
            for day in days_ordered: count = activity_wday.get(day, 0); bar = '#' * int(count * scale_wd); report += f"{day:<9}: {bar:<50} ({count})\n";
            report += "```\n"
        if activity_month:
            report += "**Activity by Month (YYYY-MM UTC):**\n"; report += "| Month   | Count |\n|---------|-------|\n";
            # Limit displayed months
            display_month_limit = 60
            month_items = list(activity_month.items())
            for i, (month_key, count) in enumerate(month_items):
                 if i >= display_month_limit:
                     report += f"| ...and {len(month_items)-display_month_limit} more | ... |\n"; break
                 report += f"| {month_key} | {count:<5} |\n";
            report += "\n"
        if activity_year:
            report += "**Activity by Year (UTC):**\n"; report += "| Year | Count |\n|------|-------|\n";
            for year, count in activity_year.items():
                report += f"| {year} | {count:<5} |\n";
            report += "\n"

    # --- Section VI: Word, Phrase & Mention Frequency --- [RENAMED SECTION]
    report += "## VI. Word, Phrase & Mention Frequency\n"
    word_freq_section = stats_data.get('word_frequency', {})
    word_freq = word_freq_section.get('word_frequency', {})
    ngram_freq = stats_data.get('ngram_frequency', {})
    mention_stats = stats_data.get('mention_stats', {})

    # Check if *any* data is available for this section
    no_freq_data = not word_freq and not any(ngram_freq.values()) and not mention_stats.get("analysis_performed")
    if no_freq_data:
        report += "No word, phrase, or mention frequency data available (requires CSV processing and/or dependencies).\n\n"
    else:
        if word_freq:
            top_n_words = len(word_freq)
            report += f"**Top {top_n_words} Most Frequent Words:**\n"; report += "*(Cleaned, stop words removed, from filtered data)*\n\n"; report += "| Word             | Count |\n|------------------|-------|\n"
            for word, count in word_freq.items(): report += f"| {word:<16} | {count:<5} |\n";
            report += "\n"
        else: report += "**Word Frequency:** *N/A or Not Calculated*\n\n"

        for n_key, n_data in ngram_freq.items():
             if n_data:
                 n = {'bigrams': 2, 'trigrams': 3}.get(n_key, '?'); top_n_phrases = len(n_data)
                 report += f"**Top {top_n_phrases} Most Frequent {n_key.capitalize()} ({n}-word phrases):**\n"; report += "*(Cleaned, stop words removed)*\n\n"; report += "| Phrase                   | Count |\n|--------------------------|-------|\n"
                 max_phrase_len = 24 # Max length for phrase column
                 for phrase, count in n_data.items():
                     # Truncate long phrases for display
                     display_phrase = phrase[:max_phrase_len] + ('...' if len(phrase) > max_phrase_len else '')
                     report += f"| {display_phrase:<{max_phrase_len+3}} | {count:<5} |\n" # Adjust padding
                 report += "\n"
             else: report += f"**{n_key.capitalize()}:** *N/A or Not Calculated*\n\n"

        # --- Add Mention Frequency Sub-section ---
        report += "**Mention Frequency:**\n"
        if mention_stats.get("analysis_performed"):
            top_users = mention_stats.get("top_user_mentions", {})
            top_subs = mention_stats.get("top_subreddit_mentions", {})
            total_user_instances = mention_stats.get("total_user_mention_instances", 0)
            total_sub_instances = mention_stats.get("total_subreddit_mention_instances", 0)

            report += f"* Total User Mention Instances: {total_user_instances}\n"
            if top_users:
                 report += f"* Top {len(top_users)} Mentioned Users (by frequency):\n"
                 for user, count in top_users.items(): report += f"  * u/{user} ({count})\n"
            else: report += "* Top Mentioned Users: *(None found)*\n"

            report += f"* Total Subreddit Mention Instances: {total_sub_instances}\n"
            if top_subs:
                 report += f"* Top {len(top_subs)} Mentioned Subreddits (by frequency):\n"
                 for sub, count in top_subs.items(): report += f"  * r/{sub} ({count})\n"
            else: report += "* Top Mentioned Subreddits: *(None found)*\n"
            report += "* *Note: Based on regex pattern matching `u/` and `r/`.*\n\n"
        else:
            reason = mention_stats.get("reason", "Unknown")
            report += f"* *Skipped ({reason})*\n\n"
        # --- End Mention Frequency Add ---

    # --- Section VII: Sentiment Trend Over Time (NEW SECTION) ---
    report += "## VII. Sentiment Trend Over Time (VADER)\n"
    sentiment_arc = stats_data.get('sentiment_arc', {})
    if sentiment_arc.get('analysis_performed'):
        arc_data = sentiment_arc.get('sentiment_arc_data', {})
        window_type = sentiment_arc.get('window_type', 'Time Period').capitalize() # Use stored window type
        if arc_data:
            report += f"**Average Sentiment (Compound Score) by {window_type}:**\n"
            report += f"*(Range: -1 Negative to +1 Positive)*\n\n"
            # Determine column width based on window type
            window_col_width = 12 if window_type == "Monthly" else 10 # Adjust as needed
            report += f"| {window_type:<{window_col_width}} | Avg. Score |\n"
            report += f"|{'-'*window_col_width}-|------------|\n"
            # Limit displayed windows if arc_data is very long
            display_arc_limit = 60
            arc_items = list(arc_data.items())
            for i, (window, avg_score) in enumerate(arc_items):
                if i >= display_arc_limit:
                     report += f"| ...and {len(arc_items)-display_arc_limit} more | ...        |\n"; break
                # Format score: show sign, align center-ish (using padding), 3 decimal places
                report += f"| {window:<{window_col_width}} | {avg_score:^+11.3f} |\n"
            report += "\n"
        else:
            report += "*No sentiment data points found to generate trend.*\n\n"
    else:
        reason = sentiment_arc.get("reason", "Unknown")
        report += f"*Analysis skipped ({reason})*\n\n"

    return report


# --- Comparison Report Formatting ---
def _format_comparison_report(stats1, stats2, user1, user2, comparison_results):
    """Formats the comparison statistics into a plain Markdown report."""
    # ***** NO CHANGES NEEDED IN THIS FUNCTION for the new single-user features *****
    # Comparison still operates on the original set of stats.
    logging.debug(f"      Formatting comparison report for /u/{user1} vs /u/{user2}...")
    report = f"# Reddit User Comparison Report: /u/{user1} vs /u/{user2}\n\n"
    dt_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S'); report += f"*Report generated: {dt_now}*\n\n"
    filter1_applied = stats1.get("_filter_applied", False); filter2_applied = stats2.get("_filter_applied", False)
    if filter1_applied or filter2_applied: report += f"*Note: Comparison values derived from base statistics generated for each user. Base statistics are typically unfiltered.*\n\n"
    else: report += f"*Note: Statistics generally based on full available history.*\n\n"

    # --- Section I: Basic Stats Comparison ---
    report += "## I. Overall Activity Comparison\n"
    u1_len = len(user1); u2_len = len(user2); max_u_len = max(u1_len, u2_len, 18)
    header_fmt = "| {:<18} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    row_fmt = "| {:<18} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
    bold_row_fmt = "| {:<18} | **{:<" + str(max_u_len+2) + "}** | **{:<" + str(max_u_len+2) + "}** |\n"
    report += header_fmt.format("Statistic", user1, user2); report += "|--------------------|-" + "-"*max_u_len + "-------|-" + "-"*max_u_len + "-------|\n"
    report += row_fmt.format("Total Posts", stats1.get('basic_counts', {}).get('total_posts', 'N/A'), stats2.get('basic_counts', {}).get('total_posts', 'N/A'))
    report += row_fmt.format("Total Comments", stats1.get('basic_counts', {}).get('total_comments', 'N/A'), stats2.get('basic_counts', {}).get('total_comments', 'N/A'))
    report += row_fmt.format("First Activity", stats1.get('time_range', {}).get('first_activity', 'N/A'), stats2.get('time_range', {}).get('first_activity', 'N/A'))
    report += row_fmt.format("Last Activity", stats1.get('time_range', {}).get('last_activity', 'N/A'), stats2.get('time_range', {}).get('last_activity', 'N/A'))
    age1_str = stats1.get('age_activity_analysis', {}).get('account_age_days', 'N/A'); age2_str = stats2.get('age_activity_analysis', {}).get('account_age_days', 'N/A')
    if age1_str != 'N/A': age1_str = f"{age1_str} days"
    if age2_str != 'N/A': age2_str = f"{age2_str} days"
    report += row_fmt.format("Account Age", age1_str, age2_str)
    eng1 = stats1.get('engagement', {}); eng2 = stats2.get('engagement', {})
    report += bold_row_fmt.format("Link Karma", eng1.get('total_link_karma', 'N/A'), eng2.get('total_link_karma', 'N/A'))
    report += bold_row_fmt.format("Comment Karma", eng1.get('total_comment_karma', 'N/A'), eng2.get('total_comment_karma', 'N/A'))
    report += bold_row_fmt.format("Combined Karma", eng1.get('total_combined_karma', 'N/A'), eng2.get('total_combined_karma', 'N/A'))
    report += row_fmt.format("Avg Post Score", eng1.get('avg_item_post_score', 'N/A'), eng2.get('avg_item_post_score', 'N/A'))
    report += row_fmt.format("Avg Comment Score", eng1.get('avg_item_comment_score', 'N/A'), eng2.get('avg_item_comment_score', 'N/A')); report += "\n"

    # --- Section II: Subreddit Activity Comparison ---
    report += "## II. Subreddit Activity Comparison\n"; sub_overlap = comparison_results.get("subreddit_overlap", {})
    div1 = stats1.get('subreddit_diversity', {}); div2 = stats2.get('subreddit_diversity', {})
    report += f"* **Shared Subreddits:** {sub_overlap.get('num_shared', 0)}\n"; report += f"* **Jaccard Index (Similarity):** {sub_overlap.get('jaccard_index', 'N/A')}\n\n"
    sub_header_fmt = "| {:<29} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    sub_row_fmt = "| {:<29} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
    sub_bold_row_fmt = "| {:<29} | **{:<" + str(max_u_len+2) + "}** | **{:<" + str(max_u_len+2) + "}** |\n"
    report += sub_header_fmt.format("Statistic", user1, user2); report += "|-------------------------------|-" + "-"*max_u_len + "-------|-" + "-"*max_u_len + "-------|\n"
    sub_act1 = stats1.get('subreddit_activity', {}); sub_act2 = stats2.get('subreddit_activity', {})
    report += sub_row_fmt.format("Unique Subs Active", len(sub_act1.get('all_active_subs', [])), len(sub_act2.get('all_active_subs', [])))
    report += sub_row_fmt.format("Unique Subs Posted", sub_act1.get('unique_subs_posted', 'N/A'), sub_act2.get('unique_subs_posted', 'N/A'))
    report += sub_row_fmt.format("Unique Subs Commented", sub_act1.get('unique_subs_commented', 'N/A'), sub_act2.get('unique_subs_commented', 'N/A'))
    report += sub_bold_row_fmt.format("Simpson Diversity Index", div1.get('simpson_diversity_index', 'N/A'), div2.get('simpson_diversity_index', 'N/A')); report += "\n"
    report += "* *(Diversity Index: 0=Lowest, 1=Highest)*\n\n"; shared_list = sub_overlap.get("shared_subreddits", [])
    if shared_list:
        report += "**Shared Subreddits List:**\n"; max_shared_display = 20;
        for i, sub in enumerate(shared_list):
            if i >= max_shared_display: report += f"* ... and {len(shared_list)-max_shared_display} more\n"; break
            report += f"* /r/{sub}\n";
        report += "\n"
    def get_top_subs(stats): posts = stats.get('subreddit_activity', {}).get('posts_per_subreddit', {}); comments = stats.get('subreddit_activity', {}).get('comments_per_subreddit', {}); combined = Counter(posts) + Counter(comments); return combined.most_common(5)
    top_subs1 = get_top_subs(stats1); report += f"**Top 5 Subreddits for /u/{user1}:**\n";
    if top_subs1:
        for i, (sub, count) in enumerate(top_subs1): report += f"* {i+1}. /r/{sub} ({count})\n"
    else: report += "* *(None)*\n"
    top_subs2 = get_top_subs(stats2); report += f"\n**Top 5 Subreddits for /u/{user2}:**\n";
    if top_subs2:
        for i, (sub, count) in enumerate(top_subs2): report += f"* {i+1}. /r/{sub} ({count})\n"
    else: report += "* *(None)*\n"; report += "\n"

    # --- Section III: Word & Phrase Frequency Comparison ---
    report += "## III. Word & Phrase Frequency Comparison\n"; word_comp = comparison_results.get("word_frequency_comparison", {}); top_n_words_comp = word_comp.get("top_n_compared", "N/A")
    report += f"**Single Words:**\n"; report += f"* Comparison based on Top {top_n_words_comp} frequent words (stop words removed).\n"; report += f"* Shared Top {top_n_words_comp} Words: {word_comp.get('num_shared_top_words', 'N/A')}\n"; report += f"* Jaccard Index (Word Similarity): {word_comp.get('jaccard_index', 'N/A')}\n\n"
    shared_word_list = word_comp.get("shared_top_words", [])
    if shared_word_list: report += "*Shared Top Words List:*\n"; report += "`" + "`, `".join(shared_word_list) + "`\n\n"
    def get_top_words(stats, n=5): freq_section = stats.get('word_frequency', {}); word_freq = freq_section.get('word_frequency', {}); return list(word_freq.items())[:n]
    top_words1 = get_top_words(stats1); report += f"*Top 5 Words for /u/{user1}:*\n";
    if top_words1:
        for i, (word, count) in enumerate(top_words1): report += f"  * {i+1}. `{word}` ({count})\n"
    else: report += "  * *(None or N/A)*\n"
    top_words2 = get_top_words(stats2); report += f"\n*Top 5 Words for /u/{user2}:*\n";
    if top_words2:
        for i, (word, count) in enumerate(top_words2): report += f"  * {i+1}. `{word}` ({count})\n"
    else: report += "  * *(None or N/A)*\n"; report += "\n"
    report += f"**Common Phrases (N-grams):**\n"; ngrams1 = stats1.get('ngram_frequency', {}); ngrams2 = stats2.get('ngram_frequency', {})
    bigrams1 = list(ngrams1.get('bigrams', {}).items())[:5]; report += f"*Top 5 Bigrams for /u/{user1}:*\n"
    if bigrams1:
        for i, (phrase, count) in enumerate(bigrams1): report += f"  * {i+1}. `{phrase}` ({count})\n"
    else: report += "  * *(None or N/A)*\n"
    bigrams2 = list(ngrams2.get('bigrams', {}).items())[:5]; report += f"\n*Top 5 Bigrams for /u/{user2}:*\n"
    if bigrams2:
        for i, (phrase, count) in enumerate(bigrams2): report += f"  * {i+1}. `{phrase}` ({count})\n"
    else: report += "  * *(None or N/A)*\n"; report += "\n"
    trigrams1 = list(ngrams1.get('trigrams', {}).items())[:5]; report += f"*Top 5 Trigrams for /u/{user1}:*\n"
    if trigrams1:
        for i, (phrase, count) in enumerate(trigrams1): report += f"  * {i+1}. `{phrase}` ({count})\n"
    else: report += "  * *(None or N/A)*\n"
    trigrams2 = list(ngrams2.get('trigrams', {}).items())[:5]; report += f"\n*Top 5 Trigrams for /u/{user2}:*\n"
    if trigrams2:
        for i, (phrase, count) in enumerate(trigrams2): report += f"  * {i+1}. `{phrase}` ({count})\n"
    else: report += "  * *(None or N/A)*\n"; report += "\n"

    # --- Section IV: Sentiment Comparison ---
    sent1 = stats1.get("sentiment_ratio", {}); sent2 = stats2.get("sentiment_ratio", {})
    skip1 = sent1.get("sentiment_analysis_skipped", True); skip2 = sent2.get("sentiment_analysis_skipped", True)
    report += "## IV. Sentiment Comparison (VADER)\n";
    if skip1 or skip2:
         reason1 = f"({sent1.get('reason', 'N/A')})" if skip1 else "(OK)"; reason2 = f"({sent2.get('reason', 'N/A')})" if skip2 else "(OK)"
         report += f"*Sentiment analysis skipped or unavailable for: /u/{user1} {reason1}, /u/{user2} {reason2}.*\n\n"
    else:
         sent_header_fmt = "| {:<29} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
         sent_row_fmt = "| {:<29} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
         sent_float_row_fmt = "| {:<29} | {:<" + str(max_u_len+3) + ".1f} | {:<" + str(max_u_len+3) + ".1f} |\n"
         report += sent_header_fmt.format("Metric", user1, user2); report += "|-------------------------------|-" + "-"*max_u_len + "-------|-" + "-"*max_u_len + "-------|\n"
         report += sent_row_fmt.format("Avg. Compound Score", sent1.get('avg_compound_score', 'N/A'), sent2.get('avg_compound_score', 'N/A'))
         report += sent_row_fmt.format("Positive:Negative Ratio", sent1.get('pos_neg_ratio', 'N/A'), sent2.get('pos_neg_ratio', 'N/A'))
         total1 = sent1.get('total_items_sentiment_analyzed', 0); total2 = sent2.get('total_items_sentiment_analyzed', 0)
         pos_perc1 = (sent1.get('positive_count',0)*100/total1) if total1 > 0 else 0.0; pos_perc2 = (sent2.get('positive_count',0)*100/total2) if total2 > 0 else 0.0
         neg_perc1 = (sent1.get('negative_count',0)*100/total1) if total1 > 0 else 0.0; neg_perc2 = (sent2.get('negative_count',0)*100/total2) if total2 > 0 else 0.0
         report += sent_float_row_fmt.format("Positive Items (%)", pos_perc1, pos_perc2)
         report += sent_float_row_fmt.format("Negative Items (%)", neg_perc1, neg_perc2); report += "\n"

    # --- Section V: Content Style Comparison ---
    report += "## V. Content Style Comparison\n"; style_header_fmt = "| {:<29} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    style_row_fmt = "| {:<29} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
    report += style_header_fmt.format("Statistic", user1, user2); report += "|-------------------------------|-" + "-"*max_u_len + "-------|-" + "-"*max_u_len + "-------|\n"
    xp1 = stats1.get('crosspost_stats', {}); xp2 = stats2.get('crosspost_stats', {})
    report += style_row_fmt.format("Crosspost Percentage", xp1.get('crosspost_percentage', 'N/A'), xp2.get('crosspost_percentage', 'N/A'))
    ed1 = stats1.get('editing_stats', {}); ed2 = stats2.get('editing_stats', {})
    report += style_row_fmt.format("Post Edit Percentage", ed1.get('edit_percentage_posts', 'N/A'), ed2.get('edit_percentage_posts', 'N/A'))
    report += style_row_fmt.format("Comment Edit Percentage", ed1.get('edit_percentage_comments', 'N/A'), ed2.get('edit_percentage_comments', 'N/A'))
    report += style_row_fmt.format("Avg. Edit Delay", ed1.get('average_edit_delay_formatted', 'N/A'), ed2.get('average_edit_delay_formatted', 'N/A'))
    rd1 = stats1.get('removal_deletion_stats', {}); rd2 = stats2.get('removal_deletion_stats', {})
    report += style_row_fmt.format("Removed Post Ratio (%)", rd1.get('posts_content_removed_percentage', 'N/A'), rd2.get('posts_content_removed_percentage', 'N/A'))
    report += style_row_fmt.format("Deleted Post Ratio (%)", rd1.get('posts_user_deleted_percentage', 'N/A'), rd2.get('posts_user_deleted_percentage', 'N/A'))
    report += style_row_fmt.format("Removed Comment Ratio (%)", rd1.get('comments_content_removed_percentage', 'N/A'), rd2.get('comments_content_removed_percentage', 'N/A'))
    report += style_row_fmt.format("Deleted Comment Ratio (%)", rd1.get('comments_user_deleted_percentage', 'N/A'), rd2.get('comments_user_deleted_percentage', 'N/A')); report += "\n"
    report += "* *Note: Removal/Deletion stats are estimates based on common markers.*\n\n"

    # --- Section VI: Temporal Pattern Comparison ---
    report += "## VI. Temporal Pattern Comparison\n"; b1 = stats1.get('activity_burstiness', {}); b2 = stats2.get('activity_burstiness', {})
    temp_header_fmt = "| {:<31} | /u/{:<" + str(max_u_len) + "} | /u/{:<" + str(max_u_len) + "} |\n"
    temp_row_fmt = "| {:<31} | {:<" + str(max_u_len+4) + "} | {:<" + str(max_u_len+4) + "} |\n"
    temp_bold_row_fmt = "| {:<31} | **{:<" + str(max_u_len+2) + "}** | **{:<" + str(max_u_len+2) + "}** |\n"
    report += temp_header_fmt.format("Statistic", user1, user2); report += "|-----------------------------------|-" + "-"*max_u_len + "-------|-" + "-"*max_u_len + "-------|\n"
    burst1_ok = b1.get('num_intervals_analyzed', 0) > 0; burst2_ok = b2.get('num_intervals_analyzed', 0) > 0
    report += temp_row_fmt.format("Mean Interval Between Activity", b1.get('mean_interval_formatted', 'N/A') if burst1_ok else 'N/A', b2.get('mean_interval_formatted', 'N/A') if burst2_ok else 'N/A')
    report += temp_row_fmt.format("Median Interval Between Activity", b1.get('median_interval_formatted', 'N/A') if burst1_ok else 'N/A', b2.get('median_interval_formatted', 'N/A') if burst2_ok else 'N/A')
    report += temp_bold_row_fmt.format("StDev Interval (Burstiness)", b1.get('stdev_interval_formatted', 'N/A') if burst1_ok else 'N/A', b2.get('stdev_interval_formatted', 'N/A') if burst2_ok else 'N/A'); report += "\n"
    report += "* *(Higher StDev indicates more 'bursty' activity vs. regular intervals)*\n\n"
    if not burst1_ok: report += f"* *Burstiness data N/A for /u/{user1} (insufficient intervals)*\n"
    if not burst2_ok: report += f"* *Burstiness data N/A for /u/{user2} (insufficient intervals)*\n"
    if not burst1_ok or not burst2_ok: report += "\n"

    return report