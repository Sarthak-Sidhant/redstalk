**Overall Purpose**

The `stats` module is designed to perform **comprehensive statistical analysis** on a Reddit user's activity data. It takes raw user data (primarily from a JSON file containing posts and comments, and potentially supplementary CSVs and user 'about' info) as input, applies optional filters, calculates a wide array of metrics, and generates human-readable reports (in Markdown format) as well as structured data outputs (in JSON format). It can also compare the statistics of two different users.

**Key Modules and Their Roles**

1.  **`single_report.py` (The Orchestrator - Single User)**
    *   **Purpose:** This is the main entry point for generating a report for a *single* user. It manages the entire workflow for one user's analysis.
    *   **Responsibilities:**
        *   Takes input parameters: file paths (JSON data, CSVs, output MD, output JSON), user 'about' data, username, filtering criteria (date range, focus/ignore subreddits), and calculation parameters (top N values, N-gram settings, etc.).
        *   **Loads Data:** Reads the primary user activity data from the specified JSON file (`_load_data_from_json`).
        *   **Applies Filters:** Filters the loaded JSON data *in memory* based on the provided date range and subreddit lists (`_apply_filters_to_data`). This creates the working dataset for calculations.
        *   **Orchestrates Calculations:** Calls the various `_calculate_...` functions within `calculations.py`, passing the *filtered* JSON data or the paths to the corresponding *filtered* CSV files as needed. It aggregates the results into a single `stats_results` dictionary.
        *   **Handles Dependencies:** Contains initial checks for optional dependencies (Pandas, VADER, NLTK) primarily for logging warnings if features will be skipped. The actual usage and error handling often reside within `calculations.py`.
        *   **Orchestrates Reporting:** Calls `reporting._format_report` to generate the Markdown report content from the `stats_results`.
        *   **Saves Outputs:** Writes the generated Markdown report to the specified file path and saves the `stats_results` dictionary (cleaned of internal keys) to the specified JSON file path.
        *   **Returns:** Returns a success boolean and the calculated `stats_results` dictionary (which might include internal keys like filter info).

2.  **`calculations.py` (The Engine)**
    *   **Purpose:** This module contains the core logic for *calculating* each individual statistic or group of related statistics. It does the heavy lifting of data processing and computation.
    *   **Responsibilities:**
        *   Defines numerous `_calculate_...` functions, each responsible for a specific metric (e.g., `_calculate_basic_counts`, `_calculate_sentiment_arc`, `_calculate_word_frequency`).
        *   Takes filtered data as input (either the in-memory filtered JSON dictionary or paths to filtered CSV files).
        *   Performs the necessary computations using standard libraries (`collections`, `datetime`, `statistics`, `math`, `re`, `csv`) and optional dependencies (`vaderSentiment`, `nltk`, `pandas`).
        *   Handles specific dependency checks and error handling *for the calculations it performs* (e.g., checking if `vader_available` before attempting sentiment analysis).
        *   Returns the calculated statistics in a structured dictionary format, ready to be consumed by `single_report.py` and `reporting.py`.
        *   Does *not* typically handle data loading or report formatting itself.

3.  **`reporting.py` (The Presenter)**
    *   **Purpose:** This module focuses solely on formatting the calculated statistics into user-friendly Markdown reports.
    *   **Responsibilities:**
        *   Defines formatting functions: `_format_report` (for single users) and `_format_comparison_report` (for comparisons).
        *   Takes the final `stats_results` dictionary (or two dictionaries for comparison) and comparison metrics as input.
        *   Structures the data into sections (Overall Summary, Content Analysis, Temporal Patterns, etc.).
        *   Formats data into tables, lists, and simple visualizations (like text-based bar charts for temporal data).
        *   Handles the presentation logic, ensuring consistency and readability.
        *   Crucially, it **does not perform any calculations** and **does not use ANSI escape codes** to ensure the output is clean Markdown.

4.  **`comparison.py` (The Comparator)**
    *   **Purpose:** Handles the specific task of comparing the statistics of two users.
    *   **Responsibilities:**
        *   Takes two pre-calculated `stats_results` dictionaries (usually from unfiltered runs of `single_report.py`) as input.
        *   Defines helper functions (`_calculate_subreddit_overlap`, `_compare_word_frequency`) to compute comparison-specific metrics (like Jaccard index).
        *   Orchestrates the comparison report generation by calling `reporting._format_comparison_report`.
        *   Saves the resulting comparison Markdown report.

5.  **`core_utils.py` (The Toolbox)**
    *   **Purpose:** Contains shared, low-level utility functions and constants used by other modules within the `stats` package.
    *   **Responsibilities:**
        *   Provides helper functions for common tasks like:
            *   Text cleaning (`clean_text`) including lowercasing, removing punctuation, numbers, URLs, and optionally stopwords.
            *   Timestamp extraction and handling (`_get_timestamp`).
            *   Formatting time durations (`_format_timedelta`).
            *   Generating N-grams (`_generate_ngrams`).
        *   Defines shared constants like `STOP_WORDS` and ANSI color codes (intended *only* for logging output, not report content).

**Workflow Summary**

*   **Single User Report:**
    1.  `single_report.generate_stats_report` is called with inputs.
    2.  It loads JSON data.
    3.  It filters the JSON data based on criteria.
    4.  It calls various functions in `calculations.py` using the filtered data/CSV paths.
    5.  `calculations.py` functions compute stats and return results.
    6.  `single_report.py` aggregates results.
    7.  It calls `reporting._format_report` with the aggregated results.
    8.  `reporting.py` creates the Markdown string.
    9.  `single_report.py` saves the Markdown and/or the results dictionary as JSON.
*   **Comparison Report:**
    1.  `comparison.generate_comparison_report` is called with two existing `stats_results` dictionaries.
    2.  It calculates overlap/comparison metrics.
    3.  It calls `reporting._format_comparison_report` with the two stats dictionaries and comparison results.
    4.  `reporting.py` creates the comparison Markdown string.
    5.  `comparison.py` saves the Markdown report.

**Features / Calculated Statistics**

The module calculates a rich set of statistics, including:

1.  **Basic Counts:** Total posts, total comments.
2.  **Time Range:** First and last activity timestamps (based on creation time).
3.  **Account Info:** Account creation date, account age (requires `about_data`).
4.  **Karma:** Link, comment, and total karma (requires `about_data`).
5.  **Subreddit Activity:** Posts/comments per subreddit, unique subs posted/commented in, list of all active subs, subreddit diversity (Simpson & Shannon indices).
6.  **Content & Text:**
    *   Word counts (total, post, comment).
    *   Unique word count.
    *   Lexical diversity.
    *   Average words per post/comment.
    *   Post types (link vs self).
    *   Question asking ratio (using NLTK).
7.  **Engagement & Scores:**
    *   Sum and average scores for posts/comments (within filtered data).
    *   Score distribution (min, max, median, quartiles).
    *   Top/Bottom N scored posts and comments (with links/snippets).
    *   Average comments per post, Top N most commented posts.
    *   Awards received (total count, number of items awarded).
8.  **Metadata & Habits:**
    *   Flair usage (user flair, post flair - counts per sub/flair combo).
    *   Editing frequency (posts/comments edited count & percentage) and average edit delay.
    *   Crossposting frequency, percentage, and top source subreddits.
    *   Content removal/deletion estimates (based on `[removed]`/`[deleted]` markers).
9.  **Temporal Patterns (UTC):**
    *   Activity distribution by hour, weekday, month, and year.
    *   Activity burstiness (mean, median, stdev of time between activities).
    *   Account age vs. activity trend estimate.
10. **Simple NLP:**
    *   Word Frequency (Top N most common words after cleaning & stopword removal).
    *   N-gram Frequency (Top K most common bigrams and trigrams).
    *   Sentiment Ratio (VADER-based pos/neg/neu counts, ratio, average compound score).
    *   Sentiment Arc (VADER-based average compound score trend over time - monthly/yearly).
    *   Mention Frequency (Top N mentioned users/subreddits based on regex).

**Dependencies**

*   **Core:** Standard Python libraries (`os`, `json`, `csv`, `logging`, `time`, `datetime`, `collections`, `statistics`, `math`, `re`).
*   **External (Required by `RedStalk`):** `reddit_utils` (assumed to be in the project structure, providing `format_timestamp`, `get_modification_date`, `_fetch_user_about_data`).
*   **Optional (for specific stats):**
    *   `vaderSentiment`: For Sentiment Ratio and Sentiment Arc.
    *   `nltk` (requires `punkt` data download): For Question Ratio.
    *   `pandas`: For Question Ratio and Mention Frequency (reading CSVs efficiently).

**Integration with RedStalk**

The `stats` module functions (`generate_stats_report`, `generate_comparison_report`) are intended to be called by the main `RedStalk` application script. This likely happens when the user provides specific command-line arguments like `--generate-stats` or `--compare-users`, passing the necessary file paths and parameters gathered by the main script.

In summary, the `stats` module is a powerful analysis component within `RedStalk`, providing detailed insights into user behavior by processing their activity data and presenting the findings in structured reports. It modularizes the calculation, reporting, and workflow logic effectively.