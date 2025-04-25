# this is a reference thing for my understanding

## Redstalk Library Overview

The Redstalk library is structured into several modules, each responsible for a specific part of the overall process. The main script, `redstalk.py`, acts as the central orchestrator, parsing user commands and directing the workflow to the appropriate specialized modules.

Think of it like a factory specializing in processing Reddit user data:

*   **`redstalk.py`**: The **Factory Manager**. Reads the overall plan (command-line args), decides which production lines to start, and supervises the main flow.
*   **`config_utils.py`**: The **Settings Department**. Keeps track of all the default and user-defined settings for how the factory should run.
*   **`reddit_utils.py`**: The **Raw Material Procurement & Initial Storage Department**. Goes out and gets raw data from Reddit, handles basic quality checks, and stores it in the raw data warehouse (the JSON file), keeping things organized by adding new material and sorting.
*   **`data_utils.py`**: The **Sorting & Basic Preparation Department**. Takes raw material from the warehouse (JSON), applies initial sorting and filtering based on the manager's instructions (date/subreddit), and puts the prepared material into standard bins (CSV files).
*   **`analysis.py`**: The **AI Preparation & Analysis Line**. Takes the prepared material from the standard bins (CSVs), formats it specifically for the AI machine, and operates the AI machine to produce analysis reports.
*   **`ai_utils.py`**: The **AI Interaction Engine**. This is the specialized machinery and operators for interacting with the AI (like measuring material size/tokens, splitting large batches/chunking, and running the main AI process).
*   **`monitoring.py`**: The **Automated Checker**. Periodically checks if new raw material is available and automatically triggers the procurement and sorting departments when it is.
*   **`stats` Package (`single_report.py`, `comparison.py`, `calculations.py`, `reporting.py` - inferred):** The **Statistics Department**.
    *   `calculations.py`: The **Measurement & Counting Team**. Performs all the specific calculations (counts, frequencies, averages) on the prepared material.
    *   `reporting.py`: The **Report Design Team**. Takes measurement results and lays them out neatly in a report format.
    *   `single_report.py`: The **Single User Report Manager**. Oversees the Measurement Team and the Report Design Team to create one user's statistical report.
    *   `comparison.py`: The **Comparison Report Manager**. Takes two sets of measurement results, does comparison-specific checks, and oversees the Report Design Team to create a comparison report.

## Detailed Module Breakdown & Responsibilities

Here's a breakdown of each module, including its core responsibilities and how it interacts with others. This level of detail is crucial for knowing where to add new functionality.

1.  **`redstalk.py`**
    *   **Role:** Command-Line Interface (CLI), Application Entry Point, Workflow Orchestrator, Top-level Argument/Filter Handler.
    *   **Core Responsibilities:**
        *   Define all command-line arguments using `argparse`.
        *   Parse arguments provided by the user.
        *   Load the configuration using `config_utils.load_config`.
        *   Set up the application's logging using `setup_logging` (including the custom `ColoredFormatter`).
        *   Determine the primary action the user wants to perform (`--compare-user`, `--generate-prompt`, `--monitor`, `--export-json-only`, `--reset-config`, or single-user processing with `--run-analysis`/`--generate-stats`).
        *   Validate argument combinations (e.g., ensure `--summarize-stats` is used with `--generate-stats`, ensure filters aren't used with `--compare-user` base stats).
        *   **Validate Filter Overlap:** Specifically checks if any subreddit is listed in both `--focus-subreddit` and `--ignore-subreddit` arguments.
        *   Conditionally initialize the Google AI model based on whether AI features are required for the chosen action, retrieving API keys from environment variables, config, or args.
        *   Dispatch execution to the appropriate function or workflow based on the determined action (e.g., call `monitor_user`, `generate_prompt_interactive`, `process_single_user_for_stats`, or orchestrate the single-user pipeline).
        *   Handle critical, top-level errors.
        *   Manage the overall flow for single-user processing: Scrape/Update -> CSV Export (applying filters) -> Fetch About -> (Stats Calculation -> AI Summary) -> (AI Analysis).
    *   **Interactions:**
        *   **Calls:** `config_utils.load_config`, `config_utils.save_config`, `setup_logging`, `reddit_utils.save_reddit_data`, `reddit_utils._fetch_user_about_data`, `csv_exporter.extract_csvs_from_json`, `monitor.monitor_user`, `ai_utils.generate_prompt_interactive`, `stats.single_report.generate_stats_report`, `stats.comparison.generate_comparison_report`, `analysis.generate_mapped_analysis`, `analysis.generate_raw_analysis`.
        *   **Receives Data From:** `config_utils.load_config`, `argparse.parse_args`, `process_single_user_for_stats`.
        *   **Passes Data To:** Almost all other called functions (config, args, filenames, model instances, filter settings).
    *   **Where to add features:**
        *   New command-line options for user input.
        *   Logic for handling new top-level actions or combinations of actions.
        *   Initial validation of argument relationships.
        *   The overall sequence of operations for a specific workflow (e.g., if a new processing step is added that happens *after* CSV but *before* stats).

2.  **`config_utils.py`**
    *   **Role:** Manage application configuration settings.
    *   **Core Responsibilities:**
        *   Define the complete set of default configuration values (`DEFAULT_CONFIG`).
        *   Load configuration from the `config.json` file.
        *   Merge loaded user settings with defaults, ensuring defaults are used if user settings are missing.
        *   Perform basic type and value validation for critical configuration items (e.g., chunk size, monitor interval).
        *   Provide a function to save the current configuration state to `config.json` (ensuring directory exists and using atomic write).
    *   **Interactions:**
        *   **Called By:** `redstalk.py` (at startup and for reset).
        *   **Provides Data To:** `redstalk.py` (the loaded config dict). The config dict is then passed down through various functions that need settings like `user_agent`.
    *   **Where to add features:**
        *   New application-level settings with default values.
        *   Validation logic for new or existing settings in `load_config`.

3.  **`reddit_utils.py`**
    *   **Role:** Interact directly with the Reddit API, handle raw JSON data fetching, persistence, and basic data extraction/formatting related to the API response structure.
    *   **Core Responsibilities:**
        *   Fetch data pages from `/user/{username}/{category}.json` (submitted/comments) with pagination, retries, timeouts, and User-Agent handling.
        *   Fetch metadata for specific items like posts (`/api/info.json`) with caching.
        *   Fetch user 'about' data (`/user/{username}/about.json`).
        *   Handle common HTTP errors (404, 403, 429) and network issues during API calls.
        *   Determine the canonical modification timestamp (`edited` vs `created_utc`) from a Reddit API item.
        *   Format Unix timestamps into human-readable strings (`format_timestamp`).
        *   Load existing raw user data from a JSON file (`load_existing_data`).
        *   Merge newly fetched data pages with existing in-memory data.
        *   Sort merged data (posts and comments separately) by modification date.
        *   Save the merged and sorted data to a JSON file (`save_reddit_data`), using atomic writes for safety.
    *   **Interactions:**
        *   **Called By:** `redstalk.py` (for initial scrape/update, export JSON only), `monitoring.py` (for checking new activity and re-scraping), `analysis.py` (for fetching external post titles).
        *   **Returns Data:** Raw JSON data (as Python dicts), formatted strings (timestamps, titles), the path to the saved JSON file.
        *   **Uses:** `requests` library, `json` library, `time` module, `os` module, `re` module, `datetime`/`timezone`. Requires `config` for User-Agent.
    *   **Where to add features:**
        *   Interacting with new Reddit API endpoints.
        *   Improving error handling for API responses.
        *   Modifying how raw data is fetched or stored in JSON.
        *   Changing the logic for determining timestamps or formatting basic API response fields.
        *   Implementing more sophisticated caching for API calls.

4.  **`data_utils.py`**
    *   **Role:** Convert the comprehensive raw JSON data into structured CSV files, applying initial date and subreddit filters.
    *   **Core Responsibilities:**
        *   Read the JSON data produced by `reddit_utils`.
        *   Iterate through posts ('t3') and comments ('t1') within the JSON structure.
        *   Apply date filtering based on the modification date.
        *   **Apply Subreddit Filtering:** Implement logic to include items based on `--focus-subreddit` and exclude items based on `--ignore-subreddit`.
        *   Extract relevant fields for CSV columns (title, body, permalink, dates, subreddit, score, flair).
        *   Format text fields for compatibility with CSV (e.g., replacing newlines).
        *   Write filtered posts to `[prefix]-posts.csv` and filtered comments to `[prefix]-comments.csv`.
        *   Handle cases where no data or all data is filtered out, potentially removing empty CSV files.
    *   **Interactions:**
        *   **Called By:** `redstalk.py` (for single user processing, CSV export), `monitoring.py` (for re-creating CSVs after data update).
        *   **Reads From:** JSON file (produced by `reddit_utils`).
        *   **Writes To:** CSV files.
        *   **Uses:** `csv` library, `json` library, `os` module, `datetime`/`timezone`, `re`. Relies on `reddit_utils.get_modification_date` and `reddit_utils.format_timestamp`.
    *   **Where to add features:**
        *   Adding new fields from the Reddit API data to the CSVs.
        *   Modifying how fields are formatted specifically for CSV.
        *   Implementing new types of filters that apply *before* analysis/stats (e.g., filter by score, filter by keyword in title/body - though keyword might be better in `calculations` if analysis requires counts).
        *   Changing the CSV file output format or delimiter.

5.  **`analysis.py`**
    *   **Role:** Prepare filtered data (from CSVs) into formats suitable for AI processing and orchestrate the AI generation calls using `ai_utils`.
    *   **Core Responsibilities:**
        *   Read filtered data from CSV files (posts and comments).
        *   **Apply Date Filtering:** Re-applies the date filter to the entries read from CSVs (this ensures consistency even if `csv_exporter`'s filtering logic is slightly off or if the input CSVs were generated without filters).
        *   Format individual posts and comments (or groups of comments under posts in 'mapped' mode) into structured string blocks for the AI prompt.
        *   In 'mapped' mode, link comments to their parent posts and optionally fetch external post titles using `reddit_utils.get_post_title_from_permalink`.
        *   Perform a quick, initial token count estimate of the entire dataset using `ai_utils.count_tokens`.
        *   Determine if the data needs to be chunked based on the estimate and the configured chunk size.
        *   If chunking is needed, call `ai_utils.chunk_items` to perform the actual chunking based on token counts.
        *   Construct the final prompt(s) including the system prompt and the data chunk(s).
        *   Call `ai_utils.perform_ai_analysis` to send the prompt(s) to the AI model and manage the generation process.
    *   **Interactions:**
        *   **Called By:** `redstalk.py` (for single user analysis), `monitoring.py` (for auto-analysis on update).
        *   **Reads From:** CSV files (produced by `csv_exporter`).
        *   **Calls:** `reddit_utils.get_post_title_from_permalink` (in mapped mode), `ai_utils.count_tokens`, `ai_utils.chunk_items`, `ai_utils.perform_ai_analysis`.
        *   **Uses:** `csv` library, `logging`, `os`, `re`, `datetime`/`timezone`, `time`. Requires `config` (for external context fetch), `model` (AI instance), `system_prompt`, `chunk_size`.
    *   **Where to add features:**
        *   Modifying the text format of entries sent to the AI (how post/comment data is structured into strings).
        *   Changing how comments are grouped under posts or how external context is presented.
        *   Implementing new AI analysis modes that require different data formatting strategies.
        *   Adjusting the logic for deciding whether or how to chunk data.

6.  **`ai_utils.py`**
    *   **Role:** Provide low-level utilities for interacting with the Google Generative AI API, token management, and text chunking.
    *   **Core Responsibilities:**
        *   Directly interface with the `google.generativeai` library (`genai`).
        *   Count tokens for any given text string using `model.count_tokens`.
        *   Implement concurrent token counting (`_get_item_token_count` used with `ThreadPoolExecutor`).
        *   Group a list of text items into token-limited chunks using the concurrent counting (`chunk_items`).
        *   Execute the core AI content generation call (`model.generate_content`) for a single large prompt or a list of chunked prompts.
        *   Handle AI responses (extracting text, identifying block reasons).
        *   Save the final AI analysis output to a specified file (`perform_ai_analysis` handles saving).
        *   Provide the interactive chat loop for generating system prompts (`generate_prompt_interactive`).
    *   **Interactions:**
        *   **Called By:** `analysis.py` (for counting, chunking, and generation execution), `redstalk.py` (for interactive prompt generation, receives the `model` instance initialized here).
        *   **Uses:** `google.generativeai` library, `time`, `os`, `re`, `concurrent.futures`, `tqdm` (for progress bars). Requires an initialized `genai.GenerativeModel` instance.
    *   **Where to add features:**
        *   Changing AI generation parameters (temperature, top_k, etc.).
        *   Implementing different strategies for chunking or token management.
        *   Adding error handling specific to the `google.generativeai` library.
        *   Enhancing the interactive prompt generation logic or AI instructions.

7.  **`monitoring.py`**
    *   **Role:** Continuously monitor a user's Reddit activity for new items and trigger updates.
    *   **Core Responsibilities:**
        *   Run an infinite loop with a defined sleep interval.
        *   Determine the timestamp of the most recent item currently saved in the user's JSON file using `reddit_utils.load_existing_data` and `reddit_utils.get_modification_date`.
        *   Fetch a small number of the user's *latest* items (posts/comments) from the Reddit API using `reddit_utils.get_reddit_data`.
        *   Compare the timestamps of fetched items against the last known timestamp to detect new activity.
        *   If new activity is detected, call `reddit_utils.save_reddit_data` with `force_scrape=False` to trigger an incremental scrape and merge.
        *   After the JSON is updated, call `csv_exporter.extract_csvs_from_json` to re-create the CSV files with the new data (note: filters are *not* currently applied here, it extracts all current JSON data).
        *   Optionally, if configured, call `analysis.generate_raw_analysis` or `analysis.generate_mapped_analysis` to run analysis on the updated dataset.
        *   Handle user interruption (Ctrl+C).
    *   **Interactions:**
        *   **Called By:** `redstalk.py`.
        *   **Calls:** `reddit_utils.load_existing_data`, `reddit_utils.get_modification_date`, `reddit_utils.get_reddit_data`, `reddit_utils.save_reddit_data`, `csv_exporter.extract_csvs_from_json`. Conditionally calls `analysis.generate_raw_analysis`, `analysis.generate_mapped_analysis`.
        *   **Uses:** `logging`, `time`, `os`, `datetime`. Requires `config`, `model`, `system_prompt`, `chunk_size`, `sort_descending`, `analysis_mode`, etc., if auto-analysis is enabled.
    *   **Where to add features:**
        *   Implementing more sophisticated detection of new activity (e.g., checking for updates to existing items).
        *   Adding new actions to perform when new activity is detected (e.g., sending a notification).
        *   Adding configuration for *which* analysis/stats to run on update.
        *   Passing filter arguments down to `extract_csvs_from_json` during monitor updates if needed (currently unfiltered CSVs are generated).

8.  **`stats` Package (Inferred modules: `calculations.py`, `reporting.py`)**
    *   **`calculations.py` (Inferred)**
        *   **Role:** Perform quantitative analysis on the filtered data (from CSVs).
        *   **Core Responsibilities:** Count total posts/comments, calculate average scores, perform word frequency analysis, conduct sentiment analysis, identify top/bottom items by score, etc.
        *   **Interactions:** Called by `stats.single_report.generate_stats_report` and potentially functions within `stats.comparison`. Reads filtered CSVs or operates on in-memory data derived from them.
        *   **Where to add features:** Implementing any new statistical calculation (e.g., time-of-day analysis, subreddit interaction counts, specific keyword tracking).
    *   **`reporting.py` (Inferred)**
        *   **Role:** Format calculated statistical results into Markdown reports.
        *   **Core Responsibilities:** Take structured data (dictionaries) from calculations/comparison and format it into human-readable Markdown text, including headers, lists, tables, etc. (This module should *not* perform calculations itself).
        *   **Interactions:** Called by `stats.single_report.generate_stats_report` and `stats.comparison.generate_comparison_report`.
        *   **Where to add features:** Changing the layout or formatting of the Markdown reports, adding new sections for new calculations.
    *   **`single_report.py` (Inferred structure based on `redstalk.py` usage)**
        *   **Role:** Orchestrate the statistics generation process for a single user.
        *   **Core Responsibilities:**
            *   Take paths to filtered CSV files (and possibly original JSON/about data).
            *   Call the `calculations.py` module/functions to perform all necessary calculations on the data from the CSVs.
            *   Collect the results from calculations into a structured dictionary.
            *   Optionally save this raw result dictionary to a JSON file (`--stats-output-json`).
            *   Call the `reporting.py` module/functions to format the results into a Markdown report.
            *   Save the Markdown report to a file (`--generate-stats` output).
            *   Return the calculated results dictionary (used by `redstalk.py`'s `process_single_user_for_stats` helper for comparison).
        *   **Interactions:**
            *   **Called By:** `redstalk.py` (directly for `--generate-stats` action, or indirectly via `process_single_user_for_stats` helper).
            *   **Calls:** `calculations.py` (inferred), `reporting.py` (inferred).
            *   **Reads From:** Filtered CSV files, possibly original JSON/About data.
            *   **Writes To:** Markdown file, optionally JSON file.
            *   **Returns:** A tuple: (success_boolean, results_dictionary).
        *   **Where to add features:** Managing the flow of a single user's stats generation, deciding which calculations to run, coordinating input/output for the stats process.
    *   **`comparison.py` (Inferred structure based on `redstalk.py` usage)**
        *   **Role:** Orchestrate the comparison of statistics between two users.
        *   **Core Responsibilities:**
            *   Take the calculated stats dictionaries for two users (produced by `single_report.py`, obtained via `process_single_user_for_stats` helper).
            *   Perform comparison-specific analysis (e.g., find common subreddits, calculate differences in scores or frequencies).
            *   Call the `reporting.py` module/functions to format the comparison results into a Markdown report.
            *   Save the comparison report to a file.
        *   **Interactions:**
            *   **Called By:** `redstalk.py`.
            *   **Receives Data From:** `redstalk.py` (the stats dictionaries for user1 and user2).
            *   **Calls:** `reporting.py` (inferred). May call internal comparison calculation functions or `calculations.py`.
        *   **Where to add features:** Implementing new ways to compare user stats, changing the logic of the comparison report.

## Key Takeaways for Adding New Features:

1.  **Identify the Core Task:** What is the fundamental job of the new feature?
    *   Is it getting data from Reddit? -> `reddit_utils.py`
    *   Is it saving/loading the main JSON? -> `reddit_utils.py`
    *   Is it converting JSON to CSV or applying date/subreddit filters? -> `data_utils.py`
    *   Is it a new statistical calculation (counts, averages, frequencies)? -> `stats/calculations.py`
    *   Is it changing how stats results are formatted into a report? -> `stats/reporting.py`
    *   Is it preparing data specifically for AI analysis or managing AI calls/chunking? -> `analysis.py` (prep/orchestration), `ai_utils.py` (low-level AI interaction).
    *   Is it monitoring for changes? -> `monitoring.py`
    *   Is it a new application setting? -> `config_utils.py`
    *   Is it a new command-line option or a new high-level sequence of operations? -> `redstalk.py`

2.  **Add Arguments in `redstalk.py`:** If the new feature needs user input or configuration, add the corresponding argument(s) using `argparse` in `redstalk.py`.

3.  **Handle Configuration in `config_utils.py`:** If the new feature involves a setting that should persist, add it to `DEFAULT_CONFIG`, loading, and saving logic in `config_utils.py`.

4.  **Pass Data Down:** Ensure that any necessary arguments, configuration settings, or intermediate data (like file paths, calculated results dictionaries, model instances) are passed down through the function calls to the module that needs them. Avoid having low-level modules read command-line arguments or config directly; the orchestrator (`redstalk.py` or `monitoring.py`, `analysis.py` etc.) should pass *only* the relevant information.

5.  **Respect Module Boundaries:** Try to keep each module focused on its specific responsibilities. Don't add API calling logic to `analysis.py` or data formatting logic to `reddit_utils.py`.

By following this structure and the breakdown of responsibilities, you can ensure new code is placed logically within the Redstalk library, making it easier to maintain and understand.