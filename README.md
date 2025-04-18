# RedStalk
### Reddit User AI Profiler

*Currently uses the `gemini-pro` model by default (configurable in the script). Chunking occurs for data exceeding ~1M tokens.*

This script scrapes the public post and comment history of a specified Reddit user, processes the text, and then uses the Google Gemini AI to generate a detailed character analysis based on their language and activity.

## What it Does

1.  **Scrapes Reddit:** Fetches public posts and comments for a given username. It tries to be efficient by only fetching new or updated items if you run it again for the same user (unless you force a full re-scrape).
2.  **Processes Data:** Cleans up the text slightly and organizes it into `.json` and `.csv` files, including the creation date and a direct link (permalink as a full URL) for each post and comment.
3.  **Analyzes with AI:** Sends the user's text data (potentially in chunks if it's very long) to the Google Gemini AI.
4.  **Uses Custom Instructions:** You provide a separate text file (e.g., `prompt.txt`) that tells the AI *how* to perform the analysis (e.g., what personality traits to look for, how to structure the output).
5.  **Generates Profile:** Saves the AI's analysis as a timestamped Markdown (`.md`) file. It can create two types of profiles:
    *   **Mapped:** Tries to group comments under the posts they replied to (Default).
    *   **Raw:** Simply lists all posts and comments chronologically.

## Features

*   Incremental scraping to save time and avoid redundant fetches.
*   Option to scrape only comments (`--scrape-comments-only`).
*   Option to force a full re-scrape (`--force-scrape`).
*   Includes timestamps and full permalink URLs in data passed to the AI.
*   Loads AI instructions (system prompt) from an external file.
*   Supports "Mapped" and "Raw" analysis output formats (`--raw-analysis`).
*   Handles large amounts of text by splitting it into chunks for the AI.
*   **Configurable Chunk Size:** Control the target token limit per chunk via command-line (`--chunk-size`) or config file.
*   **Configuration File (`config.json`):** Set default values for output directory, prompt file path, chunk size, and optionally the API key.
*   **Flexible API Key Handling:** Prioritizes API key sources: Environment Variable -> `config.json` -> Command-line flag.
*   **Timestamped Output:** Analysis filenames include the date (e.g., `username_charc_mapped_20231028.md`).
*   **Config Reset:** Easily reset `config.json` to defaults (`--reset-config`).
*   Organizes all output neatly into a directory for the specific user.
*   Configurable via command-line arguments with sensible defaults.

## Prerequisites

*   **Python 3:** Version 3.7 or higher is recommended.
*   **pip:** Python's package installer (usually comes with Python).
*   **Google Gemini API Key:** You need an API key from Google AI Studio or Google Cloud. Get one here: [https://aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)

## Basic Setup

1.  **Get the Script:** Download or clone the `main.py` (or `reddit_analyzer.py` as generated previously) script file to your computer.
2.  **Install Libraries:** Open your terminal or command prompt, navigate to the directory where you saved the script, and run:
    ```bash
    pip install requests google-generativeai
    ```
3.  **Create Prompt File:** Create a plain text file named `prompt.txt` (or use the `--prompt-file` argument to specify a different path/name) in the same directory. Paste the instructions for the AI into this file. This file tells the AI what kind of analysis you want.
4.  **(Optional) Create Config File:** Create a file named `config.json` in the same directory to set custom defaults. If you don't create it, the script uses built-in defaults.

    *Example `config.json`:*
    ```json
    {
        "default_output_dir": "reddit_profiles",
        "default_prompt_file": "prompts/analyzer_v2.txt",
        "default_chunk_size": 400000,
        "api_key": null
    }
    ```
    *(Set `"api_key"` to your key string here if you want to use the config file as a source for it, otherwise keep it `null` or omit the line).*

## Configuration: The API Key

The script needs your Google Gemini API key. It looks for the key in this order:

1.  **Environment Variable (Highest Priority):** Checks for `GOOGLE_API_KEY`. This is the recommended method for security.
    *   **Linux/macOS:** `export GOOGLE_API_KEY="YOUR_KEY_HERE"`
    *   **Windows CMD:** `set GOOGLE_API_KEY=YOUR_KEY_HERE`
    *   **Windows PowerShell:** `$env:GOOGLE_API_KEY="YOUR_KEY_HERE"`
2.  **Config File (`config.json`) (Middle Priority):** Checks for a non-null `"api_key"` field inside `config.json`.
3.  **Command-Line Argument (Lowest Priority):** Checks for the `--api-key` flag.

The script uses the *first* valid key it finds following this priority. You only need to provide the key using **one** of these methods. If no key is found from any source, the script will exit with an error.

## How to Run the Script

Open your terminal or command prompt and navigate to the directory containing `main.py` (or your script name).

**Example 1: Using Defaults (Config/ENV VAR Key)**

*   Assumes `config.json` exists for defaults OR you're using built-in defaults.
*   Assumes API key is set via `GOOGLE_API_KEY` environment variable OR in `config.json`.
*   Performs default mapped analysis.

```bash
python main.py <reddit_username>
```
(Replace <reddit_username>)

**Example 2: Overriding Output Dir & Prompt File**
*   Uses the API key from ENV VAR or config.json.
*   Specifies a different output directory and prompt file.

```
python main.py <reddit_username> --output-dir analysis_results --prompt-file custom_prompt.txt
```
**Example 3: Using Command-Line API Key (Fallback)**
*   Only use --api-key if the key isn't set in ENV or config.json.

```
python main.py <reddit_username> --api-key "YOUR_API_KEY_HERE"
```
**Example 4: Raw Analysis with Custom Chunk Size**
*   Uses the --raw-analysis flag.
*   Overrides the default chunk size.
```
python main.py <reddit_username> --raw-analysis --chunk-size 250000
```

**Example 5: Forcing a Re-scrape**
```
python main.py <reddit_username> --force-scrape
```

**Example 6: Resetting Configuration**
*   Resets config.json to the script's built-in defaults (including "api_key": null).
```
python main.py --reset-config
```

**HELP**
*   To see all available command-line options, their defaults, and descriptions, run:
```
python main.py --help
```

### Output Files
The script will create a directory structure like this (default output directory is data unless changed):
```
<output_directory>/
└── <reddit_username>/
    ├── <username>.json                      # Raw scraped data from Reddit
    ├── <username>-posts.csv                 # Processed posts in CSV format
    ├── <username>-comments.csv              # Processed comments in CSV format
    ├── <username>_charc_mapped_YYYYMMDD.md  # AI analysis (mapped, timestamped)
    └── <username>_charc_raw_YYYYMMDD.md     # AI analysis (raw, timestamped, if used)
    └── config.json                          # (Optional) Configuration file in script directory
```
## Important Notes
*   API Costs: Using the Google Gemini API may incur costs. Check Google Cloud or AI Studio pricing.
*   Reddit API Limits: The script includes a 2-second delay between fetches. Avoid very frequent runs. Consider customizing the User-Agent in the script if using heavily.
*   Ethical Use: Use this tool responsibly and be mindful of privacy and the ethics of analyzing public user data.
*   AI Accuracy: AI analysis can be insightful but is not infallible. It may contain inaccuracies, biases, or misinterpretations. Critical review of the output is recommended.
