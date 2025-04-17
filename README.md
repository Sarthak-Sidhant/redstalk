# RedStalk
### Reddit User AI Profiler

*currently uses the gemini-2.0-flash model with 1M context window*

This script scrapes the public post and comment history of a specified Reddit user, processes the text, and then uses the Google Gemini AI to generate a detailed character analysis based on their language and activity.

## What it Does

1.  **Scrapes Reddit:** Fetches public posts and comments for a given username. It tries to be efficient by only fetching new or updated items if you run it again for the same user (unless you force a full re-scrape).
2.  **Processes Data:** Cleans up the text slightly and organizes it into `.json` and `.csv` files, including the creation date and a direct link (permalink) for each post and comment.
3.  **Analyzes with AI:** Sends the user's text data (potentially in chunks if it's very long) to the Google Gemini AI.
4.  **Uses Custom Instructions:** You provide a separate text file (`prompt.txt`) that tells the AI *how* to perform the analysis (e.g., what personality traits to look for, how to structure the output).
5.  **Generates Profile:** Saves the AI's analysis as a Markdown (`.md`) file. It can create two types of profiles:
    *   **Mapped:** Tries to group comments under the posts they replied to (Default).
    *   **Raw:** Simply lists all posts and comments chronologically.

## Features

*   Incremental scraping to save time and avoid redundant fetches.
*   Option to scrape only comments.
*   Option to force a full re-scrape.
*   Includes timestamps and permalinks in data passed to the AI.
*   Loads AI instructions (system prompt) from an external file for easy customization.
*   Supports "Mapped" and "Raw" analysis output formats.
*   Handles large amounts of text by splitting it into chunks for the AI.
*   Organizes all output neatly into a directory for the specific user.
*   Configurable via command-line arguments.

## Prerequisites

*   **Python 3:** Version 3.7 or higher is recommended.
*   **pip:** Python's package installer (usually comes with Python).
*   **Google Gemini API Key:** You need an API key from Google AI Studio or Google Cloud to use the analysis feature. Get one here: [https://aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)

## Basic Setup

1.  **Get the Script:** Download or clone the `main.py` script file to your computer.
2.  **Install Libraries:** Open your terminal or command prompt, navigate to the directory where you saved the script, and run:
    ```
    pip install requests google-generativeai
    ```
3.  **Create Prompt File:** Create a plain text file named `prompt.txt` (or any name you like) in the same directory. Paste the instructions for the AI into this file. See the example prompt provided earlier or write your own! This file tells the AI what kind of analysis you want.

## Configuration: The API Key

The script needs your Google Gemini API key to work. You have two ways to provide it:

**Method 1: Command-Line Argument (Less Secure)**

*   You can pass the key directly when you run the script using the `--api-key` flag.
*   **Warning:** This might store your key in your shell history.

**Method 2: Environment Variable (Recommended)**

*   This is generally a safer way to handle keys.
*   **How to set it:**
    *   **Linux/macOS:** In your terminal, run `export GOOGLE_API_KEY="YOUR_API_KEY_HERE"` (replace with your actual key). This usually lasts for your current terminal session. To make it permanent, add this line to your shell profile file (like `.bashrc`, `.zshrc`, or `.profile`).
    *   **Windows (Command Prompt):** Run `set GOOGLE_API_KEY=YOUR_API_KEY_HERE`. This lasts for the current session.
    *   **Windows (PowerShell):** Run `$env:GOOGLE_API_KEY="YOUR_API_KEY_HERE"`. This lasts for the current session.
    *   **Windows (Permanent):** Search for "Environment Variables" in the Start menu and add `GOOGLE_API_KEY` as a new User variable with your key as the value. You might need to restart your terminal or PC.
*   If you set the environment variable, you **don't** need to use the `--api-key` argument when running the script.

## How to Run the Script

Open your terminal or command prompt and navigate to the directory containing `main.py` and your `prompt.txt`.

**Example 1: Basic Run (Mapped Analysis)**

*   Uses the `--api-key` argument.
*   Assumes your prompt file is named `prompt.txt`.
*   Performs the default mapped analysis.

```
python main.py <reddit_username> --api-key "YOUR_API_KEY_HERE" --prompt-file prompt.txt
```
(Replace <reddit_username> and "YOUR_API_KEY_HERE")

**Example 2: Basic Run (Using Environment Variable)**
*  Make sure you've set the GOOGLE_API_KEY environment variable first!
*  No need for --api-key.

```
python main.py <reddit_username> --prompt-file prompt.txt
```
(Replace <reddit_username>)

**Example 3: Raw Analysis (Using Environment Variable)**
*    Uses the --raw-analysis flag to get a sequential list instead of mapped comments.

```
python main.py <reddit_username> --prompt-file prompt.txt --raw-analysis
```
**Example 4: Specifying Output Directory and Forcing Scrape**
*  Uses --output-dir to save files elsewhere (e.g., analysis_results).
*  Uses --force-scrape to fetch all data again, ignoring any previously saved JSON.
```
python main.py <reddit_username> --api-key "YOUR_KEY" --prompt-file prompt.txt --output-dir analysis_results --force-scrape
```

**HELP**
*  to see all command line options
```python main.py --help```

### Output Files
The script will create a directory structure like this:
By default, <output_directory> is data. You can change it with --output-dir.
```
<output_directory>/
└── <reddit_username>/
    ├── <username>.json                 # Raw scraped data from Reddit
    ├── <username>-posts.csv            # Processed posts in CSV format
    ├── <username>-comments.csv         # Processed comments in CSV format
    ├── <username>_character_sketch.md  # The AI-generated profile (mapped analysis)
    └── <username>_character_sketch_raw.md # The AI-generated profile (raw analysis, if used)
```

## Important Notes
*  API Costs: Using the Google Gemini API may incur costs based on the amount of text processed. Check Google's pricing.
*  Reddit API Limits: The script includes a small delay between requests to be polite to Reddit's API. Avoid running it too frequently or aggressively for many users. The script uses a basic User-Agent; customizing it might be slightly better practice if used heavily.
*  Ethical Use: Be mindful of the ethical implications of analyzing and profiling individuals, even based on public data. Use this tool responsibly.
*  AI Accuracy: The AI's analysis is based on patterns in language. It can be insightful, but it can also be inaccurate, biased, or make incorrect inferences. Always review the output critically.

*  this whole readme was generated by gemini, i just added the help section, i wont bother checking if it was right or wrong.
