# stats/core_utils.py
"""
This module contains core utility functions and constants used by other
modules within the 'stats' package, such as calculations.py and comparison.py.
These utilities handle common tasks like text cleaning, timestamp manipulation,
and formatting.
"""

import re # Used for pattern matching in text cleaning (URLs, mentions)
import logging # For logging messages (debug, warning, error)
from datetime import datetime, timezone, timedelta # Used for handling timestamps and durations
import math # Imported potentially for calculations, but not directly used in the provided functions. Keeping it as it was in the original.
import statistics # Imported potentially for calculations, but not directly used in the provided functions. Keeping it as it was in the original.

# --- ANSI Codes (for logging ONLY) ---
# These are special character sequences used to add color and style
# to the output in the terminal. This helps make log messages more
# readable and highlight warnings/errors.
# IMPORTANT: These codes should NOT be used in the functions that generate
# the final report output (e.g., in reporting.py), as they are specific
# to terminal display and would appear as raw codes in a file like a Markdown report.
CYAN = "\033[36m"; # Cyan color
RESET = "\033[0m"; # Reset to default color and style
BOLD = "\033[1m"; # Bold text
DIM = "\033[2m"; # Dim/faint text (often not well supported)
GREEN = "\033[32m"; # Green color
RED = "\033[31m"; # Red color
YELLOW = "\033[33m" # Yellow color


# --- Helper Functions & Constants ---
# These functions provide common utilities needed across statistical calculations.

STOP_WORDS = set([
    # --- Common English Stop Words ---
    # These are very common words (articles, prepositions, pronouns, etc.)
    # that often don't carry significant meaning for topic analysis or
    # word frequency. They are typically removed during text cleaning.
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
    'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers',
    'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which',
    'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
    'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if',
    'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between',
    'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out',
    'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why',
    'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
    'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't",
    'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
    "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't",
    'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't",
    'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't",

    # --- Common Reddit/Context-Specific Terms ---
    # Words that frequently appear in Reddit data but are often not
    # informative for topic analysis. Includes common conversational fillers,
    # terms related to the platform itself, and indicators of deleted/removed content.
    'like', 'get', 'also', 'would', 'could', 'one', 'post', 'comment', 'people', 'subreddit', 'even', 'use',
    'go', 'make', 'see', 'know', 'think', 'time', 'really', 'say', 'well', 'thing', 'good', 'much', 'need',
    'want', 'look', 'way', 'user', 'reddit', 'www', 'https', 'http', 'com', 'org', 'net', 'edit', 'op',
    'deleted', 'removed', 'image', 'video', 'link', 'source', 'title', 'body', 'self', 'text', 'post', 'karma',
    'amp', 'gt', 'lt', # HTML/Markdown entities often found in raw text
    "dont", "cant", "wont", "couldnt", "shouldnt", "wouldnt", "isnt", "arent", # Common contractions without apostrophes
    # --- Potential Hindi/Desi Terms (based on presence in list) ---
    # If the analysis is expected to include content with these terms,
    # they might be included as stop words if they are common and not
    # indicative of specific topics. Review if this list needs adjustment.
    "bhai","bhi","hai","let","ha","nahi","thats","thi","ki","kya","koi","kuch","bhi","sab","sabhi","sabse","sabka","sare","saare","saaray","hi",
    'however', 'therefore', 'moreover', 'furthermore', 'besides', 'anyway', 'actually', 'basically', 'literally', 'totally',
    'simply', 'maybe', 'perhaps', 'likely', 'possibly', 'certainly', 'obviously', 'clearly', 'indeed', 'something', 'someone',
    'anything', 'anyone', 'everything', 'everyone', 'lot', 'little', 'many', 'few', 'several', 'various', 'enough', 'quite',
    'rather', 'such', 'account', 'share', 'commented', 'upvoted', 'downvoted', 'thread', 'page', 'today', 'yesterday',
    'tomorrow', 'now', 'later', 'soon', 'eventually', 'always', 'never', 'often', 'sometimes', 'rarely', 'put', 'take', 'give',
    'made', 'went', # More common English words
    'mera', 'meri', 'mere', 'tumhara', 'tumhari', 'tumhare', 'uska', 'uski', 'uske', 'humara', 'humari', # More Hindi/Desi terms
    'humare', 'inka', 'inki', 'inke', 'unka', 'unki', 'unke', 'aapka', 'aapki', 'aapke', 'toh', 'yaar', 'acha', 'ab', 'phir',
    'kyunki', 'lekin', 'magar', 'aur', 'bhi', 'tab', 'jab', 'agar', 'wahi', 'ussi', 'usse', 'ismein', 'usmein', 'yahan',
    'wahan', 'idhar', 'udhar', 'hona', 'karna', 'jana', 'aana', 'dena', 'lena', 'milna', 'dekhna', 'bolna', 'sunna', 'baat',
    'log', 'kaam', 'cheez', 'tarah', 'waqt', 'din', 'raat', 'shaam', 'subah', 'arre', 'oh', 'haan', 'nahin', 'bas', 'theek',

    # --- Tokens used to replace mentions ---
    # If clean_text replaces mentions with these strings, they should also be
    # excluded from word frequency counts.
    'username_mention', 'subreddit_mention',
])


def _get_timestamp(item_data, use_edited=True):
    """
    Retrieves a timestamp (in UTC epoch seconds as a float) from a Reddit
    item's data dictionary.

    It prioritizes the 'edited' timestamp if 'use_edited' is True, the item
    has been edited (indicated by a non-False value), and the edited timestamp
    is *after* the creation timestamp. Otherwise, it returns the 'created_utc'
    timestamp.

    Args:
        item_data (dict): The 'data' dictionary from a Reddit post or comment object.
        use_edited (bool): If True, attempt to use the 'edited' timestamp if available and valid.
                           If False, always use 'created_utc'.

    Returns:
        float: The selected timestamp as a float (epoch seconds UTC), or 0.0 if
               the timestamp is missing, invalid, or cannot be processed.
    """
    if not isinstance(item_data, dict):
        # Return 0 if the input is not a dictionary
        # logging.debug("Attempted _get_timestamp on non-dict input.")
        return 0.0

    edited_ts_raw = item_data.get("edited") # Get the raw value of 'edited' (can be False or timestamp)
    created_utc_raw = item_data.get("created_utc", 0) # Get created timestamp, default to 0

    # Check if we should try to use the edited timestamp
    if use_edited and edited_ts_raw is not None and str(edited_ts_raw).lower() != 'false':
        try:
            # Attempt to convert both edited and created timestamps to float
            edited_ts_float = float(edited_ts_raw)
            created_ts_float = float(created_utc_raw) # Ensure created is also a float for comparison

            # Only use edited time if it's strictly later than created time.
            # Sometimes 'edited' can be the same as 'created_utc' due to quirks.
            if edited_ts_float > created_ts_float:
                return edited_ts_float
            else:
                 # If edited time is same or earlier, fall back to created time.
                 return created_ts_float
        except (ValueError, TypeError):
            # If edited timestamp or created timestamp cannot be converted to float,
            # this indicates corrupted data. Fall through to try returning created_utc alone.
            pass # Pass silently and proceed to the next check

    # If use_edited is False, or edited timestamp was invalid/False/not later than created,
    # return the created timestamp.
    try:
        return float(created_utc_raw)
    except (ValueError, TypeError):
        # If even the created timestamp is invalid, return 0.0
        # logging.debug(f"Could not convert created_utc '{created_utc_raw}' to float.")
        return 0.0

def clean_text(text, remove_stopwords=True):
    """
    Cleans text by converting to lowercase, removing URLs, mentions,
    punctuation, numbers, and optionally stop words. Splits the cleaned
    text into a list of words.

    Args:
        text (str): The input text string to clean.
        remove_stopwords (bool): If True, remove words present in the STOP_WORDS set
                                 and words with length 1 or less. If False, keep
                                 all words after cleaning, including short ones.

    Returns:
        list: A list of cleaned words. Returns an empty list if the input is not a string
              or contains no meaningful words after cleaning.
    """
    # Return empty list if input is not a string
    if not isinstance(text, str):
        # logging.debug(f"clean_text received non-string input: {type(text)}")
        return []

    # Convert text to lowercase
    text = text.lower()

    # Remove URLs using regex. Replaces them with a single space.
    # This prevents URLs from being treated as words or containing characters that break cleaning.
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)

    # Replace User mentions (/u/ or u/) with a generic token 'username_mention'.
    # This allows mention frequency to be calculated separately without these
    # specific strings interfering with general word frequency or topic analysis.
    # Pattern finds '/u/' or 'u/' followed by valid username characters.
    text = re.sub(r'/?u/[\w_-]+', ' username_mention ', text)

    # Replace Subreddit mentions (/r/ or r/) with a generic token 'subreddit_mention'.
    # Similar reasoning as user mentions.
    # Pattern finds '/r/' or 'r/' followed by valid subreddit characters.
    text = re.sub(r'/?r/[\w_-]+', ' subreddit_mention ', text)

    # Remove punctuation using regex. Keeps only alphanumeric characters and whitespace.
    # Note: This removes hyphens and apostrophes within words (e.g., "well-being" becomes "well being", "it's" becomes "its").
    # A more complex regex would be needed to preserve internal punctuation if desired.
    text = re.sub(r'[^\w\s]', '', text)

    # Remove numbers using regex. Replaces sequences of digits with a space.
    text = re.sub(r'\d+', '', text)

    # Split the cleaned text into a list of words based on whitespace
    words = text.split()

    # --- Optional Stop Word Removal ---
    if remove_stopwords:
        # Filter the list of words:
        # 1. Exclude words that are in the STOP_WORDS set.
        # 2. Exclude words with length 1 or less (often remaining punctuation or single letters).
        cleaned_words = [word for word in words if word not in STOP_WORDS and len(word) > 1]
    else:
        # If stop words are not removed, just remove any empty strings
        # that might have resulted from the cleaning process (e.g., multiple spaces).
        cleaned_words = [word for word in words if len(word) > 0]

    return cleaned_words

def _format_timedelta(seconds):
    """
    Formats a duration given in seconds into a human-readable string
    using the largest appropriate unit (seconds, minutes, hours, or days).

    Args:
        seconds (int or float): The duration in seconds.

    Returns:
        str: A formatted string representing the duration (e.g., "15.5s", "2.3m", "1.8h", "5.2d"),
             or "N/A" if the input is invalid (None, not a number, or negative).
    """
    # Check for invalid input
    if seconds is None or not isinstance(seconds, (int, float)) or seconds < 0:
        return "N/A"

    # Format using appropriate unit
    if seconds < 60:
        return f"{seconds:.1f}s" # Format seconds to 1 decimal place
    elif seconds < 3600: # Less than an hour
        return f"{seconds/60:.1f}m" # Format minutes to 1 decimal place
    elif seconds < 86400: # Less than a day
        return f"{seconds/3600:.1f}h" # Format hours to 1 decimal place
    else: # One day or more
        return f"{seconds/86400:.1f}d" # Format days to 1 decimal place

def _generate_ngrams(words, n):
    """
    Generates N-grams (sequences of n words) from a list of words.

    Args:
        words (list): A list of words. Expected to be already cleaned and tokenized.
        n (int): The size of the N-gram (e.g., 2 for bigrams, 3 for trigrams). Must be > 0.

    Yields:
        tuple: A tuple containing the words for one N-gram.

    Note: This is a simple implementation that does not include padding (adding
          start/end tokens). It will yield `len(words) - n + 1` n-grams if `len(words) >= n`.
    """
    # Return nothing (empty generator) if the list of words is shorter than the requested n-gram size
    if len(words) < n:
        return

    # Iterate through the word list, taking slices of size n
    # The loop runs from the start (index 0) up to the point where
    # a slice of size n can be taken without going out of bounds.
    for i in range(len(words) - n + 1):
        # Yield a tuple containing the words from the current position (i)
        # up to i + n. Using a tuple makes n-grams hashable, suitable for Counters.
        yield tuple(words[i:i+n])