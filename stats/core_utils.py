import re
import logging
from datetime import datetime, timezone, timedelta
import math
import statistics # For standard deviation in burstiness calc (though calc is elsewhere)

# --- ANSI Codes (for logging ONLY) ---
# These should NOT be used in the report formatting functions in reporting.py
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; GREEN = "\033[32m"; RED = "\033[31m"; YELLOW = "\033[33m"

# --- Helper Functions & Constants ---

STOP_WORDS = set([
    # --- Full list ---
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
    'like', 'get', 'also', 'would', 'could', 'one', 'post', 'comment', 'people', 'subreddit', 'even', 'use',
    'go', 'make', 'see', 'know', 'think', 'time', 'really', 'say', 'well', 'thing', 'good', 'much', 'need',
    'want', 'look', 'way', 'user', 'reddit', 'www', 'https', 'http', 'com', 'org', 'net', 'edit', 'op',
    'deleted', 'removed', 'image', 'video', 'link', 'source', 'title', 'body', 'self', 'text', 'post', 'karma',
    'amp', 'gt', 'lt', "dont", "cant", "wont", "couldnt", "shouldnt", "wouldnt", "isnt", "arent", "bhai","bhi","hai","let","ha","nahi","thats","thi","ki","kya","koi","kuch","bhi","sab","sabhi","sabse","sabka","sare","saare","saaray","hi",
    'however', 'therefore', 'moreover', 'furthermore', 'besides', 'anyway', 'actually', 'basically', 'literally', 'totally',
    'simply', 'maybe', 'perhaps', 'likely', 'possibly', 'certainly', 'obviously', 'clearly', 'indeed', 'something', 'someone',
    'anything', 'anyone', 'everything', 'everyone', 'lot', 'little', 'many', 'few', 'several', 'various', 'enough', 'quite',
    'rather', 'such', 'account', 'share', 'commented', 'upvoted', 'downvoted', 'thread', 'page', 'today', 'yesterday',
    'tomorrow', 'now', 'later', 'soon', 'eventually', 'always', 'never', 'often', 'sometimes', 'rarely', 'put', 'take', 'give',
    'made', 'went', 'mera', 'meri', 'mere', 'tumhara', 'tumhari', 'tumhare', 'uska', 'uski', 'uske', 'humara', 'humari',
    'humare', 'inka', 'inki', 'inke', 'unka', 'unki', 'unke', 'aapka', 'aapki', 'aapke', 'toh', 'yaar', 'acha', 'ab', 'phir',
    'kyunki', 'lekin', 'magar', 'aur', 'bhi', 'tab', 'jab', 'agar', 'wahi', 'ussi', 'usse', 'ismein', 'usmein', 'yahan',
    'wahan', 'idhar', 'udhar', 'hona', 'karna', 'jana', 'aana', 'dena', 'lena', 'milna', 'dekhna', 'bolna', 'sunna', 'baat',
    'log', 'kaam', 'cheez', 'tarah', 'waqt', 'din', 'raat', 'shaam', 'subah', 'arre', 'oh', 'haan', 'nahin', 'bas', 'theek',
    # Added common markers to avoid counting them in frequency if clean_text is used carelessly elsewhere
    'username_mention', 'subreddit_mention',
])


def _get_timestamp(item_data, use_edited=True):
    """Gets created or edited timestamp (UTC float) from item data."""
    if not isinstance(item_data, dict): return 0
    edited_ts = item_data.get("edited")
    created_utc = item_data.get("created_utc", 0)
    if use_edited and edited_ts and str(edited_ts).lower() != 'false':
        try:
            edited_ts_float = float(edited_ts)
            created_ts_float = float(created_utc) # Ensure created is float too
            # Only use edited time if it's actually later than created time
            if edited_ts_float > created_ts_float: return edited_ts_float
            else: return created_ts_float # Fallback to created if edited is same or earlier
        except (ValueError, TypeError):
            pass # Fall through to return created_utc if edited is invalid
    try:
        return float(created_utc)
    except (ValueError, TypeError):
        return 0

def clean_text(text, remove_stopwords=True):
    """Improved text cleaning."""
    if not isinstance(text, str): return []
    text = text.lower()
    # URLs
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)
    # User mentions (/u/ or u/)
    text = re.sub(r'/?u/[\w_-]+', ' username_mention ', text)
    # Subreddit mentions (/r/ or r/)
    text = re.sub(r'/?r/[\w_-]+', ' subreddit_mention ', text)
    # Punctuation (keep internal hyphens/apostrophes if needed, but this removes them)
    text = re.sub(r'[^\w\s]', '', text)
    # Numbers
    text = re.sub(r'\d+', '', text)
    # Split into words
    words = text.split()
    if remove_stopwords:
        # Remove stop words and short words (<=1 char)
        words = [word for word in words if word not in STOP_WORDS and len(word) > 1]
    else:
        # Just remove empty strings resulting from multiple spaces
        words = [word for word in words if len(word) > 0]
    return words

def _format_timedelta(seconds):
    """Formats a duration in seconds into a human-readable string (s, m, h, d)."""
    if seconds is None or not isinstance(seconds, (int, float)) or seconds < 0: return "N/A"
    if seconds < 60: return f"{seconds:.1f}s"
    elif seconds < 3600: return f"{seconds/60:.1f}m"
    elif seconds < 86400: return f"{seconds/3600:.1f}h"
    else: return f"{seconds/86400:.1f}d"

def _generate_ngrams(words, n):
    """Helper to generate n-grams from a list of words."""
    # Simple implementation without padding
    if len(words) < n:
        return # Return an empty generator if not enough words
    for i in range(len(words) - n + 1):
        yield tuple(words[i:i+n])
