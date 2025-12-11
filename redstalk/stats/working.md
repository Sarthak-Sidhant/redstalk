# small reference explanation for me of what this module does
## Basic Working of Everything Involved (Simplified)

Okay, let's break down the whole process of generating a stats report from start to finish in simple terms. Imagine a factory with different specialized workers and machines.

1.  **Starting Point: The Raw Data Files**
    *   You have downloaded a bunch of your Reddit activity – posts and comments. The main RedStalk tool saves this in a big **JSON file**.
    *   It might also create separate **CSV files** (like `posts.csv`, `comments.csv`) which are just simpler table versions of some of that data, useful for certain tasks.
    *   Sometimes, RedStalk also fetches your public **"about" information** (like your total karma and when your account was created).

2.  **Your Instructions: The Command Line**
    *   When you run RedStalk, you give it instructions like "Generate a report for `u/username`" and tell it where to find the data files and where to save the report.
    *   You can also give it extra instructions, like "Only look at posts from the last year," or "Only look at comments in `r/AskReddit`," or "Tell me the top 10 most frequent words." These are your **filters** and **calculation settings**.

3.  **The Manager: `single_report.py`**
    *   Think of `single_report.py` as the main manager for a single user's report.
    *   It takes all your instructions from the command line.
    *   It tells the "Data Loader" worker to open and read the big JSON file.
    *   It then tells the "Filtering" worker to go through the loaded JSON data and keep *only* the posts/comments that match your date range and subreddit filters.
    *   If the filters are very strict and *no* data is left, the manager stops and reports that nothing was found.
    *   If there's filtered data left, the manager now has the specific set of items it needs to analyze.

4.  **The Workers: `calculations.py`**
    *   The manager (`single_report.py`) now tells the `calculations.py` team to start working.
    *   The `calculations.py` team has lots of specialized workers, each good at figuring out one specific thing (like counting posts, finding the average score, counting words, doing sentiment analysis, finding mentions, etc.).
    *   The manager gives these workers the *filtered* data (either the data it filtered in memory from the JSON, or tells them to read from the specific filtered CSV files).
    *   These workers might need special tools (like the Pandas machine for reading CSVs or the VADER machine for sentiment) – they check if these tools are available before trying to use them. If a tool is missing, they just report that they had to skip that specific task.
    *   Each worker does its job and gives its result (just the numbers or lists for their specific task) back to the manager.

5.  **The Organizer: `single_report.py` (Again)**
    *   The manager (`single_report.py`) collects *all* the results from the `calculations.py` workers into one big dictionary of numbers and findings.

6.  **The Designer: `reporting.py`**
    *   The manager (`single_report.py`) now takes that big dictionary of results and gives it to the `reporting.py` designer.
    *   The designer's job is just to make this data look nice in a report format. They arrange everything into sections, create tables, lists, etc., and write it all out as standard text formatted for Markdown.
    *   This designer *doesn't* do any math, and they *don't* use any fancy screen colors – just clean text formatting.

7.  **Saving the Results: `single_report.py` (Final Step)**
    *   The manager (`single_report.py`) takes the formatted Markdown text from the designer and saves it to the output file you specified.
    *   If you asked for it, it also takes that big dictionary of results (the raw numbers) and saves it as a JSON file, which is useful if other programs want to read the data later.

8.  **Shared Tools: `core_utils.py`**
    *   Throughout this process, many workers and the manager need to do small common tasks, like cleaning up text or dealing with timestamps.
    *   They use the shared toolbox, `core_utils.py`, which has these helpful small functions ready to go.

9.  **Comparing Two Users: `comparison.py`**
    *   If you want to compare two users, you would typically run the whole process above *twice* first, once for each user, saving their results as JSON files.
    *   Then, you'd use a different command that tells RedStalk to compare those two saved result JSON files.
    *   This triggers the `comparison.py` module. It loads the two result dictionaries.
    *   It has its own workers that do *specific* comparison tasks (like finding overlapping subreddits or common words).
    *   It then gives *these* comparison results, along with the two original result dictionaries, to the `reporting.py` designer, but tells it to use the `_format_comparison_report` function instead to make a comparison report.
    *   Finally, `comparison.py` saves this comparison Markdown report.

In essence, the process is: **Load -> Filter -> Calculate -> Format -> Save**, with different modules specializing in each step and `single_report.py` acting as the main orchestrator for one user's data, while `comparison.py` orchestrates the comparison of two sets of results.