# ai_utils.py
"""
This module provides utility functions for interacting with the Google Generative AI
API (Gemini models), specifically focusing on token management, chunking large
amounts of text data for processing, and performing AI analysis. It includes
features for concurrent token counting to improve performance during chunking
and an interactive assistant for generating system prompts.
"""

# Import necessary libraries
import logging # For logging information, warnings, and errors
import google.generativeai as genai # The core library for interacting with Google's AI models
import time # For adding delays (if needed for rate limits) and measuring execution time
import os # For interacting with the operating system, like creating directories
import re # For regular expressions (used in interactive prompt cleanup)
import concurrent.futures # Provides ThreadPoolExecutor for concurrent execution
from tqdm import tqdm # For displaying progress bars, useful for long-running tasks like token counting

# Define ANSI escape codes for colored terminal output (makes logs/messages more readable)
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; MAGENTA = "\033[35m"; YELLOW = "\033[33m"; GREEN = "\033[32m"; RED = "\033[31m"

# --- Configuration ---
# This constant controls the maximum number of token counting calls that can run
# simultaneously when processing a large list of items for chunking.
# Adjusting this number can balance API rate limits, network latency, and CPU
# utilization. Start low and increase if stable and beneficial.
MAX_CONCURRENT_TOKEN_CALLS = 10


# --- Core AI Functions ---

def count_tokens(model, text):
    """
    Counts the number of tokens in a given text string using the specified
    GenerativeModel instance's built-in count_tokens method.

    Handles non-string inputs and empty strings gracefully. Logs errors
    if the API call fails.

    Args:
        model: An instance of google.generativeai.GenerativeModel.
        text: The string text whose tokens need to be counted.

    Returns:
        The number of tokens (int) if successful and text is non-empty,
        0 if text is not a string or is empty/whitespace, or None if an
        API error occurred during the count.
    """
    # Check if the input is a string; log a warning and return 0 if not.
    if not isinstance(text, str):
        logging.warning(f"      ⚠️ Cannot count tokens on non-string input: type {type(text)}")
        return 0 # Return 0 for non-strings
    # Check for empty or whitespace-only strings; return 0 as they have no meaningful tokens.
    if not text.strip():
        return 0 # Return 0 for empty strings

    try:
        # Call the model's count_tokens method. This is an API call.
        # A small delay *could* be added here per call if *individual* call
        # rate limits were still an issue even with concurrency, but typically
        # handling concurrency via ThreadPoolExecutor is more effective.
        # time.sleep(0.02) # e.g., 20ms delay - currently commented out
        token_count = model.count_tokens(text).total_tokens
        return token_count
    except Exception as e:
        # Log any exceptions that occur during the API call for counting tokens.
        # This is crucial for diagnosing issues with the API connection or input format.
        logging.error(f"      💥 Error counting tokens ({type(e).__name__}): {e}. Text snippet: '{DIM}{text[:70]}...{RESET}'")
        return None # Return None to indicate a clear failure in counting


# --- Worker function for concurrent token counting ---
def _get_item_token_count(item_tuple, model):
    """
    A helper function designed to be run by a ThreadPoolExecutor.
    It takes an item tuple (original_index, item_text) and the model,
    calls count_tokens, and returns the result along with the original index.

    This allows token counting for multiple items to happen concurrently
    across different threads.

    Args:
        item_tuple: A tuple containing (original_index, item_text).
        model: An instance of google.generativeai.GenerativeModel.

    Returns:
        A tuple (original_index, item_text, token_count), where token_count
        is the result from count_tokens (int, 0, or None). Returns (index, text, None)
        if an unhandled exception occurs within the worker.
    """
    index, item_text = item_tuple
    try:
        # Call the main token counting function. count_tokens already handles API errors.
        token_count = count_tokens(model, item_text)
        # No extra sleep needed here; if count_tokens had internal retries or backoff,
        # that would be sufficient. Concurrency handles the load.
        return (index, item_text, token_count)
    except Exception as e:
        # This catches any *unexpected* exceptions that might occur within the worker
        # itself, although count_tokens is designed to catch API errors.
        logging.error(f"      ❌ Unhandled exception in token count worker for item index {index}: {e}")
        return (index, item_text, None) # Ensure failure is propagated


# --- Modified Chunking Function with Concurrent Pre-calculation ---
def chunk_items(items: list, model: genai.GenerativeModel, max_chunk_tokens: int, item_separator="\n\n---\n\n"):
    """
    Groups a list of item strings into chunks, ensuring each chunk (plus
    separators) stays within a specified token limit.

    It first concurrently pre-calculates token counts for all items using a
    ThreadPoolExecutor to speed up the process. Then, it sequentially groups
    the sized items into chunks.

    Args:
        items: A list of string items to be chunked (e.g., Reddit posts/comments).
        model: An instance of google.generativeai.GenerativeModel used for counting tokens.
        max_chunk_tokens: The maximum number of tokens allowed per chunk.
        item_separator: The string used to join items within a chunk. Its token
                        count is factored into chunk size calculation.

    Returns:
        A list of strings, where each string is a chunk containing one or more
        items joined by the separator. Returns an empty list if no valid chunks
        could be created.
    """
    logging.info(f"      🧩 Grouping {len(items)} items into chunks <= {max_chunk_tokens} tokens...")
    overall_start_time = time.time()

    # --- Filter empty/whitespace items initially ---
    # Create a list of items that are actually non-empty strings. Store their
    # original index for later sorting.
    initial_items_to_process = []
    items_skipped_empty_initial = 0
    for i, item_text in enumerate(items):
        if isinstance(item_text, str) and item_text.strip():
            initial_items_to_process.append((i, item_text)) # Store as (original_index, text)
        else:
            items_skipped_empty_initial += 1
            # Log skipped empty items if needed (can be verbose)
            # logging.debug(f"         Skipping empty/non-string item at original index {i}")

    # Log how many items were skipped initially due to being empty or not strings.
    if items_skipped_empty_initial > 0:
        logging.info(f"         (Skipped {items_skipped_empty_initial} empty/non-string items before token calculation)")

    # If no valid items remain, return an empty list immediately.
    if not initial_items_to_process:
        logging.warning("      ⚠️ No valid items left after initial filtering. Returning empty list.")
        return []

    # --- Concurrent Pre-calculation of Token Counts ---
    # Use a ThreadPoolExecutor to run _get_item_token_count for multiple items
    # simultaneously, up to MAX_CONCURRENT_TOKEN_CALLS workers.
    logging.info(f"      ⏳ Concurrently pre-calculating token counts for {len(initial_items_to_process)} items using up to {MAX_CONCURRENT_TOKEN_CALLS} workers...")
    precalc_start_time = time.time()

    # Dictionary to store the results of the token counting, keyed by original index.
    # This structure handles potential gaps if some items fail counting.
    sized_item_results = {} # Stores {original_index: {'text': ..., 'tokens': ...}}
    items_skipped_token_error_precalc = 0 # Counter for items that failed token counting

    # Use a context manager for ThreadPoolExecutor to ensure threads are properly shut down.
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TOKEN_CALLS) as executor:
        # Submit token counting tasks to the executor for each item.
        # Store a mapping from the Future object (representing the running task)
        # back to the original item tuple, useful for error reporting.
        future_to_item = {
            executor.submit(_get_item_token_count, item_tuple, model): item_tuple
            for item_tuple in initial_items_to_process
        }

        # Iterate over the completed futures as they finish using concurrent.futures.as_completed.
        # tqdm is wrapped around this to provide a progress bar in the terminal.
        for future in tqdm(concurrent.futures.as_completed(future_to_item), total=len(initial_items_to_process), desc="      Calculating token counts", unit="item"):
            try:
                # Retrieve the result from the completed future. This blocks until the result is available
                # but since we are iterating `as_completed`, it won't block unnecessarily.
                # The result is a tuple: (original_index, item_text, token_count or None)
                original_index, item_text, token_count = future.result()

                # Store the successful results in the dictionary.
                # We only care about valid, non-zero token counts for chunking.
                if token_count is not None and token_count > 0:
                    sized_item_results[original_index] = {'text': item_text, 'tokens': token_count}
                elif token_count == 0:
                     # Log items that resulted in 0 tokens after stripping (might indicate issues)
                     logging.warning(f"      ⚠️ Item index {original_index} resulted in 0 tokens. Skipping.")
                     items_skipped_token_error_precalc += 1 # Count this as skipped
                else: # token_count is None (error occurred during counting or in worker)
                    items_skipped_token_error_precalc += 1
                    # Error is already logged by count_tokens or the worker, no need to log again here.

            except Exception as exc:
                # This catches exceptions raised by `future.result()`, which typically means
                # an exception occurred *within* the worker function itself that wasn't caught.
                # The worker (_get_item_token_count) has a try/except, so this is a fallback.
                item_tuple = future_to_item[future]
                original_index = item_tuple[0]
                logging.error(f"      ❌ Exception retrieving result for item index {original_index}: {exc}")
                items_skipped_token_error_precalc += 1


    precalc_elapsed = time.time() - precalc_start_time # Time taken for concurrent token counting
    valid_items_count = len(sized_item_results) # Number of items for which we successfully got token counts
    logging.info(f"      ✅ Concurrent pre-calculation finished in {precalc_elapsed:.2f}s.")
    logging.info(f"         Successfully sized {valid_items_count} items.")

    # Report on items skipped during pre-calculation due to errors or zero tokens.
    if items_skipped_token_error_precalc > 0:
        logging.warning(f"         (Skipped {items_skipped_token_error_precalc} items due to token counting errors or zero tokens)")

    # If no items were successfully sized, we cannot chunk.
    if valid_items_count == 0:
        logging.error("      ❌ No items could be successfully sized. Cannot proceed with chunking.")
        return []

    # --- Prepare data for sequential grouping (ensure original order) ---
    # Create a list of the successfully sized item data, sorted by their original index.
    # This preserves the original order of items from the input list.
    valid_item_data_sorted = []
    # Iterate up to the original number of items to ensure we consider every possible index.
    for i in range(len(items)):
         if i in sized_item_results:
             valid_item_data_sorted.append(sized_item_results[i]) # Append the item info if it was successfully sized


    # --- Grouping based on pre-calculated counts (Fast Sequential Part) ---
    # Now that we have token counts, the grouping itself is a fast sequential process.
    logging.info(f"      ⚙️ Grouping {len(valid_item_data_sorted)} valid items into chunks...")
    grouping_start_time = time.time()

    chunks = [] # The final list of chunked strings
    current_chunk_items_text = [] # List to hold the text of items in the current chunk being built
    current_chunk_tokens = 0 # Token count of the items currently in current_chunk_items_text
    oversized_items_count = 0 # Counter for single items that are larger than the max chunk size

    # Count the tokens of the separator string once. This is less critical for performance
    # now that item counting is concurrent, but still avoids repeated counting.
    separator_tokens = count_tokens(model, item_separator)
    if separator_tokens is None:
        logging.warning("      ⚠️ Failed to count separator tokens, estimating as 5.")
        separator_tokens = 5 # Use a fallback estimate if counting fails
    # logging.debug(f"         Separator tokens estimated as: {separator_tokens}") # Log if needed

    # Iterate through the sorted list of sized items.
    for item_info in valid_item_data_sorted:
        item_text = item_info['text']
        item_token_count = item_info['tokens']
        # The original_index is implicit by the list order now.

        # Calculate the total tokens if this item were added to the current chunk.
        # If the chunk is currently empty, adding the first item doesn't require a separator.
        # Otherwise, adding an item means adding the item's tokens PLUS the separator's tokens.
        tokens_if_added = item_token_count + (separator_tokens if current_chunk_items_text else 0)

        # --- Handle items that are larger than the maximum chunk size by themselves ---
        if item_token_count > max_chunk_tokens:
            logging.warning(f"      ⚠️ Single item ({item_token_count} tokens) exceeds max chunk tokens ({max_chunk_tokens}). Adding as its own chunk.")
            oversized_items_count += 1
            # Before adding the oversized item as a new chunk, finalize any pending chunk
            # that was being built.
            if current_chunk_items_text:
                chunks.append(item_separator.join(current_chunk_items_text))
                # logging.debug(f"            Finalized previous chunk ({len(current_chunk_items_text)} items, {current_chunk_tokens} tk) before oversized item.")
            # Add the oversized item as a single-item chunk.
            chunks.append(item_text)
            # Reset the current chunk accumulation variables as we've started fresh after the oversized item.
            current_chunk_items_text = []
            current_chunk_tokens = 0
            continue # Move to the next item

        # --- Add item to the current chunk or start a new one ---
        # Check if adding the current item would exceed the max chunk token limit.
        if current_chunk_tokens + tokens_if_added <= max_chunk_tokens:
            # If it fits, add the item's text to the current chunk's item list
            current_chunk_items_text.append(item_text)
            # Update the current chunk's token count.
            current_chunk_tokens += tokens_if_added
        else:
            # If adding the item would exceed the limit, finalize the current chunk.
            # Join the items collected so far with the separator and add to the chunks list.
            if current_chunk_items_text:
                chunks.append(item_separator.join(current_chunk_items_text))
                # logging.debug(f"         Finalized chunk {len(chunks)} with {len(current_chunk_items_text)} items ({current_chunk_tokens} tokens).")

            # Start a new chunk with the current item.
            current_chunk_items_text = [item_text]
            # The token count for the *new* chunk starts with just this item's tokens,
            # as no separator is needed before the very first item in a chunk.
            current_chunk_tokens = item_token_count
            # logging.debug(f"         Started new chunk {len(chunks)+1} with item ({item_token_count} tokens).")

    # --- Add the very last chunk if it has items ---
    # After iterating through all items, the last chunk being built needs to be finalized and added.
    if current_chunk_items_text:
        chunks.append(item_separator.join(current_chunk_items_text))
        # logging.debug(f"         Finalized last chunk {len(chunks)} with {len(current_chunk_items_text)} items ({current_chunk_tokens} tokens).")

    grouping_elapsed = time.time() - grouping_start_time # Time taken for the sequential grouping
    total_elapsed = time.time() - overall_start_time # Total time for filtering, pre-calc, and grouping

    # --- Final Summary Logging ---
    # Report the results of the chunking process.
    logging.info(f"      ✅ Finished grouping into {BOLD}{len(chunks)}{RESET} chunks.")
    logging.info(f"         Time: Pre-calculation={precalc_elapsed:.2f}s (concurrent), Grouping={grouping_elapsed:.2f}s, Total={total_elapsed:.2f}s")
    if oversized_items_count > 0:
        logging.warning(f"         ({oversized_items_count} items were larger than max chunk size and placed in their own chunks)")
    # Calculate and log the number of items successfully grouped into standard chunks (excluding oversized ones).
    num_standard_chunks = len(chunks) - oversized_items_count
    num_items_in_standard_chunks = len(valid_item_data_sorted) - oversized_items_count
    if num_standard_chunks > 0: # Avoid logging if all items were oversized or zero sized
        logging.info(f"         Grouped {num_items_in_standard_chunks} items within {num_standard_chunks} standard chunks.")

    return chunks


# --- Main Analysis Function ---
def perform_ai_analysis(model, system_prompt, entries: list, output_file, chunk_size):
    """
    Performs AI analysis on a list of data entries (e.g., Reddit posts/comments).
    It estimates the total token count of the data and chunks it if necessary
    before sending it to the AI model for generation.

    Args:
        model: An instance of google.generativeai.GenerativeModel for analysis.
        system_prompt: The system prompt string to guide the AI's analysis.
        entries: A list of data entries (strings) to be analyzed.
        output_file: The file path where the analysis result should be saved.
        chunk_size: The maximum token size for each chunk of data sent to the model.

    Returns:
        True if the analysis process completed and saved successfully, False otherwise.
        Note: Success here means the process finished and saved a file; the file
              might contain error messages if generation failed for some chunks.
    """
    # Check if there are any entries to process.
    if not entries:
        logging.error("   ❌ No data entries provided for AI analysis. Aborting.")
        return False

    logging.info(f"   🤖 Performing AI analysis on {len(entries)} entries...")
    logging.debug(f"      Output file target: {CYAN}{output_file}{RESET}")
    logging.debug(f"      Chunk size target: {chunk_size} tokens")

    # Define the generation configuration for the AI model.
    # This includes parameters like temperature, top_p, top_k, and max output tokens.
    generation_config = genai.GenerationConfig(
        temperature=0.7, top_p=0.95, top_k=40, max_output_tokens=32768, # Example values
    )
    logging.debug(f"      Generation config: {generation_config}")

    # --- Determine Separator and Perform Fast Estimate ---
    # The separator used for joining items within a chunk.
    item_separator = "\n\n---\n\n"
    logging.debug(f"      Using item separator for joining/chunking: '{item_separator.encode('unicode_escape').decode()}'")

    logging.info(f"      🔢 Performing initial token count estimate for {len(entries)} entries...")
    estimate_start_time = time.time()

    # Prepare entries for a fast estimate by joining them all together.
    # Filter out non-string or empty entries first for a more accurate estimate.
    valid_entries_for_estimate = [str(e) for e in entries if isinstance(e, str) and e.strip()]
    if not valid_entries_for_estimate:
         logging.error("   ❌ No valid, non-empty entries found to perform token estimate.")
         return False
    full_text_estimate = item_separator.join(valid_entries_for_estimate)

    # Count tokens on the entire estimated text. This is a single API call.
    estimated_total_tokens = count_tokens(model, full_text_estimate)
    estimate_elapsed_time = time.time() - estimate_start_time

    # If the estimate failed, we cannot proceed.
    if estimated_total_tokens is None:
        logging.error("   ❌ Failed to perform initial token count estimate. Cannot proceed with analysis.")
        return False

    logging.info(f"      📊 Initial estimate: {BOLD}{estimated_total_tokens}{RESET} tokens ({estimate_elapsed_time:.2f}s). Target chunk size: {chunk_size}.")

    # --- Conditional Chunking Decision ---
    chunks_to_process = []
    # Determine if chunking is necessary based on the initial estimate vs. the chunk size.
    needs_chunking = estimated_total_tokens > chunk_size

    if needs_chunking:
        logging.warning(f"      ⚠️ Initial estimate ({estimated_total_tokens}) exceeds target ({chunk_size}). {BOLD}Detailed chunking required...{RESET}")
        # If chunking is needed, call the `chunk_items` function to perform the detailed
        # concurrent token counting and sequential grouping.
        chunks_to_process = chunk_items(entries, model, chunk_size, item_separator=item_separator)

        # Check the result of the chunking process.
        if chunks_to_process is None: # Should not happen with current logic, but defensive check
            logging.error("   ❌ Chunking function returned None unexpectedly. Aborting analysis.")
            return False
        if not chunks_to_process: # Check if chunking resulted in an empty list
             logging.error("   ❌ Chunking process resulted in an empty list of chunks (all items may have been skipped or failed). Cannot proceed.")
             return False
    else:
        # If the initial estimate fits, process the data as a single chunk.
        logging.info(f"      ✅ Initial estimate fits within target. Processing as a single chunk.")
        # Use the estimated full text as the single chunk.
        chunks_to_process = [full_text_estimate]

    # --- Process the Determined Chunks ---
    total_chunks = len(chunks_to_process)
    final_result = "" # Variable to store the final combined analysis text
    all_chunk_results = [] # List to store results for each chunk when processing multiple
    errors_occurred = False # Flag to track if any errors happened during generation
    api_calls_made = 0 # Counter for the number of API calls made for generation

    logging.info(f"   🚀 Processing {BOLD}{total_chunks}{RESET} chunk(s) for AI generation...")
    processing_start_time = time.time() # Start timer for the generation phase

    # --- [ SINGLE CHUNK PROCESSING LOGIC ] ---
    # If there's only one chunk (either originally or after chunking resulted in one),
    # send the entire prompt with the data as a single API call.
    if total_chunks == 1:
        logging.info(f"      Generating analysis for the single data chunk...")
        # Ensure system prompt is a string.
        if not isinstance(system_prompt, str): system_prompt = str(system_prompt)
        # Construct the full prompt for the single chunk.
        full_prompt = f"{system_prompt}\n\n--- START OF REDDIT DATA ---\n\n{chunks_to_process[0]}\n\n--- END OF REDDIT DATA ---\n\n{BOLD}Analysis:{RESET}"
        # Count tokens for the final prompt before sending (good practice).
        prompt_token_count = count_tokens(model, full_prompt) # Still need single calls here
        logging.debug(f"         Single chunk prompt token count: {prompt_token_count}")

        try:
            # Make the API call to generate content for the single chunk.
            api_calls_made += 1
            response = model.generate_content(contents=full_prompt, generation_config=generation_config)

            # Process the API response.
            if hasattr(response, 'text') and response.text is not None:
                # If successful, store the generated text.
                final_result = response.text
                logging.info(f"      ✅ Successfully received analysis for single chunk.")
            # Handle cases where the prompt was blocked by safety filters.
            elif hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                 block_reason = response.prompt_feedback.block_reason or "Unknown"
                 safety_ratings_str = str(getattr(response.prompt_feedback, 'safety_ratings', 'N/A'))
                 logging.error(f"      ❌ Analysis failed/blocked for single chunk. Reason: {block_reason}. Ratings: {safety_ratings_str}")
                 # Add an error message to the result to indicate failure.
                 final_result = f"[ERROR: Analysis generation failed or was blocked. Reason: {block_reason}. Safety Ratings: {safety_ratings_str}]"
                 errors_occurred = True # Set the error flag
            else:
                 # Handle cases where the response is unexpected (no text and no block reason).
                 response_str = str(response)[:200] if response else "None" # Log a snippet of the response
                 logging.error(f"      ❌ Analysis failed: No text content or block reason in response. Response snippet: {response_str}...")
                 final_result = f"[ERROR: Analysis generation failed. No text content received. Response: {response_str}...]"
                 errors_occurred = True # Set the error flag
        except Exception as e:
            # Catch any other exceptions during the API call (network issues, invalid parameters, etc.)
            logging.error(f"      🔥 Error generating content for single chunk: {e}", exc_info=True) # Log the full traceback
            final_result = f"[ERROR: Exception during analysis generation: {type(e).__name__} - {e}]" # Add an error message to the result
            errors_occurred = True # Set the error flag

    # --- [ MULTIPLE CHUNK PROCESSING LOGIC ] ---
    # If there are multiple chunks, process each chunk sequentially.
    else:
        logging.info(f"      Generating analysis chunk by chunk ({total_chunks} chunks)...")
        # Iterate through each generated chunk.
        for i, chunk_text in enumerate(chunks_to_process):
            chunk_start_time = time.time() # Timer for the current chunk
            logging.info(f"      Processing chunk {BOLD}{i+1}/{total_chunks}{RESET}...")
            # Ensure system prompt is a string.
            if not isinstance(system_prompt, str): system_prompt = str(system_prompt)
            # Construct the prompt for the current chunk, explicitly mentioning its index.
            prompt_for_chunk = (
                f"{system_prompt}\n\n"
                f"--- START OF DATA CHUNK {i+1}/{total_chunks} ---\n\n"
                f"{chunk_text}\n\n"
                f"--- END OF DATA CHUNK {i+1}/{total_chunks} ---\n\n"
                f"Provide your analysis based *only* on the data provided in this chunk (Chunk {i+1}/{total_chunks}). "
                f"Focus on the content within the 'START' and 'END' delimiters for this specific chunk.\n\n"
                f"{BOLD}Analysis for Chunk {i+1}:{RESET}" # Explicitly ask for analysis labelled for this chunk
            )
            # Count tokens for the chunk prompt.
            prompt_token_count = count_tokens(model, prompt_for_chunk) # Still need single calls here
            logging.debug(f"         Chunk {i+1} prompt token count: {prompt_token_count}")

            try:
                # Make the API call for the current chunk.
                api_calls_made += 1
                response = model.generate_content(contents=prompt_for_chunk, generation_config=generation_config)

                # Process the API response for the chunk.
                if hasattr(response, 'text') and response.text is not None:
                    # If successful, append the result to the list of chunk results.
                    chunk_result = response.text
                    all_chunk_results.append(chunk_result)
                    elapsed = time.time() - chunk_start_time # Time taken for this chunk
                    logging.info(f"      ✅ Successfully received analysis for chunk {i+1} ({elapsed:.2f}s).")
                    # Add a small delay between chunk calls to respect potential rate limits.
                    if i < total_chunks - 1:
                        delay = 1.0
                        logging.debug(f"         😴 Delaying {delay:.1f}s before next chunk...")
                        time.sleep(delay)
                # Handle blocked chunks.
                elif hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                     block_reason = response.prompt_feedback.block_reason or "Unknown"
                     safety_ratings_str = str(getattr(response.prompt_feedback, 'safety_ratings', 'N/A'))
                     logging.error(f"      ❌ Analysis failed/blocked for chunk {i+1}. Reason: {block_reason}. Ratings: {safety_ratings_str}")
                     # Add an error message result for this chunk.
                     all_chunk_results.append(f"\n\n[ERROR PROCESSING CHUNK {i+1}/{total_chunks}: Blocked - {block_reason}. Safety Ratings: {safety_ratings_str}]\n\n")
                     errors_occurred = True # Set the error flag
                     time.sleep(2) # Add a longer delay after an error
                else:
                    # Handle unexpected empty response for a chunk.
                    response_str = str(response)[:200] if response else "None"
                    logging.error(f"      ❌ Analysis failed for chunk {i+1}: No text content or block reason. Response: {response_str}...")
                    all_chunk_results.append(f"\n\n[ERROR PROCESSING CHUNK {i+1}/{total_chunks}: No text content received. Response: {response_str}...]\n\n")
                    errors_occurred = True # Set the error flag
                    time.sleep(2) # Add a longer delay after an error
            except Exception as e:
                # Catch exceptions during the API call for a specific chunk.
                logging.error(f"      🔥 Error generating content for chunk {i+1}: {e}", exc_info=True)
                all_chunk_results.append(f"\n\n[EXCEPTION PROCESSING CHUNK {i+1}/{total_chunks}: {type(e).__name__} - {e}]\n\n") # Add an error message result
                errors_occurred = True # Set the error flag
                time.sleep(5) # Add a longer delay after an exception

        logging.info("      ✍️ Combining results from multiple chunks...")
        # Combine the results from all chunks into a single final string.
        final_result = f"--- AI ANALYSIS BASED ON {total_chunks} CHUNK(S) ---\n\n"
        for i, result in enumerate(all_chunk_results):
            final_result += f"---------- ANALYSIS FOR CHUNK {i+1}/{total_chunks} ----------\n\n{result}\n\n"
        final_result += f"--- END OF COMBINED ANALYSIS ({total_chunks} CHUNKS) ---"

    processing_elapsed = time.time() - processing_start_time # Total time for the generation phase
    logging.info(f"   🏁 Finished AI generation ({api_calls_made} API calls) for {total_chunks} chunk(s) ({processing_elapsed:.2f}s).")

    # --- [ SAVE RESULT LOGIC ] ---
    # Save the final combined analysis result to the specified output file.
    logging.info(f"   💾 Saving analysis result to {CYAN}{output_file}{RESET}...")
    try:
        # Ensure the directory for the output file exists.
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        # Write the final result to the file, using UTF-8 encoding.
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(final_result)
        logging.info(f"   ✅ Analysis saved successfully.")

        # Return True if the process finished and saved a file, but note if errors occurred during generation.
        if errors_occurred:
             logging.warning(f"   ⚠️ Analysis completed, but output file {CYAN}{output_file}{RESET} contains one or more error messages from generation.")
             # Returning False here indicates that *something* went wrong during the AI processing,
             # even if a file was saved.
             return False
        return True # Indicate overall success if no generation errors occurred
    except IOError as e:
        # Catch and log errors related to file saving.
        logging.error(f"   ❌ Error saving analysis to {CYAN}{output_file}{RESET}: {e}")
        return False # Indicate failure
    except Exception as e:
        # Catch any other unexpected errors during the save process.
        logging.error(f"   ❌ An unexpected error occurred while saving analysis to {CYAN}{output_file}{RESET}: {e}")
        return False # Indicate failure


# --- Interactive Prompt Generation ---
def generate_prompt_interactive(model, prompt_dir):
    """
    Runs an interactive chat session with the AI model to help the user
    create and save a system prompt for future analysis tasks.

    The session allows the user to describe their desired analysis, get
    clarifying questions from the AI, refine the prompt, and finally save it.

    Args:
        model: An instance of google.generativeai.GenerativeModel to act as
               the prompt generation assistant.
        prompt_dir: The directory path where generated prompts should be saved.
    """
    logging.info(f"🖊️ Starting interactive prompt generation...")
    # Ensure the directory for prompts exists.
    os.makedirs(prompt_dir, exist_ok=True)

    # Print introductory messages for the user.
    print(f"\n--- {BOLD}{MAGENTA}Prompt Generation Assistant{RESET} ---")
    print("Describe the kind of analysis or profile you want to create.")
    print("The assistant will ask clarifying questions.")
    print(f"Type '{BOLD}SAVE{RESET}' when you are satisfied with the generated prompt.")
    print(f"Type '{BOLD}QUIT{RESET}' to exit without saving.")
    print("-------------------------------------\n")

    try:
        # Initialize the chat session with a specific system message for the AI assistant.
        # This message instructs the AI on how to behave during the prompt creation process.
        chat_history = [
             {'role':'user', 'parts': ["""
You are an AI assistant specialized in helping users craft effective *system prompts* for analyzing Reddit user data (posts/comments) with large language models like Gemini.
Your primary goal is collaborative prompt engineering. Understand the user's desired analysis (e.g., personality traits, interests, writing style, sentiment, specific topics, potential biases, toxicity level, argument patterns, helpfulness, etc.).
Ask targeted, clarifying questions to refine these requirements. Suggest potential analysis angles if the user is unsure.
When the user types 'SAVE' or indicates satisfaction, synthesize the conversation into a final, well-structured system prompt.
Crucially, output *only the final prompt text itself* when asked to save – no extra conversational text, explanations, markdown formatting like ```, or apologies. Just the raw prompt.
Keep your conversational turns concise and focused on prompt creation. Use Markdown for clarity in your *conversational* responses (like lists or bolding) but NOT in the final saved prompt.
Before saving, present the draft prompt clearly for confirmation.
If generation fails or is blocked, explain why briefly and allow the user to continue or quit.
"""]},
             {'role':'model', 'parts': ["Okay, I understand. I'm ready to help you create a system prompt for analyzing Reddit data. What kind of insights are you hoping to gain from the user's activity?"]}
        ]
        # Start the chat session with the initial history.
        chat = model.start_chat(history=chat_history)

        # Main loop for the interactive chat session.
        while True:
            try:
                # Get user input.
                user_input = input(f"{BOLD}You:{RESET} ")
            except EOFError:
                 # Handle end-of-file (e.g., user pressing Ctrl+D).
                 print(f"\n{YELLOW}Exiting prompt generation (EOF detected).{RESET}")
                 return # Exit the function
            if not user_input: # Ignore empty inputs
                continue

            user_input_upper = user_input.strip().upper() # Get uppercase input for commands

            # --- Handle user commands ---
            if user_input_upper == "QUIT":
                # If user types 'QUIT', exit the loop and function.
                print(f"\n{YELLOW}Exiting prompt generation.{RESET}")
                return
            elif user_input_upper == "SAVE":
                # If user types 'SAVE', attempt to generate the final prompt.
                print(f"\n{BOLD}Assistant:{RESET} {DIM}Okay, synthesizing the final prompt based on our discussion...{RESET}")
                try:
                     # Craft a specific request to the AI to output *only* the final prompt.
                     # This is a meta-prompt sent *to* the chat model *about* the chat history.
                     final_prompt_request = "Generate the final system prompt based *only* on our conversation history about the desired analysis. Output *nothing* but the raw text of the system prompt itself."

                     # Include the entire chat history plus the final prompt request in the content for the API call.
                     request_content = chat.history + [{'role':'user', 'parts': [final_prompt_request]}]

                     # Configure safety settings to allow more flexibility during prompt generation,
                     # assuming the *output* prompt is checked later.
                     safety_settings_gen = {'HARM_CATEGORY_HARASSMENT': 'BLOCK_NONE',
                                            'HARM_CATEGORY_HATE_SPEECH': 'BLOCK_NONE',
                                            'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'BLOCK_NONE',
                                            'HARM_CATEGORY_DANGEROUS_CONTENT': 'BLOCK_NONE'}
                     # Use a lower temperature for prompt generation to make it less creative and more direct.
                     generation_config_gen = genai.GenerationConfig(temperature=0.3)

                     # Send the request to the model.
                     response = model.generate_content(request_content,
                                                       generation_config=generation_config_gen,
                                                       safety_settings=safety_settings_gen)

                     # Process the response to get the final prompt text.
                     final_prompt = None
                     if hasattr(response, 'text') and response.text and response.text.strip():
                         final_prompt = response.text.strip()
                         # Clean up potential markdown code blocks if the AI mistakenly adds them.
                         if final_prompt.startswith("```") and final_prompt.endswith("```"):
                              final_prompt = re.sub(r'^```[a-zA-Z]*\n?', '', final_prompt) # Remove leading ```(lang)
                              final_prompt = re.sub(r'\n?```$', '', final_prompt) # Remove trailing ```
                              final_prompt = final_prompt.strip() # Final strip

                     # If no prompt text was generated, report the error.
                     if not final_prompt:
                          block_reason_str = "Unknown reason"
                          if hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                              block_reason_str = response.prompt_feedback.block_reason
                          logging.error(f"Failed to generate final prompt text. Block Reason: {block_reason_str}. Response: {response}")
                          print(f"{RED}Assistant:{RESET} I couldn't generate a final prompt text (Reason: {block_reason_str}). Please try describing your needs more or type '{BOLD}QUIT{RESET}'.")
                          continue # Go back to the start of the chat loop

                     # --- Present and save the generated prompt ---
                     print(f"\n--- {BOLD}{GREEN}Generated System Prompt{RESET} ---")
                     print(f"{CYAN}{final_prompt}{RESET}") # Display the prompt clearly
                     print("------------------------------")

                     # Ask the user to confirm saving, discard, or refine.
                     while True:
                          confirm = input(f"Save this prompt? ({GREEN}yes{RESET}/{RED}no{RESET}/{YELLOW}refine{RESET}): ").strip().lower()
                          if confirm in ['yes', 'y', 'no', 'n', 'refine', 'r']: break
                          else: print(f"{YELLOW}Please enter 'yes', 'no', or 'refine'.{RESET}")

                     if confirm in ['yes', 'y']:
                         # If user confirms saving, ask for a filename.
                         while True:
                             prompt_name = input(f"Enter a filename for this prompt (e.g., 'personality_v1'): ").strip()
                             if not prompt_name: print(f"{YELLOW}Filename cannot be empty.{RESET}"); continue
                             # Validate filename to prevent issues (basic check).
                             if not re.match(r'^[\w\-. ]+$', prompt_name): print(f"{YELLOW}Invalid filename. Use letters, numbers, hyphens, underscores, dots, spaces.{RESET}"); continue

                             # Construct the full file path.
                             filename = os.path.join(prompt_dir, f"{prompt_name}.txt")
                             # Check if the file already exists and ask for overwrite confirmation.
                             if os.path.exists(filename):
                                overwrite = input(f"File '{CYAN}{filename}{RESET}' already exists. Overwrite? ({GREEN}yes{RESET}/{RED}no{RESET}): ").strip().lower()
                                if overwrite not in ['yes', 'y']: continue # If not confirmed, ask for a new name

                             try:
                                 # Save the prompt to the file.
                                 with open(filename, "w", encoding="utf-8") as f: f.write(final_prompt)
                                 print(f"✅ {BOLD}Prompt saved to {CYAN}{filename}{RESET}")
                                 return # Exit the function after successful save
                             except IOError as e:
                                  # Handle file saving errors.
                                  print(f"❌ {RED}Error saving prompt:{RESET} {e}")
                                  try_again = input("Try saving again with a different name? (yes/no): ").strip().lower()
                                  if try_again not in ['yes', 'y']: return # Exit if user doesn't want to try again
                             except Exception as e:
                                  # Catch any other unexpected errors during saving.
                                  print(f"❌ {RED}An unexpected error occurred during saving:{RESET} {e}"); return # Exit on unexpected error
                     elif confirm in ['no', 'n']:
                         # If user chooses not to save, inform them and continue chat.
                         print(f"{YELLOW}Prompt not saved. You can continue refining or type '{BOLD}QUIT{RESET}'.{RESET}")
                         # Add a note to the chat history about the discarded prompt to help the AI
                         # understand the flow if the user continues.
                         chat.history.append({'role':'model', 'parts': [f"(Generated draft prompt, user chose not to save):\n{final_prompt}"]})
                         chat.history.append({'role':'user', 'parts': ["No, I don't want to save that one."]})
                     else: # Refine
                         # If user chooses to refine, inform them and continue chat.
                         print(f"{YELLOW}Okay, let's continue refining. What would you like to change or add?{RESET}")
                         # Add a note to the chat history about the prompt needing refinement.
                         chat.history.append({'role':'model', 'parts': [f"(Generated draft prompt, user wants to refine):\n{final_prompt}"]})
                         chat.history.append({'role':'user', 'parts': ["I want to refine the prompt."]})

                except Exception as e:
                    # Catch exceptions during the final prompt generation phase.
                    print(f"\n{RED}Assistant:{RESET} An error occurred generating the final prompt: {e}")
                    logging.error(f"Error during final prompt generation: {e}", exc_info=True)

            # --- Handle regular chat turns ---
            else:
                # If not a command, send the user's input to the chat model as a regular turn.
                try:
                    print(f"{BOLD}Assistant:{RESET} {DIM}Thinking...{RESET}", end='\r') # Show a thinking indicator
                    response = chat.send_message(user_input) # Send the message
                    print(" " * 25, end='\r') # Clear the thinking message

                    # Process the AI's response.
                    if hasattr(response, 'text') and response.text:
                         # If text is received, print it.
                         print(f"\n{BOLD}Assistant:{RESET} {response.text}\n")
                    elif hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                         # If the response was blocked, inform the user.
                         block_reason_conv = response.prompt_feedback.block_reason
                         print(f"\n{RED}Assistant:{RESET} My response was blocked (Reason: {block_reason_conv}). Could you please rephrase or ask differently?\n")
                    else:
                         # Handle unexpected empty responses.
                         print(f"\n{RED}Assistant:{RESET} I received an empty response. Please try again.\n")
                         logging.warning(f"Empty response received during chat: {response}")

                except Exception as e:
                    # Catch any exceptions during sending/receiving chat messages.
                    print(f"\n{RED}Assistant:{RESET} An error occurred processing your input: {e}")
                    logging.error(f"Error sending message to chat model: {e}", exc_info=True)
                    time.sleep(1) # Add a small delay after an error before the next input

    except Exception as e:
        # Catch critical exceptions that prevent the chat from even starting.
        logging.critical(f"Failed to initialize interactive prompt generation: {e}", exc_info=True)
        print(f"\n{RED}An error prevented the prompt generation assistant from starting:{RESET} {e}")