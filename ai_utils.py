# ai_utils.py
import logging
import google.generativeai as genai
import time
import os
import re
import concurrent.futures # Added for ThreadPoolExecutor
from tqdm import tqdm # Added for progress bar

# Import ANSI codes
CYAN = "\033[36m"; RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"; MAGENTA = "\033[35m"; YELLOW = "\033[33m"; GREEN = "\033[32m"; RED = "\033[31m"

# --- Configuration ---
# Adjust this number based on testing and potential API rate limits
# Start lower (e.g., 5 or 10) and increase if stable.
MAX_CONCURRENT_TOKEN_CALLS = 10


# --- Core AI Functions ---

def count_tokens(model, text):
    """Counts the number of tokens in a given text using the provided model."""
    if not isinstance(text, str):
        # Logged as warning, not error, as it might be expected input sometimes
        logging.warning(f"      ⚠️ Cannot count tokens on non-string input: type {type(text)}")
        return 0 # Return 0 for non-strings
    if not text.strip(): # Check for empty or whitespace-only strings
        return 0 # Return 0 for empty strings
    try:
        # Add a small delay *per call* if rate limits are suspected even with concurrency
        # time.sleep(0.02) # e.g., 20ms delay
        token_count = model.count_tokens(text).total_tokens
        return token_count
    except Exception as e:
        # Log token counting errors more prominently during the call
        logging.error(f"      💥 Error counting tokens ({type(e).__name__}): {e}. Text snippet: '{DIM}{text[:70]}...{RESET}'")
        return None # Return None to indicate failure clearly

# --- Worker function for concurrent token counting ---
def _get_item_token_count(item_tuple, model):
    """
    Worker function for ThreadPoolExecutor.
    Takes a tuple (index, item_text) and the model.
    Returns (index, item_text, token_count or None).
    """
    index, item_text = item_tuple
    try:
        token_count = count_tokens(model, item_text)
        # No need for extra sleep here unless count_tokens itself doesn't handle retries/backoff
        return (index, item_text, token_count)
    except Exception as e:
        # Log error specific to this item within the worker context
        logging.error(f"      ❌ Unhandled exception in token count worker for item index {index}: {e}")
        return (index, item_text, None) # Ensure failure is propagated


# --- Modified Chunking Function with Concurrent Pre-calculation ---
def chunk_items(items: list, model: genai.GenerativeModel, max_chunk_tokens: int, item_separator="\n\n---\n\n"):
    """
    Groups a list of item strings into chunks based on token limits,
    using concurrent pre-calculation for token counts.
    """
    logging.info(f"      🧩 Grouping {len(items)} items into chunks <= {max_chunk_tokens} tokens...")
    overall_start_time = time.time()

    # --- Filter empty/whitespace items initially ---
    initial_items_to_process = []
    items_skipped_empty_initial = 0
    for i, item_text in enumerate(items):
        if isinstance(item_text, str) and item_text.strip():
            initial_items_to_process.append((i, item_text)) # Store as (original_index, text)
        else:
            items_skipped_empty_initial += 1
            # Log skipped empty items if needed (can be verbose)
            # logging.debug(f"         Skipping empty/non-string item at original index {i}")

    if items_skipped_empty_initial > 0:
        logging.info(f"         (Skipped {items_skipped_empty_initial} empty/non-string items before token calculation)")

    if not initial_items_to_process:
        logging.warning("      ⚠️ No valid items left after initial filtering. Returning empty list.")
        return []

    # --- Concurrent Pre-calculation of Token Counts ---
    logging.info(f"      ⏳ Concurrently pre-calculating token counts for {len(initial_items_to_process)} items using up to {MAX_CONCURRENT_TOKEN_CALLS} workers...")
    precalc_start_time = time.time()
    # Pre-allocate list to store results in original order if needed, or use dict
    # Using a dictionary is often easier for sparse results
    sized_item_results = {} # Stores {original_index: {'text': ..., 'tokens': ...}}
    items_skipped_token_error_precalc = 0
    processed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TOKEN_CALLS) as executor:
        # Create futures, passing the model to the worker
        future_to_item = {
            executor.submit(_get_item_token_count, item_tuple, model): item_tuple
            for item_tuple in initial_items_to_process
        }

        # Process futures as they complete with a progress bar
        for future in tqdm(concurrent.futures.as_completed(future_to_item), total=len(initial_items_to_process), desc="      Calculating token counts", unit="item"):
            try:
                original_index, item_text, token_count = future.result()
                processed_count += 1

                if token_count is not None and token_count > 0: # Store only valid, non-zero counts
                    sized_item_results[original_index] = {'text': item_text, 'tokens': token_count}
                elif token_count == 0:
                     # Log items that resulted in 0 tokens (might indicate issues)
                     logging.warning(f"      ⚠️ Item index {original_index} resulted in 0 tokens. Skipping.")
                     items_skipped_token_error_precalc += 1 # Count as skipped
                else: # token_count is None (error occurred)
                    items_skipped_token_error_precalc += 1
                    # Error is already logged by count_tokens or the worker

            except Exception as exc:
                # Catch potential errors from future.result() itself, though worker should handle most
                item_tuple = future_to_item[future]
                original_index = item_tuple[0]
                logging.error(f"      ❌ Exception retrieving result for item index {original_index}: {exc}")
                items_skipped_token_error_precalc += 1


    precalc_elapsed = time.time() - precalc_start_time
    valid_items_count = len(sized_item_results)
    logging.info(f"      ✅ Concurrent pre-calculation finished in {precalc_elapsed:.2f}s.")
    logging.info(f"         Successfully sized {valid_items_count} items.")

    if items_skipped_token_error_precalc > 0:
        logging.warning(f"         (Skipped {items_skipped_token_error_precalc} items due to token counting errors or zero tokens)")

    if valid_items_count == 0:
        logging.error("      ❌ No items could be successfully sized. Cannot proceed with chunking.")
        return []

    # --- Prepare data for sequential grouping (ensure original order) ---
    # Sort the valid results by their original index to maintain order
    valid_item_data_sorted = []
    for i in range(len(items)): # Iterate up to the original number of items
         if i in sized_item_results:
             valid_item_data_sorted.append(sized_item_results[i]) # Append if successfully sized


    # --- Grouping based on pre-calculated counts (Fast Sequential Part) ---
    logging.info(f"      ⚙️ Grouping {len(valid_item_data_sorted)} valid items into chunks...")
    grouping_start_time = time.time()
    chunks = []
    current_chunk_items_text = [] # Store the text of items in the current chunk
    current_chunk_tokens = 0
    oversized_items_count = 0

    # Count separator tokens once - less critical now, but good practice
    separator_tokens = count_tokens(model, item_separator)
    if separator_tokens is None:
        logging.warning("      ⚠️ Failed to count separator tokens, estimating as 5.")
        separator_tokens = 5
    # Only log if needed: logging.debug(f"         Separator tokens estimated as: {separator_tokens}")


    for item_info in valid_item_data_sorted: # Iterate through sorted, valid items
        item_text = item_info['text']
        item_token_count = item_info['tokens']
        # original_index is implicitly handled by list order now

        # Calculate tokens needed if this item is added
        tokens_if_added = item_token_count + (separator_tokens if current_chunk_items_text else 0)

        # Handle items intrinsically too large
        if item_token_count > max_chunk_tokens:
            logging.warning(f"      ⚠️ Single item ({item_token_count} tokens) exceeds max chunk tokens ({max_chunk_tokens}). Adding as its own chunk.")
            # Logging original index might be harder here unless stored in item_info
            oversized_items_count += 1
            # Finalize the previous chunk
            if current_chunk_items_text:
                chunks.append(item_separator.join(current_chunk_items_text))
                # logging.debug(f"            Finalized previous chunk ({len(current_chunk_items_text)} items, {current_chunk_tokens} tk) before oversized item.")
            # Add the oversized item as its own chunk
            chunks.append(item_text)
            current_chunk_items_text = []
            current_chunk_tokens = 0
            continue

        # Add item to the current chunk or start a new one
        if current_chunk_tokens + tokens_if_added <= max_chunk_tokens:
            current_chunk_items_text.append(item_text)
            current_chunk_tokens += tokens_if_added
        else:
            # Finalize the current chunk
            if current_chunk_items_text:
                chunks.append(item_separator.join(current_chunk_items_text))
                # logging.debug(f"         Finalized chunk {len(chunks)} with {len(current_chunk_items_text)} items ({current_chunk_tokens} tokens).")

            # Start a new chunk with the current item
            current_chunk_items_text = [item_text]
            current_chunk_tokens = item_token_count # First item doesn't add separator tokens yet
            # logging.debug(f"         Started new chunk {len(chunks)+1} with item ({item_token_count} tokens).")

    # Add the very last chunk if it has items
    if current_chunk_items_text:
        chunks.append(item_separator.join(current_chunk_items_text))
        # logging.debug(f"         Finalized last chunk {len(chunks)} with {len(current_chunk_items_text)} items ({current_chunk_tokens} tokens).")

    grouping_elapsed = time.time() - grouping_start_time
    total_elapsed = time.time() - overall_start_time

    # --- Final Summary Logging ---
    logging.info(f"      ✅ Finished grouping into {BOLD}{len(chunks)}{RESET} chunks.")
    logging.info(f"         Time: Pre-calculation={precalc_elapsed:.2f}s (concurrent), Grouping={grouping_elapsed:.2f}s, Total={total_elapsed:.2f}s")
    if oversized_items_count > 0:
        logging.warning(f"         ({oversized_items_count} items were larger than max chunk size and placed in their own chunks)")
    # Log total valid items processed in grouping stage
    logging.info(f"         Grouped {len(valid_item_data_sorted) - oversized_items_count} items within {len(chunks) - oversized_items_count} standard chunks.")

    return chunks


# --- Main Analysis Function ---
# No changes needed in perform_ai_analysis itself, as it correctly calls the modified chunk_items
def perform_ai_analysis(model, system_prompt, entries: list, output_file, chunk_size):
    """Performs AI analysis on entries, chunking ONLY IF necessary."""
    if not entries:
        logging.error("   ❌ No data entries provided for AI analysis. Aborting.")
        return False

    logging.info(f"   🤖 Performing AI analysis on {len(entries)} entries...")
    logging.debug(f"      Output file target: {CYAN}{output_file}{RESET}")
    logging.debug(f"      Chunk size target: {chunk_size} tokens")

    generation_config = genai.GenerationConfig(
        temperature=0.7, top_p=0.95, top_k=40, max_output_tokens=32768,
    )
    logging.debug(f"      Generation config: {generation_config}")

    # --- Determine Separator and Perform Fast Estimate ---
    item_separator = "\n\n---\n\n"
    logging.debug(f"      Using item separator for joining/chunking: '{item_separator.encode('unicode_escape').decode()}'")

    logging.info(f"      🔢 Performing initial token count estimate for {len(entries)} entries...")
    estimate_start_time = time.time()
    valid_entries_for_estimate = [str(e) for e in entries if isinstance(e, str) and e.strip()]
    if not valid_entries_for_estimate:
         logging.error("   ❌ No valid, non-empty entries found to perform token estimate.")
         return False
    full_text_estimate = item_separator.join(valid_entries_for_estimate)

    estimated_total_tokens = count_tokens(model, full_text_estimate)
    estimate_elapsed_time = time.time() - estimate_start_time

    if estimated_total_tokens is None:
        logging.error("   ❌ Failed to perform initial token count estimate. Cannot proceed with analysis.")
        return False

    logging.info(f"      📊 Initial estimate: {BOLD}{estimated_total_tokens}{RESET} tokens ({estimate_elapsed_time:.2f}s). Target chunk size: {chunk_size}.")

    # --- Conditional Chunking Decision ---
    chunks_to_process = []
    needs_chunking = estimated_total_tokens > chunk_size

    if needs_chunking:
        logging.warning(f"      ⚠️ Initial estimate ({estimated_total_tokens}) exceeds target ({chunk_size}). {BOLD}Detailed chunking required...{RESET}")
        # *** Call the NEW concurrent chunk_items ***
        chunks_to_process = chunk_items(entries, model, chunk_size, item_separator=item_separator)

        if chunks_to_process is None: # Defensive check
            logging.error("   ❌ Chunking function returned None unexpectedly. Aborting analysis.")
            return False
        if not chunks_to_process:
             logging.error("   ❌ Chunking process resulted in an empty list of chunks (all items may have been skipped or failed). Cannot proceed.")
             return False
    else:
        logging.info(f"      ✅ Initial estimate fits within target. Processing as a single chunk.")
        chunks_to_process = [full_text_estimate]

    # --- Process the Determined Chunks ---
    total_chunks = len(chunks_to_process)
    final_result = ""
    all_chunk_results = []
    errors_occurred = False
    api_calls_made = 0

    logging.info(f"   🚀 Processing {BOLD}{total_chunks}{RESET} chunk(s) for AI generation...")
    processing_start_time = time.time()

    # --- [ SINGLE CHUNK PROCESSING LOGIC - UNCHANGED ] ---
    if total_chunks == 1:
        logging.info(f"      Generating analysis for the single data chunk...")
        if not isinstance(system_prompt, str): system_prompt = str(system_prompt)
        full_prompt = f"{system_prompt}\n\n--- START OF REDDIT DATA ---\n\n{chunks_to_process[0]}\n\n--- END OF REDDIT DATA ---\n\n{BOLD}Analysis:{RESET}"
        prompt_token_count = count_tokens(model, full_prompt) # Still need single calls here
        logging.debug(f"         Single chunk prompt token count: {prompt_token_count}")

        try:
            api_calls_made += 1
            response = model.generate_content(contents=full_prompt, generation_config=generation_config)
            if hasattr(response, 'text') and response.text is not None:
                final_result = response.text
                logging.info(f"      ✅ Successfully received analysis for single chunk.")
            elif hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                 block_reason = response.prompt_feedback.block_reason or "Unknown"
                 safety_ratings_str = str(getattr(response.prompt_feedback, 'safety_ratings', 'N/A'))
                 logging.error(f"      ❌ Analysis failed/blocked for single chunk. Reason: {block_reason}. Ratings: {safety_ratings_str}")
                 final_result = f"[ERROR: Analysis generation failed or was blocked. Reason: {block_reason}. Safety Ratings: {safety_ratings_str}]"
                 errors_occurred = True
            else:
                 response_str = str(response)[:200] if response else "None"
                 logging.error(f"      ❌ Analysis failed: No text content or block reason in response. Response snippet: {response_str}...")
                 final_result = f"[ERROR: Analysis generation failed. No text content received. Response: {response_str}...]"
                 errors_occurred = True
        except Exception as e:
            logging.error(f"      🔥 Error generating content for single chunk: {e}", exc_info=True)
            final_result = f"[ERROR: Exception during analysis generation: {type(e).__name__} - {e}]"
            errors_occurred = True

    # --- [ MULTIPLE CHUNK PROCESSING LOGIC - UNCHANGED ] ---
    else:
        logging.info(f"      Generating analysis chunk by chunk ({total_chunks} chunks)...")
        for i, chunk_text in enumerate(chunks_to_process):
            chunk_start_time = time.time()
            logging.info(f"      Processing chunk {BOLD}{i+1}/{total_chunks}{RESET}...")
            if not isinstance(system_prompt, str): system_prompt = str(system_prompt)
            prompt_for_chunk = (
                f"{system_prompt}\n\n"
                f"--- START OF DATA CHUNK {i+1}/{total_chunks} ---\n\n"
                f"{chunk_text}\n\n"
                f"--- END OF DATA CHUNK {i+1}/{total_chunks} ---\n\n"
                f"Provide your analysis based *only* on the data provided in this chunk (Chunk {i+1}/{total_chunks}). "
                f"Focus on the content within the 'START' and 'END' delimiters for this specific chunk.\n\n"
                f"{BOLD}Analysis for Chunk {i+1}:{RESET}"
            )
            prompt_token_count = count_tokens(model, prompt_for_chunk) # Still need single calls here
            logging.debug(f"         Chunk {i+1} prompt token count: {prompt_token_count}")

            try:
                api_calls_made += 1
                response = model.generate_content(contents=prompt_for_chunk, generation_config=generation_config)
                if hasattr(response, 'text') and response.text is not None:
                    chunk_result = response.text
                    all_chunk_results.append(chunk_result)
                    elapsed = time.time() - chunk_start_time
                    logging.info(f"      ✅ Successfully received analysis for chunk {i+1} ({elapsed:.2f}s).")
                    if i < total_chunks - 1:
                        delay = 1.0
                        logging.debug(f"         😴 Delaying {delay:.1f}s before next chunk...")
                        time.sleep(delay)
                elif hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                     block_reason = response.prompt_feedback.block_reason or "Unknown"
                     safety_ratings_str = str(getattr(response.prompt_feedback, 'safety_ratings', 'N/A'))
                     logging.error(f"      ❌ Analysis failed/blocked for chunk {i+1}. Reason: {block_reason}. Ratings: {safety_ratings_str}")
                     all_chunk_results.append(f"\n\n[ERROR PROCESSING CHUNK {i+1}/{total_chunks}: Blocked - {block_reason}. Safety Ratings: {safety_ratings_str}]\n\n")
                     errors_occurred = True
                     time.sleep(2)
                else:
                    response_str = str(response)[:200] if response else "None"
                    logging.error(f"      ❌ Analysis failed for chunk {i+1}: No text content or block reason. Response: {response_str}...")
                    all_chunk_results.append(f"\n\n[ERROR PROCESSING CHUNK {i+1}/{total_chunks}: No text content received. Response: {response_str}...]\n\n")
                    errors_occurred = True
                    time.sleep(2)
            except Exception as e:
                logging.error(f"      🔥 Error generating content for chunk {i+1}: {e}", exc_info=True)
                all_chunk_results.append(f"\n\n[EXCEPTION PROCESSING CHUNK {i+1}/{total_chunks}: {type(e).__name__} - {e}]\n\n")
                errors_occurred = True
                time.sleep(5)

        logging.info("      ✍️ Combining results from multiple chunks...")
        final_result = f"--- AI ANALYSIS BASED ON {total_chunks} CHUNK(S) ---\n\n"
        for i, result in enumerate(all_chunk_results):
            final_result += f"---------- ANALYSIS FOR CHUNK {i+1}/{total_chunks} ----------\n\n{result}\n\n"
        final_result += f"--- END OF COMBINED ANALYSIS ({total_chunks} CHUNKS) ---"

    processing_elapsed = time.time() - processing_start_time
    logging.info(f"   🏁 Finished AI generation ({api_calls_made} API calls) for {total_chunks} chunk(s) ({processing_elapsed:.2f}s).")

    # --- [ SAVE RESULT LOGIC - UNCHANGED ] ---
    logging.info(f"   💾 Saving analysis result to {CYAN}{output_file}{RESET}...")
    try:
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(final_result)
        logging.info(f"   ✅ Analysis saved successfully.")
        if errors_occurred:
             logging.warning(f"   ⚠️ Analysis completed, but output file {CYAN}{output_file}{RESET} contains one or more error messages from generation.")
             return False
        return True
    except IOError as e:
        logging.error(f"   ❌ Error saving analysis to {CYAN}{output_file}{RESET}: {e}")
        return False
    except Exception as e:
        logging.error(f"   ❌ An unexpected error occurred while saving analysis to {CYAN}{output_file}{RESET}: {e}")
        return False


# --- Interactive Prompt Generation ---
# No changes needed in generate_prompt_interactive
def generate_prompt_interactive(model, prompt_dir):
    """Handles the interactive chat session to generate a system prompt."""
    logging.info(f"🖊️ Starting interactive prompt generation...")
    os.makedirs(prompt_dir, exist_ok=True)
    print(f"\n--- {BOLD}{MAGENTA}Prompt Generation Assistant{RESET} ---")
    print("Describe the kind of analysis or profile you want to create.")
    print("The assistant will ask clarifying questions.")
    print(f"Type '{BOLD}SAVE{RESET}' when you are satisfied with the generated prompt.")
    print(f"Type '{BOLD}QUIT{RESET}' to exit without saving.")
    print("-------------------------------------\n")

    try:
        # Initial system message for the chat assistant itself
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
        chat = model.start_chat(history=chat_history)

        while True:
            try:
                user_input = input(f"{BOLD}You:{RESET} ")
            except EOFError:
                 print(f"\n{YELLOW}Exiting prompt generation (EOF detected).{RESET}")
                 return
            if not user_input: continue

            user_input_upper = user_input.strip().upper()
            if user_input_upper == "QUIT":
                print(f"\n{YELLOW}Exiting prompt generation.{RESET}")
                return
            elif user_input_upper == "SAVE":
                print(f"\n{BOLD}Assistant:{RESET} {DIM}Okay, synthesizing the final prompt based on our discussion...{RESET}")
                try:
                     final_prompt_request = "Generate the final system prompt based *only* on our conversation history about the desired analysis. Output *nothing* but the raw text of the system prompt itself."
                     request_content = chat.history + [{'role':'user', 'parts': [final_prompt_request]}]
                     safety_settings_gen = {'HARM_CATEGORY_HARASSMENT': 'BLOCK_NONE',
                                            'HARM_CATEGORY_HATE_SPEECH': 'BLOCK_NONE',
                                            'HARM_CATEGORY_SEXUALLY_EXPLICIT': 'BLOCK_NONE',
                                            'HARM_CATEGORY_DANGEROUS_CONTENT': 'BLOCK_NONE'}
                     generation_config_gen = genai.GenerationConfig(temperature=0.3)

                     response = model.generate_content(request_content,
                                                       generation_config=generation_config_gen,
                                                       safety_settings=safety_settings_gen)

                     final_prompt = None
                     if hasattr(response, 'text') and response.text and response.text.strip():
                         final_prompt = response.text.strip()
                         if final_prompt.startswith("```") and final_prompt.endswith("```"):
                              final_prompt = re.sub(r'^```[a-zA-Z]*\n?', '', final_prompt)
                              final_prompt = re.sub(r'\n?```$', '', final_prompt)
                              final_prompt = final_prompt.strip()

                     if not final_prompt:
                          block_reason_str = "Unknown reason"
                          if hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                              block_reason_str = response.prompt_feedback.block_reason
                          logging.error(f"Failed to generate final prompt text. Block Reason: {block_reason_str}. Response: {response}")
                          print(f"{RED}Assistant:{RESET} I couldn't generate a final prompt text (Reason: {block_reason_str}). Please try describing your needs more or type '{BOLD}QUIT{RESET}'.")
                          continue

                     print(f"\n--- {BOLD}{GREEN}Generated System Prompt{RESET} ---")
                     print(f"{CYAN}{final_prompt}{RESET}")
                     print("------------------------------")

                     while True:
                          confirm = input(f"Save this prompt? ({GREEN}yes{RESET}/{RED}no{RESET}/{YELLOW}refine{RESET}): ").strip().lower()
                          if confirm in ['yes', 'y', 'no', 'n', 'refine', 'r']: break
                          else: print(f"{YELLOW}Please enter 'yes', 'no', or 'refine'.{RESET}")

                     if confirm in ['yes', 'y']:
                         while True:
                             prompt_name = input(f"Enter a filename for this prompt (e.g., 'personality_v1'): ").strip()
                             if not prompt_name: print(f"{YELLOW}Filename cannot be empty.{RESET}"); continue
                             if not re.match(r'^[\w\-. ]+$', prompt_name): print(f"{YELLOW}Invalid filename. Use letters, numbers, hyphens, underscores, dots, spaces.{RESET}"); continue

                             filename = os.path.join(prompt_dir, f"{prompt_name}.txt")
                             if os.path.exists(filename):
                                overwrite = input(f"File '{CYAN}{filename}{RESET}' already exists. Overwrite? ({GREEN}yes{RESET}/{RED}no{RESET}): ").strip().lower()
                                if overwrite not in ['yes', 'y']: continue

                             try:
                                 with open(filename, "w", encoding="utf-8") as f: f.write(final_prompt)
                                 print(f"✅ {BOLD}Prompt saved to {CYAN}{filename}{RESET}")
                                 return
                             except IOError as e:
                                  print(f"❌ {RED}Error saving prompt:{RESET} {e}")
                                  try_again = input("Try saving again with a different name? (yes/no): ").strip().lower()
                                  if try_again not in ['yes', 'y']: return
                             except Exception as e:
                                  print(f"❌ {RED}An unexpected error occurred during saving:{RESET} {e}"); return
                     elif confirm in ['no', 'n']:
                         print(f"{YELLOW}Prompt not saved. You can continue refining or type '{BOLD}QUIT{RESET}'.{RESET}")
                         chat.history.append({'role':'model', 'parts': [f"(Generated draft prompt, user chose not to save):\n{final_prompt}"]})
                         chat.history.append({'role':'user', 'parts': ["No, I don't want to save that one."]})
                     else: # Refine
                         print(f"{YELLOW}Okay, let's continue refining. What would you like to change or add?{RESET}")
                         chat.history.append({'role':'model', 'parts': [f"(Generated draft prompt, user wants to refine):\n{final_prompt}"]})
                         chat.history.append({'role':'user', 'parts': ["I want to refine the prompt."]})

                except Exception as e:
                    print(f"\n{RED}Assistant:{RESET} An error occurred generating the final prompt: {e}")
                    logging.error(f"Error during final prompt generation: {e}", exc_info=True)

            else: # Regular chat turn
                try:
                    print(f"{BOLD}Assistant:{RESET} {DIM}Thinking...{RESET}", end='\r')
                    response = chat.send_message(user_input)
                    print(" " * 25, end='\r') # Clear thinking message
                    if hasattr(response, 'text') and response.text:
                         print(f"\n{BOLD}Assistant:{RESET} {response.text}\n")
                    elif hasattr(response, 'prompt_feedback') and response.prompt_feedback and response.prompt_feedback.block_reason:
                         block_reason_conv = response.prompt_feedback.block_reason
                         print(f"\n{RED}Assistant:{RESET} My response was blocked (Reason: {block_reason_conv}). Could you please rephrase or ask differently?\n")
                    else:
                         print(f"\n{RED}Assistant:{RESET} I received an empty response. Please try again.\n")
                         logging.warning(f"Empty response received during chat: {response}")

                except Exception as e:
                    print(f"\n{RED}Assistant:{RESET} An error occurred processing your input: {e}")
                    logging.error(f"Error sending message to chat model: {e}", exc_info=True)
                    time.sleep(1)

    except Exception as e:
        logging.critical(f"Failed to initialize interactive prompt generation: {e}", exc_info=True)
        print(f"\n{RED}An error prevented the prompt generation assistant from starting:{RESET} {e}")