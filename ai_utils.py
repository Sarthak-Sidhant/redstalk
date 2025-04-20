# ai_utils.py
import logging
import google.generativeai as genai
import time
import os
import re

# --- Core AI Functions ---

def count_tokens(model, text):
    """Counts the number of tokens in a given text using the provided model."""
    if not isinstance(text, str):
        logging.warning(f"Cannot count tokens on non-string input: type {type(text)}")
        return 0
    if not text:
        return 0
    try:
        # Add a short delay IF making many calls might be an issue elsewhere,
        # but avoid it for the single estimate call.
        # time.sleep(0.05) # Consider adding small delay ONLY if rate limits become an issue
        return model.count_tokens(text).total_tokens
    except Exception as e:
        # Make errors more prominent if counting fails
        logging.error(f"💥 Error counting tokens ({type(e).__name__}): {e}. Text starts with: '{text[:100]}...'")
        return None

# chunk_items remains the same as the previous version - it correctly groups items
# when chunking is *known* to be necessary.
def chunk_items(items: list, model: genai.GenerativeModel, max_chunk_tokens: int, item_separator="\n\n---\n\n"):
    """
    Groups a list of item strings into chunks based on token limits.
    This function assumes chunking is *necessary* and performs the grouping.
    """
    chunks = []
    current_chunk_items = []
    current_chunk_tokens = 0
    separator_tokens = count_tokens(model, item_separator)
    if separator_tokens is None:
        logging.warning("Failed to count separator tokens, estimating as 5.")
        separator_tokens = 5

    logging.info(f"Grouping {len(items)} items into chunks <= {max_chunk_tokens} tokens.")
    start_time = time.time() # Time the grouping process

    for i, item_text in enumerate(items):
        if not item_text or not item_text.strip():
            logging.debug(f"Skipping empty item at index {i}")
            continue

        # Log progress during chunking grouping more visibly
        if (i + 1) % 100 == 0:
             logging.info(f"  ... grouping item {i+1}/{len(items)}")

        item_token_count = count_tokens(model, item_text)
        if item_token_count is None:
            logging.error(f"Failed to count tokens for item {i+1} during chunk grouping. Aborting chunking.")
            return None # Critical failure during grouping

        tokens_if_added = item_token_count + (separator_tokens if current_chunk_items else 0)

        # Handle oversized items
        if item_token_count > max_chunk_tokens:
            logging.warning(f"Single item {i+1} ({item_token_count} tokens) exceeds max chunk tokens ({max_chunk_tokens}). Adding as its own chunk.")
            if current_chunk_items:
                chunks.append(item_separator.join(current_chunk_items))
            chunks.append(item_text)
            current_chunk_items = []
            current_chunk_tokens = 0
            continue

        # Add to current chunk or start a new one
        if current_chunk_tokens + tokens_if_added <= max_chunk_tokens:
            current_chunk_items.append(item_text)
            current_chunk_tokens += tokens_if_added
        else:
            if current_chunk_items:
                chunks.append(item_separator.join(current_chunk_items))
            current_chunk_items = [item_text]
            current_chunk_tokens = item_token_count

    # Add the last chunk
    if current_chunk_items:
        chunks.append(item_separator.join(current_chunk_items))

    elapsed_time = time.time() - start_time
    logging.info(f"Finished grouping items into {len(chunks)} chunks ({elapsed_time:.2f}s).")
    # Log final chunk sizes is still useful for debugging
    for j, chunk_content in enumerate(chunks):
        chk_tokens = count_tokens(model, chunk_content)
        logging.debug(f"Final Chunk {j+1}/{len(chunks)} token count: {chk_tokens if chk_tokens is not None else 'ERR'}")

    return chunks


# --- Main Analysis Function with Correct Conditional Chunking ---
def perform_ai_analysis(model, system_prompt, entries: list, output_file, chunk_size):
    """
    Performs AI analysis on a list of entries, chunking ONLY IF necessary
    based on a single initial token estimate compared to chunk_size.

    Args:
        model: The generative AI model.
        system_prompt: The base prompt.
        entries: A list of strings, each a formatted post/comment.
        output_file: Path to save the analysis.
        chunk_size: Maximum tokens per chunk (used directly for chunking threshold).
    """
    if not entries:
        logging.error("❌ No data entries provided for AI analysis. Aborting.")
        return False

    generation_config = genai.GenerationConfig(
        temperature=0.7,
        top_p=0.95,
        top_k=40,
        max_output_tokens=8192,
    )

    # --- Determine Separator and Perform Fast Estimate ---
    item_separator = "\n\n---\n\n" if any("---" in entry for entry in entries[:10]) else "\n\n"
    logging.debug(f"Using item separator: '{item_separator.encode('unicode_escape').decode()}'")

    logging.info(f"Performing initial token count estimate for {len(entries)} items...")
    start_time = time.time()
    # Join ALL entries first for a single count
    full_text_estimate = item_separator.join(entries)
    estimated_total_tokens = count_tokens(model, full_text_estimate)
    elapsed_time = time.time() - start_time

    if estimated_total_tokens is None:
        logging.error("❌ Failed to perform initial token count estimate. Cannot determine if chunking is needed.")
        return False

    logging.info(f"Initial estimate: {estimated_total_tokens} tokens ({elapsed_time:.2f}s). Target chunk size: {chunk_size}.")

    # --- Conditional Chunking Decision ---
    chunks_to_process = []
    needs_chunking = estimated_total_tokens > chunk_size

    if needs_chunking:
        logging.warning(f"Initial estimate ({estimated_total_tokens}) exceeds target chunk size ({chunk_size}). Detailed chunking required...")
        # Only NOW do we call the item-by-item chunking function
        chunks_to_process = chunk_items(entries, model, chunk_size, item_separator=item_separator)
        if chunks_to_process is None: # Check if chunk_items failed
            logging.error("❌ Failed during detailed chunk grouping after initial estimate indicated need.")
            return False
    else:
        logging.info("Initial estimate fits within chunk size. Processing as a single chunk.")
        # Use the already joined text from the estimation step
        chunks_to_process = [full_text_estimate]

    # --- Process the Determined Chunks ---
    if not chunks_to_process:
         # This case might happen if chunk_items returns an empty list, though unlikely with the checks
         logging.error("❌ No chunks available for processing (chunking might have failed or input was empty).")
         return False

    total_chunks = len(chunks_to_process)
    final_result = ""
    all_chunk_results = []

    if total_chunks == 1:
        logging.info("Processing the single data chunk...")
        full_prompt = f"{system_prompt}\n\n--- START OF REDDIT DATA ---\n\n{chunks_to_process[0]}\n\n--- END OF REDDIT DATA ---"
        try:
            response = model.generate_content(contents=full_prompt, generation_config=generation_config)
            if response and hasattr(response, 'text'):
                final_result = response.text
                logging.info("Successfully received analysis for single chunk.")
            else:
                block_reason = response.prompt_feedback.block_reason if response and response.prompt_feedback else "Unknown"
                logging.error(f"Analysis failed/blocked for single chunk. Reason: {block_reason}")
                final_result = f"[ERROR: Analysis generation failed or was blocked. Reason: {block_reason}]"
        except Exception as e:
            logging.error(f"Error generating content for single chunk: {e}", exc_info=True)
            final_result = f"[Error during analysis: {e}]"

    else: # Process multiple chunks generated by chunk_items
        logging.info(f"Processing {total_chunks} generated chunks for analysis...")
        for i, chunk_text in enumerate(chunks_to_process):
            logging.info(f"Generating analysis for chunk {i+1}/{total_chunks}...")
            prompt_for_chunk = f"{system_prompt}\n\n--- START OF DATA CHUNK {i+1}/{total_chunks} ---\n\n{chunk_text}\n\n--- END OF DATA CHUNK {i+1}/{total_chunks} ---\n\nBased *only* on the data provided in this chunk (and the overall instructions in the initial prompt), perform the analysis for this segment."
            try:
                response = model.generate_content(contents=prompt_for_chunk, generation_config=generation_config)
                if response and hasattr(response, 'text'):
                    chunk_result = response.text
                    all_chunk_results.append(chunk_result)
                    logging.info(f"Successfully received analysis for chunk {i+1}.")
                    if i < total_chunks - 1: time.sleep(1.5) # Delay between chunks
                else:
                    block_reason = response.prompt_feedback.block_reason if response and response.prompt_feedback else "Unknown"
                    logging.error(f"No text content received for chunk {i+1}. Block reason: {block_reason}")
                    all_chunk_results.append(f"\n\n[ERROR PROCESSING CHUNK {i+1}/{total_chunks}: Blocked - {block_reason}]\n\n")
            except Exception as e:
                logging.error(f"Error generating content for chunk {i+1}: {e}", exc_info=True)
                all_chunk_results.append(f"\n\n[EXCEPTION PROCESSING CHUNK {i+1}/{total_chunks}: {e}]\n\n")
                time.sleep(5) # Longer delay on error

        # Combine results from multiple chunks
        final_result = f"--- ANALYSIS BASED ON {total_chunks} CHUNK(S) ---\n\n"
        for i, result in enumerate(all_chunk_results):
            final_result += f"---------- ANALYSIS FOR CHUNK {i+1}/{total_chunks} ----------\n\n{result}\n\n"

    # --- Save Result ---
    try:
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(final_result)
        logging.info(f"✅ Analysis saved to {output_file}")
        # Check if the result indicates an error happened during generation
        if "[Error during analysis:" in final_result or "[ERROR:" in final_result or "[EXCEPTION PROCESSING CHUNK" in final_result:
             logging.warning("Analysis completed, but output file contains error messages.")
             return False # Indicate partial failure
        return True
    except IOError as e:
        logging.error(f"❌ Error saving analysis to {output_file}: {e}")
        return False
    except Exception as e:
        logging.error(f"❌ An unexpected error occurred while saving analysis: {e}")
        return False


# --- Interactive Prompt Generation (Keep as is) ---
def generate_prompt_interactive(model, prompt_dir):
    """Handles the interactive chat session to generate a system prompt."""
    # ... (implementation remains the same) ...
    logging.info("Starting interactive prompt generation...")
    os.makedirs(prompt_dir, exist_ok=True)
    print("\n--- Prompt Generation Assistant ---")
    print("Describe the kind of analysis or profile you want to create.")
    print("The assistant will ask clarifying questions.")
    print("Type 'SAVE' when you are satisfied with the generated prompt.")
    print("Type 'QUIT' to exit without saving.")
    print("-------------------------------------\n")

    try:
        # Start chat with specific instructions for the assistant's role
        chat_history = [
            {'role':'user', 'parts': ["""
You are an AI assistant helping a user create a system prompt for analyzing Reddit user data (posts and comments) using another AI model (like Gemini).
Your goal is to understand the user's requirements for the analysis (e.g., personality traits, interests, writing style, specific topics, sentiment, tone, potential biases, etc.) and formulate a clear, concise, and effective *system prompt* based on the conversation.
Ask clarifying questions to refine the requirements.
When the user seems ready or types 'SAVE', synthesize the conversation into a final system prompt.
Present *only the final prompt text itself*, without any extra conversational text before or after it. Just output the raw prompt.
Before asking the user to save, present the prompt clearly.
Keep your own conversational responses concise.
"""]},
            {'role':'model', 'parts': ["Okay, I understand. I will help you create a system prompt for analyzing Reddit data. Tell me what kind of analysis you'd like to perform."]}
        ]
        chat = model.start_chat(history=chat_history)

        while True:
            user_input = input("You: ")
            if not user_input: continue # Skip empty input

            user_input_upper = user_input.strip().upper()
            if user_input_upper == "QUIT":
                print("Exiting prompt generation.")
                return
            elif user_input_upper == "SAVE":
                print("\nAssistant: Generating final prompt based on our discussion...")
                try:
                     # Ask the model to generate the prompt based on history
                     final_prompt_request = "Based on our conversation history, generate the final system prompt text now. Remember to output *only* the prompt text itself, with no surrounding explanations or formatting."
                     # Use generate_content for a single response, include history
                     request_content = chat.history + [{'role':'user', 'parts': [final_prompt_request]}]
                     response = model.generate_content(request_content)

                     if not response or not hasattr(response, 'text') or not response.text.strip():
                          print("Assistant: I couldn't generate a final prompt text. Please try describing your needs more or type 'QUIT'.")
                          continue

                     final_prompt = response.text.strip()
                     # Clean up potential markdown code fences if model adds them
                     if final_prompt.startswith("```") and final_prompt.endswith("```"):
                          final_prompt = re.sub(r'^```[a-zA-Z]*\n', '', final_prompt)
                          final_prompt = re.sub(r'\n```$', '', final_prompt)
                          final_prompt = final_prompt.strip()

                     print("\n--- Generated System Prompt ---")
                     print(final_prompt)
                     print("------------------------------")

                     confirm = input("Save this prompt? (yes/no): ").strip().lower()
                     if confirm in ['yes', 'y']:
                         while True:
                             prompt_name = input("Enter a filename for this prompt (e.g., 'personality_focus'): ").strip()
                             if not prompt_name:
                                 print("Filename cannot be empty.")
                                 continue
                             if not re.match(r'^[\w\-. ]+$', prompt_name):
                                 print("Invalid filename. Use letters, numbers, hyphens, underscores, dots, spaces.")
                                 continue

                             filename = os.path.join(prompt_dir, f"{prompt_name}.txt")
                             if os.path.exists(filename):
                                overwrite = input(f"File '{filename}' already exists. Overwrite? (yes/no): ").strip().lower()
                                if overwrite not in ['yes', 'y']:
                                    continue

                             try:
                                 with open(filename, "w", encoding="utf-8") as f: f.write(final_prompt)
                                 print(f"✅ Prompt saved to {filename}")
                                 return # Exit after successful save
                             except IOError as e:
                                 print(f"❌ Error saving prompt: {e}")
                                 try_again = input("Try saving again? (yes/no): ").strip().lower()
                                 if try_again not in ['yes', 'y']: return
                             except Exception as e:
                                 print(f"❌ An unexpected error occurred during saving: {e}")
                                 return
                     else:
                         print("Prompt not saved. You can continue refining or type 'QUIT'.")
                         # No need to update history here unless needed for context carry-over

                except Exception as e:
                    print(f"\nAssistant: An error occurred generating the final prompt: {e}")
                    logging.error(f"Error during final prompt generation: {e}", exc_info=True)

            else: # Regular chat turn
                try:
                    # Send message maintains history automatically
                    response = chat.send_message(user_input)
                    print(f"\nAssistant: {response.text}\n")
                except Exception as e:
                    print(f"\nAssistant: An error occurred: {e}")
                    logging.error(f"Error sending message to chat model: {e}", exc_info=True)
                    time.sleep(1)

    except Exception as e:
        logging.critical(f"Failed to initialize interactive prompt generation: {e}", exc_info=True)
        print(f"\nAn error prevented the prompt generation assistant from starting: {e}")