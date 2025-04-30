#!/bin/bash

# --- Konfig ---
# !! IMPORTANT: Replace this with the ACTUAL FULL PATH to YOUR redstalk project directory !!
REDSTALK_DIR="/home/<USER>/redstalk" # <--- CHANGE THIS LINE

# --- Skript Logic ---

echo "Attempting to navigate to RedStalk directory: $REDSTALK_DIR"
# Check if directory exists before trying to cd
if [ -d "$REDSTALK_DIR" ]; then
    cd "$REDSTALK_DIR"
    echo "Successfully changed directory to $(pwd)"
else
    echo "ERROR: RedStalk directory not found at '$REDSTALK_DIR'"
    echo "Please edit this script ($0) and set the REDSTALK_DIR variable correctly."
    exit 1 # Exit if the directory doesn't exist
fi

echo "Attempting to activate virtual environment..."
# Check if activation script exists
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "Virtual environment activated. Your prompt should now show '(venv)'."
else
    echo "ERROR: Virtual environment activation script not found at '$REDSTALK_DIR/venv/bin/activate'"
    echo "Make sure you have created the virtual environment inside the RedStalk directory."
    # exit 1 # Uncomment to stop if venv activation fails
    echo "WARNING: Proceeding without activated venv."
fi

# --- Define and Export the function HERE ---
echo "Defining function 'rs' for 'python redstalk.py'..."
rs() {
    # Execute the python script, passing all arguments given to 'rs'
    python redstalk.py "$@"
}
export -f rs  # <--- EXPORT THE FUNCTION
# -----------------------------------------

echo "--------------------------------------------------"
echo "Setup complete. You are now in the RedStalk directory with the venv active."
# Update the instructions - it behaves like an alias for the user
echo "Function defined: Type 'rs' instead of 'python redstalk.py'" # <--- UPDATED MESSAGE
echo "Example: rs SomeUser --generate-stats"                      # <--- EXAMPLE ADDED
echo "Type 'exit' or press Ctrl+D to leave this environment and return to your original shell."
echo "--------------------------------------------------"

# Launch a new instance of the user's default shell.
# This new shell inherits the current environment (changed directory, activated venv, AND THE EXPORTED FUNCTION).
exec "$SHELL"

# Code below this line will not execute because 'exec' replaces the process
