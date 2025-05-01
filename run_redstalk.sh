#!/usr/bin/env bash
# This script is designed to help you set up a dedicated shell environment for working with RedStalk.
# It aims to make getting started easier by automating a few steps.
# It will automatically find your RedStalk project folder (by looking at where *this* script is),
# set up a Python virtual environment ('venv') if it's not there, activate it,
# install project dependencies from 'requirements.txt', and create simple shortcut commands.
# !! IMPORTANT: Please place this script in the main RedStalk project directory !!
# (This is the folder that contains 'redstalk.py' and 'requirements.txt', and where the 'venv' folder should be created.)

# --- Basic Setup Configuration ---
VENV_DIR_NAME="venv" # This is the name we'll use for the virtual environment folder.
REQUIREMENTS_FILE_NAME="requirements.txt" # The file listing the Python packages your project needs.

# --- Starting the Setup Process ---

# 1. Figuring out where we are.
# ---------------------------------------------------------------------------------
# This command figures out the full path to the directory containing *this* script.
# We'll assume this is the root of your RedStalk project.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "--> Okay, first step: I've determined this script is located here: '$SCRIPT_DIR'"
echo "    (We'll treat this as your main RedStalk project directory.)"


# 2. Defining the paths for important files and folders.
# ---------------
REDSTALK_SCRIPT_PATH="$SCRIPT_DIR/redstalk.py" # The path to the main RedStalk Python script.
REQUIREMENTS_FILE_PATH="$SCRIPT_DIR/$REQUIREMENTS_FILE_NAME" # The path to the requirements file.
VENV_DIR_PATH="$SCRIPT_DIR/$VENV_DIR_NAME" # The path where the virtual environment will live.
VENV_ACTIVATE_SCRIPT="$VENV_DIR_PATH/bin/activate" # The script used to activate the virtual environment.

# 3. A quick check to make sure 'redstalk.py' is where we expect it.
# ----------------------------------
# This helps confirm we're in the right project directory.
if [ ! -f "$REDSTALK_SCRIPT_PATH" ]; then
    echo "!! Uh oh, something's not quite right. I expected to find 'redstalk.py' in '$SCRIPT_DIR',"
    echo "!! but it doesn't seem to be there."
    echo "!! Please make sure you put *this* setup script inside your main RedStalk project folder (the one with redstalk.py)."
    exit 1 # Exiting because we can't proceed without finding the main script.
fi
echo "    Confirmed: Found the main script '$REDSTALK_SCRIPT_PATH'. We're on the right track."

# 4. Changing our current location to the RedStalk project directory.
# -----------------------------------
# This ensures any relative paths used by RedStalk will work correctly.
echo "--> Now, let's change the current directory to '$SCRIPT_DIR'..."
if cd "$SCRIPT_DIR"; then
    echo "    Success: We are now in: $(pwd)"
else
    echo "!! Sorry, I couldn't change the directory to '$SCRIPT_DIR'."
    echo "!! Please check if you have the necessary permissions for that folder."
    exit 1 # Exiting if we can't get into the project directory.
fi

# 5. Checking for and creating the virtual environment if needed.
# -------------------------------------------------------
# A virtual environment keeps your project's dependencies separate from your system Python.
if [ ! -f "$VENV_ACTIVATE_SCRIPT" ]; then
    echo "--> I didn't find the virtual environment activation script at '$VENV_ACTIVATE_SCRIPT'."
    echo "--> It looks like we need to create the virtual environment '$VENV_DIR_NAME'..."

    PYTHON_EXECUTABLE=""
    # Let's find a suitable Python interpreter to create the venv. We prefer python3.
    if command -v python3 &> /dev/null; then
        PYTHON_EXECUTABLE="python3"
    elif command -v python &> /dev/null; then # Fallback to 'python' if python3 isn't found.
        PYTHON_EXECUTABLE="python"
    else
        echo "!! ERROR: I couldn't find a 'python3' or 'python' command on your system."
        echo "!! I need Python to create the virtual environment. Please install Python 3 and ensure it's in your system's PATH."
        exit 1 # Cannot create venv without Python.
    fi

    echo "    Using Python interpreter: '$PYTHON_EXECUTABLE'"
    if "$PYTHON_EXECUTABLE" -m venv "$VENV_DIR_NAME"; then
        echo "    Success: Virtual environment '$VENV_DIR_NAME' created at '$VENV_DIR_PATH'."
        # Let's double-check that the activate script was created as expected.
        if [ ! -f "$VENV_ACTIVATE_SCRIPT" ]; then
             echo "!! WARNING: The venv creation command ran, but the activation script '$VENV_ACTIVATE_SCRIPT' is missing!"
             echo "!! This might indicate an issue with your Python installation's venv module or file permissions."
             exit 1 # The venv isn't usable without the activate script.
        fi
    else
        echo "!! ERROR: Failed to create the virtual environment using '$PYTHON_EXECUTABLE -m venv $VENV_DIR_NAME'."
        echo "!! Please check permissions for the directory, and ensure your Python venv module is set up correctly."
        echo "!! (On Debian/Ubuntu, you might need to install 'python3-venv' with: sudo apt install python3-venv)"
        exit 1 # Cannot proceed without a working virtual environment.
    fi
else
    echo "--> Found an existing virtual environment activation script: '$VENV_ACTIVATE_SCRIPT'."
    echo "    Looks like the virtual environment is already set up."
fi

# 6. Activating the virtual environment.
# -------------------------------
echo "--> Attempting to activate the virtual environment..."
# The 'source' command runs the activation script in the current shell.
if [ -f "$VENV_ACTIVATE_SCRIPT" ]; then
    source "$VENV_ACTIVATE_SCRIPT"
    # We can check if the VIRTUAL_ENV variable is set to confirm activation.
    if [ -n "$VIRTUAL_ENV" ]; then
        echo "    Success: Virtual environment activated (VIRTUAL_ENV=$VIRTUAL_ENV)."
        echo "             You should now see '($VENV_DIR_NAME)' or similar at the start of your command prompt."
    else
        # This is less common but possible.
        echo "!! WARNING: I ran the source command for '$VENV_ACTIVATE_SCRIPT', but the VIRTUAL_ENV variable wasn't set."
        echo "!!          Activation might not have completed fully. You might want to inspect the activate script or try manually sourcing it."
    fi
else
    # This error should ideally not happen if step 5 was successful.
    echo "!! CRITICAL ERROR: The activation script '$VENV_ACTIVATE_SCRIPT' is missing even after trying to create/find it."
    echo "!! There might be a serious issue with the venv creation or your file system."
    exit 1 # Cannot activate if the script is missing.
fi

# 6.5 Installing/updating project dependencies using pip (inside the activated environment).
# ----------------------------------------------------
# We do this after activating the venv so pip installs into the isolated environment.
if [ -f "$REQUIREMENTS_FILE_PATH" ]; then
    echo "--> Found the dependencies list in '$REQUIREMENTS_FILE_NAME'. I'll use pip to install/update them..."
    # The 'pip' command used here is the one provided by the activated virtual environment.
    if pip install -r "$REQUIREMENTS_FILE_PATH"; then
        echo "    Success: Dependencies installed/updated from '$REQUIREMENTS_FILE_NAME'."
    else
        echo "!! ERROR: Failed to install dependencies using 'pip install -r $REQUIREMENTS_FILE_NAME'."
        echo "!! Please review the error messages above, check the contents of '$REQUIREMENTS_FILE_NAME', and ensure you have an internet connection."
        exit 1 # Exiting, as the project might not work without dependencies.
    fi
else
    echo "--> No '$REQUIREMENTS_FILE_NAME' file found in '$SCRIPT_DIR'."
    echo "    Skipping dependency installation. If your RedStalk project requires specific packages, you might need to install them manually later."
fi


# 7. Setting up convenient shortcut commands ('rstalk', 'redstalk').
# -----------------------------------------
echo "--> Creating helper functions 'rstalk' and 'redstalk' to make running the script easier."

# This function will execute the 'redstalk.py' script using the correct Python interpreter.
rstalk() {
    # Determine which python executable to use. Prefer the one inside our activated venv.
    local python_cmd="python" # Default fallback if other checks fail.
    # Check if the VIRTUAL_ENV variable is set and if a python executable exists within that path.
    if [[ -n "$VIRTUAL_ENV" && -x "$VIRTUAL_ENV/bin/python" ]]; then
        python_cmd="$VIRTUAL_ENV/bin/python" # Use the python from the activated venv.
    elif command -v python3 &> /dev/null; then # If venv check fails, try the system's python3.
         python_cmd="python3"
    fi
    # Execute the redstalk.py script. "$@" passes along any arguments given to the function.
    # We execute it relative to the current directory, which is the project root.
    "$python_cmd" "redstalk.py" "$@"
}

# Creating a second function with the full name for clarity or preference.
redstalk() {
    # Same logic as the 'rstalk' function.
    local python_cmd="python"
    if [[ -n "$VIRTUAL_ENV" && -x "$VIRTUAL_ENV/bin/python" ]]; then
        python_cmd="$VIRTUAL_ENV/bin/python"
    elif command -v python3 &> /dev/null; then
         python_cmd="python3"
    fi
    # Run the script!
    "$python_cmd" "redstalk.py" "$@"
}

# Export these functions so they are available in the new shell session we're about to start.
export -f rstalk # Making the 'rstalk' function available.
export -f redstalk # Making the 'redstalk' function available.

echo "    Success: Functions 'rstalk' and 'redstalk' are now defined and ready."
echo "             They will automatically run 'redstalk.py' using the Python from your virtual environment."

# 8. Launching a new shell environment with everything set up.
# -------------------------------
echo ""
echo "--------------------------------------------------"
echo "      RedStalk Environment Setup Complete!      "
echo "--------------------------------------------------"
echo "You are now in the project directory: $(pwd)"
echo "The Python virtual environment ('$VENV_DIR_NAME') is active."
echo "Project dependencies from '$REQUIREMENTS_FILE_NAME' (if found) have been installed."
echo ""
echo "You can now use the shortcut commands 'rstalk' or 'redstalk'."
echo "   Instead of typing 'python redstalk.py ...', you can just type:"
echo "   Example:   rstalk SomeUsername --generate-stats --output-dir ./reports"
echo ""
echo "To leave this special RedStalk environment and return to your normal shell, just type 'exit' or press Ctrl+D."
echo "--------------------------------------------------"
echo ""

# This command replaces the current script process with a new instance of your default shell.
# The new shell inherits the activated virtual environment and the exported functions.
# This is how you stay in the setup environment after the script finishes its setup steps.
exec "$SHELL"

# Any commands below 'exec' will not be reached or executed because the process is replaced.
# echo "This message will not appear."
