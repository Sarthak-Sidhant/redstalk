# RedStalk

**RedStalk** is a powerful, customizable command-line tool for scraping, analyzing, and generating insights from Reddit user history. It combines robust statistical analysis with advanced AI capabilities (Google Gemini, OpenRouter, NVIDIA AI) to create deep behavioral profiles and summaries.

---

## 🚀 Features

*   **Multi-Provider AI Analysis:**
    *   **Google Gemini:** Native integration for cost-effective, high-context analysis.
    *   **OpenRouter:** Access nearly any LLM (Claude 3.5 Sonnet, DeepSeek R1, Llama 3) via generic API.
    *   **NVIDIA AI:** Support for NVIDIA's high-performance hosted models (e.g., Llama 3.1 Nemotron).
*   **Deep Statistical Reports:** Generates detailed Markdown reports covering activity times, word frequency, sentiment analysis (VADER), subreddit diversity, and more.
*   **Advanced Analysis Modes:**
    *   `mapped`: Context-aware analysis (groups comments under posts).
    *   `subreddit_persona`: **NEW!** Aggregates activity by community to analyze personality shifts across subreddits.
*   **User Comparison:** Compare two users side-by-side to find shared interests and behavioral differences.
*   **Monitoring:** Watch a user for new activity and trigger auto-updates.
*   **Data Export:** JSON and CSV exports of user history.

---

## 📦 Installation

### Option 1: Install with pipx (Recommended for CLI use)
This isolates RedStalk's dependencies from your system python.
```bash
# Install directly from the directory
pipx install .

# Enable NVIDIA support (install optional extras manually if needed, or pipx inject)
pipx inject redstalk langchain-nvidia-ai-endpoints python-dotenv
```

### Option 2: Developer/Editable Install
Best if you want to modify the code.
```bash
git clone https://github.com/yourusername/redstalk.git
cd redstalk
python3 -m venv venv
source venv/bin/activate
pip install -e .
```

---

## 🔑 Configuration & API Keys

RedStalk looks for keys in **Environment Variables** (Recommended) or `config.json`.

### 1. Google Gemini (Default)
*   **Env Var:** `GOOGLE_API_KEY`
*   **Get Key:** [Google AI Studio](https://aistudio.google.com/)

### 2. OpenRouter (Access DeepSeek, Claude, etc.)
*   **Env Var:** `OPENROUTER_API_KEY`
*   **Get Key:** [OpenRouter](https://openrouter.ai/keys)

### 3. NVIDIA AI (Llama 3 Nemotron, etc.)
*   **Env Var:** `NVIDIA_API_KEY`
*   **Get Key:** [NVIDIA NIM](https://build.nvidia.com/)

### config.json
On first run, a `config.json` will be created. You can set defaults here:
```json
{
    "default_model_name": "gemini-1.5-pro",
    "user_agent": "RedStalk/2.0 (by /u/YourUsername)"
}
```
**CRITICAL:** You **must** set a unique `user_agent` in `config.json` or via `--user-agent` flag to comply with Reddit API rules.

---

## 📖 Usage & Examples

Once installed, use the `redstalk` command.

### 1. Basic Statistical Report
Scrape a user and generate a Markdown stats report (no AI).
```bash
redstalk u/SomeUser --generate-stats
```

### 2. Gemini AI Profile
Generate an AI-driven behavioral profile using the default Gemini model.
```bash
redstalk u/DeepFuckingValue --run-analysis
```

### 3. Subreddit Persona Analysis (OSINT)
Analyze how a user's personality changes depending on the subreddit they are in.
```bash
redstalk u/Spez --run-analysis --analysis-mode subreddit_persona
```

### 4. Using OpenRouter (e.g., DeepSeek R1)
Use the `--provider` and `--model-name` flags.
```bash
redstalk u/SomeUser --run-analysis \
  --provider openrouter \
  --model-name deepseek/deepseek-r1
```

### 5. Using NVIDIA AI
```bash
redstalk u/SomeUser --run-analysis \
  --provider nvidia \
  --model-name "nvidia/llama-3.1-nemotron-70b-instruct"
```

### 6. User Comparison
Compare two users to see overlaps in subreddits and speech.
```bash
redstalk --compare-user u/UserA u/UserB
```

### 7. Filtering Data
Limit analysis to specific topics or timeframes.
```bash
redstalk u/PoliticsUser --run-analysis \
  --focus-subreddit politics news \
  --start-date 2024-01-01
```

---

## 🛠 Command Reference

| Flag | Description |
|------|-------------|
| `--run-analysis` | Run AI behavioral analysis. |
| `--generate-stats` | Generate statistical MD/JSON reports. |
| `--provider [name]` | Choose AI: `gemini`, `openrouter`, `nvidia`. |
| `--model-name [id]` | Override the default model ID. |
| `--analysis-mode` | `mapped` (default), `raw`, or `subreddit_persona`. |
| `--compare-user A B` | Unfiltered comparison of two users. |
| `--monitor [user]` | Run in background, checking for updates. |

---

## 🧪 Testing (Manual CLI)

Run these commands to verify installation and features:

1.  **Verify Install:** `redstalk --help`
2.  **Basic Scrape:** `redstalk AutoModerator --generate-stats`
3.  **Persona Analysis:** `redstalk AutoModerator --run-analysis --analysis-mode subreddit_persona --provider gemini`
