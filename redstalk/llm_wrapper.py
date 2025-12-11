
import logging
import os
import time
from abc import ABC, abstractmethod

# Try importing google.generativeai, handle if missing
try:
    import google.generativeai as genai
    HAS_GEMINI = True
except ImportError:
    HAS_GEMINI = False

# Try importing openai, handle if missing
try:
    from openai import OpenAI
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

# Try importing langchain_nvidia_ai_endpoints for NVIDIA support
try:
    from langchain_nvidia_ai_endpoints import ChatNVIDIA
    from dotenv import load_dotenv
    HAS_NVIDIA = True
except ImportError:
    HAS_NVIDIA = False

class LLMProvider(ABC):
    @abstractmethod
    def configure(self, api_key):
        pass

    @abstractmethod
    def count_tokens(self, text):
        pass

    @abstractmethod
    def generate_content(self, text, system_prompt=None):
        pass

    @property
    @abstractmethod
    def name(self):
        pass

class GeminiProvider(LLMProvider):
    def __init__(self, model_name):
        self.model_name = model_name
        self.model = None

    def configure(self, api_key):
        if not HAS_GEMINI:
            raise ImportError("google-generativeai package is not installed.")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(self.model_name)
    
    @property
    def name(self):
        return self.model_name

    def count_tokens(self, text):
        if not self.model:
            raise ValueError("Model not configured")
        # Gemini's count_tokens returns an object with total_tokens
        try:
            return self.model.count_tokens(text).total_tokens
        except Exception as e:
            logging.error(f"Error counting tokens with Gemini: {e}")
            return len(text) // 4  # Fallback approximation

    def generate_content(self, text, system_prompt=None):
        if not self.model:
            raise ValueError("Model not configured")
        
        # Construct the full prompt if system prompt is provided.
        # Gemini Python SDK supports system_instruction in newer versions, 
        # but the existing code manually prepends it. We'll stick to the manual prepend 
        # inside the perform_ai_analysis function in ai_utils.py usually, 
        # but here we provide a method that strictly takes 'text'. 
        # existing ai_utils.py passes the full combined prompt. 
        # We will assume 'text' here IS the full prompt/message.
        
        generation_config = genai.GenerationConfig(
            temperature=0.7, top_p=0.95, top_k=40, max_output_tokens=32768,
        )
        
        try:
            response = self.model.generate_content(contents=text, generation_config=generation_config)
            if hasattr(response, 'text'):
                return response.text
            elif hasattr(response, 'prompt_feedback'):
                return f"[BLOCKED: {response.prompt_feedback}]"
            else:
                return "[No response text]"
        except Exception as e:
            logging.error(f"Gemini generation error: {e}")
            raise e

class OpenRouterProvider(LLMProvider):
    def __init__(self, model_name, site_url=None, app_name=None):
        self.model_name = model_name
        self.client = None
        self.site_url = site_url or "https://github.com/sarthak-sidhant/redstalk"
        self.app_name = app_name or "RedStalk CLI"

    def configure(self, api_key):
        if not HAS_OPENAI:
            raise ImportError("openai package is not installed. Please pip install openai.")
        
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
            default_headers={
                "HTTP-Referer": self.site_url,
                "X-Title": self.app_name,
            }
        )

    @property
    def name(self):
        return f"openrouter/{self.model_name}"

    def count_tokens(self, text):
        # OpenRouter doesn't have a universal token counting endpoint.
        # We'll use a rough estimation: 1 token ~= 4 characters for English.
        return len(text) // 4

    def generate_content(self, text, system_prompt=None):
        if not self.client:
            raise ValueError("Client not configured")

        messages = []
        messages.append({"role": "user", "content": text})

        try:
            completion = self.client.chat.completions.create(
                extra_headers={
                    "HTTP-Referer": self.site_url,
                    "X-Title": self.app_name,
                },
                model=self.model_name,
                messages=messages,
                temperature=0.7,
            )
            return completion.choices[0].message.content
        except Exception as e:
            logging.error(f"OpenRouter generation error: {e}")
            raise e

class NvidiaProvider(LLMProvider):
    def __init__(self, model_name):
        self.model_name = model_name
        self.llm = None

    def configure(self, api_key):
        if not HAS_NVIDIA:
            raise ImportError("langchain-nvidia-ai-endpoints package is not installed. Please pip install langchain-nvidia-ai-endpoints python-dotenv")
        
        # Load dotenv as requested, although we manually set env var largely.
        load_dotenv()
        
        # Set the API Key in the environment variable so ChatNVIDIA can find it
        os.environ["NVIDIA_API_KEY"] = api_key
        
        try:
            # Initialize the model with the correct naming convention (org/model)
            self.llm = ChatNVIDIA(model=self.model_name)
        except Exception as e:
            logging.warning(f"Error initializing specific NVIDIA model '{self.model_name}': {e}. Falling back to default 'meta/llama3-8b-instruct' check.")
            try:
                self.llm = ChatNVIDIA(model="meta/llama3-8b-instruct") # Fallback as per user request example logic (modified slightly for class struct)
            except Exception as e2:
                logging.error(f"Failed to initialize NVIDIA fallback model: {e2}")
                raise e2

    @property
    def name(self):
        return f"nvidia/{self.model_name}"

    def count_tokens(self, text):
        # ChatNVIDIA via langchain might not expose direct token counting easily without a tokenizer.
        # Fallback to character estimation.
        return len(text) // 4

    def generate_content(self, text, system_prompt=None):
        if not self.llm:
            raise ValueError("Model not configured")

        # You can pass the string prompt directly
        try:
            # Note: system_prompt support depends on how we structure the call.
            # Redstalk passes full prompt in 'text' currently for other providers,
            # but if system_prompt is separate, we effectively append/prepend content.
            # Here we assume 'text' contains everything needed.
            result = self.llm.invoke(text)
            return result.content
        except Exception as e:
            logging.error(f"An error occurred during NVIDIA generation: {e}")
            # Try listing models for debugging help
            try:
                logging.info(f"Available NVIDIA models: {[m.id for m in ChatNVIDIA.get_available_models()]}")
            except:
                pass
            raise e


def get_llm_provider(provider_name, model_name, **kwargs):
    if provider_name.lower() == "gemini":
        return GeminiProvider(model_name)
    elif provider_name.lower() == "openrouter":
        return OpenRouterProvider(model_name, **kwargs)
    elif provider_name.lower() == "nvidia":
        return NvidiaProvider(model_name)
    else:
        raise ValueError(f"Unknown provider: {provider_name}")
