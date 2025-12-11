
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
        # If system prompt is passed separately (which we might need to handle), add it.
        # However, looking at ai_utils.py, it embeds system prompt in the text.
        # We might need to adjust ai_utils.py or handle it here.
        # For now, we treat 'text' as the full user message. 
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
                # max_tokens=32768 # Some models might not support this high, let defaults handle or be conservative
            )
            return completion.choices[0].message.content
        except Exception as e:
            logging.error(f"OpenRouter generation error: {e}")
            raise e

def get_llm_provider(provider_name, model_name, **kwargs):
    if provider_name.lower() == "gemini":
        return GeminiProvider(model_name)
    elif provider_name.lower() == "openrouter":
        return OpenRouterProvider(model_name, **kwargs)
    else:
        raise ValueError(f"Unknown provider: {provider_name}")
