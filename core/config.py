import os
import streamlit as st
import google.generativeai as genai
from dotenv import load_dotenv

class Config:
    """Central configuration class for the SEO Hub application."""
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Get the seo_hub directory (where the databases are)
        self.PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
        
        # Database paths with correct location
        self.URLS_DB_PATH = os.path.join(self.PROJECT_ROOT, 'urls_analysis.db')
        self.RANKINGS_DB_PATH = os.path.join(self.PROJECT_ROOT, 'rankings.db')
        self.AIMODELS_DB_PATH = os.path.join(self.PROJECT_ROOT, 'aimodels.db')

        
        # API Configurations
        self.setup_gemini_api()
        
        # Constants
        self.MAX_CONTENT_CHARS = 30000  # Max characters for content analysis
        self.URL_BATCH_SIZE = 450       # Number of URLs to process in one batch
        self.PROCESS_DELAY = 5          # Delay between URL processing in seconds
        
        # HTTP Headers
        self.REQUEST_HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

    def setup_gemini_api(self):
        """Configure Gemini API with appropriate settings."""
        self.GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "Not configured")
        self.GEMINI_MODEL_NAME = "gemini-1.5-flash"
        
        genai.configure(api_key=self.GEMINI_API_KEY)
        
        self.GENERATION_CONFIG = {
            "temperature": 1,
            "top_p": 0.95,
            "top_k": 40,
            "max_output_tokens": 8192,
            "response_mime_type": "text/plain",
        }
        
        self.gemini_model = genai.GenerativeModel(
            model_name=self.GEMINI_MODEL_NAME,
            generation_config=self.GENERATION_CONFIG,
        )

    def check_database_exists(self, db_path):
        """Check if a database file exists."""
        if not os.path.exists(db_path):
            st.error(f"Database '{db_path}' does not exist. Please create the database first.")
            return False
        return True

    def get_streamlit_config(self):
        """Return Streamlit-specific configurations."""
        return {
            "page_title": "SEO Monitoring Hub",
            "layout": "wide",
            "initial_sidebar_state": "expanded"
        }

    def get_time_periods(self):
        """Return standard time periods for analysis."""
        return {
            "Last 7 days": 7,
            "Last 14 days": 14,
            "Last 30 days": 30,
            "Last 90 days": 90,
            "Last 180 days": 180,
        }

# Create a global instance of Config
config = Config()