import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
from tld import get_fld
from urllib.parse import urlparse
import time
import sqlite3
import os
from dotenv import load_dotenv
import logging
from pathlib import Path
from tqdm import tqdm
import json

# Setup logging
logging.basicConfig(
    filename='rank_tracking.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

# Configuration
SERPER_API_KEY = os.getenv('SERPER_API_KEY')
if not SERPER_API_KEY:
    raise ValueError("Please set SERPER_API_KEY in .env file")

DB_PATH = "rankings.db"
PROGRESS_FILE = "rank_tracking_progress.json"

class RankTracker:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.progress = self.load_progress()

    def load_progress(self) -> Dict:
        """Load progress from checkpoint file."""
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                return {}
        return {}

    def save_progress(self, keyword: str):
        """Save progress checkpoint."""
        self.progress[keyword] = True
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(self.progress, f)

    def create_tables(self):
        """Create necessary database tables."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT UNIQUE,
                created_at DATE DEFAULT CURRENT_DATE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rankings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword_id INTEGER,
                domain TEXT,
                position INTEGER,
                check_date DATE,
                url TEXT,
                FOREIGN KEY (keyword_id) REFERENCES keywords (id)
            )
        ''')
        
        conn.commit()
        conn.close()

    def extract_domain(self, url: str) -> str:
        """Extract main domain from URL."""
        try:
            return get_fld(url, fail_silently=True) or urlparse(url).netloc
        except Exception:
            return url

    def search_google(self, keyword: str) -> List[Dict]:
        """Search Google through Serper API."""
        headers = {
            "X-API-KEY": SERPER_API_KEY,
            "Content-Type": "application/json",
            "location": "California, United States"
        }
        
        payload = {
            "q": keyword,
            "num": 10
        }
        
        try:
            response = requests.post(
                "https://google.serper.dev/search",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            time.sleep(2)  # Rate limiting
            return response.json().get('organic', [])
        except Exception as e:
            logging.error(f"Search error for {keyword}: {str(e)}")
            return []

    def read_keywords_from_csv(self, filepath: str) -> List[str]:
        """Read keywords from CSV file."""
        try:
            df = pd.read_csv(filepath)
            return df['keyword'].unique().tolist()
        except Exception as e:
            logging.error(f"CSV error: {str(e)}")
            return []

    def get_or_create_keyword_id(self, keyword: str) -> int:
        """Get keyword ID or create if not exists."""
        self.cursor.execute("SELECT id FROM keywords WHERE keyword = ?", (keyword,))
        result = self.cursor.fetchone()
        
        if result:
            return result[0]
        
        self.cursor.execute("INSERT INTO keywords (keyword) VALUES (?)", (keyword,))
        return self.cursor.lastrowid

    def process_keywords(self, filepath: str):
        """Process keywords with progress tracking."""
        self.create_tables()
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor