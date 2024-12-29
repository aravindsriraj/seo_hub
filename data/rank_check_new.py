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
        logging.info("Database tables created/verified")

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
            if 'keyword' not in df.columns:
                raise ValueError("CSV must contain 'keyword' column")
            keywords = df['keyword'].unique().tolist()
            logging.info(f"Loaded {len(keywords)} unique keywords from CSV")
            return keywords
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
        self.cursor = self.conn.cursor()

        try:
            keywords = self.read_keywords_from_csv(filepath)
            if not keywords:
                logging.error("No keywords found to process")
                return

            today = datetime.now().date()
            remaining_keywords = [k for k in keywords if k not in self.progress]
            
            print(f"\nTotal keywords: {len(keywords)}")
            print(f"Already processed: {len(keywords) - len(remaining_keywords)}")
            print(f"Remaining to process: {len(remaining_keywords)}\n")
            
            with tqdm(total=len(remaining_keywords), desc="Processing Keywords") as pbar:
                for keyword in remaining_keywords:
                    try:
                        keyword_id = self.get_or_create_keyword_id(keyword)
                        
                        # Search progress indicator
                        tqdm.write(f"\nSearching: {keyword}")
                        results = self.search_google(keyword)

                        if results:
                            for position, result in enumerate(results, 1):
                                domain = self.extract_domain(result.get('link', ''))
                                self.cursor.execute("""
                                    INSERT INTO rankings (keyword_id, domain, position, check_date, url)
                                    VALUES (?, ?, ?, ?, ?)
                                """, (keyword_id, domain, position, today, result.get('link', '')))

                            self.conn.commit()
                            self.save_progress(keyword)
                            tqdm.write(f"✓ Saved {len(results)} rankings for: {keyword}")
                        else:
                            tqdm.write(f"⚠ No results found for: {keyword}")

                        pbar.update(1)

                    except Exception as e:
                        logging.error(f"Error processing {keyword}: {str(e)}")
                        tqdm.write(f"⚠ Error processing: {keyword}")
                        self.conn.rollback()
                        continue

        finally:
            if self.conn:
                self.conn.close()

    def display_rankings_summary(self):
        """Display comprehensive ranking summary."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT MAX(check_date) FROM rankings")
            latest_date = cursor.fetchone()[0]

            if not latest_date:
                print("\nNo rankings data found")
                return

            print(f"\nRankings Summary for {latest_date}")
            print("=" * 50)

            # Overall statistics
            cursor.execute("""
                WITH LatestRankings AS (
                    SELECT k.keyword, r.position, r.domain, r.url
                    FROM keywords k
                    JOIN rankings r ON k.id = r.keyword_id
                    WHERE r.check_date = ?
                )
                SELECT 
                    COUNT(CASE WHEN position <= 3 THEN 1 END) as top_3,
                    COUNT(CASE WHEN position BETWEEN 4 AND 10 THEN 1 END) as top_4_10,
                    ROUND(AVG(position), 1) as avg_position,
                    COUNT(DISTINCT keyword) as total_keywords,
                    COUNT(DISTINCT domain) as unique_domains
                FROM LatestRankings
            """, (latest_date,))

            summary = cursor.fetchone()
            print("\nOverall Statistics:")
            print("-" * 20)
            print(f"Total Keywords Tracked: {summary[3]}")
            print(f"Keywords in Top 3: {summary[0]}")
            print(f"Keywords in Positions 4-10: {summary[1]}")
            print(f"Average Position: {summary[2]}")
            print(f"Unique Domains: {summary[4]}")

            # Top performing domains
            print("\nTop Performing Domains:")
            print("-" * 20)
            cursor.execute("""
                WITH LatestRankings AS (
                    SELECT k.keyword, r.position, r.domain
                    FROM keywords k
                    JOIN rankings r ON k.id = r.keyword_id
                    WHERE r.check_date = ?
                )
                SELECT 
                    domain,
                    COUNT(*) as appearances,
                    ROUND(AVG(position), 1) as avg_position,
                    COUNT(CASE WHEN position <= 3 THEN 1 END) as top_3_count
                FROM LatestRankings
                GROUP BY domain
                HAVING appearances > 1
                ORDER BY avg_position ASC
                LIMIT 5
            """, (latest_date,))

            for domain, apps, avg_pos, top_3 in cursor.fetchall():
                print(f"\nDomain: {domain}")
                print(f"  Total Appearances: {apps}")
                print(f"  Average Position: {avg_pos}")
                print(f"  Keywords in Top 3: {top_3}")

            print("\nDetailed report saved to ranking_summary.log")
            
            # Save detailed report to file
            with open('ranking_summary.log', 'w') as f:
                f.write(f"Ranking Summary Report - {latest_date}\n")
                f.write("=" * 50 + "\n\n")
                
                cursor.execute("""
                    SELECT k.keyword, r.position, r.domain, r.url
                    FROM keywords k
                    JOIN rankings r ON k.id = r.keyword_id
                    WHERE r.check_date = ?
                    ORDER BY k.keyword, r.position
                """, (latest_date,))
                
                current_keyword = None
                for keyword, position, domain, url in cursor.fetchall():
                    if keyword != current_keyword:
                        f.write(f"\nKeyword: {keyword}\n")
                        current_keyword = keyword
                    f.write(f"  {position}. {domain} - {url}\n")

        finally:
            conn.close()

if __name__ == "__main__":
    # Check if keywords.csv exists
    KEYWORDS_CSV = "ranking.csv"
    
    if not os.path.exists(KEYWORDS_CSV):
        print("Error: Please create a ranking.csv file with a 'keyword' column")
        exit(1)
    
    try:
        # Initialize and run tracker
        print("\nInitializing rank tracking process...")
        tracker = RankTracker()
        
        # Process keywords
        print("\nStarting keyword processing...")
        tracker.process_keywords(KEYWORDS_CSV)
        
        # Display final summary
        print("\nGenerating ranking summary...")
        tracker.display_rankings_summary()
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Progress has been saved.")
        print("Run the script again to resume from the last processed keyword.")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        logging.error(f"Fatal error: {str(e)}")
        print("Check rank_tracking.log for details.")