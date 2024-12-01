import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict
from tld import get_fld
from urllib.parse import urlparse
import time
import sqlite3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SERPER_API_KEY = os.getenv('SERPER_API_KEY')
if not SERPER_API_KEY:
    raise ValueError("Please set SERPER_API_KEY in your .env file")

SERPER_API_URL = "https://google.serper.dev/search"
DB_PATH = "rankings.db"

def create_tables():
    """Create the necessary database tables if they don't exist."""
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

def extract_domain(url: str) -> str:
    """Extract the main domain from a URL."""
    try:
        return get_fld(url, fail_silently=True) or urlparse(url).netloc
    except Exception:
        return url

def search_google(keyword: str) -> List[Dict]:
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
            SERPER_API_URL,
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        data = response.json()
        return data.get('organic', [])
        
    except requests.exceptions.RequestException as e:
        print(f"Error searching for {keyword}: {str(e)}")
        return []
    finally:
        # Add delay to respect rate limits
        time.sleep(2)

def read_keywords_from_csv(filepath: str) -> List[str]:
    """Read keywords from a CSV file."""
    try:
        df = pd.read_csv(filepath)
        return df['keyword'].unique().tolist()
    except Exception as e:
        print(f"Error reading CSV file: {str(e)}")
        return []

def get_or_create_keyword_id(cursor, keyword: str) -> int:
    """Get keyword ID from database or create if it doesn't exist."""
    cursor.execute("SELECT id FROM keywords WHERE keyword = ?", (keyword,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    cursor.execute("INSERT INTO keywords (keyword) VALUES (?)", (keyword,))
    return cursor.lastrowid

def process_keywords(filepath: str):
    """Main function to process keywords and store rankings."""
    # Create database and tables
    create_tables()
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Read keywords
    keywords = read_keywords_from_csv(filepath)
    today = datetime.now().date()
    
    for keyword in keywords:
        try:
            # Get or create keyword ID
            keyword_id = get_or_create_keyword_id(cursor, keyword)
            
            # Get rankings from Serper
            results = search_google(keyword)
            
            # Process each result
            for position, result in enumerate(results, 1):
                domain = extract_domain(result.get('link', ''))
                
                cursor.execute("""
                    INSERT INTO rankings (keyword_id, domain, position, check_date, url)
                    VALUES (?, ?, ?, ?, ?)
                """, (keyword_id, domain, position, today, result.get('link', '')))
            
            conn.commit()
            print(f"Processed keyword: {keyword}")
            
        except Exception as e:
            print(f"Error processing keyword {keyword}: {str(e)}")
            conn.rollback()
    
    conn.close()

def display_today_rankings():
    """Display a concise summary of today's rankings."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Get latest date
        cursor.execute("SELECT MAX(check_date) FROM rankings")
        latest_date = cursor.fetchone()[0]
        
        print(f"\nRanking Summary for {latest_date}")
        print("=" * 50)
        
        # Get rankings for latest date
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
                AVG(position) as avg_position,
                COUNT(DISTINCT keyword) as total_keywords
            FROM LatestRankings
        """, (latest_date,))
        
        summary = cursor.fetchone()
        print(f"Keywords in Top 3: {summary[0]}")
        print(f"Keywords in Positions 4-10: {summary[1]}")
        print(f"Average Position: {summary[2]:.1f}")
        print(f"Total Keywords Tracked: {summary[3]}")
        
        # Domain summary
        print("\nDomain Performance:")
        print("-" * 50)
        cursor.execute("""
            WITH LatestRankings AS (
                SELECT k.keyword, r.position, r.domain
                FROM keywords k
                JOIN rankings r ON k.id = r.keyword_id
                WHERE r.check_date = ?
            )
            SELECT 
                domain,
                COUNT(*) as keywords_ranked,
                AVG(position) as avg_position,
                COUNT(CASE WHEN position <= 3 THEN 1 END) as top_3_count
            FROM LatestRankings
            GROUP BY domain
        """, (latest_date,))
        
        domain_stats = cursor.fetchall()
        for domain, ranked, avg_pos, top_3 in domain_stats:
            print(f"\nDomain: {domain}")
            print(f"Total Keywords Ranked: {ranked}")
            print(f"Average Position: {avg_pos:.1f}")
            print(f"Keywords in Top 3: {top_3}")
        
    except Exception as e:
        print(f"Error displaying rankings: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Check if keywords.csv exists
    if not os.path.exists("keywords_for_ranking.csv"):
        print("Please create a keywords.csv file with a 'keyword' column")
        exit(1)
    
    # Process keywords
    process_keywords("keywords_for_ranking.csv")
    
    # Display results
    print("\nToday's Rankings:")
    display_today_rankings()