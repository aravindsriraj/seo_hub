import sqlite3
from datetime import datetime
from typing import List, Dict, Optional
from seo_hub.data.dev_config import URL_TRACKER_DB_PATH

class URLTrackerDB:
    def __init__(self):
        self.db_path = URL_TRACKER_DB_PATH
        self._init_db()
    
    def _init_db(self):
        """Initialize database and create tables if they don't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create sitemap tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sitemap_tracking (
                id INTEGER PRIMARY KEY,
                sitemap_url TEXT UNIQUE,
                last_processed TIMESTAMP,
                status TEXT
            )
        """)
        
        # Create URL tracking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS url_tracking (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                sitemap_url TEXT,
                word_count INTEGER,
                date_published TIMESTAMP,
                date_modified TIMESTAMP,
                last_checked TIMESTAMP,
                status TEXT,
                FOREIGN KEY (sitemap_url) REFERENCES sitemap_tracking(sitemap_url)
            )
        """)
        
        conn.commit()
        conn.close()

    def add_sitemap(self, sitemap_url: str) -> bool:
        """Add a new sitemap URL to track."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO sitemap_tracking (sitemap_url, last_processed, status)
                VALUES (?, ?, ?)
            """, (sitemap_url, datetime.now(), 'added'))
            
            conn.commit()
            conn.close()
            return True
            
        except sqlite3.IntegrityError:
            print(f"Sitemap {sitemap_url} already exists")
            return False
        except Exception as e:
            print(f"Error adding sitemap: {e}")
            return False
    
    def get_sitemaps(self) -> List[Dict]:
        """Get all tracked sitemaps."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM sitemap_tracking")
            rows = cursor.fetchall()
            
            # Convert to list of dicts
            sitemaps = []
            for row in rows:
                sitemaps.append({
                    'id': row[0],
                    'sitemap_url': row[1],
                    'last_processed': row[2],
                    'status': row[3]
                })
            
            conn.close()
            return sitemaps
            
        except Exception as e:
            print(f"Error getting sitemaps: {e}")
            return []
    
    def update_url(self, url: str, sitemap_url: str, word_count: int, 
                  date_published: str, date_modified: str) -> bool:
        """Insert or update a URL's information."""
        print(f"Updating URL: {url}")  # Debugging line
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO url_tracking (
                    url, sitemap_url, word_count, 
                    date_published, date_modified, last_checked, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    word_count=excluded.word_count,
                    date_modified=excluded.date_modified,
                    last_checked=excluded.last_checked,
                    status=excluded.status
            """, (
                url, sitemap_url, word_count, 
                date_published, date_modified, 
                datetime.now(), 'updated'
            ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error updating URL {url}: {e}")
            return False
    
    def get_url_info(self, url: str) -> Optional[Dict]:
        print(f"Fetching info for URL: {url}")
        """Get information about a specific URL."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM url_tracking 
                WHERE url = ?
            """, (url,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'url': row[1],
                    'sitemap_url': row[2],
                    'word_count': row[3],
                    'date_published': row[4],
                    'date_modified': row[5],
                    'last_checked': row[6],
                    'status': row[7]
                }
            return None
            
        except Exception as e:
            print(f"Error getting URL info: {e}")
            return None
        finally:
            conn.close()


    def get_all_urls(self) -> List[Dict]:
        """Get all tracked URLs."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM url_tracking")
            rows = cursor.fetchall()
            
            urls = []
            for row in rows:
                urls.append({
                    'id': row[0],
                    'url': row[1],
                    'sitemap_url': row[2],
                    'word_count': row[3],
                    'date_published': row[4],
                    'date_modified': row[5],
                    'last_checked': row[6],
                    'status': row[7]
                })
            
            conn.close()
            return urls
            
        except Exception as e:
            print(f"Error getting URLs: {e}")
            return []