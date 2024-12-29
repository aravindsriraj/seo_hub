import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

class URLTrackerDB:
    def __init__(self):
        self.db_path = 'url_tracker.db'
        self._init_db()
    
    def _init_db(self):
        """Initialize database with required tables."""
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
        
        # Create URL tracking table with enhanced fields
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS url_tracking (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                sitemap_url TEXT,
                word_count INTEGER,
                date_published TEXT,
                date_modified TEXT,
                last_checked TIMESTAMP,
                status TEXT,
                domain_name TEXT,
                FOREIGN KEY (sitemap_url) REFERENCES sitemap_tracking(sitemap_url)
            )
        """)
        
        conn.commit()
        conn.close()

    def get_url_info(self, url: str) -> Optional[Dict]:
        """Get full information about a URL."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT id, url, sitemap_url, word_count, 
                       date_published, date_modified, last_checked, 
                       status, domain_name
                FROM url_tracking 
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
                    'status': row[7],
                    'domain_name': row[8]
                }
            return None
            
        except Exception as e:
            print(f"Error getting URL info: {e}")
            return None
        finally:
            conn.close()

    def update_url(self, url: str, sitemap_url: str, word_count: int = 0, 
                  date_published: str = None, date_modified: str = None,
                  status: str = 'processed') -> bool:
        """Insert or update URL information."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            domain_name = urlparse(url).netloc
            
            cursor.execute("""
                INSERT INTO url_tracking (
                    url, sitemap_url, word_count, 
                    date_published, date_modified,
                    last_checked, status, domain_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    sitemap_url = excluded.sitemap_url,
                    word_count = excluded.word_count,
                    date_published = COALESCE(excluded.date_published, date_published),
                    date_modified = COALESCE(excluded.date_modified, date_modified),
                    last_checked = excluded.last_checked,
                    status = excluded.status,
                    domain_name = excluded.domain_name
            """, (
                url, sitemap_url, word_count,
                date_published, date_modified,
                current_time, status, domain_name
            ))
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"Error updating URL {url}: {e}")
            return False
        finally:
            conn.close()

    def update_last_checked(self, url: str) -> bool:
        """Update only the last_checked timestamp."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            
            cursor.execute("""
                UPDATE url_tracking 
                SET last_checked = ?
                WHERE url = ?
            """, (current_time, url))
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"Error updating last_checked for {url}: {e}")
            return False
        finally:
            conn.close()

    def get_sitemaps(self) -> List[Dict]:
        """Get list of tracked sitemaps."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT sitemap_url, last_processed, status
                FROM sitemap_tracking
                ORDER BY last_processed DESC
            """)
            
            sitemaps = []
            for row in cursor.fetchall():
                sitemaps.append({
                    'sitemap_url': row[0],
                    'last_processed': row[1],
                    'status': row[2]
                })
            
            return sitemaps
            
        except Exception as e:
            print(f"Error getting sitemaps: {e}")
            return []
        finally:
            conn.close()

    def update_sitemap_status(self, sitemap_url: str, status: str) -> bool:
        """Update sitemap processing status."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            current_time = datetime.now().isoformat()
            
            cursor.execute("""
                INSERT INTO sitemap_tracking (sitemap_url, last_processed, status)
                VALUES (?, ?, ?)
                ON CONFLICT(sitemap_url) DO UPDATE SET
                    last_processed = excluded.last_processed,
                    status = excluded.status
            """, (sitemap_url, current_time, status))
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"Error updating sitemap status: {e}")
            return False
        finally:
            conn.close()


