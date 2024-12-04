import sqlite3
from datetime import datetime, timedelta
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
        
        # Check if last_processed column exists
        cursor.execute("PRAGMA table_info(url_tracking)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'last_processed' not in columns:
            cursor.execute("""
                ALTER TABLE url_tracking
                ADD COLUMN last_processed TIMESTAMP
            """)
        
        conn.commit()
        conn.close()

    def update_url(self, url: str, sitemap_url: str, word_count: int = 0, 
                date_published: str = None, date_modified: str = None) -> bool:
        """Insert or update a URL's information with step-by-step debug."""
        print("\n" + "="*50)
        print(f"Processing URL: {url}")
        print(f"From sitemap: {sitemap_url}")
        print(f"Input values:")
        print(f"- Word count: {word_count}")
        print(f"- Published date: {date_published}")
        print(f"- Modified date: {date_modified}")
        
        try:
            print("\nStep 1: Opening database connection")
            print(f"Database path: {self.db_path}")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            print("\nStep 2: Checking if URL exists")
            cursor.execute("SELECT url FROM url_tracking WHERE url = ?", (url,))
            exists = cursor.fetchone() is not None
            print(f"URL exists in database: {exists}")
            
            print("\nStep 3: Preparing insert/update")
            current_time = datetime.now().strftime('%Y-%m-%d')
            values = (
                url,
                sitemap_url,
                word_count or 0,
                date_published,
                date_modified,
                current_time,
                'active'
            )
            print("Values prepared for insert:", values)
            
            print("\nStep 4: Executing database operation")
            cursor.execute("""
                INSERT INTO url_tracking (
                    url, 
                    sitemap_url, 
                    word_count,
                    date_published,
                    date_modified,
                    last_checked,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    sitemap_url = excluded.sitemap_url,
                    word_count = excluded.word_count,
                    date_published = COALESCE(excluded.date_published, date_published),
                    date_modified = COALESCE(excluded.date_modified, date_modified),
                    last_checked = CURRENT_TIMESTAMP,
                    status = 'active'
            """, values)
            
            print("Database operation executed")
            print(f"Rows affected: {cursor.rowcount}")
            
            print("\nStep 5: Committing transaction")
            conn.commit()
            print("Transaction committed")
            
            conn.close()
            print("\nOperation completed successfully")
            print("="*50)
            return True
            
        except Exception as e:
            print("\nERROR OCCURRED:")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            if isinstance(e, sqlite3.Error):
                print(f"SQLite error code: {e.sqlite_errorcode}" if hasattr(e, 'sqlite_errorcode') else "No error code")
                print(f"SQLite error name: {e.sqlite_errorname}" if hasattr(e, 'sqlite_errorname') else "No error name")
            
            if 'conn' in locals():
                print("Closing database connection after error")
                conn.close()
            print("="*50)
            return False
    
    def get_url_info(self, url: str) -> Optional[Dict]:
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
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            
            return None
            
        except Exception as e:
            print(f"Error getting URL info: {e}")
            return None
        finally:
            if 'conn' in locals():
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
        
    def update_sitemap_status(self, sitemap_url: str, status: str):
        """Update sitemap processing status."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT OR REPLACE INTO sitemap_tracking 
                (sitemap_url, last_processed, status) 
                VALUES (?, ?, ?)
            """, (sitemap_url, datetime.now().isoformat(), status))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error updating sitemap status: {e}")
            if 'conn' in locals():
                conn.close()
            return False

    def add_sitemap(self, sitemap_url: str) -> bool:
        """Add a new sitemap URL to track."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clean the URL - remove any whitespace or newlines
            sitemap_url = sitemap_url.strip()
            
            cursor.execute("""
                INSERT INTO sitemap_tracking (sitemap_url, last_processed, status)
                VALUES (?, CURRENT_TIMESTAMP, 'pending')
            """, (sitemap_url,))
            
            conn.commit()
            conn.close()
            st.success(f"Added sitemap: {sitemap_url}")
            return True
            
        except sqlite3.IntegrityError:
            st.warning(f"Sitemap {sitemap_url} already exists")
            return False
        except Exception as e:
            st.error(f"Error adding sitemap: {str(e)}")
            return False
                    
    def get_sitemaps(self) -> List[Dict]:
        """Get all tracked sitemaps."""
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
            
            conn.close()
            return sitemaps
            
        except Exception as e:
            print(f"Error getting sitemaps: {e}")
            if 'conn' in locals():
                conn.close()
            return []