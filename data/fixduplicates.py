import sqlite3
import streamlit as st
from seo_hub.core.config import config

def fix_url_duplicates():
    """Fix duplicate URLs in the database and add unique constraint."""
    conn = sqlite3.connect(config.URLS_DB_PATH)
    cursor = conn.cursor()
    
    try:
        # First get the actual table structure
        st.write("Checking table structure...")
        cursor.execute("PRAGMA table_info(urls)")
        columns = cursor.fetchall()
        st.write("Current table columns:")
        for col in columns:
            st.write(f"- {col[1]} ({col[2]})")

        # Check for duplicates
        st.write("Checking for duplicate URLs...")
        cursor.execute("""
            SELECT url, COUNT(*) as count 
            FROM urls 
            GROUP BY url 
            HAVING count > 1
        """)
        
        duplicates = cursor.fetchall()
        if duplicates:
            st.warning(f"Found {len(duplicates)} URLs with duplicates")
            
            # Drop temporary table if it exists
            st.write("Cleaning up any existing temporary tables...")
            cursor.execute("DROP TABLE IF EXISTS urls_temp")
            
            # Create temporary table with same structure as original
            st.write("Creating temporary table...")
            cursor.execute("""
                CREATE TABLE urls_temp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    domain_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    summary TEXT,
                    category TEXT,
                    primary_keyword TEXT,
                    estimated_word_count INTEGER,
                    datePublished TEXT,
                    dateModified TEXT
                )
            """)
            
            # Copy data to temp table, keeping only the latest entry for each URL
            st.write("Copying unique entries to temporary table...")
            cursor.execute("""
                INSERT INTO urls_temp (
                    url, domain_name, status, summary, category, 
                    primary_keyword, estimated_word_count, 
                    datePublished, dateModified
                )
                SELECT 
                    url, domain_name, status, summary, category,
                    primary_keyword, estimated_word_count,
                    datePublished, dateModified
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY url 
                        ORDER BY id DESC
                    ) as rn
                    FROM urls
                )
                WHERE rn = 1
            """)
            
            # Get counts before and after
            cursor.execute("SELECT COUNT(*) FROM urls")
            before_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM urls_temp")
            after_count = cursor.fetchone()[0]
            
            st.write(f"URLs before deduplication: {before_count}")
            st.write(f"URLs after deduplication: {after_count}")
            st.write(f"Duplicates removed: {before_count - after_count}")
            
            # Drop original table
            st.write("Replacing original table...")
            cursor.execute("DROP TABLE urls")
            
            # Rename temp table to original
            cursor.execute("ALTER TABLE urls_temp RENAME TO urls")
            
            # Create index on URL for better performance
            cursor.execute("CREATE INDEX idx_urls_url ON urls(url)")
            
            conn.commit()
            st.success("Successfully removed duplicate URLs and added unique constraint")
            
        else:
            st.success("No duplicate URLs found in database")
            
            # Add unique constraint to existing table if it doesn't exist
            st.write("Adding unique constraint to URL column...")
            try:
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_urls_url ON urls(url)")
                conn.commit()
                st.success("Added unique constraint to URL column")
            except sqlite3.OperationalError as e:
                if "already exists" in str(e):
                    st.success("Unique constraint already exists on URL column")
                else:
                    raise
        
    except Exception as e:
        st.error(f"Error fixing database: {str(e)}")
        st.error("Full error details:", str(e))
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    st.title("Database Fix: Remove Duplicate URLs")
    if st.button("Run Fix"):
        fix_url_duplicates()