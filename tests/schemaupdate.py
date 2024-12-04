import sqlite3
import shutil
from datetime import datetime

def backup_databases():
    """Create backup of databases."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Backup each database
    databases = ['url_tracker.db', 'urls_analysis.db']
    for db in databases:
        backup_name = f"{db}.{timestamp}.backup"
        shutil.copy2(db, backup_name)
        print(f"Created backup: {backup_name}")

def update_url_tracker_db():
    """Update url_tracker.db schema."""
    print("\nUpdating url_tracker.db...")
    conn = sqlite3.connect('url_tracker.db')
    cursor = conn.cursor()
    
    # Add new columns
    try:
        cursor.execute('ALTER TABLE url_tracking ADD COLUMN discovery_date TIMESTAMP')
        print("Added discovery_date column")
    except sqlite3.OperationalError:
        print("discovery_date column already exists")

    try:
        cursor.execute('ALTER TABLE url_tracking ADD COLUMN domain_name TEXT')
        print("Added domain_name column")
    except sqlite3.OperationalError:
        print("domain_name column already exists")

    # Update domain names for existing URLs
    cursor.execute('''
        UPDATE url_tracking 
        SET domain_name = SUBSTR(url, INSTR(url, '://') + 3, 
            CASE 
                WHEN INSTR(SUBSTR(url, INSTR(url, '://') + 3), '/') = 0 
                THEN LENGTH(SUBSTR(url, INSTR(url, '://') + 3))
                ELSE INSTR(SUBSTR(url, INSTR(url, '://') + 3), '/') - 1
            END)
        WHERE domain_name IS NULL
    ''')
    print("Updated domain names for existing URLs")

    # Add indexes
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_url_tracking_url ON url_tracking(url)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_url_tracking_sitemap ON url_tracking(sitemap_url)')
    print("Added indexes")

    conn.commit()
    conn.close()

def update_urls_analysis_db():
    """Update urls_analysis.db schema."""
    print("\nUpdating urls_analysis.db...")
    conn = sqlite3.connect('urls_analysis.db')
    cursor = conn.cursor()
    
    # Add new columns
    try:
        cursor.execute('ALTER TABLE urls ADD COLUMN last_analyzed TIMESTAMP')
        print("Added last_analyzed column")
    except sqlite3.OperationalError:
        print("last_analyzed column already exists")

    try:
        cursor.execute('ALTER TABLE urls ADD COLUMN analysis_version TEXT')
        print("Added analysis_version column")
    except sqlite3.OperationalError:
        print("analysis_version column already exists")

    # Add indexes
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_urls_url ON urls(url)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain_name)')
    print("Added indexes")

    conn.commit()
    conn.close()

def main():
    print("Starting database updates...")
    
    try:
        # Backup first
        backup_databases()
        
        # Update schemas
        update_url_tracker_db()
        update_urls_analysis_db()
        
        print("\nDatabase updates completed successfully!")
        
    except Exception as e:
        print(f"\nError during update: {str(e)}")
        print("Please restore from backup if needed.")

if __name__ == "__main__":
    main()