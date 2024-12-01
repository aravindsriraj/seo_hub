# Development database configuration
URL_TRACKER_DB_PATH = "url_tracker.db"  # Replace with your actual path

# Tables we need
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sitemap_tracking (
    id INTEGER PRIMARY KEY,
    sitemap_url TEXT UNIQUE,
    last_processed TIMESTAMP,
    status TEXT
);

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
);
"""