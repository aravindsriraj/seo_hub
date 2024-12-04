
import sqlite3
import pandas as pd
from datetime import datetime
import numpy as np

def analyze_content_changes():
    """Analyze patterns of content changes across both databases."""
    
    # Connect to databases
    tracker_conn = sqlite3.connect('url_tracker.db')
    analysis_conn = sqlite3.connect('urls_analysis.db')
    
    print("\n=== Content Change Analysis ===\n")
    
    # First verify table names in each database
    def get_table_names(conn):
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return [table[0] for table in cursor.fetchall()]
    
    print("Tables in url_tracker.db:", get_table_names(tracker_conn))
    print("Tables in urls_analysis.db:", get_table_names(analysis_conn))
    
    # 1. Basic Update Statistics
    print("\n1. Overall Update Patterns:")
    query = """
    SELECT 
        domain_name,
        COUNT(*) as total_urls,
        COUNT(CASE WHEN date_modified IS NOT NULL 
                   AND date_modified != date_published THEN 1 END) as modified_urls,
        ROUND(AVG(CASE 
            WHEN date_modified IS NOT NULL AND date_published IS NOT NULL 
            THEN JULIANDAY(date_modified) - JULIANDAY(date_published) 
        END)) as avg_days_between_updates
    FROM url_tracking
    WHERE domain_name IS NOT NULL
    GROUP BY domain_name
    """
    df = pd.read_sql_query(query, tracker_conn)
    df['update_percentage'] = (df['modified_urls'] / df['total_urls'] * 100).round(2)
    print(df)
    
    # 2. Update Frequency Distribution
    print("\n2. Update Timing Distribution:")
    query = """
    SELECT 
        domain_name,
        SUM(CASE WHEN JULIANDAY(date_modified) - JULIANDAY(date_published) <= 7 THEN 1 END) as within_week,
        SUM(CASE WHEN JULIANDAY(date_modified) - JULIANDAY(date_published) BETWEEN 8 AND 30 THEN 1 END) as within_month,
        SUM(CASE WHEN JULIANDAY(date_modified) - JULIANDAY(date_published) BETWEEN 31 AND 90 THEN 1 END) as within_quarter,
        SUM(CASE WHEN JULIANDAY(date_modified) - JULIANDAY(date_published) > 90 THEN 1 END) as after_quarter
    FROM url_tracking
    WHERE date_modified IS NOT NULL AND date_published IS NOT NULL
    GROUP BY domain_name
    """
    print(pd.read_sql_query(query, tracker_conn))
    
    # 3. Recent Activity Analysis
    print("\n3. Recent Update Activity (Last 90 Days):")
    query = """
    SELECT 
        domain_name,
        COUNT(CASE WHEN date_modified >= date('now', '-30 days') THEN 1 END) as last_30_days,
        COUNT(CASE WHEN date_modified >= date('now', '-60 days') THEN 1 END) as last_60_days,
        COUNT(CASE WHEN date_modified >= date('now', '-90 days') THEN 1 END) as last_90_days
    FROM url_tracking
    GROUP BY domain_name
    """
    print(pd.read_sql_query(query, tracker_conn))
    
    # 4. Multiple Updates Analysis
    print("\n4. Articles with Multiple Updates:")
    query = """
    SELECT 
        COUNT(*) as total_articles,
        SUM(CASE WHEN date_modified > date_published THEN 1 END) as updated_once,
        AVG(CASE 
            WHEN date_modified IS NOT NULL AND date_published IS NOT NULL 
            THEN JULIANDAY(date_modified) - JULIANDAY(date_published) 
        END) as avg_days_to_first_update
    FROM url_tracking
    """
    print(pd.read_sql_query(query, tracker_conn))
    
    # 5. Storage Impact Analysis
    print("\n5. Current Database Status:")
    tables_info = {}
    
    for conn, db_name in [(tracker_conn, 'url_tracker.db'), (analysis_conn, 'urls_analysis.db')]:
        tables = get_table_names(conn)
        tables_info[db_name] = {}
        
        for table in tables:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            tables_info[db_name][table] = count
    
    print("\nRow counts by table:")
    for db_name, tables in tables_info.items():
        print(f"\n{db_name}:")
        for table, count in tables.items():
            print(f"  {table}: {count} rows")
    
    tracker_conn.close()
    analysis_conn.close()

if __name__ == "__main__":
    analyze_content_changes()
