import sqlite3
import requests
from bs4 import BeautifulSoup
import json
import random
from datetime import datetime
import streamlit as st
from seo_hub.core.config import config

def extract_dates_from_json_ld(soup):
    """Extract dates from JSON-LD script tags."""
    dates = {'published': None, 'modified': None}
    
    # Find all JSON-LD scripts
    for script in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(script.string)
            # Handle both single object and array of objects
            if isinstance(data, list):
                data = data[0]
            
            # Check for datePublished and dateModified
            if 'datePublished' in data:
                dates['published'] = data['datePublished']
            if 'dateModified' in data:
                dates['modified'] = data['dateModified']
                
        except (json.JSONDecodeError, AttributeError):
            continue
            
    return dates

def extract_dates_from_meta(soup):
    """Extract dates from meta tags."""
    dates = {'published': None, 'modified': None}
    
    # Common meta tag patterns
    published_patterns = [
        ('meta', {'property': ['article:published_time', 'og:published_time']}),
        ('meta', {'name': ['published_time', 'article:published_time', 'publication-date']})
    ]
    
    modified_patterns = [
        ('meta', {'property': ['article:modified_time', 'og:modified_time']}),
        ('meta', {'name': ['modified_time', 'article:modified_time', 'last-modified']})
    ]
    
    # Check all published date patterns
    for tag, attrs in published_patterns:
        meta = soup.find(tag, attrs)
        if meta and meta.get('content'):
            dates['published'] = meta['content']
            break
            
    # Check all modified date patterns
    for tag, attrs in modified_patterns:
        meta = soup.find(tag, attrs)
        if meta and meta.get('content'):
            dates['modified'] = meta['content']
            break
            
    return dates

def standardize_date(date_str):
    """Convert various date formats to YYYY-MM-DD."""
    if not date_str:
        return None
        
    try:
        # Try parsing various formats
        for fmt in [
            "%Y-%m-%dT%H:%M:%S%z",  # 2022-04-18T09:00:32-04:00
            "%Y-%m-%dT%H:%M:%S.%f%z",  # With microseconds
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%B %d, %Y",
            "%d/%m/%Y",
            "%m/%d/%Y"
        ]:
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
    except Exception as e:
        st.error(f"Error standardizing date {date_str}: {str(e)}")
    return None

def fix_dates():
    """Process all URLs and fix their dates."""
    conn = sqlite3.connect(config.URLS_DB_PATH)
    cursor = conn.cursor()
    
    # Get all URLs
    cursor.execute("SELECT url FROM urls")
    urls = [row[0] for row in cursor.fetchall()]
    total_urls = len(urls)
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    results = {
        'processed': 0,
        'dates_found': 0,
        'only_modified_found': 0,
        'errors': 0
    }
    
    for idx, url in enumerate(urls):
        try:
            # Update progress every 10 URLs
            if idx % 10 == 0:
                status_text.write(f"Processing URLs: {idx}/{total_urls}")
                progress_bar.progress(idx / total_urls)
            
            # Fetch URL content
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try JSON-LD first, then meta tags
            dates = extract_dates_from_json_ld(soup)
            if not any(dates.values()):
                dates = extract_dates_from_meta(soup)
            
            # Standardize dates
            published = standardize_date(dates['published'])
            modified = standardize_date(dates['modified'])
            
            # If only modified date is found, use it for published date as well
            if modified and not published:
                published = modified
                results['only_modified_found'] += 1
            
            if published or modified:
                cursor.execute("""
                    UPDATE urls 
                    SET datePublished = ?,
                        dateModified = ?
                    WHERE url = ?
                """, (published, modified or published, url))
                conn.commit()
                results['dates_found'] += 1
            
            results['processed'] += 1
            
        except Exception as e:
            results['errors'] += 1
    
    conn.close()
    
    st.success(f"""
        Date Processing Complete:
        - Total URLs processed: {results['processed']}
        - Dates successfully updated: {results['dates_found']}
        - URLs using modified date as published: {results['only_modified_found']}
        - Errors encountered: {results['errors']}
    """)

if __name__ == "__main__":
    st.title("Fix URL Dates")
    if st.button("Process All URLs"):
        fix_dates()