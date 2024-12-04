import sqlite3
from bs4 import BeautifulSoup
import requests
from urllib.parse import urlparse
from datetime import datetime
import json
import time
from typing import Dict, Optional, Tuple
import sys
from tqdm import tqdm

class DatabaseUpdater:
    def __init__(self):
        self.tracker_db_path = 'url_tracker.db'
        self.analysis_db_path = 'urls_analysis.db'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0

    def get_dates_from_webpage(self, url: str) -> Dict[str, Optional[str]]:
        """Extract dates from webpage with improved JSON-LD parsing."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            dates = {'published': None, 'modified': None}

            # Check JSON-LD
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and '@graph' in data:
                        for item in data['@graph']:
                            if item.get('@type') in ['Article', 'WebPage']:
                                if 'datePublished' in item:
                                    dates['published'] = item['datePublished']
                                if 'dateModified' in item:
                                    dates['modified'] = item['dateModified']
                                if dates['published'] and dates['modified']:
                                    break

                except json.JSONDecodeError:
                    continue

            # Check meta tags if no dates found
            if not any(dates.values()):
                meta_tags = soup.find_all('meta')
                for meta in meta_tags:
                    if meta.get('property') in ['article:published_time', 'og:published_time'] or \
                       meta.get('name') in ['published_time', 'date']:
                        dates['published'] = meta.get('content')
                    elif meta.get('property') in ['article:modified_time', 'og:modified_time'] or \
                         meta.get('name') in ['modified_time', 'last-modified']:
                        dates['modified'] = meta.get('content')

            # Check blog info div (selectstar.com style)
            if not any(dates.values()):
                blog_info = soup.find('div', class_='blog-info__text')
                if blog_info:
                    try:
                        date_text = blog_info.get_text().strip()
                        parsed_date = datetime.strptime(date_text, '%B %d, %Y')
                        dates['published'] = parsed_date.strftime('%Y-%m-%d')
                    except ValueError:
                        pass

            # Standardize dates
            standardized_dates = {}
            for key, value in dates.items():
                if value:
                    try:
                        if 'T' in value or '+' in value or 'Z' in value:
                            clean_value = value.replace('Z', '+00:00')
                            if '.' in clean_value:
                                clean_value = clean_value.split('.')[0] + '+00:00'
                            dt = datetime.fromisoformat(clean_value)
                            standardized_dates[key] = dt.strftime('%Y-%m-%d')
                        else:
                            for fmt in ['%Y-%m-%d', '%B %d, %Y', '%Y/%m/%d']:
                                try:
                                    dt = datetime.strptime(value, fmt)
                                    standardized_dates[key] = dt.strftime('%Y-%m-%d')
                                    break
                                except ValueError:
                                    continue
                    except Exception:
                        standardized_dates[key] = None
                else:
                    standardized_dates[key] = None

            # Apply fallback: use modified as published if published not found
            if not standardized_dates.get('published') and standardized_dates.get('modified'):
                standardized_dates['published'] = standardized_dates['modified']

            return standardized_dates

        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
            return {'published': None, 'modified': None}

    def update_url_tracker(self) -> None:
        """Update url_tracker database with dates and status."""
        conn = sqlite3.connect(self.tracker_db_path)
        cursor = conn.cursor()

        # Get total count for progress bar
        cursor.execute("SELECT COUNT(*) FROM url_tracking")
        total_urls = cursor.fetchone()[0]

        # Get all URLs
        cursor.execute("SELECT id, url FROM url_tracking")
        urls = cursor.fetchall()

        print(f"\nProcessing {total_urls} URLs in url_tracker.db")
        for id, url in tqdm(urls, total=total_urls):
            try:
                current_time = datetime.now().isoformat()
                
                # Extract dates
                dates = self.get_dates_from_webpage(url)
                
                # Determine status
                if dates['published'] or dates['modified']:
                    status = 'date_found'
                    self.success_count += 1
                else:
                    status = 'date_not_found'
                    self.skipped_count += 1

                # Update record
                cursor.execute("""
                    UPDATE url_tracking 
                    SET date_published = COALESCE(?, date_published),
                        date_modified = COALESCE(?, date_modified),
                        last_checked = ?,
                        status = ?,
                        domain_name = ?
                    WHERE id = ?
                """, (
                    dates['published'],
                    dates['modified'],
                    current_time,
                    status,
                    urlparse(url).netloc,
                    id
                ))
                
                conn.commit()
                self.processed_count += 1

            except Exception as e:
                print(f"Error updating {url}: {str(e)}")
                self.error_count += 1
                cursor.execute("""
                    UPDATE url_tracking 
                    SET last_checked = ?,
                        status = 'error'
                    WHERE id = ?
                """, (current_time, id))
                conn.commit()

            # Sleep to prevent overwhelming servers
            time.sleep(0.5)

        conn.close()

    def update_analysis_db(self) -> None:
        """Update urls_analysis database with dates and status."""
        conn = sqlite3.connect(self.analysis_db_path)
        cursor = conn.cursor()

        # Get total count for progress bar
        cursor.execute("SELECT COUNT(*) FROM urls")
        total_urls = cursor.fetchone()[0]

        # Get all URLs
        cursor.execute("SELECT id, url FROM urls")
        urls = cursor.fetchall()

        print(f"\nProcessing {total_urls} URLs in urls_analysis.db")
        for id, url in tqdm(urls, total=total_urls):
            try:
                current_time = datetime.now().isoformat()
                
                # Extract dates
                dates = self.get_dates_from_webpage(url)
                
                # Determine status
                if dates['published'] or dates['modified']:
                    status = 'date_found'
                    self.success_count += 1
                else:
                    status = 'date_not_found'
                    self.skipped_count += 1

                # Update record
                cursor.execute("""
                    UPDATE urls 
                    SET datePublished = COALESCE(?, datePublished),
                        dateModified = COALESCE(?, dateModified),
                        last_analyzed = ?,
                        status = ?
                    WHERE id = ?
                """, (
                    dates['published'],
                    dates['modified'],
                    current_time,
                    status,
                    id
                ))
                
                conn.commit()
                self.processed_count += 1

            except Exception as e:
                print(f"Error updating {url}: {str(e)}")
                self.error_count += 1
                cursor.execute("""
                    UPDATE urls 
                    SET last_analyzed = ?,
                        status = 'error'
                    WHERE id = ?
                """, (current_time, id))
                conn.commit()

            # Sleep to prevent overwhelming servers
            time.sleep(0.5)

        conn.close()

    def print_summary(self) -> None:
        """Print summary of updates."""
        print("\nUpdate Summary:")
        print(f"Total Processed: {self.processed_count}")
        print(f"Successful Updates: {self.success_count}")
        print(f"Skipped (No Dates): {self.skipped_count}")
        print(f"Errors: {self.error_count}")

    def run_update(self) -> None:
        """Run the complete database update."""
        start_time = time.time()
        
        print("Starting database update...")
        print("This will update all URLs in both databases.")
        confirm = input("Continue? (y/n): ")
        
        if confirm.lower() != 'y':
            print("Update cancelled.")
            return

        try:
            print("\nUpdating url_tracker database...")
            self.update_url_tracker()

            print("\nUpdating urls_analysis database...")
            self.update_analysis_db()

            duration = time.time() - start_time
            print(f"\nUpdate completed in {duration:.2f} seconds")
            self.print_summary()

        except Exception as e:
            print(f"Critical error during update: {str(e)}")
            print("Please check database integrity.")

if __name__ == "__main__":
    updater = DatabaseUpdater()
    updater.run_update()
