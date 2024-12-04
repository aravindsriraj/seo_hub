from datetime import datetime, timedelta
import time
import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urlparse
import google.generativeai as genai
import streamlit as st
from seo_hub.core.config import config

class WebScraper:
    def __init__(self):
        genai.configure(api_key=config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(
            model_name=config.GEMINI_MODEL_NAME,
            generation_config=config.GENERATION_CONFIG
        )
        
        self.db_path = config.URLS_DB_PATH
        self.headers = config.REQUEST_HEADERS
        self.base_delay = 5

    def _init_db_schema(self):
        """Ensure database has required columns."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Add last_processed column if it doesn't exist
            cursor.execute("""
                ALTER TABLE urls 
                ADD COLUMN last_processed TIMESTAMP
            """)
        except sqlite3.OperationalError:
            # Column already exists
            pass
            
        conn.commit()
        conn.close()

    def extract_dates_from_json_ld(self, soup):
        """Extract dates from JSON-LD script tags."""
        try:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string)
                    # Handle both single object and array of objects
                    if isinstance(data, list):
                        data = data[0]
                    
                    date_published = data.get('datePublished')
                    date_modified = data.get('dateModified')
                    
                    if date_published or date_modified:
                        return {
                            'published': self.standardize_date(date_published) if date_published else None,
                            'modified': self.standardize_date(date_modified) if date_modified else None
                        }
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            print(f"Error parsing JSON-LD: {e}")
        return {'published': None, 'modified': None}

    def extract_dates_from_meta(self, soup):
        """Extract dates from meta tags."""
        dates = {'published': None, 'modified': None}
        
        published_patterns = [
            ('meta', {'property': ['article:published_time', 'og:published_time']}),
            ('meta', {'name': ['published_time', 'article:published_time', 'publication-date']})
        ]
        
        modified_patterns = [
            ('meta', {'property': ['article:modified_time', 'og:modified_time']}),
            ('meta', {'name': ['modified_time', 'article:modified_time', 'last-modified']})
        ]
        
        for tag, attrs in published_patterns:
            meta = soup.find(tag, attrs)
            if meta and meta.get('content'):
                dates['published'] = meta['content']
                break
                
        for tag, attrs in modified_patterns:
            meta = soup.find(tag, attrs)
            if meta and meta.get('content'):
                dates['modified'] = meta['content']
                break
                
        return dates

    def standardize_date(self, date_str):
        """Convert various date formats to YYYY-MM-DD."""
        if not date_str:
            return None
        
        try:
            # Remove timezone info if present
            if 'T' in date_str:
                date_str = date_str.split('T')[0]
            
            # Convert to datetime and then to string
            return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
            
        except Exception as e:
            print(f"Error standardizing date {date_str}: {e}")
            return None

    def extract_content(self, url: str) -> dict:
        """Extract content and metadata from URL."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # First try JSON-LD
            dates = self.extract_dates_from_json_ld(soup)
            
            # If no dates found in JSON-LD, try meta tags
            if not dates['published'] and not dates['modified']:
                published_meta = (
                    soup.find('meta', {'property': ['article:published_time', 'og:published_time']}) or
                    soup.find('meta', {'name': 'published_time'}) or
                    soup.find('meta', {'name': 'date'}) or
                    soup.find('meta', {'property': 'article:published'})
                )
                
                modified_meta = (
                    soup.find('meta', {'property': ['article:modified_time', 'og:modified_time']}) or
                    soup.find('meta', {'name': 'modified_time'}) or
                    soup.find('meta', {'name': 'last-modified'})
                )
                
                dates['published'] = self.standardize_date(published_meta.get('content')) if published_meta else None
                dates['modified'] = self.standardize_date(modified_meta.get('content')) if modified_meta else None

            # Clean content
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()

            text = soup.get_text(separator=' ', strip=True)

            return {
                'domain_name': urlparse(url).netloc,
                'content': text[:50000],
                'estimated_word_count': len(text.split()),
                'date_published': dates['published'],
                'date_modified': dates['modified'] or dates['published'],
                'status': 'Fetched'
            }
                
        except Exception as e:
            st.error(f"Error extracting content from {url}: {str(e)}")
            return {
                'domain_name': urlparse(url).netloc,
                'content': '',
                'estimated_word_count': 0,
                'date_published': None,
                'date_modified': None,
                'status': 'Failed'
            }
        
    def _get_url_data(self, url: str) -> dict:
        """Get URL data including last processed time."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT dateModified, status, summary, category, primary_keyword, last_processed
            FROM urls 
            WHERE url = ?
        """, (url,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'dateModified': result[0],
                'status': result[1],
                'summary': result[2],
                'category': result[3],
                'primary_keyword': result[4],
                'last_processed': result[5]
            }
        return None

    def set_url_status_pending(self, url: str):
        """Set URL status to pending in database."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # First check if URL exists
        cursor.execute("SELECT 1 FROM urls WHERE url = ?", (url,))
        exists = cursor.fetchone() is not None
        
        if exists:
            cursor.execute("""
                UPDATE urls 
                SET status = 'Pending'
                WHERE url = ?
            """, (url,))
        else:
            # If URL doesn't exist, insert it with Pending status
            cursor.execute("""
                INSERT INTO urls (url, domain_name, status)
                VALUES (?, ?, 'Pending')
            """, (url, urlparse(url).netloc))
        
        conn.commit()
        conn.close()

    def analyze_with_gemini(self, content: str, url: str, max_retries=3) -> dict:
        """Analyze content using Gemini API with explicit status setting."""
        for attempt in range(max_retries):
            delay = self.base_delay * (2 ** attempt)
            time.sleep(delay)
            
            try:
                chat = self.model.start_chat(history=[])
                prompt = f"""
                Analyze this webpage content from {url}. The content is:
                {content[:5000]}

                Provide ONLY these three pieces of information in this exact format:
                Summary: <Write a concise 2-3 sentence summary of what this page is about>
                Category: <One of: Product Page, Comparison Page, Integration Page, Educational Page, Blog Post, News Article, Documentation, Other>
                Primary Keyword: <Extract the single most important keyword or term this content focuses on>
                """

                response = chat.send_message(prompt)
                
                lines = response.text.strip().split('\n')
                summary = next((line.split(': ', 1)[1] for line in lines if line.startswith('Summary')), 'N/A')
                category = next((line.split(': ', 1)[1] for line in lines if line.startswith('Category')), 'Other')
                keyword = next((line.split(': ', 1)[1] for line in lines if line.startswith('Primary Keyword')), 'N/A')

                return {
                    'summary': summary.strip(),
                    'category': category.strip(),
                    'primary_keyword': keyword.strip(),
                    'status': 'Processed'  # Explicitly set to Processed
                }

            except Exception as e:
                error_msg = str(e)
                st.error(f"Attempt {attempt + 1} failed: {error_msg}")
                if attempt < max_retries - 1 and "429" in error_msg:
                    st.warning(f"Rate limit hit. Retrying in {delay} seconds...")
                    continue
                return {
                    'summary': 'Error during analysis',
                    'category': 'Error',
                    'primary_keyword': 'N/A',
                    'status': 'Failed'  # Explicitly set to Failed
                }

    def process_url(self, url: str, sitemap_url: str = None) -> dict:
        """Process URL and update both tracking and analysis databases."""
        try:
            print("\nPROCESSING URL:", url)
            
            # Step 1: Extract content and metadata
            print("Step 1: Extracting content...")
            metadata = self.extract_content(url)
            
            if metadata['content']:
                print("Content extracted successfully")
                print(f"Word count: {metadata.get('estimated_word_count')}")
                print(f"Dates found - Published: {metadata.get('date_published')}, Modified: {metadata.get('date_modified')}")
                
                # Step 2: Get Gemini analysis
                print("\nStep 2: Getting Gemini analysis...")
                analysis = self.analyze_with_gemini(metadata['content'], url)
                print("Analysis completed")
                
                # Step 3: Update url_tracker.db
                print("\nStep 3: Updating URL tracker database...")
                tracking_success = self.url_tracker.update_url(
                    url=url,
                    sitemap_url=sitemap_url,
                    word_count=metadata.get('estimated_word_count', 0),
                    date_published=metadata.get('date_published'),
                    date_modified=metadata.get('date_modified')
                )
                print(f"Tracking database update: {'Success' if tracking_success else 'Failed'}")
                
                # Step 4: Update urls_analysis.db
                print("\nStep 4: Updating analysis database...")
                print("Inserting URL into analysis database...")
                analysis_insert = db_ops.insert_urls([(url, metadata['domain_name'])])
                print(f"URL insert result: {analysis_insert}")
                
                print("Updating with analysis results...")
                analysis_success = db_ops.update_url_analysis(
                    url_id=analysis_insert,
                    summary=analysis.get('summary', 'N/A'),
                    category=analysis.get('category', 'Other'),
                    primary_keyword=analysis.get('primary_keyword', 'N/A'),
                    status='Processed'
                )
                print(f"Analysis database update: {'Success' if analysis_success else 'Failed'}")
                
                return {
                    'status': 'processed',
                    'tracking_success': tracking_success,
                    'analysis_success': analysis_success
                }
                
            else:
                print("No content could be extracted")
                return {
                    'status': 'failed',
                    'error': 'No content extracted'
                }
                
        except Exception as e:
            print(f"Error processing URL: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }
    def process_urls(self, urls: list) -> dict:
        """Process URLs with clean, concise status display."""
        total_urls = len(urls)
        
        # Create single container for all status information
        status_display = st.empty()
        
        # Initialize stats
        stats = {
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'pending': total_urls,
            'current_url': None,
            'last_status': None
        }
        
        def update_status_display():
            """Update the status display in a single container."""
            completed = stats['processed'] + stats['failed'] + stats['skipped']
            progress = completed / total_urls
            
            # Create status message
            status_md = f"""
            ### URL Processing Status ({completed}/{total_urls})
            {'▓' * int(progress * 20)}{'░' * (20 - int(progress * 20))} {progress:.1%}
            
            **Currently Processing:** {stats['current_url'] or 'N/A'}
            **Last Action:** {stats['last_status'] or 'Starting...'}
            
            ✅ Processed: {stats['processed']} | ⏭️ Skipped: {stats['skipped']} | ❌ Failed: {stats['failed']}
            """
            status_display.markdown(status_md)
        
        # Initial display
        update_status_display()
        
        for idx, url in enumerate(urls, 1):
            try:
                stats['current_url'] = url
                stats['last_status'] = "Checking URL status..."
                update_status_display()
                
                # Process URL
                result = self.process_url(url)
                
                # Update stats based on result
                if result.get('skip_update'):
                    stats['skipped'] += 1
                    stats['pending'] -= 1
                    stats['last_status'] = f"⏭️ Skipped: {result.get('skip_reason', 'Unknown reason')}"
                elif result['status'] == 'Failed':
                    stats['failed'] += 1
                    stats['pending'] -= 1
                    stats['last_status'] = f"❌ Failed: {result.get('summary', 'Unknown error')}"
                else:
                    stats['processed'] += 1
                    stats['pending'] -= 1
                    stats['last_status'] = f"✅ Processed ({result.get('process_reason', 'Unknown reason')}): {result.get('category', 'N/A')}"
                
                # Update database if needed
                if not result.get('skip_update'):
                    self.update_database(url, result)
                
                update_status_display()
                
            except Exception as e:
                stats['failed'] += 1
                stats['pending'] -= 1
                stats['last_status'] = f"❌ Error: {str(e)}"
                update_status_display()
        
        # Final summary
        status_display.markdown(f"""
        ## Processing Complete
        
        Total URLs processed: {total_urls}
        - ✅ Successfully processed: {stats['processed']}
        - ⏭️ Skipped: {stats['skipped']}
        - ❌ Failed: {stats['failed']}
        """)
        
        return stats

    def update_database(self, url: str, data: dict):
        """Update URL data in database with processing timestamp."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        current_time = datetime.now().isoformat()
        
        cursor.execute("""
            INSERT OR REPLACE INTO urls (
                url, domain_name, status, summary, category, primary_keyword,
                estimated_word_count, datePublished, dateModified, last_processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            url,
            data['domain_name'],
            data['status'],
            data.get('summary', 'N/A'),
            data.get('category', 'Other'),
            data.get('primary_keyword', 'N/A'),
            data['estimated_word_count'],
            data['date_published'],
            data['date_modified'],
            current_time
        ))
        
        conn.commit()
        conn.close()

        """Update URL data in database."""
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # First check if URL exists
            cursor.execute("SELECT 1 FROM urls WHERE url = ?", (url,))
            exists = cursor.fetchone() is not None
            
            if exists:
                cursor.execute("""
                    UPDATE urls 
                    SET domain_name = ?,
                        status = ?,
                        summary = ?,
                        category = ?,
                        primary_keyword = ?,
                        estimated_word_count = ?,
                        datePublished = ?,
                        dateModified = ?
                    WHERE url = ?
                """, (
                    data['domain_name'],
                    data['status'],
                    data.get('summary', 'N/A'),
                    data.get('category', 'Other'),
                    data.get('primary_keyword', 'N/A'),
                    data['estimated_word_count'],
                    data['date_published'],
                    data['date_modified'],
                    url
                ))
            else:
                cursor.execute("""
                    INSERT INTO urls (
                        url, domain_name, status, summary, category,
                        primary_keyword, estimated_word_count,
                        datePublished, dateModified
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    url,
                    data['domain_name'],
                    data['status'],
                    data.get('summary', 'N/A'),
                    data.get('category', 'Other'),
                    data.get('primary_keyword', 'N/A'),
                    data['estimated_word_count'],
                    data['date_published'],
                    data['date_modified']
                ))
            
            conn.commit()
            
        except Exception as e:
            st.error(f"Database error for {url}: {str(e)}")
            conn.rollback()
        finally:
            conn.close()