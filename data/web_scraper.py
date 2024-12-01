import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urlparse
import google.generativeai as genai
from datetime import datetime
import time
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
        dates = {'published': None, 'modified': None}
        
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    data = data[0]
                
                if 'datePublished' in data:
                    dates['published'] = data['datePublished']
                if 'dateModified' in data:
                    dates['modified'] = data['dateModified']
                    
            except (json.JSONDecodeError, AttributeError):
                continue
                
        return dates

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
            for fmt in [
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f%z",
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
        except Exception:
            return None
        return None

    def extract_content(self, url: str) -> dict:
        """Extract content and metadata with improved date handling."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract dates from JSON-LD first, then meta tags
            dates = self.extract_dates_from_json_ld(soup)
            if not any(dates.values()):
                dates = self.extract_dates_from_meta(soup)
            
            # Standardize dates
            published = self.standardize_date(dates['published'])
            modified = self.standardize_date(dates['modified'])
            
            # If only modified date is found, use it for published date
            if modified and not published:
                published = modified

            # Clean content
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()

            text = soup.get_text(separator=' ', strip=True)

            return {
                'domain_name': urlparse(url).netloc,
                'content': text[:50000],
                'estimated_word_count': len(text.split()),
                'date_published': published,
                'date_modified': modified or published,
                'status': 'Fetched'
            }

        except Exception as e:
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

    def process_url(self, url: str) -> dict:
        """Process a URL with clear logic for processing vs skipping."""
        # First check if URL exists and get its data
        existing_data = self._get_url_data(url)
        
        # Extract current content and metadata
        metadata = self.extract_content(url)
        
        # Determine if URL needs processing
        needs_processing = False
        skip_reason = None
        
        if not existing_data:
            # New URL - needs processing
            needs_processing = True
            process_reason = "New URL"
        elif existing_data['status'] == 'Pending':
            # Pending status - needs processing
            needs_processing = True
            process_reason = "Status was Pending"
        elif existing_data['status'] == 'Failed':
            # Failed status - needs reprocessing
            needs_processing = True
            process_reason = "Previous attempt Failed"
        elif metadata['date_modified'] and existing_data['dateModified']:
            # Both dates available - check if changed
            if metadata['date_modified'] != existing_data['dateModified']:
                needs_processing = True
                process_reason = "Content modified"
            else:
                skip_reason = "Content unchanged"
        else:
            # Can't determine if changed - skip if already processed
            if existing_data['status'] == 'Processed':
                skip_reason = "Already processed"
            else:
                needs_processing = True
                process_reason = "Status not Processed"
        
        # If we should skip, return existing data
        if not needs_processing:
            return {
                **metadata,
                'status': existing_data['status'],
                'summary': existing_data['summary'],
                'category': existing_data['category'],
                'primary_keyword': existing_data['primary_keyword'],
                'skip_update': True,
                'skip_reason': skip_reason
            }
        
        # If content extraction failed
        if not metadata['content']:
            metadata['status'] = 'Failed'
            return metadata
        
        # Process with Gemini
        analysis = self.analyze_with_gemini(metadata['content'], url)
        metadata.update(analysis)
        metadata['process_reason'] = process_reason
        
        return metadata

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