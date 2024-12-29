from datetime import datetime, timedelta
import time, re, requests
from typing import Optional
from bs4 import BeautifulSoup, Comment
import json
from urllib.parse import urlparse
import google.generativeai as genai
import streamlit as st
from core.config import config
from data.operations import db_ops
from ratelimit import limits, sleep_and_retry

class WebScraper:
    def __init__(self):
        genai.configure(api_key=config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(
            model_name=config.GEMINI_MODEL_NAME,
            generation_config=config.GENERATION_CONFIG
        )
        self.headers = config.REQUEST_HEADERS
        self.base_delay = 5  # Base delay for rate limiting
        self.analysis_version = "1.0"  # Track analysis version

    @sleep_and_retry
    @limits(calls=30, period=60)  # Rate limit: 30 calls per minute
    def extract_content(self, url: str) -> dict:
        """Extract content with improved handling and rate limiting."""
        try:
            print(f"Making request to: {url}")  # Debug
            response = requests.get(url, headers=self.headers, timeout=10)
            print(f"Response status code: {response.status_code}")  # Debug
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract dates with improved parsing
            print("Extracting dates...")  # Debug
            dates = self._extract_dates(soup)
            
            # Clean content more thoroughly
            print("Cleaning content...")  # Debug
            content = self._clean_content(soup)
            
            # Calculate word count more accurately
            print("Calculating word count...")  # Debug
            word_count = self._calculate_word_count(content)

            return {
                'domain_name': urlparse(url).netloc,
                'content': content,
                'estimated_word_count': word_count,
                'date_published': dates['published'],
                'date_modified': dates['modified'],
                'status': 'Fetched',
                'extraction_timestamp': datetime.now().isoformat()
            }
            
        except requests.RequestException as e:
            print(f"Request error for {url}: {str(e)}")  # Debug
            return self._generate_error_response(url, "request_error")
        except Exception as e:
            print(f"Error extracting content from {url}: {str(e)}")  # Debug
            return self._generate_error_response(url, "extraction_error")
    
    def _extract_dates_from_meta(self, soup: BeautifulSoup) -> dict:
        """Extract dates from meta tags."""
        dates = {'published': None, 'modified': None}
        
        # Try published date (fix the attribute specifications)
        published = (
            soup.find('meta', attrs={'property': 'article:published_time'})
            or soup.find('meta', attrs={'property': 'og:published_time'})
            or soup.find('meta', attrs={'name': 'published_time'})
            or soup.find('meta', attrs={'name': 'date:published'})
        )
        if published:
            dates['published'] = self._standardize_date(published.get('content'))
        
        # Try modified date (fix the attribute specifications)
        modified = (
            soup.find('meta', attrs={'property': 'article:modified_time'})
            or soup.find('meta', attrs={'property': 'og:modified_time'})
            or soup.find('meta', attrs={'name': 'modified_time'})
            or soup.find('meta', attrs={'name': 'date:modified'})
        )
        if modified:
            dates['modified'] = self._standardize_date(modified.get('content'))
            
        # If no published date but modified exists, use modified as published
        if not dates['published'] and dates['modified']:
            print(f"No published date found, using modified date: {dates['modified']}")  # Debug
            dates['published'] = dates['modified']
        
        return dates

    def _extract_dates_from_html(self, soup: BeautifulSoup) -> dict:
        """Extract dates from HTML elements."""
        dates = {'published': None, 'modified': None}
        
        # Look for common date-containing elements
        date_elements = [
            # Common class names for date elements
            soup.find(class_='blog-info__text'),
            soup.find(class_='date-ttle'),
            soup.find(class_='post-date'),
            soup.find(class_='article-date'),
            soup.find(class_='publish-date'),
            soup.find(class_='blog-hero_content-info'),
            # Add any other common class names
        ]

        # Try to find a date in any of these elements
        for element in date_elements:
            if element and element.string:
                try:
                    # Try to parse the date string
                    date_str = element.string.strip()
                    parsed_date = datetime.strptime(date_str, '%B %d, %Y')
                    # If we found a valid date and don't have a published date yet
                    if not dates['published']:
                        dates['published'] = parsed_date.strftime('%Y-%m-%d')
                        print(f"Found published date in HTML element: {dates['published']}")
                except ValueError:
                    continue
        
        return dates

    def _extract_dates(self, soup: BeautifulSoup) -> dict:
        """Extract all possible dates before making final determination."""
        dates = {
            'published': None,
            'modified': None
        }
        
        # 1. Try JSON-LD first
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                # Handle array of items in @graph
                if isinstance(data, dict) and '@graph' in data:
                    for item in data['@graph']:
                        if isinstance(item, dict):
                            if 'datePublished' in item and not dates['published']:
                                dates['published'] = self._standardize_date(item['datePublished'])
                                print(f"Found published date in JSON-LD @graph: {dates['published']}")
                            if 'dateModified' in item and not dates['modified']:
                                dates['modified'] = self._standardize_date(item['dateModified'])
                                print(f"Found modified date in JSON-LD @graph: {dates['modified']}")
                
                # Handle direct properties
                elif isinstance(data, dict):
                    if 'datePublished' in data and not dates['published']:
                        dates['published'] = self._standardize_date(data['datePublished'])
                        print(f"Found published date in JSON-LD: {dates['published']}")
                    if 'dateModified' in data and not dates['modified']:
                        dates['modified'] = self._standardize_date(data['dateModified'])
                        print(f"Found modified date in JSON-LD: {dates['modified']}")
            except json.JSONDecodeError:
                continue

        # 2. Try meta tags if still missing dates
        if not dates['published'] or not dates['modified']:
            meta_dates = self._extract_dates_from_meta(soup)
            if not dates['published']:
                dates['published'] = meta_dates.get('published')
            if not dates['modified']:
                dates['modified'] = meta_dates.get('modified')

        # 3. Try HTML elements if still missing dates
        if not dates['published'] or not dates['modified']:
            html_dates = self._extract_dates_from_html(soup)
            if not dates['published']:
                dates['published'] = html_dates.get('published')
            if not dates['modified']:
                dates['modified'] = html_dates.get('modified')

        # 4. Only use modified as published if no published date found
        if not dates['published'] and dates['modified']:
            print(f"No published date found, using modified date: {dates['modified']}")
            dates['published'] = dates['modified']

        # Debug output
        print(f"Final dates extracted - Published: {dates['published']}, Modified: {dates['modified']}")
        
        return dates

    def _clean_content(self, soup: BeautifulSoup) -> str:
        """Improved content cleaning."""
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 
                           'aside', 'form', 'iframe']):
            element.decompose()

        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Get text with better spacing
        text = ' '.join(soup.stripped_strings)
        
        # Clean up whitespace
        text = ' '.join(text.split())
        
        return text

    def _calculate_word_count(self, content: str) -> int:
        """More accurate word count calculation."""
        # Remove special characters
        content = re.sub(r'[^\w\s]', ' ', content)
        
        # Split by whitespace and filter empty strings
        words = [word for word in content.split() if word.strip()]
        
        return len(words)
    @sleep_and_retry
    @limits(calls=10, period=60)  # Rate limit: 10 calls per minute
    
    def _parse_gemini_response(self, response_text: str) -> dict:
        """Parse Gemini response with error handling."""
        try:
            lines = response_text.strip().split('\n')
            result = {}
            
            for line in lines:
                if ':' in line:
                    key, value = line.split(':', 1)
                    result[key.strip().lower()] = value.strip()

            return {
                'summary': result.get('summary', 'N/A'),
                'category': result.get('category', 'Other'),
                'primary_keyword': result.get('primary keyword', 'N/A'),
                'status': 'Processed'
            }
            
        except Exception as e:
            st.error(f"Error parsing Gemini response: {str(e)}")
            return {
                'summary': 'Error parsing response',
                'category': 'Error',
                'primary_keyword': 'N/A',
                'status': 'Failed'
            }

    def _generate_error_response(self, url: str, error_type: str) -> dict:
        """Generate consistent error response."""
        return {
            'domain_name': urlparse(url).netloc,
            'content': '',
            'estimated_word_count': 0,
            'date_published': None,
            'date_modified': None,
            'status': 'Failed',
            'error_type': error_type,
            'error_timestamp': datetime.now().isoformat()
        }

    def _standardize_date(self, date_str: str) -> Optional[str]:
        """Standardize date format with better handling."""
        if not date_str:
            return None

        try:
            # Handle ISO format with timezone
            if 'T' in date_str or '+' in date_str or 'Z' in date_str:
                clean_value = date_str.replace('Z', '+00:00')
                if '.' in clean_value:
                    clean_value = clean_value.split('.')[0] + '+00:00'
                dt = datetime.fromisoformat(clean_value)
                return dt.strftime('%Y-%m-%d')

            # Try various date formats
            for fmt in [
                '%Y-%m-%d',
                '%B %d, %Y',
                '%b %d, %Y',
                '%Y/%m/%d',
                '%d/%m/%Y',
                '%m/%d/%Y'
            ]:
                try:
                    return datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            return None

        except Exception:
            return None
