import time
from typing import List, Tuple, Dict, Optional
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import requests
from bs4 import BeautifulSoup
import streamlit as st
from ..core.config import config
from ..data.operations import db_ops

class URLService:
    """Handles URL processing and content analysis."""
    
    @staticmethod
    def extract_domain_name(url: str) -> str:
        """Extract domain name from URL."""
        parsed_url = urlparse(url)
        return parsed_url.netloc

    @classmethod
    def extract_urls_from_xml(cls, xml_url: str) -> List[str]:
        """Extract URLs from XML sitemap."""
        try:
            response = requests.get(xml_url, headers=config.REQUEST_HEADERS)
            if response.status_code != 200:
                st.error(f"Failed to fetch XML. Status code: {response.status_code}")
                return []

            tree = ET.ElementTree(ET.fromstring(response.content))
            root = tree.getroot()
            namespace = {
                'ns': 'http://www.w3.org/2005/Atom',
                'xhtml': 'http://www.w3.org/1999/xhtml'
            }
            
            # Extract URLs from both loc tags and x-default links
            urls = [elem.text for elem in root.findall(".//ns:loc", namespace)]
            x_default_links = [
                elem.get('href') 
                for elem in root.findall(".//xhtml:link[@hreflang='x-default']", namespace)
            ]
            
            return urls + x_default_links
            
        except Exception as e:
            st.error(f"Error parsing XML: {str(e)}")
            return []

    def process_sitemap(self, sitemap_url: str) -> int:
        """Process sitemap and store URLs in database."""
        urls = self.extract_urls_from_xml(sitemap_url)
        if not urls:
            return 0
            
        url_data = [(url, self.extract_domain_name(url)) for url in urls]
        db_ops.insert_urls(url_data)
        return len(urls)

class ContentAnalyzer:
    """Handles content analysis using Gemini API."""
    
    def __init__(self):
        self.model = config.gemini_model
    
    def fetch_content(self, url: str) -> Optional[str]:
        """Fetch and clean webpage content."""
        try:
            response = requests.get(url, headers=config.REQUEST_HEADERS, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            for tag in soup(["script", "style", "meta", "noscript"]):
                tag.decompose()
            
            text = soup.get_text(separator='\n', strip=True)
            return text[:config.MAX_CONTENT_CHARS]
            
        except Exception as e:
            st.error(f"Error fetching URL {url}: {str(e)}")
            return None

    def analyze_content(self, url: str, content: str) -> Tuple[str, str, str]:
        """Analyze content using Gemini API."""
        try:
            chat_session = self.model.start_chat(history=[])
            prompt = (
                f"Analyze the following webpage content:\n\n"
                f"URL: {url}\n"
                f"Content: {content}\n\n"
                f"Provide the output in the following structured format:\n"
                f"Summary: <A concise summary of the webpage content.>\n"
                f"Category: <A single category that best describes the content.>\n"
                f"Primary Keyword: <For educational pages, provide the primary keyword.>\n"
            )
            
            response = chat_session.send_message(prompt)
            return self.parse_response(response.text)
            
        except Exception as e:
            st.error(f"Error analyzing content: {str(e)}")
            return "Error", "Error", "N/A"

    @staticmethod
    def parse_response(response_text: str) -> Tuple[str, str, str]:
        """Parse structured response from Gemini API."""
        try:
            lines = response_text.split("\n")
            summary = next((line.split(": ", 1)[1] for line in lines 
                          if line.startswith("Summary")), "N/A")
            category = next((line.split(": ", 1)[1] for line in lines 
                           if line.startswith("Category")), "Uncategorized")
            keyword = next((line.split(": ", 1)[1] for line in lines 
                          if line.startswith("Primary Keyword")), "N/A")
            
            return summary.strip(), category.strip(), keyword.strip()
            
        except Exception as e:
            st.error(f"Error parsing response: {str(e)}")
            return "N/A", "Uncategorized", "N/A"

class RankingService:
    """Handles position tracking and ranking analysis."""
    
    @classmethod
    def analyze_domain_rankings(cls, domain: str, date_range: Tuple[str, str]) -> Dict:
        """Analyze ranking positions for a domain."""
        df = db_ops.get_ranking_data([domain], date_range[0], date_range[1])
        
        return {
            "positions_1_3": len(df[df['position'].between(1, 3)]),
            "positions_4_5": len(df[df['position'].between(4, 5)]),
            "positions_6_10": len(df[df['position'].between(6, 10)]),
            "average_position": df['position'].mean(),
            "total_keywords": len(df['keyword'].unique())
        }

class LLMAnalyzer:
    """Handles LLM response analysis."""
    
    @classmethod
    def analyze_mentions(cls, model_name: str) -> Dict:
        """Analyze mention statistics for a specific model."""
        df = db_ops.get_llm_mention_data(model_name)
        
        total_mentions = df['true_count'].sum()
        total_responses = total_mentions + df['false_count'].sum()
        mention_rate = (total_mentions / total_responses * 100) if total_responses > 0 else 0
        
        return {
            "total_mentions": total_mentions,
            "total_responses": total_responses,
            "mention_rate": mention_rate,
            "daily_mentions": df
        }

class ContentProcessor:
    """Orchestrates the content processing workflow."""
    
    def __init__(self):
        self.url_service = URLService()
        self.content_analyzer = ContentAnalyzer()

    def process_pending_urls(self, batch_size: int = None) -> None:
        """Process a batch of pending URLs."""
        if batch_size is None:
            batch_size = config.URL_BATCH_SIZE
            
        urls = db_ops.get_pending_urls(batch_size)
        total_urls = len(urls)
        
        if total_urls == 0:
            st.warning("No URLs to process.")
            return
            
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for index, url_data in enumerate(urls):
            url_id, url = url_data[0], url_data[1]
            
            # Fetch and analyze content
            content = self.content_analyzer.fetch_content(url)
            if content:
                summary, category, keyword = self.content_analyzer.analyze_content(url, content)
                db_ops.update_url_analysis(url_id, summary, category, keyword)
            else:
                db_ops.update_url_analysis(url_id, "Error", "Error", "N/A", status="Failed")
            
            # Update progress
            progress = (index + 1) / total_urls
            progress_bar.progress(progress)
            status_text.text(
                f"Processing: {index + 1}/{total_urls} URLs ({progress * 100:.2f}%)"
            )
            
            time.sleep(config.PROCESS_DELAY)

# Create global instances of services
url_service = URLService()
content_analyzer = ContentAnalyzer()
ranking_service = RankingService()
llm_analyzer = LLMAnalyzer()
content_processor = ContentProcessor()