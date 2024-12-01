from typing import List, Dict
from datetime import datetime
import streamlit as st
from seo_hub.data.url_tracker_db import URLTrackerDB
from seo_hub.data.xml_parser import extract_urls_from_xml
from seo_hub.data.web_scraper import WebScraper

class SitemapManager:
    def __init__(self):
        self.db = URLTrackerDB()
        self.scraper = WebScraper()
    
    def add_sitemap(self, sitemap_url: str) -> bool:
        """Add a new sitemap to track."""
        return self.db.add_sitemap(sitemap_url)
    
    def get_sitemaps(self) -> List[Dict]:
        """Get all tracked sitemaps."""
        return self.db.get_sitemaps()
    
    def process_sitemap(self, sitemap_url: str) -> Dict:
        """
        Process a single sitemap and update URLs.
        Returns stats about the processing.
        """
        stats = {
            'total_urls': 0,
            'new_urls': 0,
            'updated_urls': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
        
        try:
            # Extract URLs from sitemap
            st.write("Extracting URLs from sitemap...")
            urls = extract_urls_from_xml(sitemap_url)
            
            if not urls:
                st.warning("No URLs found in sitemap")
                return stats
            
            stats['total_urls'] = len(urls)
            st.success(f"Found {len(urls)} URLs in sitemap")

            # Process URLs using the improved WebScraper process_urls method
            processing_stats = self.scraper.process_urls(urls)
            
            # Update stats
            stats.update({
                'new_urls': processing_stats['processed'],
                'updated_urls': processing_stats['unchanged'],
                'errors': processing_stats['failed'],
                'end_time': datetime.now()
            })
            stats['duration'] = stats['end_time'] - stats['start_time']
            
            return stats
            
        except Exception as e:
            st.error(f"Error processing sitemap {sitemap_url}: {e}")
            stats['errors'] += 1
            stats['end_time'] = datetime.now()
            stats['duration'] = stats['end_time'] - stats['start_time']
            return stats
    
    def process_all_sitemaps(self) -> List[Dict]:
        """Process all tracked sitemaps and return stats."""
        sitemaps = self.get_sitemaps()
        results = []
        
        st.write("### Processing Sitemaps")
        
        for sitemap in sitemaps:
            st.write(f"\n## Processing sitemap: {sitemap['sitemap_url']}")
            stats = self.process_sitemap(sitemap['sitemap_url'])
            results.append({
                'sitemap_url': sitemap['sitemap_url'],
                'stats': stats
            })
        
        return results