from datetime import datetime, timedelta
import time
from typing import Dict
import streamlit as st
from seo_hub.data.sitemap_manager import SitemapManager
from urllib.parse import urlparse

class SitemapView:
    def __init__(self):
        self.sitemap_manager = SitemapManager()
    
    def render(self):
        """Render the sitemap management section."""
        with st.sidebar:
            st.subheader("Sitemap Management")
            
            # Show existing sitemaps with selection
            st.write("### Select Sitemaps to Process")
            sitemaps = self.sitemap_manager.get_sitemaps()
            
            # Create selection box for each sitemap
            selected_sitemaps = []
            for sitemap in sitemaps:
                if st.checkbox(sitemap['sitemap_url'], key=f"select_{sitemap['sitemap_url']}"):
                    selected_sitemaps.append(sitemap)
                st.caption(f"Last processed: {sitemap['last_processed']}")
            
            # Process selected sitemaps button
            if st.button("Update Selected Sitemaps"):
                if not selected_sitemaps:
                    st.warning("Please select at least one sitemap to process")
                    return
                
                st.write("🔄 Starting sitemap processing...")
                start_time = time.time()
                
                # Create containers for progress
                stats_container = st.empty()
                progress_bar = st.progress(0)
                current_url = st.empty()
                
                # Process each selected sitemap
                for sitemap in selected_sitemaps:
                    sitemap_url = sitemap['sitemap_url']
                    domain = sitemap_url.split('/')[2]  # Get domain from URL
                    
                    st.write(f"Processing sitemap: {sitemap_url}")
                    
                    # Process sitemap and show results
                    results = self.sitemap_manager.process_sitemap(
                        sitemap_url,
                        status_container=stats_container,
                        start_time=start_time
                    )
                    
                    # Show completion for this sitemap
                    st.success(f"""
                    ✅ Completed {domain}:
                    - Total URLs: {results.get('total_urls', 0)}
                    - New URLs: {results.get('new_urls', 0)}
                    - Updated: {results.get('updated_urls', 0)}
                    - Skipped: {results.get('skipped_urls', 0)}
                    - Failed: {results.get('failed_urls', 0)}
                    """)
                
                # Clear progress displays
                progress_bar.empty()
                current_url.empty()
                stats_container.empty()