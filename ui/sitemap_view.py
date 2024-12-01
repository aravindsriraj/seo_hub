import streamlit as st
from data.sitemap_manager import SitemapManager

class SitemapView:
    def __init__(self):
        self.sitemap_manager = SitemapManager()
    
    def render(self):
        """Render the sitemap management section in the sidebar."""
        with st.sidebar:
            st.subheader("Sitemap Management")
            
            # Add new sitemap
            with st.expander("Add New Sitemap"):
                new_sitemap = st.text_input("Sitemap URL")
                if st.button("Add Sitemap"):
                    if new_sitemap:
                        if self.sitemap_manager.add_sitemap(new_sitemap):
                            st.success(f"Added sitemap: {new_sitemap}")
                        else:
                            st.error("Failed to add sitemap")
            
            # Show existing sitemaps
            st.write("### Tracked Sitemaps")
            sitemaps = self.sitemap_manager.get_sitemaps()
            for sitemap in sitemaps:
                st.write(f"- {sitemap['sitemap_url']}")
                st.write(f"  Last processed: {sitemap['last_processed']}")
            
            # Process sitemaps button
            if st.button("Update All Sitemaps"):
                with st.spinner("Processing sitemaps..."):
                    results = self.sitemap_manager.process_all_sitemaps()
                    
                    # Show results
                    st.write("### Processing Results")
                    for result in results:
                        stats = result['stats']
                        st.write(f"Sitemap: {result['sitemap_url']}")
                        st.write(f"- Total URLs: {stats['total_urls']}")
                        st.write(f"- New URLs: {stats['new_urls']}")
                        st.write(f"- Updated URLs: {stats['updated_urls']}")
                        st.write(f"- Errors: {stats['errors']}")
                        st.write(f"- Duration: {stats['duration']}")