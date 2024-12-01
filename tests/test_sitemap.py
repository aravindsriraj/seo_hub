import streamlit as st
from typing import List, Dict
from seo_hub.data.sitemap_manager import SitemapManager
from seo_hub.data.url_tracker_db import URLTrackerDB

def test_sitemap_workflow():
    """Test the complete sitemap workflow."""
    st.write("### Testing Sitemap Management")
    db = URLTrackerDB()
    manager = SitemapManager()
    
    # Test 1: Add a test sitemap
    test_sitemap = "https://www.alation.com/post-sitemap.xml"
    st.write(f"\nTest 1: Adding sitemap {test_sitemap}")
    
    success = manager.add_sitemap(test_sitemap)
    st.write(f"Add sitemap result: {'Success' if success else 'Failed'}")
    
    # Test 2: Verify sitemap is in database
    st.write("\nTest 2: Checking stored sitemaps")
    sitemaps = manager.get_sitemaps()
    st.write("Stored sitemaps:", sitemaps)
    
    # Test 3: Process sitemap
    st.write("\nTest 3: Processing sitemap")
    stats = manager.process_sitemap(test_sitemap)
    
    st.write("Processing stats:")
    st.write(f"- Total URLs: {stats['total_urls']}")
    st.write(f"- New URLs: {stats['new_urls']}")
    st.write(f"- Updated URLs: {stats['updated_urls']}")
    st.write(f"- Errors: {stats['errors']}")
    st.write(f"- Duration: {stats['duration']}")
    
    # Test 4: Check stored URLs
    st.write("\nTest 4: Checking stored URLs")
    conn = db.db_path
    st.write(f"Database location: {conn}")
    
    urls = db.get_all_urls()
    st.write(f"Total URLs stored: {len(urls) if urls else 0}")

if __name__ == "__main__":
    st.title("Sitemap Management Test")
    if st.button("Run Tests"):
        test_sitemap_workflow()