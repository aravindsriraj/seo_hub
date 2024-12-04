from datetime import datetime, timedelta  
from typing import Dict, List
import time 
from seo_hub.data.operations import db_ops
import streamlit as st
from seo_hub.data.url_tracker_db import URLTrackerDB
from seo_hub.data.web_scraper import WebScraper
from seo_hub.data.xml_parser import extract_urls_from_xml

class SitemapManager:
    def __init__(self):
        self.url_tracker = URLTrackerDB()
        self.web_scraper = WebScraper()

    def process_sitemap(self, sitemap_url: str, status_container, start_time) -> Dict:
        """Process a single sitemap with live status updates."""
        stats = {
            'total_urls': 0,
            'new_urls': 0,
            'updated_urls': 0,
            'errors': 0
        }
        
        try:
            urls = extract_urls_from_xml(sitemap_url)
            if not urls:
                status_container.warning("⚠️ No URLs found in sitemap")
                return stats
            
            stats['total_urls'] = len(urls)
            progress_bar = st.progress(0)
            current_url = st.empty()
            recent_updates = st.empty()
            
            # Process URLs with live updates
            for idx, url in enumerate(urls, 1):
                try:
                    # Update progress displays
                    progress = idx / len(urls)
                    progress_bar.progress(progress)
                    
                    elapsed = timedelta(seconds=int(time.time() - start_time))
                    current_url.markdown(f"""
                    URLs Found: {len(urls)}
                    ✓ Processed: {idx}/{len(urls)} URLs
                    Current: {url}
                    Time Elapsed: {elapsed}
                    """)
                    
                    # Process URL with Gemini analysis and database updates
                    result = self._process_single_url(url, sitemap_url)
                    
                    # Update stats based on result status
                    if result['status'] == 'success':
                        if result.get('is_new', False):
                            stats['new_urls'] += 1
                        else:
                            stats['updated_urls'] += 1
                    else:
                        stats['errors'] += 1
                        
                    # Update recent stats
                    recent_updates.markdown(f"""
                    Recent Updates:
                    ✓ Added: {stats['new_urls']} new URLs
                    🔄 Updated: {stats['updated_urls']} existing URLs
                    ⚠️ Errors: {stats['errors']} URLs
                    """)
                        
                except Exception as e:
                    stats['errors'] += 1
                    st.error(f"❌ Error processing URL {url}: {str(e)}")
                    continue
            
            return stats
                
        except Exception as e:
            st.error(f"Error processing sitemap {sitemap_url}: {str(e)}")
            return stats

    def _process_single_url(self, url: str, sitemap_url: str) -> dict:
        """Internal method to process a single URL."""
        try:
            # Extract content
            metadata = self.web_scraper.extract_content(url)
            if not metadata['content']:
                return {'status': 'failed', 'error': 'No content extracted'}
                
            # Get Gemini analysis
            analysis = self.web_scraper.analyze_with_gemini(metadata['content'], url)
            
            # Check if URL exists
            is_new = not self._url_exists(url)
            
            # Update url_tracker.db
            tracking_success = self.url_tracker.update_url(
                url=url,
                sitemap_url=sitemap_url,
                word_count=metadata.get('estimated_word_count', 0),
                date_published=metadata.get('date_published'),
                date_modified=metadata.get('date_modified')
            )
            
            # Update urls_analysis.db
            if tracking_success:
                analysis_insert = db_ops.insert_urls([(url, metadata['domain_name'])])
                if analysis_insert is not None:
                    analysis_success = db_ops.update_url_analysis(
                        url_id=None,
                        summary=analysis.get('summary', 'N/A'),
                        category=analysis.get('category', 'Other'),
                        primary_keyword=analysis.get('primary_keyword', 'N/A'),
                        status='Processed'
                    )
                    
                    return {
                        'status': 'success',
                        'is_new': is_new,
                        'tracking_update': True,
                        'analysis_update': analysis_success
                    }
            
            return {
                'status': 'failed',
                'error': 'Database update failed'
            }
            
        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e)
            }

    def _url_exists(self, url: str) -> bool:
        """Check if URL exists in tracking database."""
        info = self.url_tracker.get_url_info(url)
        return info is not None

    def _update_sitemap_status(self, sitemap_url: str, status: str):
        """Update sitemap processing status."""
        try:
            self.url_tracker.update_sitemap_status(sitemap_url, status)
        except Exception as e:
            st.error(f"Error updating sitemap status: {str(e)}")

    def get_sitemaps(self) -> List[Dict]:
        """Get list of tracked sitemaps."""
        return self.url_tracker.get_sitemaps()

    def get_url_stats(self) -> str:
        """Get current URL statistics from the database."""
        try:
            conn = sqlite3.connect(self.url_tracker.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_urls,
                    sitemap_url,
                    MAX(last_checked) as last_update
                FROM url_tracking
                GROUP BY sitemap_url
            """)
            
            stats = cursor.fetchall()
            conn.close()
            
            if not stats:
                return "No URLs found in database"
            
            results = ["**URL Counts by Sitemap:**"]
            total_urls = 0
            
            for total, sitemap, last_update in stats:
                total_urls += total
                sitemap_name = sitemap.split('//')[-1]
                results.append(f"- {sitemap_name}: {total:,} URLs")
            
            results.append(f"\n**Total URLs in Database:** {total_urls:,}")
            results.append(f"**Last Update:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return "\n".join(results)
            
        except Exception as e:
            return f"Error getting URL stats: {str(e)}"
    
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

    def parse_sitemap(self, sitemap_url: str) -> List[str]:
        """Parse sitemap and return list of URLs."""
        return extract_urls_from_xml(sitemap_url)
    
    def process_url(self, url: str, sitemap_url: str) -> dict:
        """Process URL with complete flow including analysis."""
        try:
            print("\nBEGINNING URL PROCESSING")
            print(f"URL: {url}")
            print(f"Sitemap: {sitemap_url}")
            
            # Step 1: Extract content
            print("\nStep 1: Extracting content")
            metadata = self.web_scraper.extract_content(url)
            
            if not metadata['content']:
                print("No content extracted")
                return {
                    'status': 'failed',
                    'details': 'No content could be extracted'
                }
            
            print("Content extracted successfully")
            print(f"Word count: {metadata.get('estimated_word_count')}")
            
            # Step 2: Check if URL exists in url_tracking
            existing_data = self.url_tracker.get_url_info(url)
            print(f"\nStep 2: URL exists in tracker: {existing_data is not None}")
            
            # Step 3: Get Gemini analysis
            print("\nStep 3: Getting Gemini analysis")
            analysis = self.web_scraper.analyze_with_gemini(metadata['content'], url)
            print("Analysis completed:", analysis)
            
            # Step 4: Update databases
            print("\nStep 4: Updating databases")
            
            # Update url_tracker.db
            print("Updating URL tracker...")
            tracking_success = self.url_tracker.update_url(
                url=url,
                sitemap_url=sitemap_url,
                word_count=metadata.get('estimated_word_count', 0),
                date_published=metadata.get('date_published'),
                date_modified=metadata.get('date_modified')
            )
            print(f"Tracker update: {'Success' if tracking_success else 'Failed'}")
            
            # Update urls_analysis.db
            print("\nUpdating analysis database...")
            analysis_insert = db_ops.insert_urls([(url, metadata['domain_name'])])
            if analysis_insert:
                analysis_success = db_ops.update_url_analysis(
                    url_id=None,  # Let it find the latest inserted ID
                    summary=analysis.get('summary', 'N/A'),
                    category=analysis.get('category', 'Other'),
                    primary_keyword=analysis.get('primary_keyword', 'N/A'),
                    status='Processed'
                )
                print(f"Analysis update: {'Success' if analysis_success else 'Failed'}")
            
            return {
                'status': 'success',
                'tracking_update': tracking_success,
                'analysis_update': analysis_success if analysis_insert else False
            }
            
        except Exception as e:
            print(f"Error in process_url: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }