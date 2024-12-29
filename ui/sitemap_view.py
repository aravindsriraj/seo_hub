from datetime import datetime
import json
import os
import shutil
import streamlit as st
from data.sitemap_manager import SitemapManager
from ui.components import ProgressTracker

class SitemapView:
    def render(self):
        """Render sitemap management section"""
        st.header("Sitemap Processing")

        try:
            # Load sitemaps configuration
            if not os.path.exists('sitemaps.json'):
                st.error("sitemaps.json not found. Please create it with your sitemap configurations.")
                return
                
            with open('sitemaps.json', 'r') as f:
                sitemaps_config = json.load(f)

            # Processing options
            st.subheader("Select Processing Type")
            options = {
                "new_urls": st.checkbox("Process New URLs", value=True,
                    help="Process URLs not currently in the database"),
                "updated_content": st.checkbox("Process Updated Content",
                    help="Process URLs with modified dates newer than last check"),
                "missing_metadata": st.checkbox("Process Missing Metadata",
                    help="Process URLs missing basic metadata like dates and word count"),
                "missing_enrichment": st.checkbox("Process Missing Enrichment",
                    help="Process URLs missing summary, category, or keywords"),
                "force_update": st.checkbox("Force Update All",
                    help="Process all URLs regardless of current status")
            }

            # Sitemap selection
            st.subheader("Select Sitemaps")
            selected_sitemaps = []
            for sitemap in sitemaps_config.get('sitemaps', []):
                if sitemap.get('enabled') and st.checkbox(sitemap.get('name', sitemap['url']), 
                                                        key=sitemap['url']):
                    selected_sitemaps.append(sitemap)

            # Current stats before processing
            st.subheader("Current Status")
            stats = SitemapManager.get_processing_stats()
            st.write(stats)

            # Process button
            if st.button("Start Processing"):
                if not selected_sitemaps:
                    st.warning("Please select at least one sitemap to process")
                    return

                # Create backup
                try:
                    backup_dir = "backups"
                    if not os.path.exists(backup_dir):
                        os.makedirs(backup_dir)
                    backup_time = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_file = f"backup_{backup_time}.db"
                    backup_path = os.path.join(backup_dir, backup_file)
                    shutil.copy2("urls_analysis.db", backup_path)
                    st.success(f"Created backup: {backup_file}")
                except Exception as e:
                    st.error(f"Failed to create backup: {str(e)}")
                    return

                # Initialize progress tracking
                progress_tracker = ProgressTracker()
                progress_tracker.initialize_progress()

                # Process each selected sitemap
                total_stats = {
                    'urls_processed': 0,
                    'new_urls': 0,
                    'updated_urls': 0,
                    'errors': 0
                }

                sitemap_manager = SitemapManager()

                for sitemap in selected_sitemaps:
                    try:
                        st.write(f"Processing: {sitemap['name']}")
                        results = sitemap_manager.process_sitemap(
                            sitemap_url=sitemap['url'],
                            options=options,
                            status_container=progress_tracker.status_text
                        )
                        
                        # Update total stats
                        for key in total_stats:
                            if key in results:
                                total_stats[key] += results[key]

                        st.success(f"""
                        ✅ Completed {sitemap['name']}:
                        - URLs Processed: {results.get('urls_processed', 0)}
                        - New: {results.get('new_urls', 0)}
                        - Updated: {results.get('updated_urls', 0)}
                        - Errors: {results.get('errors', 0)}
                        """)

                    except Exception as e:
                        st.error(f"Error processing {sitemap['name']}: {str(e)}")
                        continue

                # Show final summary
                st.markdown(f"""
                ## Processing Complete

                ### Overall Statistics:
                - URLs Processed: {total_stats['urls_processed']}
                - New URLs: {total_stats['new_urls']}
                - Updated URLs: {total_stats['updated_urls']}
                - Errors: {total_stats['errors']}
                """)

                # Show updated stats
                st.subheader("Updated Status")
                stats = SitemapManager.get_processing_stats()
                st.write(stats)

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
