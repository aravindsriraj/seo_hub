from datetime import datetime
import time
from typing import Dict
import streamlit as st
from urllib.parse import urlparse
from data.web_scraper import WebScraper
from data.xml_parser import extract_urls_from_xml
from data.operations import db_ops

class SitemapManager:
    def __init__(self):
        self.web_scraper = WebScraper()

    @staticmethod
    def get_processing_stats() -> Dict:
        """Get current processing statistics"""
        return db_ops.get_processing_stats()

    
    def _should_process_url(self, url: str, existing_data: Dict, options: Dict) -> bool:
        """Determine if URL should be processed based on options."""
        if options['force_update']:
            return True

        if not existing_data and options['new_urls']:
            return True

        if existing_data:
            if existing_data.get('status') in ['date_not_found', 'error']:
                return False

            if options['missing_metadata'] and (
                not existing_data.get('estimated_word_count') or
                not existing_data.get('datePublished')
            ):
                return True

            if options['updated_content'] and existing_data.get('dateModified'):
                return True

            if options['missing_enrichment'] and (
                not existing_data.get('summary') or
                not existing_data.get('category') or
                not existing_data.get('primary_keyword')
            ):
                return True

        return False

    def _needs_enrichment(self, existing_data: Dict, options: Dict) -> bool:
        """Determine if URL needs Gemini analysis."""
        if options['force_update']:
            return True

        if not existing_data:
            return True

        if options['missing_enrichment'] and (
            not existing_data.get('summary') or
            not existing_data.get('category') or
            not existing_data.get('primary_keyword')
        ):
            return True

        return False
    
    def process_sitemap(self, sitemap_url: str, options: Dict, status_container) -> Dict:
        """Process a sitemap based on selected options."""
        stats = {
            'urls_processed': 0,
            'new_urls': 0,
            'updated_urls': 0,
            'errors': 0
        }

        try:
            urls = extract_urls_from_xml(sitemap_url)
            if not urls:
                print("⚠️ No URLs found in sitemap")
                status_container.warning("⚠️ No URLs found in sitemap")
                return stats

            stats['urls_processed'] = len(urls)
            progress_bar = st.progress(0)
            current_url = st.empty()

            for idx, url in enumerate(urls, 1):
                try:
                    progress = idx / len(urls)
                    progress_bar.progress(progress)
                    
                    terminal_status = [f"\nProcessing URL {idx}/{len(urls)}: {url}"]
                    ui_status = [f"Processing ({idx}/{len(urls)}): {url}"]
                    
                    # Check existing data
                    existing_data = db_ops.get_url_info(url)
                    if existing_data:
                        status = f"Existing URL - Last processed: {existing_data.get('last_analyzed', 'unknown')}"
                        terminal_status.append(status)
                        ui_status.append(status)
                    else:
                        status = "New URL"
                        terminal_status.append(status)
                        ui_status.append(status)
                    
                    if self._should_process_url(url, existing_data, options):
                        # Extract and process content
                        metadata = self.web_scraper.extract_content(url)
                        
                        # Show what was found
                        if metadata.get('datePublished'):
                            msg = f"Published Date: {metadata['datePublished']}"
                            terminal_status.append(msg)
                            ui_status.append(msg)
                        if metadata.get('dateModified'):
                            msg = f"Modified Date: {metadata['dateModified']}"
                            terminal_status.append(msg)
                            ui_status.append(msg)
                        if metadata.get('estimated_word_count'):
                            msg = f"Word Count: {metadata['estimated_word_count']}"
                            terminal_status.append(msg)
                            ui_status.append(msg)

                        metadata_to_save = {k: v for k, v in metadata.items() if k != 'status'}
                        current_status = metadata.get('status', 'pending')
                        
                        # Update database
                        success = db_ops.update_url(
                            url=url,
                            status=current_status,
                            **metadata_to_save
                        )
                        
                        if success:
                            if existing_data:
                                stats['updated_urls'] += 1
                                # Create detailed update message
                                updates = []
                                if options['updated_content']:
                                    if metadata.get('dateModified') != existing_data.get('dateModified'):
                                        updates.append("content updated")
                                if options['missing_metadata']:
                                    if not existing_data.get('estimated_word_count'):
                                        updates.append("added word count")
                                    if not existing_data.get('datePublished'):
                                        updates.append("added dates")
                                if options['missing_enrichment']:
                                    if not existing_data.get('summary'):
                                        updates.append("added summary")
                                    if not existing_data.get('category'):
                                        updates.append("added category")
                                
                                update_msg = "✅ Updated: " + (", ".join(updates) if updates else "no changes needed")
                            else:
                                stats['new_urls'] += 1
                                update_msg = "✅ New URL Added"
                        else:
                            stats['errors'] += 1
                            update_msg = "❌ Update Failed"
                        
                        terminal_status.append(update_msg)
                        ui_status.append(update_msg)
                        
                    else:
                        reason = []
                        if existing_data:
                            if existing_data.get('status') in ['date_not_found', 'error']:
                                reason.append("previous processing error")
                            elif not options['force_update'] and not options['updated_content']:
                                reason.append("no content update needed")
                            elif not options['missing_metadata'] and not options['missing_enrichment']:
                                reason.append("no enrichment needed")
                        
                        skip_msg = f"⏭️ Skipped - {', '.join(reason) if reason else 'no updates needed'}"
                        terminal_status.append(skip_msg)
                        ui_status.append(skip_msg)
                    
                    # Display status in both places
                    print("\n".join(terminal_status))
                    current_url.markdown("\n".join(ui_status))
                    
                except Exception as e:
                    error_msg = f"❌ Error processing URL: {str(e)}"
                    print(error_msg)
                    stats['errors'] += 1
                    current_url.error(error_msg)
                    continue
            return stats

        except Exception as e:
            error_msg = f"Error processing sitemap: {str(e)}"
            print(error_msg)
            status_container.error(error_msg)
        return stats
