import streamlit as st
from core.config import config
from core.services import content_processor, url_service
from data.operations import db_ops
from ui.views import views
from ui.components import progress
from ui.qa_view import QAView
from ui.sitemap_view import SitemapView
from ui.seo_qa_view import SEOQAView

def initialize_app():
    """Initialize the application and database connections."""
    # Set up Streamlit configuration
    st_config = config.get_streamlit_config()
    st.set_page_config(**st_config)
    
    # Initialize databases
    # Ensure URLs database is set up before proceeding. If setup fails, stop the app.
    if not db_ops.setup_urls_database():
        st.stop()

def setup_sidebar():
    """Configure and render the sidebar."""
    with st.sidebar:
        st.header("Controls")

        # Model Information
   
        with st.expander("Model Information", expanded=False):
            api_key = config.GEMINI_API_KEY
            masked_key = "****" + api_key[-4:] if api_key != "Not configured" else "Not configured"
            
            st.info(
                f"**Current Model:** {config.GEMINI_MODEL_NAME}\n\n"
                f"**API Key:** {masked_key}"
            )

        # Analysis Controls
        # with st.expander("Analysis Controls", expanded=False):
        #     # New row
        #     col3, col4 = st.columns(2)
        #     with col3:
        #         if st.button("Analyze URLs", use_container_width=True):
        #             content_processor.process_pending_urls()

        # Database Controls
        # with st.expander("Database Controls", expanded=False):
        #     # Add Column
        #     new_column_name = st.text_input("Add New Column Name")
        #     if st.button("Add Column"):
        #         if new_column_name:
        #             db_ops.add_column('urls', new_column_name)
        #             st.success(f"Column '{new_column_name}' added successfully.")
        #         else:
        #             st.error("Please enter a valid column name.")

        #     # Drop Column
        #     columns = db_ops.get_column_names('urls', config.URLS_DB_PATH)
        #     column_to_drop = st.selectbox("Select Column to Drop", columns)
        #     if st.button("Drop Column"):
        #         try:
        #             db_ops.drop_column('urls', column_to_drop)
        #             st.success(f"Column '{column_to_drop}' dropped successfully.")
        #         except ValueError as e:
        #             st.error(str(e))

        # Settings
        with st.expander("Settings", expanded=False):
            sitemap_url = st.text_input(
                "Enter Sitemap URL (XML)", 
                placeholder="https://example.com/sitemap.xml"
            )
            if st.button("Load Sitemap"):
                if sitemap_url:
                    with progress.initialize_progress() as prog:
                        count = url_service.process_sitemap(sitemap_url)
                        if count > 0:
                            st.success(f"Successfully ingested {count} URLs from the sitemap.")
                        else:
                            st.warning("No URLs found in the provided sitemap.")

        sitemap_view = SitemapView()
        sitemap_view.render()

def main():
    """Main application entry point."""
    # Initialize application
    initialize_app()
    
    # Display application title
    st.title("SEO Monitoring Hub")
    
    # Setup sidebar
    setup_sidebar()
    
    # Create main navigation tabs
    tabs = st.tabs([
        "Key Statistics",
        "View Raw Data",
        "Insights",
        "Position Tracking",
        "LLM Tracker",
        "Q&A",
        "SEO Intelligence"
    ])
    
    # Render each tab's content
    with tabs[0]:
        views["dashboard"].render()
        
    with tabs[1]:
        views["data"].render()
        
    with tabs[2]:
        views["insights"].render()
        
    with tabs[3]:
        views["position"].render()
        
    with tabs[4]:
        views["llm"].render()

    with tabs[5]: 
        views["qa"].render()
    
    with tabs[6]:
        SEOQAView().render()

    # Add footer
    st.markdown("---")
    st.caption("SEO Monitoring Hub © 2024")

def handle_error(func):
    """Decorator for error handling."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            if st.button("Show Details"):
                st.exception(e)
    return wrapper

@handle_error
def run_app():
    """Run the application with error handling."""
    main()

if __name__ == "__main__":
    run_app()
