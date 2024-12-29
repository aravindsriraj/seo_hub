import streamlit as st
from data.query_executor import QueryExecutor
from core.config import config


class SEOQAView:
    def __init__(self):
        self.executor = QueryExecutor(
            rankings_db=config.RANKINGS_DB_PATH,
            urls_db=config.URLS_DB_PATH,
            aimodels_db=config.AIMODELS_DB_PATH
        )
    
    def render(self):
        st.title("SEO Intelligence Q&A")
        
        st.write("""
        Ask questions about your SEO performance, rankings, and content.
        Examples:
        - How are we performing for data catalog keywords?
        - What are our top ranking keywords?
        - Show our ranking trends against competitors
        """)
        
        # Question input
        question = st.text_input("Ask a question:")
        
        if question:
            with st.spinner("Analyzing..."):
                # Get response
                explanation, data, viz = self.executor.execute(question)
                
                # Show explanation
                st.write("### Analysis")
                st.write(explanation)
                
                # Show visualization if available
                if viz is not None:
                    st.plotly_chart(viz, use_container_width=True)
                
                # Show data if available
                if not data.empty:
                    with st.expander("View Data"):
                        st.dataframe(data)
