import streamlit as st
from typing import List, Dict, Any
from analysis.engine import CompetitiveAnalysisEngine

class QAView:
    @staticmethod
    def render():
        st.header("Competitive Intelligence Q&A")
        
        analysis_engine = CompetitiveAnalysisEngine()
        
        # Analysis timeframe selector
        timeframe = st.selectbox(
            "Analysis timeframe",
            options=[
                "Last 24 hours",
                "Last 7 days",
                "Last 14 days",
                "Last 30 days"
            ],
            index=0
        )
        
        # Convert timeframe to days
        days_map = {
            "Last 24 hours": 1,
            "Last 7 days": 7,
            "Last 14 days": 14,
            "Last 30 days": 30
        }
        days = days_map[timeframe]
        
        # Create tabs for different analyses
        tabs = st.tabs([
            "Content Updates",
            "Ranking Changes",
            "LLM Mentions",
            "Cross Analysis"
        ])
        
        # Content Updates Tab
        with tabs[0]:
            st.subheader("Content Analysis")
            if st.button("Analyze Content Updates"):
                with st.spinner("Analyzing content..."):
                    content_analysis = analysis_engine.analyze_content_updates(days)
                    st.markdown(content_analysis['analysis'])
                    with st.expander("View Raw Data"):
                        st.dataframe(content_analysis['raw_data'])
        
        # Rankings Tab
        with tabs[1]:
            st.subheader("Rankings Analysis")
            if st.button("Analyze Ranking Changes"):
                with st.spinner("Analyzing rankings..."):
                    ranking_analysis = analysis_engine.analyze_ranking_movements(days)
                    st.markdown(ranking_analysis['analysis'])
                    with st.expander("View Raw Data"):
                        st.dataframe(ranking_analysis['raw_data'])
        
        # LLM Mentions Tab
        with tabs[2]:
            st.subheader("LLM Mention Analysis")
            
            # Get available keywords first
            keywords = analysis_engine.get_available_keywords()
            
            # Create two columns for the controls
            col1, col2 = st.columns([2, 1])
            
            with col1:
                selected_keyword = st.selectbox(
                    "Select keyword to analyze",
                    options=keywords,
                    index=None,
                    placeholder="Choose a keyword..."
                )
            
            with col2:
                analyze_button = st.button("Analyze LLM Mentions")

            # Create separate sections for debug info and results
            if analyze_button and selected_keyword:
                # Create two columns for the main content
                left_col, right_col = st.columns([3, 1])
                
                with left_col:
                    st.subheader("Analysis Results")
                    with st.spinner(f"Analyzing mentions for '{selected_keyword}'..."):
                        result = analysis_engine.analyze_llm_mentions(
                            days=days,
                            selected_keyword=selected_keyword
                        )
                        st.markdown(result['analysis'])
                
                with right_col:
                    with st.expander("Debug Information", expanded=False):
                        st.write("Model:", config.GEMINI_MODEL_NAME)
                        st.write("API Key (last 4):", f"****{config.GEMINI_API_KEY[-4:]}")
                        st.write("Total rows:", len(result['raw_data']))
                        st.write("Token count:", result.get('token_count', 'N/A'))
                
                # Raw data expander at the bottom
                with st.expander("View Raw Data", expanded=False):
                    st.dataframe(result['raw_data'])
                    
            elif analyze_button:
                st.warning("Please select a keyword to analyze")

        # Cross Analysis Tab
        with tabs[3]:
            st.subheader("Cross-Metric Analysis")
            if st.button("Analyze All Metrics"):
                with st.spinner("Performing cross-metric analysis..."):
                    cross_analysis = analysis_engine.cross_analyze_metrics(days)
                    st.markdown(cross_analysis['analysis'])
                    with st.expander("View Raw Data"):
                        for key, df in cross_analysis['raw_data'].items():
                            st.subheader(key.title())
                            st.dataframe(df)
