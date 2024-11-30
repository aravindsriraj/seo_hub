import streamlit as st
from seo_hub.analysis.engine import CompetitiveAnalysisEngine

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
        
        # Analysis sections
        st.subheader("Content Analysis")
        if st.button("Analyze Content Updates"):
            with st.spinner("Analyzing content..."):
                content_analysis = analysis_engine.analyze_content_updates(days)
                st.markdown(content_analysis['analysis'])
                with st.expander("View Raw Data"):
                    st.dataframe(content_analysis['raw_data'])
        
        st.subheader("Rankings Analysis")
        if st.button("Analyze Ranking Changes"):
            with st.spinner("Analyzing rankings..."):
                ranking_analysis = analysis_engine.analyze_ranking_movements(days)
                st.markdown(ranking_analysis['analysis'])
                with st.expander("View Raw Data"):
                    st.dataframe(ranking_analysis['raw_data'])
        
        st.subheader("LLM Mention Analysis")
        if st.button("Analyze LLM Mentions"):
            with st.spinner("Analyzing LLM mentions..."):
                llm_analysis = analysis_engine.analyze_llm_mentions(days)
                st.markdown(llm_analysis['analysis'])
                with st.expander("View Raw Data"):
                    st.dataframe(llm_analysis['raw_data'])
        
        st.subheader("Cross-Metric Analysis")
        if st.button("Analyze All Metrics"):
            with st.spinner("Performing cross-metric analysis..."):
                cross_analysis = analysis_engine.cross_analyze_metrics(days)
                st.markdown(cross_analysis['analysis'])
                with st.expander("View Raw Data"):
                    for key, df in cross_analysis['raw_data'].items():
                        st.subheader(key.title())
                        st.dataframe(df)