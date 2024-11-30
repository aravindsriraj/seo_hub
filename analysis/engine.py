import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, Any, List
import google.generativeai as genai
from ..core.config import config
from ..data.operations import db_ops

class CompetitiveAnalysisEngine:
    """Engine for analyzing competitive intelligence data."""
    
    def __init__(self):
        self.model = config.gemini_model
        api_key = config.GEMINI_API_KEY
        st.write(f"Debug - Using API key ending in: {api_key[-4:]}")

    def _count_tokens(self, text: str) -> int:
        """Count tokens for a given text using Gemini's token counter."""
        try:
            result = self.model.count_tokens(text)
            return result.total_tokens
        except Exception as e:
            st.warning(f"Token counting failed: {str(e)}")
            # Fallback to rough estimation if token counting fails
            return len(text) // 4

    def analyze_content_updates(self, days: int = 1) -> Dict[str, Any]:
        """Analyze recent content updates from competitors."""
        data = db_ops.get_recent_content_updates(days)
        
        prompt = f"""
        Analyze these content updates from competitors in the past {days} days:
        {data.to_string()}
        
        Consider:
        1. What topics are they focusing on?
        2. How does this differ from their historical patterns?
        3. Are there any notable changes in their content strategy?
        4. What might be the strategic implications for Atlan?
        
        Provide insights and potential action items.
        """
        
        response = self.model.generate_content(prompt)
        return {
            'analysis': response.text,
            'raw_data': data,
            'timestamp': datetime.now()
        }

    def analyze_ranking_movements(self, days: int = 1) -> Dict[str, Any]:
        """Analyze recent ranking changes and patterns."""
        data = db_ops.get_ranking_changes(days)
        
        prompt = f"""
        Analyze these ranking changes from the past {days} days:
        {data.to_string()}
        
        Consider:
        1. What significant position changes occurred?
        2. How did Atlan's movements compare to competitors?
        3. Are there patterns in keyword clusters?
        4. What might be causing these changes?
        
        Provide insights and potential action items.
        """
        
        response = self.model.generate_content(prompt)
        return {
            'analysis': response.text,
            'raw_data': data,
            'timestamp': datetime.now()
        }

    def analyze_llm_mentions(self, days: int = 1, test_keyword: str = "data catalog tools") -> Dict[str, Any]:
        """Analyze patterns in LLM mentions with token-aware chunking."""
        
        # Debug section for API and configuration
        # st.write("=== Debug Information ===")
        # st.write(f"Model: {config.GEMINI_MODEL_NAME}")
        # api_key = config.GEMINI_API_KEY
        # st.write(f"API Key (last 4): ****{api_key[-4:]}")
        
        # Get data
        st.write("\n2. Fetching Data...")
        data = db_ops.get_llm_mention_patterns(days)
        
        if data.empty:
            st.write("No data available for analysis")
            return {
                'analysis': "No LLM data available for the specified time period.",
                'raw_data': data,
                'timestamp': datetime.now()
            }
        
        # Filter for test keyword
        data = data[data['keyword'] == test_keyword]
        st.write(f"\nFiltered for keyword: '{test_keyword}'")
        
        # Data inspection
        answer_columns = [col for col in data.columns if col.endswith('_answer')]
        st.write("\n3. Data Statistics:")
        st.write(f"Total rows for keyword: {len(data)}")
        st.write(f"Answer columns: {answer_columns}")
        
        # Create analysis prompt
        prompt = f"""
        Analyze these LLM responses for the keyword '{test_keyword}':

        {data[answer_columns].to_string()}

        Focus on:
        1. How different models discuss Atlan
        2. How different models discuss Atlan in comparison to companies and platforms mentioned. 
        -- Be specific and use bullet points.
        -- Talk about how this changes over time.
        3. Key patterns in how these companies are mentioned
        4. Context and sentiment of mentions
        5. Compare responses across different models
        6. Notable insights about the data catalog market
        """
        
        # Check token count
        token_count = self._count_tokens(prompt)
        st.write(f"\nTotal tokens in prompt: {token_count}")
        
        try:
            st.write("\nAttempting analysis...")
            response = self.model.generate_content(prompt)
            st.write("✓ Analysis successful")
            
            return {
                'analysis': response.text,
                'raw_data': data,
                'timestamp': datetime.now()
            }
            
        except Exception as e:
            error_msg = f"Analysis failed: {str(e)}"
            st.error(error_msg)
            st.write(f"Full error details: {type(e).__name__}: {str(e)}")
            return {
                'analysis': error_msg,
                'raw_data': data,
                'timestamp': datetime.now()
            }
    # def cross_analyze_metrics(self, days: int = 1) -> Dict[str, Any]:
    #     """Perform cross-metric analysis."""
    #     content_data = db_ops.get_recent_content_updates(days)
    #     ranking_data = db_ops.get_ranking_changes(days)
    #     llm_data = db_ops.get_llm_mention_patterns(days)
        
    #     # Debug information about each dataset
    #     st.write("Debug Information:")
    #     st.write(f"Content Updates rows: {len(content_data)}")
    #     st.write(f"Ranking Changes rows: {len(ranking_data)}")
    #     st.write(f"LLM Data rows: {len(llm_data)}")
        
    #     # Create the prompt
    #     prompt = f"""
    #     Analyze these cross-metric patterns from the past {days} days:
        
    #     Content Updates:
    #     {content_data.to_string()}
        
    #     Ranking Changes:
    #     {ranking_data.to_string()}
        
    #     LLM Mentions:
    #     {llm_data.to_string()}
        
    #     Consider:
    #     1. Are there connections between these different metrics?
    #     2. How do changes in one area affect others?
    #     3. What patterns or trends are emerging?
    #     4. What strategic opportunities does this reveal?
        
    #     Provide insights and action items.
    #     """
        
    #     # Debug prompt size
    #     st.write(f"Total prompt character length: {len(prompt)}")
    #     st.write("Sample of each dataset:")
    #     st.write("Content Updates (first 3 rows):")
    #     st.write(content_data.head(3))
    #     st.write("Ranking Changes (first 3 rows):")
    #     st.write(ranking_data.head(3))
    #     st.write("LLM Data (first 3 rows):")
    #     st.write(llm_data.head(3))
        
    #     try:
    #         response = self.model.generate_content(prompt)
    #         return {
    #             'analysis': response.text,
    #             'raw_data': {
    #                 'content': content_data,
    #                 'rankings': ranking_data,
    #                 'llm_mentions': llm_data
    #             },
    #             'timestamp': datetime.now()
    #         }
    #     except Exception as e:
    #         error_msg = f"Analysis failed: {str(e)}"
    #         st.error(error_msg)
    #         st.write(f"Full error details: {type(e).__name__}: {str(e)}")
    #         return {
    #             'analysis': error_msg,
    #             'raw_data': {
    #                 'content': content_data,
    #                 'rankings': ranking_data,
    #                 'llm_mentions': llm_data
    #             },
    #             'timestamp': datetime.now()
    #         }    
    def analyze_content_updates(self, days: int = 1) -> Dict[str, Any]:
        """Analyze recent content updates from competitors."""
        data = db_ops.get_recent_content_updates(days)
        
        prompt = f"""
        Analyze these content updates from competitors in the past {days} days:
        {data.to_string()}
        
        Consider:
        1. What topics are they focusing on?
        2. How does this differ from their historical patterns?
        3. Are there any notable changes in their content strategy?
        4. What might be the strategic implications for Atlan?
        
        Provide insights and potential action items.
        """
        
        response = self.model.generate_content(prompt)
        return {
            'analysis': response.text,
            'raw_data': data,
            'timestamp': datetime.now()
    }

    def _chunk_dataframe(self, df: pd.DataFrame, base_prompt: str, max_tokens: int = 100000) -> List[pd.DataFrame]:
        """
        Chunk a dataframe to fit within token limits, accounting for the prompt.
        
        Args:
            df: DataFrame to chunk
            base_prompt: The prompt template that will be used with each chunk
            max_tokens: Maximum tokens per request (default: 100k)
        """
        chunks = []
        current_chunk = []
        
        # Count tokens in the base prompt
        prompt_tokens = self._count_tokens(base_prompt)
        st.write(f"Base prompt uses {prompt_tokens} tokens")
        
        available_tokens = max_tokens - prompt_tokens
        st.write(f"Available tokens for data: {available_tokens}")
        
        current_tokens = 0
        
        for idx, row in df.iterrows():
            row_text = row.to_string()
            row_tokens = self._count_tokens(row_text)
            
            if current_tokens + row_tokens > available_tokens and current_chunk:
                chunks.append(pd.DataFrame(current_chunk))
                current_chunk = []
                current_tokens = 0
                
            current_chunk.append(row)
            current_tokens += row_tokens
                
        if current_chunk:
            chunks.append(pd.DataFrame(current_chunk))
        
        return chunks

def cross_analyze_metrics(self, days: int = 1) -> Dict[str, Any]:
    """Perform cross-metric analysis with chunking."""
    content_data = db_ops.get_recent_content_updates(days)
    ranking_data = db_ops.get_ranking_changes(days)
    llm_data = db_ops.get_llm_mention_patterns(days)
    
    # Debug information
    st.write("Original Data Sizes:")
    st.write(f"Content Updates: {len(content_data)} rows")
    st.write(f"Ranking Changes: {len(ranking_data)} rows")
    st.write(f"LLM Data: {len(llm_data)} rows")
    
    # Chunk each dataset
    content_chunks = self._chunk_dataframe(content_data)
    ranking_chunks = self._chunk_dataframe(ranking_data)
    llm_chunks = self._chunk_dataframe(llm_data)
    
    st.write("Chunked Data:")
    st.write(f"Content chunks: {len(content_chunks)}")
    st.write(f"Ranking chunks: {len(ranking_chunks)}")
    st.write(f"LLM chunks: {len(llm_chunks)}")
    
    # Analyze each chunk combination
    analyses = []
    
    for c_chunk, r_chunk, l_chunk in zip(
        content_chunks[:3],  # Limit to first 3 chunks for each
        ranking_chunks[:3],
        llm_chunks[:3]
    ):
        prompt = f"""
        Analyze this subset of cross-metric patterns from the past {days} days:
        
        Content Updates:
        {c_chunk.to_string()}
        
        Ranking Changes:
        {r_chunk.to_string()}
        
        LLM Mentions:
        {l_chunk.to_string()}
        
        Consider:
        1. Are there connections between these different metrics?
        2. How do changes in one area affect others?
        3. What patterns or trends are emerging?
        """
        
        try:
            st.write(f"Processing chunk with prompt length: {len(prompt)}")
            response = self.model.generate_content(prompt)
            analyses.append(response.text)
        except Exception as e:
            st.warning(f"Chunk analysis failed: {str(e)}")
            continue
    
    if not analyses:
        error_msg = "All chunk analyses failed"
        st.error(error_msg)
        return {
            'analysis': error_msg,
            'raw_data': {
                'content': content_data,
                'rankings': ranking_data,
                'llm_mentions': llm_data
            },
            'timestamp': datetime.now()
        }
    
    # Create final summary
    summary_prompt = f"""
    Synthesize these separate analyses into a cohesive summary:

    {' '.join(analyses)}

    Provide:
    1. Overall patterns and trends
    2. Key strategic insights
    3. Recommended action items
    """
    
    try:
        final_response = self.model.generate_content(summary_prompt)
        return {
            'analysis': final_response.text,
            'raw_data': {
                'content': content_data,
                'rankings': ranking_data,
                'llm_mentions': llm_data
            },
            'timestamp': datetime.now()
        }
    except Exception as e:
        error_msg = f"Final analysis failed: {str(e)}"
        st.error(error_msg)
        return {
            'analysis': error_msg,
            'raw_data': {
                'content': content_data,
                'rankings': ranking_data,
                'llm_mentions': llm_data
            },
            'timestamp': datetime.now()
        }