import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List
import google.generativeai as genai
from ..data.operations import db_ops

class CompetitiveAnalysisEngine:
    """Engine for analyzing competitive intelligence data."""
    
    def __init__(self):
        self.model = genai.GenerativeModel(model_name="gemini-1.5-pro-latest")

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

    def analyze_llm_mentions(self, days: int = 1) -> Dict[str, Any]:
        """Analyze patterns in LLM mentions."""
        data = db_ops.get_llm_mention_patterns(days)
        
        prompt = f"""
        Analyze these LLM mention patterns from the past {days} days:
        {data.to_string()}
        
        Consider:
        1. How are different models mentioning Atlan vs competitors?
        2. Are there changes in mention patterns?
        3. What contexts trigger mentions?
        4. Are there opportunities to improve positioning?
        
        Provide insights and potential action items.
        """
        
        response = self.model.generate_content(prompt)
        return {
            'analysis': response.text,
            'raw_data': data,
            'timestamp': datetime.now()
        }

    def cross_analyze_metrics(self, days: int = 1) -> Dict[str, Any]:
        """Perform cross-metric analysis."""
        content_data = db_ops.get_recent_content_updates(days)
        ranking_data = db_ops.get_ranking_changes(days)
        llm_data = db_ops.get_llm_mention_patterns(days)
        
        prompt = f"""
        Analyze these cross-metric patterns from the past {days} days:
        
        Content Updates:
        {content_data.to_string()}
        
        Ranking Changes:
        {ranking_data.to_string()}
        
        LLM Mentions:
        {llm_data.to_string()}
        
        Consider:
        1. Are there connections between these different metrics?
        2. How do changes in one area affect others?
        3. What patterns or trends are emerging?
        4. What strategic opportunities does this reveal?
        
        Provide insights and action items.
        """
        
        response = self.model.generate_content(prompt)
        return {
            'analysis': response.text,
            'raw_data': {
                'content': content_data,
                'rankings': ranking_data,
                'llm_mentions': llm_data
            },
            'timestamp': datetime.now()
        }
    
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