import sqlite3
import pandas as pd
from typing import Dict, Any, Tuple
import google.generativeai as genai
import plotly.express as px
import streamlit as st
from seo_hub.core.config import config
from seo_hub.data.vector_store import VectorStore
from seo_hub.data.query_planner import QueryPlanner
from seo_hub.data.schema_manager import SchemaManager

class QueryExecutor:
    def __init__(self, rankings_db: str, urls_db: str, aimodels_db: str):
        self.rankings_db = rankings_db
        self.urls_db = urls_db
        self.aimodels_db = aimodels_db
        self.vector_store = VectorStore()
        self.schema_manager = SchemaManager(
            rankings_db=rankings_db,
            urls_db=urls_db,
            aimodels_db=aimodels_db
        )
        self.query_planner = QueryPlanner(
            vector_store=self.vector_store,
            schema_manager=self.schema_manager  # Pass schema_manager to QueryPlanner
        )
        
        # Initialize Gemini
        genai.configure(api_key=config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(
            model_name=config.GEMINI_MODEL_NAME,
            generation_config=config.GENERATION_CONFIG
        )

    def _execute_sql(self, query: str) -> pd.DataFrame:
        """Execute SQL query on appropriate database based on table prefixes."""
        query_lower = query.lower()
        
        # Determine which database to use based on table prefixes
        if "rankings." in query_lower:
            db_path = self.rankings_db
            # Remove database prefix from table names
            query = query.replace("rankings.", "")
        elif "urls_analysis." in query_lower:
            db_path = self.urls_db
            query = query.replace("urls_analysis.", "")
        elif "url_tracker." in query_lower:
            db_path = self.url_tracker_db
            query = query.replace("url_tracker.", "")
        elif "aimodels." in query_lower:
            db_path = self.aimodels_db
            query = query.replace("aimodels.", "")
        else:
            raise ValueError(
                "Could not determine database. Query must include table prefixes "
                "(e.g., rankings.keywords, urls_analysis.urls)"
            )
        
        try:
            conn = sqlite3.connect(db_path)
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
        except Exception as e:
            st.error(f"Error executing SQL: {str(e)}")
            st.error(f"Query: {query}")
            st.error(f"Database: {db_path}")
            return pd.DataFrame()
    
    def execute(self, user_question: str) -> Tuple[str, Any, Any]:
        """Execute a user question and return response with visualization."""
        # Get execution plan
        plan = self.query_planner.create_execution_plan(user_question)
        
        # Execute SQL query
        data = self._execute_sql(plan['sql_query'])
        
        # Generate visualization if needed
        viz = self._create_visualization(data, plan['visualization'])
        
        # Generate explanation with context
        explanation = self._generate_explanation(user_question, plan, data)
        
        return explanation, data, viz

    def _create_visualization(self, data: pd.DataFrame, viz_type: str) -> Any:
        """Create visualization based on data and type."""
        if data.empty:
            return None
            
        try:
            if 'line' in viz_type.lower():
                fig = px.line(
                    data,
                    x=data.columns[0],  # Assumes first column is x-axis
                    y=data.columns[1],  # Assumes second column is y-axis
                    color=data.columns[2] if len(data.columns) > 2 else None,
                    title="Trend Analysis"
                )
                return fig
                
            elif 'bar' in viz_type.lower():
                fig = px.bar(
                    data,
                    x=data.columns[0],
                    y=data.columns[1],
                    color=data.columns[2] if len(data.columns) > 2 else None,
                    title="Comparative Analysis"
                )
                return fig
                
            elif 'scatter' in viz_type.lower():
                fig = px.scatter(
                    data,
                    x=data.columns[0],
                    y=data.columns[1],
                    color=data.columns[2] if len(data.columns) > 2 else None,
                    title="Distribution Analysis"
                )
                return fig
                
            else:
                return None  # Default to no visualization
                
        except Exception as e:
            st.error(f"Error creating visualization: {str(e)}")
            return None

    def _generate_explanation(self, question: str, plan: Dict[str, Any], data: pd.DataFrame) -> str:
        """Generate natural language explanation of results."""
        prompt = f"""
        Analyze this SEO data and provide a clear explanation.
        
        Original Question: {question}
        
        Data Summary:
        {data.describe().to_string() if not data.empty else "No data available"}
        
        Data Sample:
        {data.head().to_string() if not data.empty else "No data available"}
        
        Required Context:
        {plan.get('required_context', 'No specific context required')}
        
        Explain the results in a clear, actionable way. Include:
        1. Direct answer to the question
        2. Key insights from the data
        3. Important trends or patterns
        4. Action items or recommendations
        
        Keep the explanation concise but informative.
        """
        
        chat = self.model.start_chat(history=[])
        response = chat.send_message(prompt)
        
        return response.text