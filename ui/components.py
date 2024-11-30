import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from typing import List, Dict, Any, Union, Tuple
from datetime import datetime, timedelta
from seo_hub.core.config import config
from seo_hub.data.operations import db_ops

class MetricsDisplay:
    """Handles the display of key metrics and statistics."""
    
    @staticmethod
    def create_metric_row(metrics: List[Dict[str, Any]], columns: int = 3):
        """Create a row of metrics in columns."""
        cols = st.columns(columns)
        for idx, metric in enumerate(metrics):
            with cols[idx % columns]:
                st.metric(
                    label=metric.get('label', ''),
                    value=metric.get('value', ''),
                    delta=metric.get('delta', None),
                    help=metric.get('help', None)
                )

    @staticmethod
    def create_status_table(data: pd.DataFrame, config: Dict[str, Any]):
        """Create a formatted status table."""
        st.dataframe(
            data,
            column_config=config,
            hide_index=True,
            use_container_width=True
        )
    pass

class ChartComponents:
    """Collection of reusable chart components."""
    
    @staticmethod
    def create_line_chart(
        df: pd.DataFrame,
        x: str,
        y: str,
        color: str,
        title: str,
        **kwargs
    ) -> go.Figure:
        """Create a line chart with consistent styling."""
        fig = px.line(
            df,
            x=x,
            y=y,
            color=color,
            title=title,
            **kwargs
        )
        
        fig.update_layout(
            height=400,
            showlegend=True,
            plot_bgcolor='white',
            xaxis={'showgrid': False},
            yaxis={'showgrid': True, 'gridwidth': 1, 'gridcolor': 'LightGray'}
        )
        
        return fig
    @staticmethod
    def create_pie_chart(
        df: pd.DataFrame,
        values: str,
        names: str,
        title: str
    ) -> go.Figure:
        """Create a pie chart with consistent styling."""
        fig = px.pie(
            df,
            values=values,
            names=names,
            title=title
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label'
        )
        
        fig.update_layout(
            showlegend=False,
            height=400,
            title={'y':0.9, 'x':0.5, 'xanchor':'center', 'yanchor':'top'}
        )
        
        return fig

    @staticmethod
    def create_scatter_plot(
        df: pd.DataFrame,
        x: str,
        y: str,
        color: str,
        title: str,
        size: str = None,
        **kwargs
    ) -> go.Figure:
        """Create a scatter plot with consistent styling."""
        fig = px.scatter(
            df,
            x=x,
            y=y,
            color=color,
            size=size,
            title=title,
            **kwargs
        )
        
        fig.update_layout(
            height=500,
            plot_bgcolor='white',
            xaxis={'showgrid': False, 'title': x},
            yaxis={'showgrid': True, 'gridwidth': 1, 'gridcolor': 'LightGray', 'title': y}
        )
        
        fig.update_traces(
            marker=dict(size=8 if not size else None, opacity=0.6)
        )
        
        return fig
    pass

class FilterComponents:
    """Reusable filter components."""
    
    @staticmethod
    def date_range_selector(
        key: str = "date_range",
        default_days: int = 30
    ) -> tuple:
        """Create a date range selector."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=default_days)
        
        return st.date_input(
            "Select Date Range",
            value=(start_date, end_date),
            max_value=end_date,
            key=key
        )

    @staticmethod
    def keyword_selector(
        multiple: bool = False,
        key: str = "keyword_selector"
    ) -> Union[str, List[str]]:
        """Create a keyword selector dropdown."""
        try:
            # Fetch keywords using db_ops instead of direct connection
            keywords = db_ops.get_keywords()
            
            if not keywords:
                st.warning("No keywords found in the database.")
                return [] if multiple else None
            
            if multiple:
                return st.multiselect(
                    "Select Keywords",
                    options=keywords,
                    default=keywords[:5] if len(keywords) >= 5 else keywords,
                    key=key
                )
            
            return st.selectbox(
                "Select Keyword",
                options=keywords,
                key=key
            )
            
        except Exception as e:
            st.error(f"Error loading keywords: {str(e)}")
            return [] if multiple else None

    @staticmethod
    def domain_selector(
        multiple: bool = False,
        key: str = "domain"
    ) -> Union[str, List[str]]:
        """Create a domain selector dropdown."""
        try:
            domains = db_ops.get_unique_domains()
            
            if not domains:
                st.warning("No domains found in the database.")
                return [] if multiple else None
            
            if multiple:
                return st.multiselect(
                    "Select Domains",
                    options=domains,
                    default=[domains[0]] if domains else None,
                    key=key
                )
            
            return st.selectbox(
                "Select Domain",
                options=domains,
                key=key
            )
            
        except Exception as e:
            st.error(f"Error loading domains: {str(e)}")
            return [] if multiple else None

class ProgressComponents:
    """Components for showing progress and status."""
    
    def __init__(self):
        self.progress_bar = None
        self.status_text = None

    def initialize_progress(self):
        """Initialize progress bar and status text."""
        self.progress_bar = st.progress(0)
        self.status_text = st.empty()
        return self

    def update_progress(self, current: int, total: int, message: str = "Processing"):
        """Update progress bar and status text."""
        if not (self.progress_bar and self.status_text):
            self.initialize_progress()
            
        progress = current / total
        self.progress_bar.progress(progress)
        self.status_text.text(
            f"{message}: {current}/{total} ({progress * 100:.2f}%)"
        )

    def clear(self):
        """Clear progress bar and status text."""
        if self.progress_bar:
            self.progress_bar.empty()
        if self.status_text:
            self.status_text.empty()

    pass

class TableComponents:
    """Reusable table components."""
    
    @staticmethod
    def create_paginated_table(
        df: pd.DataFrame,
        page_size: int = 50,
        key: str = "pagination"
    ):
        """Create a paginated table view."""
        total_rows = len(df)
        total_pages = (total_rows + page_size - 1) // page_size
        
        page = st.number_input(
            "Select page:",
            min_value=1,
            max_value=total_pages,
            value=1,
            key=key
        )
        
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_rows)
        
        return df.iloc[start_idx:end_idx]
    pass

# Create global instances of components
metrics = MetricsDisplay()
charts = ChartComponents()
filters = FilterComponents()
progress = ProgressComponents()
tables = TableComponents()