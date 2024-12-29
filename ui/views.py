import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime,timedelta
from typing import List, Dict, Any, Union
from ui.components import metrics, charts, filters, progress, tables
from core.config import config
from core.services import url_service, content_processor, ranking_service, llm_analyzer
from data.operations import db_ops
from ui.qa_view import QAView

# New Class Dashboard View Implementation

class DashboardView:
    """Implements the Key Statistics dashboard focused on database health and data quality."""
    
    @staticmethod
    def calculate_content_stats() -> Dict[str, Union[int, Dict[str, int], str]]:
        """Calculate statistics for content database."""
        try:
            conn = sqlite3.connect(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            stats = {}
            
            # Total URLs
            cursor.execute("SELECT COUNT(*) FROM urls")
            stats['total_urls'] = cursor.fetchone()[0]
            
            # Status distribution
            cursor.execute("SELECT status, COUNT(*) FROM urls GROUP BY status")
            stats['status_counts'] = dict(cursor.fetchall() or {})
            
            # URLs with dates
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN datePublished IS NOT NULL 
                          AND datePublished != '' 
                          AND datePublished != 'N/A' THEN 1 END) as with_published,
                    COUNT(CASE WHEN dateModified IS NOT NULL 
                          AND dateModified != '' 
                          AND dateModified != 'N/A' THEN 1 END) as with_modified,
                    COUNT(CASE WHEN summary IS NOT NULL 
                          AND summary != '' 
                          AND summary != 'N/A' THEN 1 END) as with_summary,
                    COUNT(CASE WHEN category IS NOT NULL 
                          AND category != '' 
                          AND category != 'N/A' THEN 1 END) as with_category,
                    MAX(last_analyzed) as latest_update
                FROM urls
            """)
            
            result = cursor.fetchone()
            if result:
                stats['urls_with_published_date'] = result[0]
                stats['urls_with_modified_date'] = result[1]
                stats['urls_with_summary'] = result[2]
                stats['urls_with_category'] = result[3]
                stats['latest_update'] = result[4] if result[4] else 'Never'
            
            conn.close()
            return stats
            
        except sqlite3.Error as e:
            st.error(f"Database error in calculate_content_stats: {str(e)}")
            return {
                'total_urls': 0,
                'status_counts': {},
                'urls_with_published_date': 0,
                'urls_with_modified_date': 0,
                'urls_with_summary': 0,
                'urls_with_category': 0,
                'latest_update': 'Error'
            }

    @staticmethod
    def calculate_ranking_stats() -> Dict[str, Union[int, float, str]]:
        """Calculate statistics for rankings database."""
        try:
            conn = sqlite3.connect(config.RANKINGS_DB_PATH)
            cursor = conn.cursor()
            
            stats = {}
            
            # Total keywords and domains
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(DISTINCT keyword) FROM keywords) as total_keywords,
                    (SELECT COUNT(*) FROM rankings) as total_rankings,
                    (SELECT COUNT(DISTINCT domain) FROM rankings) as domains_tracked,
                    (SELECT MAX(check_date) FROM rankings) as latest_check
            """)
            
            result = cursor.fetchone()
            if result:
                stats['total_keywords'] = result[0] or 0
                stats['total_rankings'] = result[1] or 0
                stats['domains_tracked'] = result[2] or 0
                stats['latest_check'] = result[3] if result[3] else 'Never'
                
                # Calculate expected rankings and completeness
                if stats['total_keywords'] > 0 and stats['domains_tracked'] > 0:
                    stats['expected_rankings'] = stats['total_keywords'] * stats['domains_tracked']
                    stats['completeness'] = (stats['total_rankings'] / stats['expected_rankings'] * 100) 
                else:
                    stats['expected_rankings'] = 0
                    stats['completeness'] = 0.0
            
            conn.close()
            return stats
            
        except sqlite3.Error as e:
            st.error(f"Database error in calculate_ranking_stats: {str(e)}")
            return {
                'total_keywords': 0,
                'total_rankings': 0,
                'domains_tracked': 0,
                'latest_check': 'Error',
                'expected_rankings': 0,
                'completeness': 0.0
            }

    @staticmethod
    def calculate_llm_stats() -> Dict[str, Union[int, float, str]]:
        """Calculate statistics for LLM analysis database."""
        try:
            conn = sqlite3.connect(config.AIMODELS_DB_PATH)
            cursor = conn.cursor()
            
            stats = {}
            
            # Get table info to count model columns
            cursor.execute("PRAGMA table_info(keyword_rankings)")
            columns = cursor.fetchall()
            
            # Count model columns (those ending with _answer)
            model_columns = [col[1] for col in columns if col[1].endswith('_answer')]
            stats['models_tracked'] = len(model_columns)
            
            # Get response counts and latest check
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_responses,
                    COUNT(DISTINCT keyword) as keywords_covered,
                    MAX(check_date) as latest_check
                FROM keyword_rankings
            """)
            
            result = cursor.fetchone()
            if result:
                stats['total_responses'] = result[0] or 0
                keywords_covered = result[1] or 0
                stats['latest_check'] = result[2] if result[2] else 'Never'
                
                # Calculate averages and coverage
                if stats['models_tracked'] > 0:
                    stats['avg_responses_per_model'] = stats['total_responses'] / stats['models_tracked']
                else:
                    stats['avg_responses_per_model'] = 0.0
                
                # Get total keywords for coverage calculation
                cursor.execute("ATTACH DATABASE ? AS rankings", (config.RANKINGS_DB_PATH,))
                cursor.execute("SELECT COUNT(DISTINCT keyword) FROM rankings.keywords")
                total_keywords = cursor.fetchone()[0] or 1  # Avoid division by zero
                
                stats['response_coverage'] = (keywords_covered / total_keywords * 100)
            
            conn.close()
            return stats
            
        except sqlite3.Error as e:
            st.error(f"Database error in calculate_llm_stats: {str(e)}")
            return {
                'models_tracked': 0,
                'total_responses': 0,
                'latest_check': 'Error',
                'avg_responses_per_model': 0.0,
                'response_coverage': 0.0
            }

    @staticmethod
    def render():
        """Render the dashboard view with balanced layout."""
        st.header("Database Health Dashboard")
        
        # Fetch all stats first
        content_stats = DashboardView.calculate_content_stats()
        ranking_stats = DashboardView.calculate_ranking_stats()
        llm_stats = DashboardView.calculate_llm_stats()
        
        # Top-level metrics in a clean row
        st.markdown("### Overview")
        metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
        
        with metrics_col1:
            st.metric(
                "Content Database",
                f"{content_stats.get('total_urls', 0):,} URLs",
                f"{len(content_stats.get('status_counts', {}))} States"
            )
        
        with metrics_col2:
            st.metric(
                "Rankings Database",
                f"{ranking_stats.get('total_keywords', 0):,} Keywords",
                f"{ranking_stats.get('domains_tracked', 0)} Domains"
            )
        
        with metrics_col3:
            st.metric(
                "LLM Analysis",
                f"{llm_stats.get('models_tracked', 0)} Models",
                f"{llm_stats.get('total_responses', 0):,} Responses"
            )
            
        # Data Quality Section - Equal sized columns
        st.markdown("### Data Quality")
        col1, col2, col3 = st.columns(3)
        
        # Content Quality
        with col1:
            with st.container(border=True):
                st.subheader("Content Quality", divider="gray")
                total_urls = content_stats.get('total_urls', 0)
                if total_urls > 0:
                    quality_metrics = {
                        "Published Date": content_stats.get('urls_with_published_date', 0) / total_urls * 100,
                        "Modified Date": content_stats.get('urls_with_modified_date', 0) / total_urls * 100,
                        "Content Summary": content_stats.get('urls_with_summary', 0) / total_urls * 100,
                        "Categorization": content_stats.get('urls_with_category', 0) / total_urls * 100
                    }
                    
                    for metric, value in quality_metrics.items():
                        indicator = "🟢" if value > 90 else "🟡" if value > 70 else "🔴"
                        st.metric(
                            metric,
                            f"{value:.1f}%",
                            indicator,
                            delta_color="normal" if value > 70 else "inverse"
                        )

        # Rankings Quality
        with col2:
            with st.container(border=True):
                st.subheader("Rankings Quality", divider="gray")
                completeness = ranking_stats.get('completeness', 0)
                st.metric(
                    "Data Completeness",
                    f"{completeness:.1f}%",
                    "🟢" if completeness > 90 else "🟡" if completeness > 70 else "🔴"
                )
                
                total_rankings = ranking_stats.get('total_rankings', 0)
                st.metric("Total Records", f"{total_rankings:,}")
                st.metric(
                    "Coverage",
                    f"{ranking_stats.get('domains_tracked', 0)} domains",
                    f"{ranking_stats.get('total_keywords', 0)} keywords"
                )

        # LLM Quality
        with col3:
            with st.container(border=True):
                st.subheader("LLM Quality", divider="gray")
                coverage = llm_stats.get('response_coverage', 0)
                st.metric(
                    "Response Coverage",
                    f"{coverage:.1f}%",
                    "🟢" if coverage > 90 else "🟡" if coverage > 70 else "🔴"
                )
                
                avg_responses = llm_stats.get('avg_responses_per_model', 0)
                st.metric("Avg Responses/Model", f"{avg_responses:,.1f}")
                st.metric(
                    "Model Coverage",
                    f"{llm_stats.get('models_tracked', 0)} models",
                    f"{llm_stats.get('total_responses', 0):,} responses"
                )

        # Database Update Status
        st.markdown("### Database Updates")
        updates_col1, updates_col2, updates_col3 = st.columns(3)
        
        with updates_col1:
            last_content = content_stats.get('latest_update', 'Never')
            is_current = last_content == datetime.now().strftime('%Y-%m-%d')
            st.metric(
                "Content Database",
                "Up to date" if is_current else "Needs update",
                f"Last Updated: {last_content}",
                delta_color="normal" if is_current else "inverse"
            )
            
        with updates_col2:
            last_rankings = ranking_stats.get('latest_check', 'Never')
            is_current = last_rankings == datetime.now().strftime('%Y-%m-%d')
            st.metric(
                "Rankings Database",
                "Up to date" if is_current else "Needs update",
                f"Last Updated: {last_rankings}",
                delta_color="normal" if is_current else "inverse"
            )
            
        with updates_col3:
            last_llm = llm_stats.get('latest_check', 'Never')
            is_current = last_llm == datetime.now().strftime('%Y-%m-%d')
            st.metric(
                "LLM Database",
                "Up to date" if is_current else "Needs update",
                f"Last Updated: {last_llm}",
                delta_color="normal" if is_current else "inverse"
            )
            
        # Processing Status Section
        st.markdown("### Processing Status")
        if content_stats.get('status_counts'):
            status_cols = st.columns(len(content_stats['status_counts']))
            for idx, (status, count) in enumerate(content_stats['status_counts'].items()):
                with status_cols[idx]:
                    st.metric(
                        status,
                        count,
                        f"{(count/content_stats['total_urls']*100):.1f}%" if content_stats['total_urls'] > 0 else "0%"
                    )

class DataView:
    """Enhanced data view with comprehensive database exploration capabilities."""
    
    @staticmethod
    def render():
        """Render the data view."""
        st.header("Raw Data Explorer")
        
        # Database selector
        db_tabs = st.tabs(["Content URLs", "Rankings", "LLM Analysis"])
        
        # Content URLs Tab
        with db_tabs[0]:
            DataView._render_urls_view()
            
        # Rankings Tab
        with db_tabs[1]:
            DataView._render_rankings_view()
            
        # LLM Analysis Tab
        with db_tabs[2]:
            DataView._render_llm_view()
    
    @staticmethod
    def _render_urls_view():
        """Render Content URLs data view."""
        st.subheader("Content Database")
        
        # Filters
        with st.expander("Filters", expanded=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                domain_filter = st.multiselect(
                    "Domain",
                    options=db_ops.get_content_domains(),
                    placeholder="All Domains",
                    key="urls_domain_filter"
                )
                status_filter = st.multiselect(
                    "Status",
                    options=["Pending", "Processed", "Failed", "Updated"],
                    placeholder="All Statuses",
                    key="urls_status_filter"
                )
            
            with col2:
                start_date = st.date_input(
                    "Start Date",
                    value=datetime.now().date() - timedelta(days=30),
                    key="urls_start_date"
                )
                end_date = st.date_input(
                    "End Date",
                    value=datetime.now().date(),
                    key="urls_end_date"
                )
                
                date_range = (start_date, end_date) if start_date and end_date else None
            
            with col3:
                search_query = st.text_input(
                    "Search URLs", 
                    placeholder="Enter keywords...",
                    key="urls_search"
                )
                min_words = st.number_input(
                    "Min Word Count", 
                    min_value=0,
                    key="urls_min_words"
                )
        
        # Get filtered data
        df = db_ops.fetch_filtered_urls(
            domains=domain_filter if domain_filter else None,
            statuses=status_filter if status_filter else None,
            date_range=date_range,
            search=search_query if search_query else None,
            min_words=min_words if min_words > 0 else None
        )
        
        # Display data
        if not df.empty:
            # Create column config with proper date handling
            column_config = {
                "url": st.column_config.LinkColumn("URL"),
                "domain_name": st.column_config.TextColumn("Domain"),
                "status": st.column_config.TextColumn("Status"),
                "datePublished": st.column_config.DateColumn(
                    "Published Date",
                    format="YYYY-MM-DD"
                ),
                "dateModified": st.column_config.DateColumn(
                    "Modified Date",
                    format="YYYY-MM-DD"
                ),
                "last_analyzed": st.column_config.DateColumn(
                    "Last Analyzed",
                    format="YYYY-MM-DD"
                )
            }
            
            st.dataframe(
                df,
                column_config=column_config,
                hide_index=True,
                use_container_width=True
            )
            
            st.markdown(f"**Showing {len(df):,} records**")
        else:
            st.info("No records found matching the selected filters")
    
    @staticmethod
    def _render_rankings_view():
        """Render Rankings data view."""
        st.subheader("Rankings Database")
        
        # Filters
        with st.expander("Filters", expanded=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                keyword_filter = st.multiselect(
                    "Keywords",
                    options=db_ops.get_keywords(),
                    placeholder="All Keywords",
                    key="rankings_keyword_filter"
                )
                domain_filter = st.multiselect(
                    "Domain",
                    options=db_ops.get_unique_domains(),
                    placeholder="All Domains",
                    key="rankings_domain_filter"
                )
            
            with col2:
                try:
                    date_range = st.date_input(
                        "Check Date Range",
                        value=[
                            (datetime.now() - timedelta(days=30)).date(),
                            datetime.now().date()
                        ],
                        key="rankings_date_range"
                    )
                except:
                    date_range = [
                        (datetime.now() - timedelta(days=30)).date(),
                        datetime.now().date()
                    ]
                
            with col3:
                position_range = st.slider(
                    "Position Range",
                    min_value=1,
                    max_value=100,
                    value=(1, 100),
                    key="rankings_position_range"
                )
        
        # Get filtered data
        df = db_ops.get_ranking_data(
            keywords=keyword_filter if keyword_filter else None,
            domains=domain_filter if domain_filter else None,
            position_range=position_range,
            date_range=date_range if isinstance(date_range, (list, tuple)) and len(date_range) == 2 else None
        )
        
        DataView._render_data_table(df, "rankings")

    @staticmethod
    def _render_llm_view():
        """Render LLM Analysis data view."""
        st.subheader("LLM Analysis Database")
        
        # Filters
        with st.expander("Filters", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                keyword_filter = st.multiselect(
                    "Keywords",
                    options=db_ops.get_keywords(),
                    placeholder="All Keywords",
                    key="llm_keyword_filter"
                )
                models = db_ops.get_model_list()
                model_filter = st.multiselect(
                    "Models",
                    options=models,
                    placeholder="All Models",
                    key="llm_model_filter"
                )
            
            with col2:
                try:
                    date_range = st.date_input(
                        "Check Date Range",
                        value=None,
                        key="llm_date_range"
                    )
                except:
                    date_range = (None, None)
                
                mention_filter = st.selectbox(
                    "Mention Filter",
                    options=["All", "With Mentions", "Without Mentions"],
                    key="llm_mention_filter"
                )
        
        # Get filtered data
        df = db_ops.get_llm_data(
            keywords=keyword_filter if keyword_filter else None,
            models=model_filter if model_filter else None,
            date_range=date_range if isinstance(date_range, tuple) and date_range[0] else None,
            mentions=mention_filter
        )
        
        DataView._render_data_table(df, "llm")
    
    @staticmethod
    def _render_data_table(df: pd.DataFrame, table_type: str):
        """Render an enhanced data table with actions."""
        if df.empty:
            st.warning("No data found matching the selected filters.")
            return
            
        # Format dates based on table type
        try:
            if table_type == "urls":
                date_columns = ['datePublished', 'dateModified', 'last_analyzed']
            elif table_type == "rankings":
                date_columns = ['created_at', 'check_date']
            elif table_type == "llm":
                date_columns = ['check_date']
            
            # Format only existing date columns
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        except Exception as e:
            st.warning(f"Note: Some date formatting could not be applied: {str(e)}")
        
        # Table stats
        st.markdown(f"**Showing {len(df):,} records**")
        
        # Export buttons
        col1, col2 = st.columns([1, 5])
        with col1:
            st.download_button(
                label="Export CSV",
                data=df.to_csv(index=False).encode('utf-8'),
                file_name=f"{table_type}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"{table_type}_export_button"
            )
        
        # Enhanced table display with type-specific column configurations
        column_config = {
            # Common configurations
            "url": st.column_config.LinkColumn("URL"),
            "domain_name": st.column_config.TextColumn("Domain"),
            "keyword": st.column_config.TextColumn("Keyword"),
        }
        
        # Add table-specific configurations
        if table_type == "urls":
            column_config.update({
                "status": st.column_config.TextColumn("Status", help="Processing status"),
                "estimated_word_count": st.column_config.NumberColumn("Words", help="Estimated word count"),
                "datePublished": st.column_config.DateColumn("Published Date"),
                "dateModified": st.column_config.DateColumn("Modified Date"),
                "last_analyzed": st.column_config.DateColumn("Last Analyzed")
            })
        elif table_type == "rankings":
            column_config.update({
                "position": st.column_config.NumberColumn("Position", help="Search position"),
                "domain": st.column_config.TextColumn("Domain", help="Website domain"),
                "check_date": st.column_config.DateColumn("Check Date")
            })
        elif table_type == "llm":
            # Add any LLM-specific column configurations
            column_config.update({
                "check_date": st.column_config.DateColumn("Check Date")
            })
        
        st.dataframe(
            df,
            column_config=column_config,
            hide_index=True,
            use_container_width=True
        )
        
        # Show completeness metrics for URLs
        if table_type == "urls" and not df.empty:
            st.markdown("### Data Completeness")
            metrics_cols = st.columns(4)
            complete_records = df.notna().mean() * 100
            for idx, (col, completeness) in enumerate(complete_records.items()):
                with metrics_cols[idx % 4]:
                    st.metric(
                        f"{table_type}_completeness_{col}",
                        f"{completeness:.1f}%",
                        delta="Complete" if completeness > 90 else "Incomplete",
                        delta_color="normal" if completeness > 90 else "inverse"
                    )

class InsightsView:
    """Implements the Content Insights view."""
    
    @staticmethod
    def render():
        """Main render method for the Insights tab."""
        st.header("Content Insights")
        
        # Date range selector for all sections
        col1, col2 = st.columns([2, 1])
        with col1:
            date_range = st.date_input(
                "Select Date Range",
                value=[datetime.now() - timedelta(days=30), datetime.now()],
                key="content_insights_date_range"
            )
        with col2:
            selected_domains = st.multiselect(
                "Select Domains",
                options=db_ops.get_unique_domains(),
                default=None,
                placeholder="All Domains"
            )

        # Create tabs for different analyses
        tabs = st.tabs([
            "Category Analysis", 
            "Content Length", 
            "Keyword Analysis",
            "Recent Activity"
        ])

        # Category Analysis Tab
        with tabs[0]:
            InsightsView._render_category_distribution()

        # Content Length Tab
        with tabs[1]:
            InsightsView._render_word_count_analysis(date_range)

        # Keyword Analysis Tab
        with tabs[2]:
            InsightsView._render_keyword_analysis()

        # Recent Activity Tab
        with tabs[3]:
            InsightsView._render_recent_activity()

    @staticmethod
    def _render_category_distribution():
        """Render category distribution analysis with improved layout."""
        st.subheader("Content Category Distribution")
        
        # Get category data
        df = db_ops.get_category_distribution()
        if not df.empty:
            # Create two columns
            col1, col2 = st.columns(2)
            
            # Process each domain
            domains = sorted(df['domain_name'].unique())
            for idx, domain in enumerate(domains):
                # Alternate between columns
                with col1 if idx % 2 == 0 else col2:
                    domain_data = df[df['domain_name'] == domain]
                    total_articles = domain_data['count'].sum()
                    
                    # Create pie chart
                    fig = px.pie(
                        domain_data,
                        values='count',
                        names='category',
                        title=f"{domain}<br><sup>Total: {total_articles:,} articles</sup>"
                    )
                    
                    # Update layout and traces
                    fig.update_layout(
                        height=400,  # Taller chart
                        title_x=0.5,  # Center title
                        margin=dict(t=60, l=20, r=20, b=20),  # Adjust margins
                        showlegend=False,
                        legend=dict(
                            yanchor="top",
                            y=0.99,
                            xanchor="left",
                            x=0.01
                        )
                    )
                    
                    # Update pie chart appearance
                    fig.update_traces(
                        textposition='inside',
                        textinfo='percent+label',
                        insidetextorientation='radial',
                        hovertemplate="<b>%{label}</b><br>" +
                                    "Count: %{value}<br>" +
                                    "Percentage: %{percent:.1%}<extra></extra>"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Add metrics below chart
                    top_categories = domain_data.nlargest(3, 'count')
                    st.markdown("**Top Categories:**")
                    for _, row in top_categories.iterrows():
                        percentage = (row['count'] / total_articles) * 100
                        st.markdown(f"- {row['category']}: {row['count']:,} ({percentage:.1f}%)")
    @staticmethod
    def _render_word_count_analysis(date_range):
        """Render word count analysis."""
        st.subheader("Content Length Analysis")
        
        # Get word count data
        word_count_df = db_ops.get_word_count_data(
            start_date=date_range[0] if isinstance(date_range, (list, tuple)) else None,
            end_date=date_range[1] if isinstance(date_range, (list, tuple)) else None
        )
        
        if not word_count_df.empty:
            # Calculate metrics by domain
            metrics = word_count_df.groupby('domain_name')['Word Count'].agg([
                ('mean', 'mean'),
                ('median', 'median')
            ]).round(0)
            
            # Display metrics
            cols = st.columns(4)
            for idx, (domain, stats) in enumerate(metrics.iterrows()):
                with cols[idx % 4]:
                    st.metric(
                        label=str(domain),
                        value=f"Avg: {stats['mean']:.0f}",
                        delta=f"Median: {stats['median']:.0f}"
                    )

            # Trend chart
            fig = px.scatter(
                word_count_df,
                x='Date',
                y='Word Count',
                color='domain_name',
                opacity=0.6,
                hover_data=['url']
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No word count data available for the selected date range")


    @staticmethod
    def _render_keyword_analysis():
        """Render keyword distribution analysis."""
        st.subheader("Primary Keywords Distribution")
        
        df = db_ops.get_keyword_distribution()
        if not df.empty:
            fig = px.bar(
                df,
                x='Count',
                y='Keyword',
                color='Domain',
                orientation='h',
                height=600
            )
            st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def _render_recent_activity():
        """Render recent activity analysis."""
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Recently Published")
            recent_published = db_ops.fetch_urls_published_last_7_days()
            if recent_published:
                st.dataframe(
                    pd.DataFrame(recent_published, 
                               columns=["Domain", "URL", "Published Date"]),
                    column_config={
                        "URL": st.column_config.LinkColumn("URL"),
                        "Published Date": st.column_config.DateColumn(
                            "Published",
                            format="MMM DD, YYYY"
                        )
                    },
                    hide_index=True
                )

        with col2:
            st.subheader("Recently Modified")
            recent_modified = db_ops.fetch_urls_modified_last_7_days()
            if recent_modified:
                st.dataframe(
                    pd.DataFrame(recent_modified, 
                               columns=["Domain", "URL", "Modified Date", "Published Date"]),
                    column_config={
                        "URL": st.column_config.LinkColumn("URL"),
                        "Modified Date": st.column_config.DateColumn(
                            "Modified",
                            format="MMM DD, YYYY"
                        ),
                        "Published Date": st.column_config.DateColumn(
                            "Published",
                            format="MMM DD, YYYY"
                        )
                    },
                    hide_index=True
                )


class PositionView:
    @staticmethod
    def render():
        """Render the position tracking view."""
        st.header("Search Position Tracking")
        
        # Get all keywords
        all_keywords = db_ops.get_keywords()
        
        # Get date range for initial data
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=90)
        
        # Get all ranking data for summary
        rankings_df = db_ops.get_ranking_data(
            keywords=all_keywords,
            start_date=start_date,
            end_date=end_date
        )
        
        if not rankings_df.empty:
            # Render summary dashboard with all data
            PositionView._render_summary_dashboard(rankings_df)
            PositionView._render_competitive_landscape(rankings_df)
            PositionView._render_key_changes(rankings_df)

            with st.expander("View Detailed Analysis", expanded=False):
                selected_keywords = filters.keyword_selector(multiple=True)
                selected_date_range = filters.date_range_selector()
                
                if selected_keywords:
                    filtered_df = db_ops.get_ranking_data(
                        keywords=selected_keywords,
                        date_range=selected_date_range
                    )
                    
                    st.subheader("Latest Rankings")
                    PositionView._render_latest_rankings(filtered_df)
                else:
                    st.info("Select keywords above to see detailed rankings.")
        else:
            st.warning("No ranking data available for analysis.")

    @staticmethod
    def _render_summary_dashboard(df: pd.DataFrame):
        """Render summary metrics dashboard."""
        latest_date = df['check_date'].max()
        latest_data = df[df['check_date'] == latest_date]
        
        # Filter for our domain
        our_domain = "atlan.com"
        our_rankings = latest_data[latest_data['domain'] == our_domain]
        
        # Get previous date for movement comparison
        all_dates = sorted(df['check_date'].unique())
        if len(all_dates) > 1:
            prev_date = all_dates[-2]
            prev_data = df[
                (df['check_date'] == prev_date) & 
                (df['domain'] == our_domain)
            ]
        
        st.subheader(f"Rankings Summary - {latest_date.strftime('%Y-%m-%d')}")
        
        # Create metric rows
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_keywords = len(latest_data['keyword'].unique())
            st.metric(
                "Total Keywords",
                total_keywords
            )
        
        with col2:
            first_page = len(our_rankings[our_rankings['position'] <= 10])
            first_page_pct = (first_page / total_keywords * 100) if total_keywords > 0 else 0
            st.metric(
                "First Page Rankings",
                f"{first_page} ({first_page_pct:.1f}%)"
            )
        
        with col3:
            top_3 = len(our_rankings[our_rankings['position'] <= 3])
            top_3_pct = (top_3 / total_keywords * 100) if total_keywords > 0 else 0
            st.metric(
                "Top 3 Positions",
                f"{top_3} ({top_3_pct:.1f}%)"
            )
        
        with col4:
            if not our_rankings.empty:
                avg_position = our_rankings['position'].mean()
                if len(all_dates) > 1 and not prev_data.empty:
                    prev_avg = prev_data['position'].mean()
                    delta = prev_avg - avg_position  # Positive delta is good (moved up in rankings)
                    st.metric(
                        "Average Position",
                        f"{avg_position:.1f}",
                        delta=f"{abs(delta):.1f}",
                        delta_color="normal" if delta > 0 else "inverse"
                    )
                else:
                    st.metric("Average Position", f"{avg_position:.1f}")
            else:
                st.metric("Average Position", "N/A")

    @staticmethod
    def _render_competitive_landscape(df: pd.DataFrame):
        """Render competitive landscape analysis."""
        latest_date = df['check_date'].max()
        latest_data = df[df['check_date'] == latest_date]
        
        st.subheader("Competitive Landscape")
        
        # Share of Voice (First page presence)
        first_page_data = latest_data[latest_data['position'] <= 10].copy()
        domain_share = (
            first_page_data.groupby('domain')
            .agg({
                'keyword': 'count',
                'position': 'mean'
            })
            .sort_values('keyword', ascending=False)
            .reset_index()
        )
        
        # Calculate percentages
        total_first_page = len(first_page_data)
        domain_share['share_percentage'] = domain_share['keyword'] / total_first_page * 100
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("Share of Voice (First Page Rankings)")
            fig = px.bar(
                domain_share,
                x='domain',
                y='share_percentage',
                text=domain_share['keyword'].astype(str),
                title="Domain Presence in First Page Results",
                labels={
                    'domain': 'Domain',
                    'share_percentage': 'Share of First Page (%)',
                }
            )
            fig.update_traces(textposition='auto')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Average Positions")
            # Sort by average position for top domains
            top_domains = domain_share.nlargest(5, 'keyword')
            for _, row in top_domains.iterrows():
                st.metric(
                    row['domain'],
                    f"Pos: {row['position']:.1f}",
                    f"{row['keyword']} keywords"
                )

    @staticmethod
    def _render_position_trends(df: pd.DataFrame):
        """Render position trends chart."""
        if not df.empty:
            fig = px.line(
                df[df['position'] <= 10],
                x='check_date',
                y='position',
                color='domain',
                title='Ranking Positions Over Time'
            )
            
            fig.update_layout(
                yaxis_title="Position",
                yaxis_autorange="reversed",  # Lower position numbers at top
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No trend data available for the selected filters.")

    @staticmethod
    def _render_latest_rankings(df: pd.DataFrame):
        """Render latest rankings table."""
        if not df.empty:
            latest_date = df['check_date'].max()
            latest_rankings = df[
                (df['check_date'] == latest_date) & 
                (df['position'] <= 10)
            ].copy()
            
            if not latest_rankings.empty:
                st.dataframe(
                    latest_rankings,
                    column_config={
                        "keyword": st.column_config.TextColumn("Keyword"),
                        "position": st.column_config.NumberColumn("Position", format="%d"),
                        "domain": st.column_config.TextColumn("Domain"),
                        "url": st.column_config.LinkColumn("URL")
                    },
                    hide_index=True
                )
            else:
                st.info("No rankings in top 10 positions for the selected filters.")
        else:
            st.info("No ranking data available.")

    @staticmethod
    def _render_key_changes(df: pd.DataFrame):
        """Render key changes and opportunities section."""
        st.subheader("Key Changes & Opportunities")
        
        # Get last two dates for comparison
        dates = sorted(df['check_date'].unique())
        if len(dates) < 2:
            st.warning("Not enough historical data for movement analysis")
            return
            
        current_date = dates[-1]
        previous_date = dates[-2]
        
        current_data = df[df['check_date'] == current_date]
        previous_data = df[df['check_date'] == previous_date]
        
        col1, col2 = st.columns(2)
        
        with col1:
            with st.container(border=True):
                st.subheader("🎯 Biggest Movements", divider="gray")
                
                # Calculate position changes
                changes = {}
                for _, current in current_data[current_data['domain'] == 'atlan.com'].iterrows():
                    prev = previous_data[
                        (previous_data['keyword'] == current['keyword']) & 
                        (previous_data['domain'] == 'atlan.com')
                    ]
                    if not prev.empty:
                        change = prev.iloc[0]['position'] - current['position']
                        changes[current['keyword']] = {
                            'change': change,
                            'current': current['position'],
                            'previous': prev.iloc[0]['position']
                        }
                
                # Show top improvements and drops
                improvements = {k: v for k, v in changes.items() if v['change'] > 0}
                drops = {k: v for k, v in changes.items() if v['change'] < 0}
                
                if improvements:
                    st.markdown("**Top Improvements** 📈")
                    for keyword, data in sorted(improvements.items(), 
                                             key=lambda x: x[1]['change'], 
                                             reverse=True)[:3]:
                        st.metric(
                            keyword,
                            f"Position: {data['current']}",
                            f"↑ {data['change']} spots",
                            delta_color="normal"
                        )
                
                if drops:
                    st.markdown("**Biggest Drops** 📉")
                    for keyword, data in sorted(drops.items(), 
                                             key=lambda x: x[1]['change'])[:3]:
                        st.metric(
                            keyword,
                            f"Position: {data['current']}",
                            f"↓ {abs(data['change'])} spots",
                            delta_color="inverse"
                        )
        
        with col2:
            with st.container(border=True):
                st.subheader("🎲 Opportunities", divider="gray")
                
                # Find keywords where we're close to first page
                near_first_page = current_data[
                    (current_data['domain'] == 'atlan.com') &
                    (current_data['position'].between(11, 15))
                ]
                
                if not near_first_page.empty:
                    st.markdown("**Near First Page** 🚀")
                    for _, row in near_first_page.iterrows():
                        st.metric(
                            row['keyword'],
                            f"Position: {row['position']}",
                            "Close to first page!"
                        )
                
                # Find competitor drops
                competitor_drops = []
                for domain in current_data['domain'].unique():
                    if domain != 'atlan.com':
                        domain_current = current_data[current_data['domain'] == domain]
                        domain_previous = previous_data[previous_data['domain'] == domain]
                        
                        for _, current in domain_current.iterrows():
                            prev = domain_previous[domain_previous['keyword'] == current['keyword']]
                            if not prev.empty and current['position'] > prev.iloc[0]['position'] + 5:
                                competitor_drops.append({
                                    'keyword': current['keyword'],
                                    'domain': domain,
                                    'drop': current['position'] - prev.iloc[0]['position']
                                })
                
                if competitor_drops:
                    st.markdown("**Competitor Drops** 👀")
                    for drop in sorted(competitor_drops, key=lambda x: x['drop'], reverse=True)[:3]:
                        st.metric(
                            f"{drop['keyword']} ({drop['domain']})",
                            f"Dropped {drop['drop']} spots",
                            "Opportunity!"
                        )
        
        # Render ranking distribution heatmap
        st.subheader("Ranking Distribution", divider="gray")
        PositionView._render_ranking_heatmap(df[df['domain'] == 'atlan.com'])

    @staticmethod
    def _render_ranking_heatmap(df: pd.DataFrame):
        """Render heatmap showing ranking distribution over time."""
        if df.empty:
            st.info("No ranking data available for heatmap")
            return
            
        # Create position buckets: individual 1-10 and 10+
        positions = list(range(1, 11))  # 1 through 10
        position_labels = [str(i) for i in positions] + ['10+']
        
        # Prepare data for heatmap
        heatmap_data = []
        for date in sorted(df['check_date'].unique()):
            date_data = df[df['check_date'] == date]
            row = {'date': date.strftime('%Y-%m-%d')}
            
            # Count keywords at each position 1-10
            for pos in positions:
                row[str(pos)] = len(date_data[date_data['position'] == pos])
            
            # Count everything above position 10
            row['10+'] = len(date_data[date_data['position'] > 10])
            
            heatmap_data.append(row)
        
        heatmap_df = pd.DataFrame(heatmap_data)
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_df[position_labels].values.T,
            x=heatmap_df['date'],
            y=position_labels,
            colorscale='Blues',
            text=heatmap_df[position_labels].values.T,
            texttemplate="%{text}",
            textfont={"size": 12},
            hoverongaps=False
        ))
        
        fig.update_layout(
            title="Keyword Distribution by Position",
            xaxis_title="Date",
            yaxis_title="Position",
            height=600,  # Increased height for better visibility of individual positions
            yaxis_autorange='reversed'  # Put position 1 at the top
        )
        
        st.plotly_chart(fig, use_container_width=True)

class LLMView:
    @staticmethod
    def render():
        st.header("LLM Analysis")
        
        # Mention Rates
        st.subheader("Mention Rates by Date")
        LLMView._render_mention_rates()
        
        # Mention Trends
        st.subheader("Mention Trends")
        LLMView._render_mention_trends()
        
        # Competitor Analysis
        st.subheader("Competitor Analysis")
        LLMView._render_competitor_analysis()

    @staticmethod
    def _render_mention_rates():
        """Render mention rates summary."""
        df = db_ops.get_mention_rates()
        
        st.dataframe(
            df,
            column_config={
                "Date": st.column_config.TextColumn(
                    "Date",
                    width="medium"
                ),
                **{
                    col: st.column_config.NumberColumn(
                        col,
                        format="%.1f%%",
                        width="small"
                    ) for col in df.columns if col != "Date"
                }
            },
            hide_index=True,
            use_container_width=True
        )

    @staticmethod
    def _render_mention_trends():
        """Render mention trends charts."""
        for model in db_ops.get_model_list():
            df = db_ops.get_llm_mention_data(model)
            
            plot_df = pd.DataFrame({
                'Date': df['check_date'],
                'Count': df['true_count'],
                'Type': 'Mentions'
            })
            
            plot_df = pd.concat([
                plot_df,
                pd.DataFrame({
                    'Date': df['check_date'],
                    'Count': df['false_count'],
                    'Type': 'No Mentions'
                })
            ])
            
            fig = px.line(
                plot_df,
                x='Date',
                y='Count',
                color='Type',
                title=f"Mentions Over Time - {model.replace('_', ' ').title()}"
            )
            
            fig.update_layout(
                height=400,
                xaxis_title="Date",
                yaxis_title="Number of Responses",
                hovermode='x unified',
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def _render_competitor_analysis():
        """Render competitor analysis."""
        competitor_data = db_ops.get_competitor_mentions()
        
        fig = px.line(
            competitor_data,
            x='Date',
            y='Mentions',
            color='Company',
            title="Competitor Mentions Over Time"
        )
        
        fig.update_layout(
            height=500,
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            yaxis_title="Number of Mentions",
            xaxis_title="Date"
        )
        
        st.plotly_chart(fig, use_container_width=True)



# Export views for use in main.py
views = {
    "dashboard": DashboardView,
    "data": DataView,
    "insights": InsightsView,
    "position": PositionView,
    "llm": LLMView,
    "qa": QAView 
}