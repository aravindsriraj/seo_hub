from datetime import datetime,timedelta
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any, Union
from seo_hub.ui.components import metrics, charts, filters, progress, tables
from seo_hub.core.config import config
from seo_hub.core.services import url_service, content_processor, ranking_service, llm_analyzer
from seo_hub.data.operations import db_ops
from seo_hub.ui.qa_view import QAView

class DashboardView:
    """Implements the Key Statistics dashboard tab."""
    
    @staticmethod
    def calculate_content_stats() -> dict:
        """Calculate statistics for content database."""
        import sqlite3
        conn = sqlite3.connect(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        stats = {}
        
        try:
            # Total URLs
            cursor.execute("SELECT COUNT(*) FROM urls")
            stats['total_urls'] = cursor.fetchone()[0]
            
            # Status distribution
            cursor.execute("SELECT status, COUNT(*) FROM urls GROUP BY status")
            stats['status_counts'] = dict(cursor.fetchall())
            
            # URLs with dates
            cursor.execute("SELECT COUNT(*) FROM urls WHERE datePublished IS NOT NULL")
            stats['urls_with_published_date'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM urls WHERE dateModified IS NOT NULL")
            stats['urls_with_modified_date'] = cursor.fetchone()[0]
            
            # Content completeness
            cursor.execute("SELECT COUNT(*) FROM urls WHERE summary IS NOT NULL AND summary != 'N/A'")
            stats['urls_with_summary'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM urls WHERE category IS NOT NULL AND category != 'N/A'")
            stats['urls_with_category'] = cursor.fetchone()[0]
            
        except Exception as e:
            st.error(f"Error calculating content stats: {str(e)}")
            stats = {}
            
        conn.close()
        return stats

    @staticmethod
    def calculate_ranking_stats() -> dict:
        """Calculate statistics for rankings database."""
        import sqlite3
        try:
            conn = sqlite3.connect(config.RANKINGS_DB_PATH)
            cursor = conn.cursor()
            
            stats = {}
            
            # Total keywords tracked
            cursor.execute("SELECT COUNT(DISTINCT keyword) FROM keywords")
            stats['total_keywords'] = cursor.fetchone()[0]
            
            # Total ranking records
            cursor.execute("SELECT COUNT(*) FROM rankings")
            stats['total_rankings'] = cursor.fetchone()[0]
            
            # Latest check date
            cursor.execute("SELECT MAX(check_date) FROM rankings")
            stats['latest_check'] = cursor.fetchone()[0]
            
            # Domains tracked
            cursor.execute("SELECT COUNT(DISTINCT domain) FROM rankings")
            stats['domains_tracked'] = cursor.fetchone()[0]
            
            conn.close()
            return stats
        except Exception as e:
            st.error(f"Error calculating ranking stats: {str(e)}")
            return {}

    @staticmethod
    def calculate_llm_stats() -> dict:
        """Calculate statistics for LLM analysis database."""
        import sqlite3
        try:
            conn = sqlite3.connect(config.AIMODELS_DB_PATH)
            cursor = conn.cursor()
            
            stats = {}
            
            # Get column info to identify model columns
            cursor.execute("PRAGMA table_info(keyword_rankings)")
            columns = cursor.fetchall()
            
            # Count model columns (those ending with _answer)
            model_columns = [col[1] for col in columns if col[1].endswith('_answer')]
            stats['models_tracked'] = len(model_columns)
            
            # Latest check date
            cursor.execute("SELECT MAX(check_date) FROM keyword_rankings")
            stats['latest_check'] = cursor.fetchone()[0]
            
            # Total responses
            cursor.execute("SELECT COUNT(*) FROM keyword_rankings")
            stats['total_responses'] = cursor.fetchone()[0]
            
            conn.close()
            return stats
        except Exception as e:
            st.error(f"Error calculating LLM stats: {str(e)}")
            return {}
        
    @staticmethod
    def render():
        st.header("Database Dashboard")
        
        # Database Statistics Section
        st.subheader("Database Statistics")
        
        # Content Stats
        with st.expander("Content Database Metrics", expanded=True):
            content_stats = DashboardView.calculate_content_stats()
            if content_stats:
                col1, col2 = st.columns(2)
                
                with col1:
                    total = content_stats.get('total_urls', 0)
                    if total > 0:
                        st.metric("Total URLs", total)
                        st.metric("URLs with Published Date", 
                                f"{content_stats.get('urls_with_published_date', 0)} "
                                f"({content_stats.get('urls_with_published_date', 0)/total*100:.1f}%)")
                        st.metric("URLs with Modified Date",
                                f"{content_stats.get('urls_with_modified_date', 0)} "
                                f"({content_stats.get('urls_with_modified_date', 0)/total*100:.1f}%)")
                
                with col2:
                    st.metric("URLs with Summary",
                            f"{content_stats.get('urls_with_summary', 0)} "
                            f"({content_stats.get('urls_with_summary', 0)/total*100:.1f}%)")
                    st.metric("URLs with Category",
                            f"{content_stats.get('urls_with_category', 0)} "
                            f"({content_stats.get('urls_with_category', 0)/total*100:.1f}%)")

        # Rankings Stats
        with st.expander("Rankings Database Metrics", expanded=True):
            ranking_stats = DashboardView.calculate_ranking_stats()
            if ranking_stats:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Total Keywords", ranking_stats.get('total_keywords', 0))
                    st.metric("Total Rankings", ranking_stats.get('total_rankings', 0))
                
                with col2:
                    st.metric("Domains Tracked", ranking_stats.get('domains_tracked', 0))
                    st.metric("Latest Check", ranking_stats.get('latest_check', 'N/A'))

        # LLM Stats
        with st.expander("LLM Analysis Metrics", expanded=True):
            llm_stats = DashboardView.calculate_llm_stats()
            if llm_stats:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric("Models Tracked", llm_stats.get('models_tracked', 0))
                    st.metric("Total Responses", llm_stats.get('total_responses', 0))
                
                with col2:
                    st.metric("Latest Check", llm_stats.get('latest_check', 'N/A'))
                    avg_responses = (llm_stats.get('total_responses', 0) / 
                                  llm_stats.get('models_tracked', 1) if llm_stats.get('models_tracked', 0) > 0 else 0)
                    st.metric("Avg Responses per Model", f"{avg_responses:.1f}")

        st.markdown("---")
        
        # Original Dashboard Content
        total_rows, status_counts, domain_counts = db_ops.get_database_status()
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total URLs", total_rows)
            status_df = pd.DataFrame(status_counts, columns=["Status", "Count"])
            st.dataframe(status_df)

        with col2:
            st.subheader("URLs by Status")
            for status, count in status_counts:
                st.metric(label=status, value=count)

        with col3:
            st.subheader("URLs by Domain Name")
            domain_df = pd.DataFrame(domain_counts, columns=["Domain", "Count"])
            st.dataframe(domain_df)

        # Display time period analysis
        st.subheader("Content Activity Analysis")
        counts = db_ops.fetch_counts_by_time_period()
        DashboardView._display_time_period_analysis(counts)

        # Display recent activity
        st.subheader("Recent Activity")
        DashboardView._display_recent_activity()

    @staticmethod
    def _display_time_period_analysis(counts):
        """Display analysis by time period."""
        data = []
        for domain, count_data in counts.items():
            data.append([domain, "Published"] + count_data["Count of datePublished"])
            data.append([domain, "Modified"] + count_data["Count of dateModified"])

        df = pd.DataFrame(
            data,
            columns=["Domain Name", "Activity", "Last 7 days", "Last 14 days", 
                    "Last 30 days", "Last 90 days", "Last 180 days"]
        )
        st.dataframe(df)

    @staticmethod
    def _display_recent_activity():
        """Display recent content updates."""
        published = db_ops.fetch_urls_published_last_7_days()
        modified = db_ops.fetch_urls_modified_last_7_days()
        
        if published:
            st.subheader("Recently Published Pages")
            published_df = pd.DataFrame(
                published,
                columns=["Domain Name", "URL", "Date Published"]
            )
            metrics.create_status_table(published_df, {
                "Domain Name": "Domain",
                "URL": st.column_config.LinkColumn("URL"),
                "Date Published": st.column_config.DateColumn(
                    "Published Date",
                    format="MMM DD, YYYY"
                )
            })

        if modified:
            st.subheader("Recently Modified Pages")
            modified_df = pd.DataFrame(
                modified,
                columns=["Domain Name", "URL", "Date Modified", "Date Published"]
            )
            metrics.create_status_table(modified_df, {
                "Domain Name": "Domain",
                "URL": st.column_config.LinkColumn("URL"),
                "Date Modified": st.column_config.DateColumn(
                    "Modified Date",
                    format="MMM DD, YYYY"
                )
            })

class DataView:
    """Implements the Raw Data view tab."""
    
    @staticmethod
    def render():
        st.header("Raw Data View")
        
        column_names = db_ops.get_column_names("urls", config.URLS_DB_PATH)
        df = db_ops.fetch_all_urls()
        
        if df.empty:
            st.write("No data available in the database.")
            return
            
        paginated_df = tables.create_paginated_table(df)
        st.dataframe(paginated_df, use_container_width=True)

class InsightsView:
    @staticmethod
    def render():
        """Main render method for the Insights tab."""
        st.header("Content Insights")
        
        # Category Distribution Section
        st.subheader("Category Distribution by Domain")
        InsightsView._render_category_distribution()
        
        # Word Count Analysis Section
        st.subheader("Content Length Analysis")
        InsightsView._render_word_count_analysis()
        
        # Keyword Distribution Section
        st.subheader("Keyword Distribution")
        InsightsView._render_keyword_analysis()

    @staticmethod
    def _render_category_distribution():
        """Render category distribution charts."""
        df = db_ops.get_category_distribution()
        
        try:
            domains = df['domain_name'].unique()
            
            if len(domains) > 0:
                cols = st.columns(len(domains))
                
                for idx, domain in enumerate(domains):
                    with cols[idx]:
                        domain_df = df[df['domain_name'] == domain].copy()
                        domain_df = domain_df.rename(columns={
                            'domain_name': 'Domain',
                            'category': 'Category',
                            'count': 'Count'
                        })
                        
                        if not domain_df.empty:
                            fig = px.pie(
                                domain_df,
                                values='Count',
                                names='Category',
                                title=f"Categories in {domain}"
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
                            
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.write(f"No data available for {domain}")
            else:
                st.write("No domains found in the data")
                    
        except Exception as e:
            st.error(f"Error creating charts: {str(e)}")

    @staticmethod
    def _render_word_count_analysis():
        """Render word count analysis."""
        try:
            df = db_ops.get_word_count_data()
            
            df = df.rename(columns={
                'domain_name': 'Domain',
                'Date': 'Date',
                'Word Count': 'Word Count'
            })
            
            fig = px.scatter(
                df,
                x='Date',
                y='Word Count',
                color='Domain',
                title='Content Length Distribution'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error creating word count analysis: {str(e)}")

    @staticmethod
    def _render_keyword_analysis():
        """Render keyword distribution analysis."""
        try:
            df = db_ops.get_keyword_distribution()
            
            df = df.rename(columns={
                'domain_name': 'Domain',
                'primary_keyword': 'Keyword',
                'count': 'Count'
            })
            
            fig = px.scatter(
                df,
                x='Domain',
                y='Count',
                color='Domain',
                size='Count',
                text='Keyword',
                title='Keyword Distribution'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error creating keyword analysis: {str(e)}")

    @staticmethod
    def _render_content_age_analysis():
        """Render content aging analysis."""
        df = db_ops.get_content_age_data()
        
        fig1 = px.histogram(
            df,
            x='content_age_days',
            color='domain_name',
            title="Content Age Distribution",
            labels={'content_age_days': 'Age (Days)', 'count': 'Number of Pages'},
            marginal="box"
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        fig2 = px.line(
            df.groupby(['year_week', 'domain_name'])['url'].count().reset_index(),
            x='year_week',
            y='url',
            color='domain_name',
            title='Publishing Patterns Over Time',
            labels={'url': 'Number of Pages Published', 'year_week': 'Week'}
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        fig3 = px.box(
            df,
            x='category',
            y='estimated_word_count',
            color='domain_name',
            title='Word Count Distribution by Category',
            points="all"
        )
        st.plotly_chart(fig3, use_container_width=True)

class PositionView:
    """Implements the Position Tracking tab."""
    
    @staticmethod
    def _render_summary_dashboard(df):
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
        
        st.subheader(f"Rankings Summary - {latest_date}")
        
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
        
        # Position range breakdown
        st.subheader("Position Range Breakdown")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            top_3 = len(our_rankings[our_rankings['position'] <= 3])
            st.metric("Positions 1-3", top_3)
        
        with col2:
            pos_4_5 = len(our_rankings[our_rankings['position'].between(4, 5)])
            st.metric("Positions 4-5", pos_4_5)
        
        with col3:
            pos_6_10 = len(our_rankings[our_rankings['position'].between(6, 10)])
            st.metric("Positions 6-10", pos_6_10)

    @staticmethod
    def _render_competitive_landscape(df):
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
        
        # Position range competition
        st.subheader("Competition by Position Range")
        
        ranges = [(1,3), (4,5), (6,10)]
        position_competition = []
        
        for start, end in ranges:
            range_data = latest_data[latest_data['position'].between(start, end)]
            domain_counts = range_data['domain'].value_counts()
            
            for domain, count in domain_counts.items():
                position_competition.append({
                    'range': f'Pos {start}-{end}',
                    'domain': domain,
                    'keywords': count
                })
        
        competition_df = pd.DataFrame(position_competition)
        
        if not competition_df.empty:
            fig = px.bar(
                competition_df,
                x='range',
                y='keywords',
                color='domain',
                title="Domain Competition by Position Range",
                barmode='group'
            )
            st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def _render_position_trends(df):
        """Render position trends chart."""
        fig = charts.create_line_chart(
            df[df['position'] <= 10],
            x='check_date',
            y='position',
            color='domain',
            title='Ranking Positions Over Time'
        )
        st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def _render_latest_rankings(df):
        """Render latest rankings table."""
        latest_date = df['check_date'].max()
        latest_rankings = df[
            (df['check_date'] == latest_date) & 
            (df['position'] <= 10)
        ].copy()
        
        metrics.create_status_table(latest_rankings, {
            "keyword": "Keyword",
            "position": st.column_config.NumberColumn("Position", format="%d"),
            "domain": "Domain",
            "url": st.column_config.LinkColumn("URL")
        })

    @staticmethod
    def render():
        st.header("Search Position Tracking")
        
        # Get all ranking data for summary
        # Get all keywords
        all_keywords = db_ops.get_keywords()
        # Get min and max dates from the database
        start_date = datetime.now() - timedelta(days=90)  # Default to last 90 days
        end_date = datetime.now()
        
        # Get all ranking data for summary
        rankings_df = db_ops.get_ranking_data(all_keywords, start_date, end_date)
        
        # Render summary dashboard with all data
        PositionView._render_summary_dashboard(rankings_df)

        # Add competitive landscape
        PositionView._render_competitive_landscape(rankings_df)
        
        st.markdown("---")  # Divider
        
        # Filtered view for trends and details
        selected_keywords = filters.keyword_selector(multiple=True)
        date_range = filters.date_range_selector()
        
        if selected_keywords:  # Only show filtered view if keywords selected
            filtered_df = db_ops.get_ranking_data(selected_keywords, date_range[0], date_range[1])
            
            st.subheader("Position Trends")
            PositionView._render_position_trends(filtered_df)
            
            st.subheader("Latest Rankings")
            PositionView._render_latest_rankings(filtered_df)
        else:
            st.info("Select keywords above to see detailed trends and rankings.")

    # @staticmethod
    # def render():
    #     st.header("Search Position Tracking")
        
    #     # Filters
    #     selected_keywords = filters.keyword_selector(multiple=True)
    #     date_range = filters.date_range_selector()
        
    #     if not selected_keywords:
    #         st.warning("Please select at least one keyword")
    #         return
            
    #     # Fetch and display ranking data
    #     rankings_df = db_ops.get_ranking_data(selected_keywords, date_range[0], date_range[1])
        
    #     # Position Trends
    #     st.subheader("Position Trends")
    #     PositionView._render_position_trends(rankings_df)
        
    #     # Latest Rankings
    #     st.subheader("Latest Rankings")
    #     PositionView._render_latest_rankings(rankings_df)

    # @staticmethod
    # def _render_position_trends(df):
    #     """Render position trends chart."""
    #     fig = charts.create_line_chart(
    #         df[df['position'] <= 10],
    #         x='check_date',
    #         y='position',
    #         color='domain',
    #         title='Ranking Positions Over Time'
    #     )
    #     st.plotly_chart(fig, use_container_width=True)

    # @staticmethod
    # def _render_latest_rankings(df):
    #     """Render latest rankings table."""
    #     latest_date = df['check_date'].max()
    #     latest_rankings = df[
    #         (df['check_date'] == latest_date) & 
    #         (df['position'] <= 10)
    #     ].copy()
        
    #     metrics.create_status_table(latest_rankings, {
    #         "keyword": "Keyword",
    #         "position": st.column_config.NumberColumn("Position", format="%d"),
    #         "domain": "Domain",
    #         "url": st.column_config.LinkColumn("URL")
    #     })

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