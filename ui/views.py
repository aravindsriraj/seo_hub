from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px
from typing import List, Dict, Any, Union
from seo_hub.ui.components import metrics, charts, filters, progress, tables
from seo_hub.core.config import config
from seo_hub.core.services import url_service, content_processor, ranking_service, llm_analyzer
from seo_hub.data.operations import db_ops
from seo_hub.ui.qa_view import QAView

class DashboardView:
    """Implements the Key Statistics dashboard tab."""
    
    @staticmethod
    def render():
        st.header("Database Dashboard")
        
        # Fetch current status
        total_rows, status_counts, domain_counts = db_ops.get_database_status()
        
        # Display overview metrics
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
        # Fetch data and add debug prints
        df = db_ops.get_category_distribution()
        
        # Debug information
        st.write("Debug Information:")
        st.write("Columns in dataframe:", df.columns.tolist())
        st.write("First few rows of data:")
        st.write(df.head())
        
        try:
            domains = df['domain_name'].unique()
            st.write("Unique domains found:", domains)
            
            if len(domains) > 0:
                cols = st.columns(len(domains))
                
                for idx, domain in enumerate(domains):
                    with cols[idx]:
                        st.write(f"Creating chart for domain: {domain}")
                        domain_df = df[df['domain_name'] == domain].copy()
                        domain_df = domain_df.rename(columns={
                            'domain_name': 'Domain',
                            'category': 'Category',
                            'count': 'Count'
                        })
                        st.write(f"Data for {domain}:")
                        st.write(domain_df)
                        
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
            st.write("Full error details:", e)

    @staticmethod
    def _render_word_count_analysis():
        """Render word count analysis."""
        try:
            df = db_ops.get_word_count_data()
            
            # Rename columns for consistency
            df = df.rename(columns={
                'domain_name': 'Domain',
                'Date': 'Date',
                'Word Count': 'Word Count'
            })
            
            fig = px.scatter(
                df,
                x='Date',
                y='Word Count',
                color='Domain',  # Changed from domain_name to Domain
                title='Content Length Distribution'
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error creating word count analysis: {str(e)}")
            st.write("Full error details:", e)

    @staticmethod
    def _render_keyword_analysis():
        """Render keyword distribution analysis."""
        try:
            df = db_ops.get_keyword_distribution()
            
            # Rename columns for consistency
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
            st.write("Full error details:", e)
    @staticmethod
    def _render_content_age_analysis():
        """Render content aging analysis."""
        df = db_ops.get_content_age_data()
        
        # Create age distribution plot
        fig1 = px.histogram(
            df,
            x='content_age_days',
            color='domain_name',
            title="Content Age Distribution",
            labels={'content_age_days': 'Age (Days)', 'count': 'Number of Pages'},
            marginal="box"
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        # Create publishing patterns plot
        fig2 = px.line(
            df.groupby(['year_week', 'domain_name'])['url'].count().reset_index(),
            x='year_week',
            y='url',
            color='domain_name',
            title='Publishing Patterns Over Time',
            labels={'url': 'Number of Pages Published', 'year_week': 'Week'}
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        # Word count by category boxplot
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
    def render():
        st.header("Search Position Tracking")
        
        # Filters
        selected_keywords = filters.keyword_selector(multiple=True)
        date_range = filters.date_range_selector()
        
        if not selected_keywords:
            st.warning("Please select at least one keyword")
            return
            
        # Fetch and display ranking data
        rankings_df = db_ops.get_ranking_data(selected_keywords, date_range[0], date_range[1])
        
        # Position Trends
        st.subheader("Position Trends")
        PositionView._render_position_trends(rankings_df)
        
        # Latest Rankings
        st.subheader("Latest Rankings")
        PositionView._render_latest_rankings(rankings_df)

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
    def _render_rankings_analysis():
        """Render rankings analysis visualizations."""
        df = db_ops.get_rankings_analysis_data()
        
        # Position volatility
        fig1 = px.scatter(
            df,
            x='check_date',
            y='position',
            color='domain',
            size='volatility',
            hover_data=['keyword'],
            title='Ranking Position Volatility',
            labels={'position': 'Position', 'check_date': 'Date'}
        )
        fig1.update_yaxes(autorange="reversed")  # Reverse y-axis so position 1 is at top
        st.plotly_chart(fig1, use_container_width=True)
        
        # Position distribution heatmap
        position_ranges = ['1-3', '4-10', '11-20', '21-50', '51-100']
        fig2 = go.Figure(data=go.Heatmap(
            z=df.pivot_table(
                values='count',
                index='domain',
                columns='position_range'
            ).values,
            x=position_ranges,
            y=df['domain'].unique(),
            colorscale='Viridis'
        ))
        fig2.update_layout(title='Position Distribution Heatmap')
        st.plotly_chart(fig2, use_container_width=True)
    
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
        
        # Configure the table display
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
    def _render_llm_enhanced_analysis():
        """Render enhanced LLM analysis."""
        df = db_ops.get_llm_enhanced_data()
        
        # Model agreement heatmap
        fig1 = px.imshow(
            df.pivot_table(
                values='agreement_score',
                index='model1',
                columns='model2'
            ),
            title='Model Agreement Heatmap'
        )
        st.plotly_chart(fig1, use_container_width=True)
        
        # Response patterns over time
        fig2 = px.line(
            df.groupby(['check_date', 'model'])['response_length'].mean().reset_index(),
            x='check_date',
            y='response_length',
            color='model',
            title='Response Length Trends'
        )
        st.plotly_chart(fig2, use_container_width=True)
        
        # Model comparison metrics
        fig3 = px.bar(
            df.groupby('model')[['accuracy', 'relevance']].mean().reset_index(),
            x='model',
            y=['accuracy', 'relevance'],
            title='Model Performance Metrics',
            barmode='group'
        )
        st.plotly_chart(fig3, use_container_width=True)

    @staticmethod
    def _render_mention_trends():
        """Render mention trends charts."""
        for model in db_ops.get_model_list():
            df = db_ops.get_llm_mention_data(model)
            
            # Create a new dataframe with the structure we want
            plot_df = pd.DataFrame({
                'Date': df['check_date'],
                'Count': df['true_count'],
                'Type': 'Mentions'
            })
            
            # Add the "No Mentions" data
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