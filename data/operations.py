import sqlite3
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta, date
from typing import List, Dict, Tuple, Any, Optional, Union
from core.config import config


class DatabaseOperations:
    """Handles all database operations for the SEO Hub application."""

    @staticmethod
    def get_connection(db_path: str="urls_analysis.db") -> sqlite3.Connection:
        """Create a database connection."""
        return sqlite3.connect(db_path)

    # ====================== URL Database Operations ======================

    def setup_urls_database(self) -> bool:
        """Initialize the URLs database with required tables."""
        if not config.check_database_exists(config.URLS_DB_PATH):
            return False
        
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        # Main URLs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                domain_name TEXT NOT NULL,
                status TEXT NOT NULL,
                summary TEXT,
                category TEXT,
                primary_keyword TEXT,
                word_count INTEGER,
                estimated_word_count INTEGER,
                datePublished TEXT,
                dateModified TEXT,
                last_analyzed TIMESTAMP,
                analysis_version TEXT
            )
        ''')
        
        # Create index for common queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_urls_domain_date 
            ON urls(domain_name, datePublished, dateModified)
        ''')
        
        # Create content history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS url_content_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url_id INTEGER,
                change_date TIMESTAMP,
                field_changed TEXT,
                old_value TEXT,
                new_value TEXT,
                word_count_delta INTEGER,
                FOREIGN KEY (url_id) REFERENCES urls(id)
            )
        ''')
        
        conn.commit()
        conn.close()
        return True

    def get_database_status(self) -> Tuple[int, List[Tuple], List[Tuple]]:
        """Get current database statistics."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM urls')
        total_rows = cursor.fetchone()[0]
        
        cursor.execute('SELECT status, COUNT(*) FROM urls GROUP BY status')
        status_counts = cursor.fetchall()
        
        cursor.execute('SELECT domain_name, COUNT(*) FROM urls GROUP BY domain_name')
        domain_counts = cursor.fetchall()
        
        conn.close()
        return total_rows, status_counts, domain_counts

    def insert_urls(self, urls: List[Tuple[str, str]]) -> Optional[int]:
        """Insert new URLs and return the last inserted ID."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            last_id = None
            for url, domain_name in urls:
                try:
                    cursor.execute("""
                        # Insert new URL, or update domain if URL already exists
                        INSERT INTO urls (url, domain_name, status)
                        VALUES (?, ?, 'pending')
                        ON CONFLICT(url) DO UPDATE SET
                            domain_name = excluded.domain_name
                        RETURNING id
                    """, (url, domain_name))
                    
                    result = cursor.fetchone()
                    if result:
                        last_id = result[0]
                except sqlite3.IntegrityError:
                    # Get existing URL ID
                    cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
                    result = cursor.fetchone()
                    if result:
                        last_id = result[0]
            
            conn.commit()
            return last_id
            
        except Exception as e:
            st.error(f"Error inserting URLs: {str(e)}")
            return None
        finally:
            conn.close()

    def get_pending_urls(self, limit: int = 450) -> List[Tuple]:
        """Get a batch of pending URLs for processing."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM urls 
            WHERE status = "Pending" 
            ORDER BY RANDOM() 
            LIMIT ?
        ''', (limit,))
        
        urls = cursor.fetchall()
        conn.close()
        return urls

    # def update_url_analysis(self, url_id: Optional[int], url: str, summary: str,  # Added url parameter
    #                     category: str, primary_keyword: str, 
    #                     status: str = 'Processed',
    #                     estimated_word_count: int = None) -> bool:
    #     """Update URL analysis results with change tracking."""
    #     try:
    #         conn = self.get_connection(config.URLS_DB_PATH)
    #         cursor = conn.cursor()
            
    #         print(f"Attempting to update analysis for URL: {url}")  # Debug
            
    #         current_time = datetime.now().isoformat()
            
    #         if url_id is None:
    #             # Get ID for specific URL instead of MAX
    #             cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
    #             result = cursor.fetchone()
    #             url_id = result[0] if result else None
    #             print(f"Retrieved URL ID: {url_id} for URL: {url}")  # Debug
                
    #             if not url_id:
    #                 print(f"No URL ID found for: {url}")  # Debug
    #                 return False
            
    #         # Get current values for change tracking
    #         cursor.execute("""
    #             SELECT summary, category, primary_keyword, estimated_word_count
    #             FROM urls WHERE id = ?
    #         """, (url_id,))
    #         current = cursor.fetchone()
    #         print(f"Current values for URL ID {url_id}: {current}")  # Debug
            
    #         # Update main record
    #         print(f"Updating record with: status={status}, summary={summary[:50]}...")  # Debug
    #         cursor.execute("""
    #             UPDATE urls 
    #             SET status = ?,
    #                 summary = ?,
    #                 category = ?,
    #                 primary_keyword = ?,
    #                 estimated_word_count = ?,
    #                 last_analyzed = ?,
    #                 analysis_version = ?
    #             WHERE id = ?
    #         """, (status, summary, category, primary_keyword, 
    #             estimated_word_count, current_time, '1.0', url_id))
            
    #         rows_affected = cursor.rowcount
    #         print(f"Rows affected by update: {rows_affected}")  # Debug
            
    #         conn.commit()
    #         return True
                
    #     except Exception as e:
    #         print(f"Error in update_url_analysis: {str(e)}")  # Debug
    #         return False
    #     finally:
    #         conn.close()
 
    def get_content_age_data(self) -> pd.DataFrame:
        """Get content age and related metrics."""
        conn = self.get_connection(config.URLS_DB_PATH)
        
        query = """
        WITH ContentMetrics AS (
            SELECT 
                domain_name,
                url,
                category,
                estimated_word_count,
                datePublished,
                dateModified,
                strftime('%Y-%W', datePublished) as year_week,
                julianday('now') - julianday(datePublished) as content_age_days,
                CASE 
                    WHEN dateModified IS NOT NULL 
                    THEN julianday(dateModified) - julianday(datePublished)
                    ELSE NULL 
                END as days_to_update
            FROM urls
            WHERE datePublished IS NOT NULL
        )
        SELECT *,
            CASE 
                WHEN content_age_days <= 30 THEN '0-30 days'
                WHEN content_age_days <= 90 THEN '31-90 days'
                WHEN content_age_days <= 180 THEN '91-180 days'
                ELSE 'Over 180 days'
            END as age_bucket
        FROM ContentMetrics
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    # ====================== Rankings Database Operations ======================
    # def get_ranking_data(self, keywords: List[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    #     """Get ranking data for specified keywords and date range."""
    #     try:
    #         conn = self.get_connection(config.RANKINGS_DB_PATH)
            
    #         # Create placeholders for SQL IN clause
    #         placeholders = ','.join(['?' for _ in keywords])
            
    #         query = f"""
    #         SELECT 
    #             k.keyword,
    #             r.check_date,
    #             r.position,
    #             r.domain,
    #             r.url
    #         FROM keywords k
    #         JOIN rankings r ON k.id = r.keyword_id
    #         WHERE k.keyword IN ({placeholders})
    #         AND r.check_date BETWEEN ? AND ?
    #         ORDER BY k.keyword, r.check_date, r.position
    #         """
            
    #         # Combine all parameters
    #         params = keywords + [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
            
    #         df = pd.read_sql_query(query, conn, params=params)
    #         conn.close()
    #         return df
            
    #     except Exception as e:
    #         st.error(f"Error fetching ranking data: {str(e)}")
    #         return pd.DataFrame()

    # def get_ranking_data(
    #     self,
    #     keywords: Optional[List[str]] = None,
    #     domains: Optional[List[str]] = None,
    #     position_range: Optional[Tuple[int, int]] = None,
    #     date_range: Optional[Tuple[datetime, datetime]] = None
    # ) -> pd.DataFrame:
    #     """Fetch ranking data with filtering options."""
    #     try:
    #         conn = self.get_connection(config.RANKINGS_DB_PATH)
            
    #         query = """
    #         SELECT r.*, k.keyword 
    #         FROM rankings r
    #         JOIN keywords k ON r.keyword_id = k.id
    #         WHERE 1=1
    #         """
    #         params = []

    #         if keywords:
    #             query += " AND k.keyword IN ({})".format(",".join("?" * len(keywords)))
    #             params.extend(keywords)

    #         if domains:
    #             query += " AND r.domain IN ({})".format(",".join("?" * len(domains)))
    #             params.extend(domains)

    #         if position_range:
    #             query += " AND r.position BETWEEN ? AND ?"
    #             params.extend(position_range)

    #         if date_range and date_range[0] and date_range[1]:
    #             query += " AND r.check_date BETWEEN ? AND ?"
    #             params.extend([date_range[0].isoformat(), date_range[1].isoformat()])

    #         df = pd.read_sql_query(query, conn, params=params)
    #         conn.close()
    #         return df

    #     except Exception as e:
    #         st.error(f"Error fetching ranking data: {str(e)}")
    #         return pd.DataFrame()

    def get_ranking_data(
        self, 
        keywords: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        domains: Optional[List[str]] = None,
        position_range: Optional[Tuple[int, int]] = None,
        date_range: Optional[Tuple[date, date]] = None  # Added for compatibility
    ) -> pd.DataFrame:
        """Get ranking data with flexible date handling.
        
        Args:
            keywords: Optional list of keywords to filter by
            start_date: Optional explicit start date
            end_date: Optional explicit end date
            domains: Optional list of domains to filter by
            position_range: Optional tuple of (min_position, max_position)
            date_range: Optional tuple of (start_date, end_date) - alternative to start_date/end_date
        """
        try:
            conn = self.get_connection(config.RANKINGS_DB_PATH)
            
            # Handle date parameters
            if date_range and len(date_range) == 2:
                start_date = date_range[0]
                end_date = date_range[1]
            
            if not start_date or not end_date:
                # Default to last 90 days if no dates provided
                end_date = datetime.now().date()
                start_date = end_date - timedelta(days=90)
            
            # Build query
            query = """
            SELECT 
                k.keyword,
                r.check_date,
                r.position,
                r.domain,
                r.url
            FROM keywords k
            JOIN rankings r ON k.id = r.keyword_id
            WHERE 1=1
            """
            params = []

            # Add filters
            if keywords:
                query += f" AND k.keyword IN ({','.join(['?' for _ in keywords])})"
                params.extend(keywords)

            query += " AND r.check_date BETWEEN ? AND ?"
            params.extend([start_date.isoformat(), end_date.isoformat()])

            if domains:
                query += f" AND r.domain IN ({','.join(['?' for _ in domains])})"
                params.extend(domains)

            if position_range and len(position_range) == 2:
                query += " AND r.position BETWEEN ? AND ?"
                params.extend(position_range)

            query += " ORDER BY k.keyword, r.check_date, r.position"
            
            df = pd.read_sql_query(query, conn, params=params)
            
            # Convert check_date to datetime
            if 'check_date' in df.columns:
                df['check_date'] = pd.to_datetime(df['check_date'])
                
            conn.close()
            return df
                
        except Exception as e:
            st.error(f"Error fetching ranking data: {str(e)}")
            return pd.DataFrame()
        
    def get_rankings_analysis_data(self) -> pd.DataFrame:
        """Get rankings analysis data."""
        conn = self.get_connection(config.RANKINGS_DB_PATH)
        
        query = """
        WITH RankingMetrics AS (
            SELECT 
                r.keyword_id,
                k.keyword,
                r.check_date,
                r.position,
                r.domain,
                LAG(r.position) OVER (
                    PARTITION BY r.keyword_id, r.domain 
                    ORDER BY r.check_date
                ) as prev_position,
                CASE 
                    WHEN r.position BETWEEN 1 AND 3 THEN '1-3'
                    WHEN r.position BETWEEN 4 AND 10 THEN '4-10'
                    WHEN r.position BETWEEN 11 AND 20 THEN '11-20'
                    WHEN r.position BETWEEN 21 AND 50 THEN '21-50'
                    ELSE '51-100'
                END as position_range
            FROM rankings r
            JOIN keywords k ON r.keyword_id = k.id
        )
        SELECT 
            *,
            ABS(COALESCE(position - prev_position, 0)) as volatility,
            COUNT(*) OVER (
                PARTITION BY domain, position_range
            ) as count
        FROM RankingMetrics
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    # ====================== AI Models Database Operations ======================
    def get_available_keywords(self) -> List[str]:
        """Get list of available keywords from the database."""
        try:
            conn = self.get_connection(config.AIMODELS_DB_PATH)
            query = """
            SELECT DISTINCT keyword 
            FROM keyword_rankings 
            WHERE check_date >= date('now', '-30 days')
            ORDER BY keyword
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            return df['keyword'].tolist()
            
        except Exception as e:
            st.error(f"Error fetching keywords: {str(e)}")
            return []
    
    def get_llm_mention_data(self, model_name: str) -> pd.DataFrame:
        """Get mention data for a specific model."""
        try:
            conn = self.get_connection(config.AIMODELS_DB_PATH)
            
            mention_col = f"{model_name}_atlan_mention"
            query = f"""
            SELECT 
                check_date,
                SUM(CASE WHEN {mention_col} = 1 THEN 1 ELSE 0 END) as true_count,
                SUM(CASE WHEN {mention_col} = 0 THEN 1 ELSE 0 END) as false_count
            FROM keyword_rankings
            GROUP BY check_date
            ORDER BY check_date
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            return df
            
        except Exception as e:
            st.error(f"Error fetching LLM mention data: {str(e)}")
            return pd.DataFrame()
    
    def get_mention_rates(self) -> pd.DataFrame:
        """Get mention rates for all models by date."""
        try:
            conn = self.get_connection(config.AIMODELS_DB_PATH)
            
            # Get all columns ending with '_atlan_mention'
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(keyword_rankings)")
            columns = [info[1] for info in cursor.fetchall()]
            mention_columns = [col for col in columns if col.endswith('_atlan_mention')]
            
            # Create the SQL query dynamically for each model
            select_clauses = []
            for col in mention_columns:
                model_name = col.replace('_atlan_mention', '')
                select_clauses.append(f"""
                    ROUND(SUM(CASE WHEN {col} = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1)
                    as "{model_name}"
                """)
            
            query = f"""
            SELECT 
                check_date as "Date",
                {', '.join(select_clauses)}
            FROM keyword_rankings
            GROUP BY check_date
            ORDER BY check_date DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # Convert date to datetime and format it
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            
            # Clean up column names
            df.columns = [col.replace('_', ' ').title() if col != 'Date' else col for col in df.columns]
            
            return df
            
        except Exception as e:
            st.error(f"Error fetching mention rates: {str(e)}")
            return pd.DataFrame()

    def get_model_list(self) -> List[str]:
        """Get list of all models in the database."""
        try:
            conn = self.get_connection(config.AIMODELS_DB_PATH)
            cursor = conn.cursor()
            
            # Get all columns ending with '_atlan_mention'
            cursor.execute("PRAGMA table_info(keyword_rankings)")
            columns = [info[1] for info in cursor.fetchall()]
            mention_columns = [col.replace('_atlan_mention', '') 
                             for col in columns if col.endswith('_atlan_mention')]
            
            conn.close()
            return mention_columns
            
        except Exception as e:
            st.error(f"Error fetching model list: {str(e)}")
            return []

    def get_competitor_mentions(self) -> pd.DataFrame:
        """Get competitor mention data."""
        try:
            conn = self.get_connection(config.AIMODELS_DB_PATH)
            
            # Get all answer columns
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(keyword_rankings)")
            columns = [info[1] for info in cursor.fetchall()]
            answer_columns = [col for col in columns if col.endswith('_answer')]
            
            # Create SQL for checking mentions
            competitors = ['atlan', 'alation', 'collibra']
            select_clauses = []
            
            for competitor in competitors:
                conditions = []
                for col in answer_columns:
                    conditions.append(f"lower({col}) LIKE '%{competitor}%'")
                select_clauses.append(f"""
                    SUM(CASE WHEN ({' OR '.join(conditions)}) THEN 1 ELSE 0 END) 
                    as {competitor}_mentions
                """)
            
            query = f"""
            SELECT 
                check_date as Date,
                {', '.join(select_clauses)}
            FROM keyword_rankings
            GROUP BY check_date
            ORDER BY check_date
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # Melt the dataframe to get it in the desired format
            df = pd.melt(
                df,
                id_vars=['Date'],
                var_name='Company',
                value_name='Mentions'
            )
            
            # Clean up company names
            df['Company'] = df['Company'].str.replace('_mentions', '').str.title()
            
            return df
            
        except Exception as e:
            st.error(f"Error fetching competitor mentions: {str(e)}")
            return pd.DataFrame()
        
    # == Recent content updates for analysis == #

    def get_recent_content_updates(self, days: int) -> pd.DataFrame:
        """
        Fetch recent content updates from the database.
        
        Parameters:
        days (int): The number of days to consider for recent updates.
        
        Returns:
        pandas.DataFrame: A dataframe containing the recent content updates.
        """
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        print("fetching new pages")
        cursor.execute("""
            SELECT 
                domain_name, 
                url, 
                datePublished, 
                dateModified,
                category,
                primary_keyword,
                estimated_word_count
            FROM urls
            WHERE dateModified >= date('now', '-' || ? || ' days')
            OR datePublished >= date('now', '-' || ? || ' days')
        """, (days,days))
        
        data = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        print(df)
        print("fetched pages")
        conn.close()
        return df
    
    # ====================== Get Ranking Changes for Analysis ===================== #

    def get_ranking_changes(self, days: int) -> pd.DataFrame:
        """Get ranking changes over the specified number of days."""
        try:
            conn = self.get_connection(config.RANKINGS_DB_PATH)
            
            query = """
            WITH RankingChanges AS (
                SELECT 
                    k.keyword,
                    r.check_date,
                    r.position,
                    r.domain,
                    r.url,
                    LAG(r.position) OVER (
                        PARTITION BY k.keyword, r.domain 
                        ORDER BY r.check_date
                    ) as previous_position
                FROM keywords k
                JOIN rankings r ON k.id = r.keyword_id
                WHERE r.check_date >= date('now', '-' || ? || ' days')
            )
            SELECT 
                keyword,
                check_date,
                domain,
                position,
                previous_position,
                COALESCE(previous_position, position) - position as position_change
            FROM RankingChanges
            WHERE previous_position IS NOT NULL
            ORDER BY check_date DESC, ABS(position_change) DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(days,))
            conn.close()
            return df
            
        except Exception as e:
            st.error(f"Error fetching ranking changes: {str(e)}")
            return pd.DataFrame() 
        
    # ==== GET LLM Mention Patterns for Analysis ====    

    def get_llm_mention_patterns(self, days: int) -> pd.DataFrame:
        """Get LLM mention patterns over the specified number of days."""
        try:
            conn = self.get_connection(config.AIMODELS_DB_PATH)
            
            # Get all answer columns
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(keyword_rankings)")
            columns = [info[1] for info in cursor.fetchall()]
            answer_columns = [col for col in columns if col.endswith('_answer')]
            
            # Create SELECT clause for all answer columns
            select_parts = ["check_date", "keyword"] + answer_columns
            
            query = f"""
            SELECT {', '.join(select_parts)}
            FROM keyword_rankings
            WHERE check_date >= date('now', '-' || ? || ' days')
            ORDER BY check_date DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(days,))
            conn.close()
            return df
            
        except Exception as e:
            st.error(f"Error fetching LLM mention patterns: {str(e)}")
            print(f"Detailed error: {str(e)}")  # Debug info
            return pd.DataFrame()

    # ====================== Database Maintenance Operations ======================
    def get_column_names(self, table: str, database: str) -> List[str]:
        """Get column names for a specified table."""
        conn = self.get_connection(database)
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM {table} LIMIT 1')
        columns = [description[0] for description in cursor.description]
        conn.close()
        return columns

    def add_column(self, table: str, column_name: str, 
                  column_type: str = 'TEXT') -> None:
        """Add a new column to a specified table."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f'ALTER TABLE {table} ADD COLUMN {column_name} {column_type}')
        conn.commit()
        conn.close()

    def drop_column(self, table: str, column_name: str) -> None:
        """Drop a column from a specified table."""
        if self.is_column_critical(column_name):
            raise ValueError(f"Cannot drop critical column: {column_name}")
            
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f'ALTER TABLE {table} DROP COLUMN {column_name}')
        conn.commit()
        conn.close()

    def fetch_counts_by_time_period(self) -> Dict[str, Dict[str, List[int]]]:
        """Fetch publication and modification counts for different time periods."""
        time_periods = {
            "Last 7 days": 7,
            "Last 14 days": 14,
            "Last 30 days": 30,
            "Last 90 days": 90,
            "Last 180 days": 180,
        }
        
        counts = {}
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        # Get all domains
        cursor.execute('SELECT DISTINCT domain_name FROM urls')
        domains = cursor.fetchall()
        
        for domain in domains:
            domain_name = domain[0]
            counts[domain_name] = {
                "Count of datePublished": [],
                "Count of dateModified": []
            }
            
            for period, days in time_periods.items():
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days)
                
                # Count for datePublished
                cursor.execute('''
                    SELECT COUNT(*) FROM urls 
                    WHERE domain_name = ? 
                    AND datePublished BETWEEN ? AND ?
                ''', (domain_name, start_date.strftime('%Y-%m-%d'), 
                     end_date.strftime('%Y-%m-%d')))
                published_count = cursor.fetchone()[0]
                counts[domain_name]["Count of datePublished"].append(published_count)
                
                # Count for dateModified
                cursor.execute('''
                    SELECT COUNT(*) FROM urls 
                    WHERE domain_name = ? 
                    AND dateModified BETWEEN ? AND ?
                ''', (domain_name, start_date.strftime('%Y-%m-%d'), 
                     end_date.strftime('%Y-%m-%d')))
                modified_count = cursor.fetchone()[0]
                counts[domain_name]["Count of dateModified"].append(modified_count)
        
        conn.close()
        return counts

    def fetch_urls_published_last_7_days(self) -> List[Tuple]:
        """Fetch URLs published in the last 7 days."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT domain_name, url, datePublished
            FROM urls
            WHERE datePublished >= date('now', '-14 days')
        ''')
        
        data = cursor.fetchall()
        conn.close()
        return data
    
    # def fetch_all_urls(self) -> pd.DataFrame:
    #     """Fetch all URLs from the database."""
    #     conn = self.get_connection(config.URLS_DB_PATH)
        
    #     # Get all column names
    #     cursor = conn.cursor()
    #     cursor.execute('SELECT * FROM urls LIMIT 1')
    #     columns = [description[0] for description in cursor.description]
        
    #     # Fetch all records
    #     query = f'SELECT {", ".join(columns)} FROM urls'
    #     df = pd.read_sql_query(query, conn)
        
    #     conn.close()
    #     return df

    def fetch_all_urls(
        self, 
        domains: Optional[List[str]] = None,
        statuses: Optional[List[str]] = None,
        date_range: Optional[Tuple[datetime, datetime]] = None,
        search: Optional[str] = None,
        min_words: int = 0,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Fetch URLs with filtering options."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            
            # Build the SELECT clause
            select_columns = ", ".join(columns) if columns else "*"
            query = f"SELECT {select_columns} FROM urls WHERE 1=1"
            params = []

            # Add filters
            if domains:
                query += " AND domain_name IN ({})".format(",".join("?" * len(domains)))
                params.extend(domains)
                
            if statuses:
                query += " AND status IN ({})".format(",".join("?" * len(statuses)))
                params.extend(statuses)
                
            if date_range and date_range[0] and date_range[1]:
                query += " AND datePublished BETWEEN ? AND ?"
                params.extend([date_range[0].isoformat(), date_range[1].isoformat()])
                
            if search:
                query += " AND (url LIKE ? OR domain_name LIKE ?)"
                search_param = f"%{search}%"
                params.extend([search_param, search_param])
                
            if min_words > 0:
                query += " AND estimated_word_count >= ?"
                params.append(min_words)

            # Execute query
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df

        except Exception as e:
            st.error(f"Error fetching URLs: {str(e)}")
            return pd.DataFrame()

    # def fetch_filtered_urls(self, start_idx: int, end_idx: int) -> pd.DataFrame:
    #     """Fetch a subset of URLs with pagination."""
    #     conn = self.get_connection(config.URLS_DB_PATH)
        
    #     query = '''
    #         SELECT *
    #         FROM urls
    #         LIMIT ? OFFSET ?
    #     '''
        
    #     df = pd.read_sql_query(query, conn, params=[end_idx - start_idx, start_idx])
    #     conn.close()
    #     return df

    # def fetch_filtered_urls(
    #     self,
    #     domains: Optional[List[str]] = None,
    #     statuses: Optional[List[str]] = None,
    #     date_range: Optional[Tuple[datetime, datetime]] = None,
    #     search: Optional[str] = None,
    #     min_words: Optional[int] = None,
    #     columns: Optional[List[str]] = None
    # ) -> pd.DataFrame:
    #     """Fetch URLs with filtering options and proper NULL handling."""
    #     try:
    #         conn = self.get_connection(config.URLS_DB_PATH)
            
    #         # Build the SELECT clause
    #         select_columns = ", ".join(columns) if columns else "*"
    #         query = f"SELECT {select_columns} FROM urls WHERE 1=1"
    #         params = []

    #         # Add filters
    #         if domains:
    #             query += f" AND domain_name IN ({','.join(['?'] * len(domains))})"
    #             params.extend(domains)
                
    #         if statuses:
    #             query += f" AND status IN ({','.join(['?'] * len(statuses))})"
    #             params.extend(statuses)
                
    #         if date_range and isinstance(date_range, tuple) and len(date_range) == 2 and date_range[0]:
    #             query += " AND (datePublished BETWEEN ? AND ? OR datePublished IS NULL)"
    #             params.extend([date_range[0].strftime('%Y-%m-%d'), date_range[1].strftime('%Y-%m-%d')])
                
    #         if search:
    #             query += " AND (url LIKE ? OR domain_name LIKE ?)"
    #             search_param = f"%{search}%"
    #             params.extend([search_param, search_param])
                
    #         if min_words:
    #             query += " AND (estimated_word_count >= ? OR estimated_word_count IS NULL)"
    #             params.append(min_words)

    #         # Execute query
    #         df = pd.read_sql_query(query, conn, params=params)
            
    #         # Convert date columns to datetime, handling NULL values
    #         date_columns = ['datePublished', 'dateModified', 'last_analyzed']
    #         for col in date_columns:
    #             if col in df.columns:
    #                 df[col] = pd.to_datetime(df[col], errors='coerce')
            
    #         conn.close()
    #         return df

    #     except Exception as e:
    #         st.error(f"Error fetching URLs: {str(e)}")
    #         return pd.DataFrame()

    def fetch_filtered_urls(
        self,
        domains: Optional[List[str]] = None,
        statuses: Optional[List[str]] = None,
        date_range: Optional[Tuple[date, date]] = None,
        search: Optional[str] = None,
        min_words: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Fetch URLs with proper date handling."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            
            # Build the SELECT clause
            select_columns = ", ".join(columns) if columns else "*"
            query = f"SELECT {select_columns} FROM urls WHERE 1=1"
            params = []

            # Add filters
            if domains:
                query += f" AND domain_name IN ({','.join(['?'] * len(domains))})"
                params.extend(domains)
                
            if statuses:
                query += f" AND status IN ({','.join(['?'] * len(statuses))})"
                params.extend(statuses)
                
            # Date range filtering
            if date_range and len(date_range) == 2:
                start_date = date_range[0].strftime('%Y-%m-%d')
                end_date = date_range[1].strftime('%Y-%m-%d')
                query += " AND datePublished BETWEEN ? AND ?"
                params.extend([start_date, end_date])

            if search:
                query += " AND (url LIKE ? OR domain_name LIKE ?)"
                search_param = f"%{search}%"
                params.extend([search_param, search_param])
                
            if min_words:
                query += " AND (estimated_word_count >= ? OR estimated_word_count IS NULL)"
                params.append(min_words)

            query += " ORDER BY datePublished DESC"
            
            # Read data
            df = pd.read_sql_query(query, conn, params=params)
            
            conn.close()
            return df

        except Exception as e:
            st.error(f"Error fetching URLs: {str(e)}")
            return pd.DataFrame()

    def fetch_urls_modified_last_7_days(self) -> List[Tuple]:
        """Fetch URLs modified in the last 7 days."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT domain_name, url, dateModified, datePublished
            FROM urls
            WHERE dateModified >= date('now', '-14 days')
            AND dateModified != datePublished
            AND dateModified IS NOT NULL
            ORDER BY dateModified DESC
        ''')
        
        data = cursor.fetchall()
        conn.close()
        return data
    
    def get_category_distribution(self) -> pd.DataFrame:
        """Get the distribution of content categories."""
        conn = self.get_connection(config.URLS_DB_PATH)
        
        query = '''
        SELECT 
            domain_name,
            category,
            COUNT(*) as count 
        FROM urls 
        WHERE category IS NOT NULL 
            AND category != ''
            AND category != 'N/A'
        GROUP BY domain_name, category
        ORDER BY domain_name, count DESC
        '''
        
        # Fetch data and explicitly set column names
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Print column names for debugging
        print("Available columns:", df.columns.tolist())
        
        return df

    def get_word_count_data(self, start_date=None, end_date=None) -> pd.DataFrame:
        """Get word count distribution over time."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            
            query = """
            SELECT 
                domain_name,
                datePublished as Date,
                estimated_word_count as 'Word Count',
                url
            FROM urls 
            WHERE datePublished IS NOT NULL 
                AND estimated_word_count IS NOT NULL
            """
            
            params = []
            if start_date and end_date:
                query += " AND datePublished BETWEEN ? AND ?"
                params.extend([start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')])
                
            query += " ORDER BY datePublished"
            
            df = pd.read_sql_query(query, conn, params=params)
            df['Date'] = pd.to_datetime(df['Date'])
            conn.close()
            return df

        except Exception as e:
            st.error(f"Error fetching word count data: {str(e)}")
            return pd.DataFrame()
    def get_keywords(self) -> List[str]:
        """Fetch all keywords from the rankings database."""
        try:
            conn = self.get_connection(config.RANKINGS_DB_PATH)
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT keyword FROM keywords ORDER BY keyword')
            keywords = [row[0] for row in cursor.fetchall()]
            conn.close()
            return keywords
        except sqlite3.Error as e:
            st.error(f"Database error while fetching keywords: {str(e)}")
            return []
        except Exception as e:
            st.error(f"Error fetching keywords: {str(e)}")
            return []
        
    def get_keyword_distribution(self) -> pd.DataFrame:
        """Get distribution of primary keywords across domains."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            
            query = """
            WITH KeywordMetrics AS (
                SELECT 
                    domain_name as Domain,
                    primary_keyword as Keyword,
                    COUNT(*) as Count,
                    AVG(estimated_word_count) as avg_word_count,
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY domain_name) as percentage
                FROM urls 
                WHERE primary_keyword IS NOT NULL 
                    AND primary_keyword != ''
                    AND primary_keyword != 'N/A'
                GROUP BY domain_name, primary_keyword
            )
            SELECT 
                Domain,
                Keyword,
                Count,
                avg_word_count as "Average Word Count",
                ROUND(percentage, 2) as "Percentage"
            FROM KeywordMetrics
            ORDER BY Domain, Count DESC
            """
            
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # Print debug information
            print("Retrieved keyword distribution data:")
            print(f"Shape: {df.shape}")
            print("Columns:", df.columns.tolist())
            print("First few rows:")
            print(df.head())
            
            return df
            
        except Exception as e:
            st.error(f"Error fetching keyword distribution: {str(e)}")
            print(f"Detailed error: {str(e)}")
            return pd.DataFrame()
        
    def get_domain_metrics(self) -> pd.DataFrame:
        """Get overall metrics by domain."""
        conn = self.get_connection(config.URLS_DB_PATH)
        
        query = '''
        SELECT 
            domain_name,
            COUNT(*) as total_pages,
            AVG(estimated_word_count) as avg_word_count,
            COUNT(DISTINCT category) as unique_categories,
            COUNT(DISTINCT primary_keyword) as unique_keywords
        FROM urls
        GROUP BY domain_name
        '''
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def get_content_timeline(self) -> pd.DataFrame:
        """Get content publishing timeline."""
        conn = self.get_connection(config.URLS_DB_PATH)
        
        query = '''
        SELECT 
            domain_name,
            datePublished,
            COUNT(*) as count
        FROM urls 
        WHERE datePublished IS NOT NULL
        GROUP BY domain_name, datePublished
        ORDER BY datePublished
        '''
        
        df = pd.read_sql_query(query, conn)
        df['datePublished'] = pd.to_datetime(df['datePublished'])
        conn.close()
        return df
    
    @staticmethod
    def is_column_critical(column_name: str) -> bool:
        """Check if a column is critical for application functionality."""
        critical_columns = ["status", "url", "domain_name"]
        return column_name in critical_columns
    
    def _track_content_change(self, cursor: sqlite3.Cursor, url_id: int, 
                            field: str, old_value: str, new_value: str) -> None:
        """Track content changes in history table."""
        cursor.execute("""
            INSERT INTO url_content_changes (
                url_id, change_date, field_changed, 
                old_value, new_value
            ) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?)
        """, (url_id, field, old_value, new_value))

    def get_content_changes(self, url_id: int) -> List[Dict]:
        """Get history of content changes for a URL."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT change_date, field_changed, old_value, new_value
                FROM url_content_changes
                WHERE url_id = ?
                ORDER BY change_date DESC
            """, (url_id,))
            
            changes = []
            for row in cursor.fetchall():
                changes.append({
                    'date': row[0],
                    'field': row[1],
                    'old_value': row[2],
                    'new_value': row[3]
                })
            
            return changes
            
        except Exception as e:
            st.error(f"Error fetching content changes: {str(e)}")
            return []
        finally:
            conn.close()

    def get_url_info(self, url: str) -> Optional[Dict]:
        """Get URL information from database."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM urls WHERE url = ?
            """, (url,))
            
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                return dict(zip(columns, row))
            return None
            
        finally:
            conn.close()

    # def update_url(self, url: str, sitemap_url: str, status: str, **kwargs) -> bool:
    #     """Update or insert URL information."""
    #     try:
    #         conn = self.get_connection(config.URLS_DB_PATH)
    #         cursor = conn.cursor()
            
    #         # Prepare fields and values
    #         fields = ['url', 'sitemap_url', 'status', 'last_processed']  # Changed from last_updated
    #         values = [url, sitemap_url, status, datetime.now().isoformat()]
            
    #         # Add additional fields from kwargs
    #         for key, value in kwargs.items():
    #             if value is not None:
    #                 fields.append(key)
    #                 values.append(value)
            
    #         # Create SQL query
    #         field_names = ', '.join(fields)
    #         placeholders = ', '.join(['?' for _ in fields])
    #         update_stmt = ', '.join(f'{f}=excluded.{f}' for f in fields if f != 'url')
            
    #         # Use upsert
    #         cursor.execute(f"""
    #             INSERT INTO urls ({field_names})
    #             VALUES ({placeholders})
    #             ON CONFLICT(url) DO UPDATE SET
    #             {update_stmt}
    #         """, values)
            
    #         conn.commit()
    #         return True
            
    #     except Exception as e:
    #         print(f"Error updating URL {url}: {str(e)}")
    #         return False
    #     finally:
    #         conn.close()

    def update_url(self, url: str, status: str, **kwargs) -> bool:
        """Update or insert URL information."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            # Only use columns that exist in the schema
            valid_columns = {
                'url', 'domain_name', 'status', 'summary', 
                'category', 'primary_keyword', 'estimated_word_count',
                'datePublished', 'dateModified', 'last_analyzed',
                'analysis_version'
            }
            
            # Prepare fields and values
            fields = ['url', 'status']
            values = [url, status]
            
            # Add additional fields from kwargs if they exist in schema
            for key, value in kwargs.items():
                if key in valid_columns and value is not None:
                    fields.append(key)
                    values.append(value)
            
            # Create SQL query
            field_names = ', '.join(fields)
            placeholders = ', '.join(['?' for _ in fields])
            update_stmt = ', '.join(f'{f}=excluded.{f}' for f in fields if f != 'url')
            
            # Use upsert
            cursor.execute(f"""
                INSERT INTO urls ({field_names})
                VALUES ({placeholders})
                ON CONFLICT(url) DO UPDATE SET
                {update_stmt}
            """, values)
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"Error updating URL {url}: {str(e)}")
            return False
        finally:
            conn.close()

    def get_processing_stats(self) -> Dict:
        """Get processing statistics."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    MIN(last_processed) as oldest,  
                    MAX(last_processed) as newest  
                FROM urls
                GROUP BY status
            """)
            
            stats = {}
            for row in cursor.fetchall():
                stats[row[0]] = {
                    'count': row[1],
                    'oldest': row[2],
                    'newest': row[3]
                }
            
            return stats
            
        finally:
            conn.close()

    def update_url_analysis(self, url: str, summary: str = None, 
                          category: str = None, primary_keyword: str = None,
                          estimated_word_count: int = None) -> bool:
        """Update URL analysis results."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            update_fields = []
            update_values = []
            
            # Add non-None fields
            if summary is not None:
                update_fields.append('summary')
                update_values.append(summary)
            if category is not None:
                update_fields.append('category')
                update_values.append(category)
            if primary_keyword is not None:
                update_fields.append('primary_keyword')
                update_values.append(primary_keyword)
            if estimated_word_count is not None:
                update_fields.append('estimated_word_count')
                update_values.append(estimated_word_count)
            
            if update_fields:
                # Add status and timestamp
                update_fields.extend(['status', 'last_analyzed'])
                update_values.extend(['processed', datetime.now().isoformat()])
                
                # Create update query
                update_stmt = ', '.join(f'{f}=?' for f in update_fields)
                cursor.execute(f"""
                    UPDATE urls 
                    SET {update_stmt}
                    WHERE url = ?
                """, [*update_values, url])
                
                conn.commit()
                return True
            
            return False
            
        except Exception as e:
            print(f"Error in update_url_analysis: {str(e)}")
            return False
        finally:
            conn.close()

    def get_urls_by_status(self, status: str = None, limit: int = 100) -> List[Dict]:
        """Get URLs with specific status."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            if status:
                cursor.execute("""
                    SELECT * FROM urls 
                    WHERE status = ?
                    LIMIT ?
                """, (status, limit))
            else:
                cursor.execute("""
                    SELECT * FROM urls 
                    LIMIT ?
                """, (limit,))
            
            columns = [description[0] for description in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            return results
            
        finally:
            conn.close()

    def get_processing_stats(self) -> Dict:
        """Get processing statistics."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    status,
                    COUNT(*) as count,
                    MIN(last_analyzed) as oldest,
                    MAX(last_analyzed) as newest
                FROM urls
                GROUP BY status
            """)
            
            stats = {}
            for row in cursor.fetchall():
                stats[row[0]] = {
                    'count': row[1],
                    'oldest': row[2],
                    'newest': row[3]
                }
            
            return stats
            
        finally:
            conn.close()

    def get_unique_domains(self) -> List[str]:
        """Get list of unique domains across all databases."""
        try:
            # Get domains from URLs database
            conn_urls = self.get_connection(config.URLS_DB_PATH)
            cursor_urls = conn_urls.cursor()
            cursor_urls.execute("SELECT DISTINCT domain_name FROM urls WHERE domain_name IS NOT NULL")
            url_domains = set(row[0] for row in cursor_urls.fetchall())
            conn_urls.close()

            # Get domains from Rankings database
            conn_rankings = self.get_connection(config.RANKINGS_DB_PATH)
            cursor_rankings = conn_rankings.cursor()
            cursor_rankings.execute("SELECT DISTINCT domain FROM rankings WHERE domain IS NOT NULL")
            ranking_domains = set(row[0] for row in cursor_rankings.fetchall())
            conn_rankings.close()

            # Combine and sort all domains
            all_domains = sorted(url_domains.union(ranking_domains))
            return all_domains

        except Exception as e:
            st.error(f"Error fetching domains: {str(e)}")
            return []

    def get_llm_data(
        self,
        keywords: Optional[List[str]] = None,
        models: Optional[List[str]] = None,
        date_range: Optional[Tuple[datetime, datetime]] = None,
        mentions: str = "All"
    ) -> pd.DataFrame:
        """Fetch LLM analysis data with filtering."""
        try:
            conn = self.get_connection(config.AIMODELS_DB_PATH)
            
            # Start with base query
            query = "SELECT * FROM keyword_rankings WHERE 1=1"
            params = []

            # Add filters
            if keywords:
                query += " AND keyword IN ({})".format(",".join("?" * len(keywords)))
                params.extend(keywords)

            if date_range and date_range[0] and date_range[1]:
                query += " AND check_date BETWEEN ? AND ?"
                params.extend([date_range[0].isoformat(), date_range[1].isoformat()])

            # Handle mention filtering
            if mentions == "With Mentions":
                conditions = []
                for model in models if models else self.get_model_list():
                    conditions.append(f"{model}_atlan_mention = 1")
                if conditions:
                    query += " AND (" + " OR ".join(conditions) + ")"
            elif mentions == "Without Mentions":
                conditions = []
                for model in models if models else self.get_model_list():
                    conditions.append(f"{model}_atlan_mention = 0")
                if conditions:
                    query += " AND (" + " AND ".join(conditions) + ")"

            # Execute query
            df = pd.read_sql_query(query, conn, params=params)
            
            # Filter columns if models specified
            if models:
                base_cols = ['keyword', 'check_date']
                model_cols = []
                for model in models:
                    model_cols.extend([f"{model}_answer", f"{model}_atlan_mention"])
                df = df[base_cols + model_cols]
                
            conn.close()
            return df

        except Exception as e:
            st.error(f"Error fetching LLM data: {str(e)}")
            return pd.DataFrame()

    def get_content_domains(self) -> List[str]:
        """Get list of unique domains from the URLs database only."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT DISTINCT domain_name 
                FROM urls 
                WHERE domain_name IS NOT NULL 
                ORDER BY domain_name
            """)
            
            domains = [row[0] for row in cursor.fetchall()]
            conn.close()
            return domains

        except Exception as e:
            st.error(f"Error fetching domains: {str(e)}")
            return []

    def inspect_date_formats(self) -> Dict[str, List[str]]:
        """Inspect actual date formats in the database."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    datePublished,
                    dateModified,
                    last_analyzed
                FROM urls 
                WHERE datePublished IS NOT NULL 
                OR dateModified IS NOT NULL 
                OR last_analyzed IS NOT NULL 
                LIMIT 5
            """)
            
            results = cursor.fetchall()
            sample_dates = {
                'datePublished': [],
                'dateModified': [],
                'last_analyzed': []
            }
            
            for row in results:
                if row[0]: sample_dates['datePublished'].append(row[0])
                if row[1]: sample_dates['dateModified'].append(row[1])
                if row[2]: sample_dates['last_analyzed'].append(row[2])
                
            conn.close()
            return sample_dates
                
        except Exception as e:
            st.error(f"Error inspecting dates: {str(e)}")
            return {}

    def get_word_count_metrics(self, start_date, end_date):
        """Get word count statistics by domain."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            query = """
                SELECT 
                    domain_name,
                    AVG(estimated_word_count) as mean,
                    AVG(estimated_word_count) as median  -- SQLite doesn't have median, using avg as placeholder
                FROM urls
                WHERE datePublished BETWEEN ? AND ?
                GROUP BY domain_name
            """
            df = pd.read_sql_query(query, conn, params=[start_date, end_date])
            conn.close()
            return df
            
        except Exception as e:
            st.error(f"Error getting word count metrics: {str(e)}")
            return pd.DataFrame()

    def get_unique_domains(self) -> List[str]:
        """Get list of unique domains."""
        try:
            conn = self.get_connection(config.URLS_DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT domain_name FROM urls ORDER BY domain_name")
            domains = [row[0] for row in cursor.fetchall()]
            conn.close()
            return domains
        except Exception as e:
            st.error(f"Error getting domains: {str(e)}")
            return []

# Create a global instance of DatabaseOperations
db_ops = DatabaseOperations()
