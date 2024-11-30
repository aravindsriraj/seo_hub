import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Any, Optional, Union
from seo_hub.core.config import config


class DatabaseOperations:
    """Handles all database operations for the SEO Hub application."""

    @staticmethod
    def get_connection(db_path: str) -> sqlite3.Connection:
        """Create a database connection."""
        return sqlite3.connect(db_path)

    # ====================== URL Database Operations ======================
    def setup_urls_database(self) -> bool:
        """Initialize the URLs database with required tables."""
        if not config.check_database_exists(config.URLS_DB_PATH):
            return False
        
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                domain_name TEXT NOT NULL,
                status TEXT NOT NULL,
                summary TEXT,
                category TEXT,
                primary_keyword TEXT,
                word_count INTEGER,
                estimated_word_count INTEGER,
                datePublished TEXT,
                dateModified TEXT
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

    def insert_urls(self, urls: List[Tuple[str, str]]) -> None:
        """Insert new URLs into the database."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        for url, domain_name in urls:
            cursor.execute('SELECT COUNT(*) FROM urls WHERE url = ?', (url,))
            exists = cursor.fetchone()[0]
            
            if exists == 0:
                cursor.execute(
                    'INSERT INTO urls (url, domain_name, status) VALUES (?, ?, ?)',
                    (url, domain_name, 'Pending')
                )
        
        conn.commit()
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

    def update_url_analysis(self, url_id: int, summary: str, category: str, 
                          primary_keyword: str, status: str = 'Processed') -> None:
        """Update URL analysis results."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE urls 
            SET status = ?, summary = ?, category = ?, primary_keyword = ? 
            WHERE id = ?
        ''', (status, summary, category, primary_keyword, url_id))
        
        conn.commit()
        conn.close()

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
    def get_ranking_data(self, keywords: List[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Get ranking data for specified keywords and date range."""
        try:
            conn = self.get_connection(config.RANKINGS_DB_PATH)
            
            # Create placeholders for SQL IN clause
            placeholders = ','.join(['?' for _ in keywords])
            
            query = f"""
            SELECT 
                k.keyword,
                r.check_date,
                r.position,
                r.domain,
                r.url
            FROM keywords k
            JOIN rankings r ON k.id = r.keyword_id
            WHERE k.keyword IN ({placeholders})
            AND r.check_date BETWEEN ? AND ?
            ORDER BY k.keyword, r.check_date, r.position
            """
            
            # Combine all parameters
            params = keywords + [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
            
            df = pd.read_sql_query(query, conn, params=params)
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
            WHERE datePublished >= date('now', '-? days')
        """, (days,))
        
        data = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        df = pd.DataFrame(data, columns=columns)
    
        conn.close()
        return df
        
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
            WHERE datePublished >= date('now', '-7 days')
        ''')
        
        data = cursor.fetchall()
        conn.close()
        return data
    
    def fetch_all_urls(self) -> pd.DataFrame:
        """Fetch all URLs from the database."""
        conn = self.get_connection(config.URLS_DB_PATH)
        
        # Get all column names
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM urls LIMIT 1')
        columns = [description[0] for description in cursor.description]
        
        # Fetch all records
        query = f'SELECT {", ".join(columns)} FROM urls'
        df = pd.read_sql_query(query, conn)
        
        conn.close()
        return df

    def fetch_filtered_urls(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Fetch a subset of URLs with pagination."""
        conn = self.get_connection(config.URLS_DB_PATH)
        
        query = '''
            SELECT *
            FROM urls
            LIMIT ? OFFSET ?
        '''
        
        df = pd.read_sql_query(query, conn, params=[end_idx - start_idx, start_idx])
        conn.close()
        return df

    def fetch_urls_modified_last_7_days(self) -> List[Tuple]:
        """Fetch URLs modified in the last 7 days."""
        conn = self.get_connection(config.URLS_DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT domain_name, url, dateModified, datePublished
            FROM urls
            WHERE dateModified >= date('now', '-7 days')
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

    def get_word_count_data(self) -> pd.DataFrame:
        """Get word count distribution over time."""
        conn = self.get_connection(config.URLS_DB_PATH)
        
        query = '''
        SELECT 
            domain_name,
            datePublished as Date,
            estimated_word_count as 'Word Count'
        FROM urls 
        WHERE datePublished IS NOT NULL 
            AND estimated_word_count IS NOT NULL
        ORDER BY datePublished
        '''
        
        df = pd.read_sql_query(query, conn)
        df['Date'] = pd.to_datetime(df['Date'])
        conn.close()
        return df

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

# Create a global instance of DatabaseOperations
db_ops = DatabaseOperations()