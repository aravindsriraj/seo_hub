import sqlite3
from typing import Dict, List
import json
import os
from datetime import datetime

class SchemaManager:
    def __init__(self, rankings_db: str, urls_db: str, aimodels_db: str):
        self.databases = {
            'rankings': rankings_db,
            'urls_analysis': urls_db,
            'aimodels': aimodels_db
        }
  
    def get_schema(self) -> str:
        """Get formatted schema for all databases with context."""
        schema_info = []
        
        # Add schema header
        schema_info.append(f"Database Schema Details (Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
        schema_info.append("\nEach database serves a specific purpose. Here's what you'll find in each:")
        
        # Database purposes
        purposes = {
            'rankings': "Contains search engine ranking data. Use for position tracking and ranking analysis.",
            'urls_analysis': "Contains analyzed content and metadata. Use for content insights and analysis.",
            'url_tracker': "Contains URL discovery and sitemap tracking data. Use for monitoring URL sources.",
            'aimodels': "Contains LLM response data. Use for analyzing AI model responses."
        }
        
        for db_name, db_path in self.databases.items():
            schema_info.append(f"\n\nDatabase: {db_name}.db")
            schema_info.append(f"Purpose: {purposes[db_name]}")
            schema_info.append("-" * 50)
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get all tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    if not table_name.startswith('sqlite_'):
                        schema_info.append(f"\nTable: {table_name}")
                        
                        # Get column info
                        cursor.execute(f'PRAGMA table_info({table_name})')
                        columns = cursor.fetchall()
                        
                        for col in columns:
                            name, type_name, notnull, dflt_value, pk = col[1:6]
                            constraints = []
                            if pk:
                                constraints.append('PRIMARY KEY')
                            if notnull:
                                constraints.append('NOT NULL')
                            if dflt_value is not None:
                                constraints.append(f'DEFAULT {dflt_value}')
                                
                            schema_info.append(f"  - {name} ({type_name}) {' '.join(constraints)}")
                        
                        # Add foreign key info
                        cursor.execute(f'PRAGMA foreign_key_list({table_name})')
                        foreign_keys = cursor.fetchall()
                        if foreign_keys:
                            schema_info.append("\n  Foreign Keys:")
                            for fk in foreign_keys:
                                schema_info.append(f"  - {fk[3]} references {fk[2]}({fk[4]})")
                
                conn.close()
                
            except Exception as e:
                schema_info.append(f"Error reading schema: {str(e)}")
        
        return "\n".join(schema_info)

    def get_query_context(self) -> str:
        """Get context for SQL query generation."""
        context = [
            "Guidelines for querying databases:",
            "",
            "1. For ranking analysis:",
            "   - Use rankings.db (tables: keywords, rankings)",
            "   - Join on keyword_id for complete ranking data",
            "   - Filter by check_date for specific timeframes",
            "",
            "2. For content analysis:",
            "   - Use urls_analysis.db (table: urls)",
            "   - Contains processed content metadata",
            "   - Use datePublished/dateModified for temporal analysis",
            "",
            "3. For URL tracking:",
            "   - Use url_tracker.db (tables: sitemap_tracking, url_tracking)",
            "   - Links sitemaps to discovered URLs",
            "",
            "4. For LLM analysis:",
            "   - Use aimodels.db (table: keyword_rankings)",
            "   - Contains model responses and mention tracking",
            "",
            "Important notes:",
            "- Always specify full table names including database context",
            "- Use appropriate joins when combining data across tables",
            "- Consider date ranges for time-sensitive queries"
        ]
        
        return "\n".join(context)

    def save_schema_snapshot(self):
        """Save current schema to knowledge base for vector storage."""
        schema = self.get_schema()
        context = self.get_query_context()
        
        snapshot = {
            'timestamp': datetime.now().isoformat(),
            'schema': schema,
            'query_context': context
        }
        
        # Save to knowledge base
        kb_path = os.path.join(os.path.dirname(self.config.PROJECT_ROOT), 
                              'knowledge_base', 'schema')
        os.makedirs(kb_path, exist_ok=True)
        
        with open(os.path.join(kb_path, 'current_schema.json'), 'w') as f:
            json.dump(snapshot, f, indent=2)
