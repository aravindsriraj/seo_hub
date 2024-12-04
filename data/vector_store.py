import chromadb
from chromadb.utils import embedding_functions
import json
from typing import List, Dict, Optional
import os

class VectorStore:
    def __init__(self, persist_directory: str = "chroma_db"):
        """Initialize ChromaDB with persistent storage."""
        self.client = chromadb.PersistentClient(path=persist_directory)
        self.embedding_func = embedding_functions.DefaultEmbeddingFunction()
        
        # Initialize collections
        self.patterns = self.client.get_or_create_collection(
            name="sql_patterns",
            embedding_function=self.embedding_func
        )
        self.trends = self.client.get_or_create_collection(
            name="trends",
            embedding_function=self.embedding_func
        )
        self.competitors = self.client.get_or_create_collection(
            name="competitors",
            embedding_function=self.embedding_func
        )
    
    def add_sql_pattern(self, question_pattern: str, sql_query: str, metadata: Dict = None):
        """Add a SQL query pattern."""
        self.patterns.add(
            documents=[sql_query],
            metadatas=[{
                "question_pattern": question_pattern,
                **(metadata or {})
            }],
            ids=[f"sql_{len(self.patterns.get()['ids']) + 1}"]
        )
    
    def add_trend(self, trend: str, source: str, date: str, metadata: Dict = None):
        """Add an industry trend."""
        self.trends.add(
            documents=[trend],
            metadatas=[{
                "source": source,
                "date": date,
                **(metadata or {})
            }],
            ids=[f"trend_{len(self.trends.get()['ids']) + 1}"]
        )
    
    def add_competitor_insight(self, competitor: str, insight: str, date: str, metadata: Dict = None):
        """Add a competitor insight."""
        self.competitors.add(
            documents=[insight],
            metadatas=[{
                "competitor": competitor,
                "date": date,
                **(metadata or {})
            }],
            ids=[f"comp_{len(self.competitors.get()['ids']) + 1}"]
        )
    
    def query_similar(self, query: str, n_results: int = 5) -> Dict:
        """Query all collections for similar content."""
        results = {
            "patterns": self.patterns.query(
                query_texts=[query],
                n_results=n_results
            ),
            "trends": self.trends.query(
                query_texts=[query],
                n_results=n_results
            ),
            "competitors": self.competitors.query(
                query_texts=[query],
                n_results=n_results
            )
        }
        return results

    def load_initial_data(self, knowledge_base_dir: str):
        """Load initial data from knowledge base directory."""
        # Load SQL patterns
        patterns_file = os.path.join(knowledge_base_dir, "patterns/sql_patterns.json")
        if os.path.exists(patterns_file):
            with open(patterns_file, 'r') as f:
                patterns = json.load(f)
                for pattern in patterns:
                    self.add_sql_pattern(
                        question_pattern=pattern['question'],
                        sql_query=pattern['sql'],
                        metadata=pattern.get('metadata')
                    )
        
        # Load trends
        trends_file = os.path.join(knowledge_base_dir, "trends/trends.json")
        if os.path.exists(trends_file):
            with open(trends_file, 'r') as f:
                trends = json.load(f)
                for trend in trends:
                    self.add_trend(
                        trend=trend['content'],
                        source=trend['source'],
                        date=trend['date'],
                        metadata=trend.get('metadata')
                    )
        
        # Load competitor insights
        competitors_file = os.path.join(knowledge_base_dir, "competitors/insights.json")
        if os.path.exists(competitors_file):
            with open(competitors_file, 'r') as f:
                insights = json.load(f)
                for insight in insights:
                    self.add_competitor_insight(
                        competitor=insight['competitor'],
                        insight=insight['content'],
                        date=insight['date'],
                        metadata=insight.get('metadata')
                    )
            """Load initial data from knowledge base directory."""
            # Load SQL patterns
            patterns_file = os.path.join(knowledge_base_dir, "patterns/sql_patterns.json")
            if os.path.exists(patterns_file):
                with open(patterns_file, 'r') as f:
                    patterns = json.load(f)
                    for pattern in patterns:
                        self.add_sql_pattern(
                            question_pattern=pattern['question'],
                            sql_query=pattern['sql'],
                            metadata=pattern.get('metadata')
                        )
            
            # Load trends
            trends_file = os.path.join(knowledge_base_dir, "trends/trends.json")
            if os.path.exists(trends_file):
                with open(trends_file, 'r') as f:
                    trends = json.load(f)
                    for trend in trends:
                        self.add_trend(
                            trend=trend['content'],
                            source=trend['source'],
                            date=trend['date'],
                            metadata=trend.get('metadata')
                        )
            
            # Load competitor insights
            competitors_file = os.path.join(knowledge_base_dir, "competitors/insights.json")
            if os.path.exists(competitors_file):
                with open(competitors_file, 'r') as f:
                    insights = json.load(f)
                    for insight in insights:
                        self.add_competitor_insight(
                            competitor=insight['competitor'],
                            insight=insight['content'],
                            date=insight['date'],
                            metadata=insight.get('metadata')
                        )