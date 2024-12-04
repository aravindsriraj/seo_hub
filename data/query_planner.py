import google.generativeai as genai
from typing import Dict, Any
from seo_hub.core.config import config
from seo_hub.data.vector_store import VectorStore
from seo_hub.data.schema_manager import SchemaManager

class QueryPlanner:
    def __init__(self, vector_store: VectorStore, schema_manager: SchemaManager):
        self.vector_store = vector_store
        self.schema_manager = schema_manager
        genai.configure(api_key=config.GEMINI_API_KEY)
        self.model = genai.GenerativeModel(
            model_name=config.GEMINI_MODEL_NAME,
            generation_config=config.GENERATION_CONFIG
        )

    def _format_patterns(self, patterns: Dict) -> str:
        """Format SQL patterns for prompt."""
        if not patterns or not patterns.get('documents'):
            return "No relevant SQL patterns found."
        
        formatted = []
        for i, (doc, metadata) in enumerate(zip(patterns['documents'], patterns.get('metadatas', []))):
            formatted.append(f"Pattern {i+1}:")
            if metadata and 'question_pattern' in metadata:
                formatted.append(f"Question: {metadata['question_pattern']}")
            formatted.append(f"SQL: {doc}\n")
        
        return "\n".join(formatted)

    def create_execution_plan(self, user_question: str) -> Dict[str, Any]:
        """Create an execution plan with proper database context."""
        schema = self.schema_manager.get_schema()
        query_context = self.schema_manager.get_query_context()
        context = self.vector_store.query_similar(user_question)
        
        prompt = f"""
        You are an SEO analysis expert. Create an SQL query plan for this question.
        
        User Question: {user_question}

        IMPORTANT: You must prefix ALL table names with their database name in your SQL query.
        Use ONLY these exact table references:
        - rankings.keywords
        - rankings.rankings
        - urls_analysis.urls
        - urls_analysis.content_analysis
        - url_tracker.url_tracking
        - url_tracker.sitemap_tracking
        - aimodels.keyword_rankings

        Available Schema:
        {schema}

        Query Guidelines:
        {query_context}

        SQL Patterns for reference:
        {self._format_patterns(context['patterns'])}

        Respond with a JSON-like structure containing:
        {{
            "question_type": "The type of analysis needed",
            "sql_query": "Your SQL query with proper database prefixes",
            "required_context": "Additional context needed",
            "visualization": "Preferred visualization type"
        }}

        ENSURE your sql_query:
        1. Uses FULL table names (e.g., 'rankings.keywords', not just 'keywords')
        2. Includes all necessary JOINs with proper database prefixes
        3. Has appropriate WHERE clauses
        4. Uses standard SQL syntax
        """

        # Get response from Gemini
        chat = self.model.start_chat(history=[])
        response = chat.send_message(prompt)
        
        # Parse and validate the plan
        plan = self._parse_gemini_response(response.text)
        
        # Validate database prefixes
        self._validate_database_prefixes(plan['sql_query'])
        
        return plan

    def _validate_database_prefixes(self, query: str) -> None:
        """Validate that all table references include database prefixes."""
        valid_prefixes = [
            'rankings.keywords',
            'rankings.rankings',
            'urls_analysis.urls',
            'urls_analysis.content_analysis',
            'url_tracker.url_tracking',
            'url_tracker.sitemap_tracking',
            'aimodels.keyword_rankings'
        ]
        
        # Convert query to lowercase for case-insensitive comparison
        query_lower = query.lower()
        
        # Check if at least one valid prefix is present
        if not any(prefix.lower() in query_lower for prefix in valid_prefixes):
            raise ValueError(
                "SQL query must include proper database prefixes. "
                "Valid prefixes are: " + ", ".join(valid_prefixes)
            )

    def _parse_gemini_response(self, response: str) -> Dict[str, Any]:
        """Parse Gemini's response into structured plan."""
        import json
        
        # Initialize default plan
        default_plan = {
            'question_type': '',
            'sql_query': '',
            'required_context': '',
            'visualization': 'table'
        }
        
        try:
            # Try to parse as JSON first
            if '{' in response and '}' in response:
                # Extract JSON-like structure from the response
                json_str = response[response.find('{'):response.rfind('}')+1]
                parsed = json.loads(json_str)
                # Merge with default plan
                return {**default_plan, **parsed}
            
            # Fallback to line-by-line parsing if JSON fails
            current_section = None
            sections = {}
            
            for line in response.split('\n'):
                line = line.strip()
                if not line:
                    continue
                    
                if line.endswith(':'):
                    current_section = line[:-1].lower().replace(' ', '_')
                    sections[current_section] = []
                elif current_section:
                    sections[current_section].append(line)
            
            # Convert sections to plan format
            plan = default_plan.copy()
            for key, value in sections.items():
                if key in plan:
                    plan[key] = ' '.join(value)
            
            return plan
            
        except Exception as e:
            print(f"Error parsing response: {e}")
            return default_plan