import os
import json
import sqlite3
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import re

# Load environment variables
load_dotenv()

# Constants
DB_PATH = "aimodels.db"
MODELS_JSON = "models.json"
KEYWORDS_CSV = "keywords.csv"
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
OUTPUT_DIR = "responses"

def sanitize_column_name(name):
    """Convert a string to a safe SQL column name."""
    safe_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    if safe_name[0].isdigit():
        safe_name = 'n_' + safe_name
    return safe_name.lower()

def load_models():
    """Load models from JSON file."""
    try:
        with open(MODELS_JSON, "r") as f:
            models_data = json.load(f)
            if not isinstance(models_data, dict) or "models" not in models_data:
                raise ValueError("Invalid models.json format. Expected 'models' key.")
            return [(model["id"], model["name"]) for model in models_data["models"]]
    except FileNotFoundError:
        print(f"Error: Models file '{MODELS_JSON}' not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in '{MODELS_JSON}'")
        return []
    except Exception as e:
        print(f"Error loading models: {str(e)}")
        return []

def get_required_columns(models):
    """Get list of required columns based on models."""
    columns = {'keyword', 'check_date'}
    for _, model_name in models:
        safe_name = sanitize_column_name(model_name)
        columns.add(f"{safe_name}_answer")
        columns.add(f"{safe_name}_atlan_mention")
    return columns

def get_existing_columns():
    """Get existing columns from the database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(keyword_rankings)")
    columns = {info[1] for info in cursor.fetchall()}
    conn.close()
    return columns

def create_table_sql(models):
    """Generate SQL for creating table with dynamic columns for each model."""
    columns = [
        "keyword TEXT",
        "check_date DATE"
    ]
    
    for _, model_name in models:
        safe_name = sanitize_column_name(model_name)
        columns.extend([
            f"{safe_name}_answer TEXT",
            f"{safe_name}_atlan_mention BOOLEAN"
        ])
    
    columns.append("PRIMARY KEY (keyword, check_date)")
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS keyword_rankings (
        {', '.join(columns)}
    )
    """
    print("Generated SQL:", sql)
    return sql

def initialize_database(models):
    """Initialize the database with the correct schema."""
    print("Initializing database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    create_sql = create_table_sql(models)
    cursor.execute(create_sql)
    
    conn.commit()
    conn.close()
    print("Database initialized.")

def validate_and_update_schema(models):
    """Check if database schema matches models and update if necessary."""
    print("Validating database schema...")
    
    # First ensure database exists
    if not os.path.exists(DB_PATH):
        print("Database doesn't exist. Creating new database...")
        initialize_database(models)
        return True
    
    # Check if table exists
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='keyword_rankings'
    """)
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        print("Table 'keyword_rankings' doesn't exist. Creating table...")
        create_sql = create_table_sql(models)
        cursor.execute(create_sql)
        conn.commit()
        conn.close()
        return True
    
    # If table exists, check columns
    required_columns = get_required_columns(models)
    existing_columns = get_existing_columns()
    
    missing_columns = required_columns - existing_columns
    if missing_columns:
        print(f"Found missing columns: {missing_columns}")
        
        for column in missing_columns:
            column_type = "BOOLEAN" if column.endswith("_atlan_mention") else "TEXT"
            try:
                print(f"Adding column: {column} ({column_type})")
                cursor.execute(f"ALTER TABLE keyword_rankings ADD COLUMN {column} {column_type}")
            except sqlite3.OperationalError as e:
                print(f"Error adding column {column}: {e}")
                conn.close()
                return False
        
        conn.commit()
        print("Schema updated successfully")
    else:
        print("Database schema is up to date")
    
    conn.close()
    return True

def ensure_database():
    """Check if database exists and create if not."""
    if not os.path.exists(DB_PATH):
        print(f"Database '{DB_PATH}' does not exist. Creating new database...")
        initialize_database(load_models())
        return True
    
    print(f"Database '{DB_PATH}' already exists.")
    
    # Check if table exists
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='keyword_rankings'
    """)
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        print("Table 'keyword_rankings' doesn't exist. Creating table...")
        create_sql = create_table_sql(load_models())
        cursor.execute(create_sql)
        conn.commit()
    
    conn.close()
    return True

def load_keywords():
    """Load keywords from CSV file."""
    try:
        df = pd.read_csv(KEYWORDS_CSV)
        if 'keyword' not in df.columns:
            raise ValueError("CSV file must contain a 'keyword' column")
        return df['keyword'].unique().tolist()
    except FileNotFoundError:
        print(f"Error: Keywords file '{KEYWORDS_CSV}' not found")
        return []
    except Exception as e:
        print(f"Error loading keywords: {str(e)}")
        return []

def call_openrouter_api(model_id, model_name, prompt):
    """Call OpenRouter API and return the response."""
    print(f"Calling OpenRouter API for model: {model_name} ({model_id}) with prompt: '{prompt}'")
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    data = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 5000
    }
    try:
        response = requests.post(OPENROUTER_ENDPOINT, headers=headers, json=data)
        response.raise_for_status()
        response_json = response.json()
        
        # Log the complete response
        log_file = log_api_response(model_name, response_json, prompt)
        print(f"Full response logged to: {log_file}")
        
        answer = response_json.get("choices", [{}])[0].get("message", {}).get("content", "")
        print(f"Received response from model '{model_name}': {answer[:100]}...")
        return answer
    except requests.exceptions.RequestException as e:
        print(f"Error calling OpenRouter API for model '{model_name}': {e}")
        if hasattr(e.response, 'text'):
            error_content = e.response.text
            print(f"Response content: {error_content}")
            log_api_response(model_name, {"error": error_content}, prompt)
        return ""

def log_api_response(model_name, response_data, keyword):
    """Log API response to a file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # Ensure output directory exists
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{OUTPUT_DIR}/{timestamp}_{sanitize_column_name(model_name)}_{keyword.replace(' ', '_')}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Model: {model_name}\n")
        f.write(f"Keyword: {keyword}\n")
        f.write("-" * 80 + "\n")
        f.write("API Response:\n")
        f.write(json.dumps(response_data, indent=2))
        f.write("\n" + "-" * 80 + "\n")
        if isinstance(response_data, dict):
            content = response_data.get("choices", [{}])[0].get("message", {}).get("content", "")
            f.write("\nExtracted Content:\n")
            f.write(content)
    
    return filename

def process_keywords():
    """Process all keywords from CSV and store responses in the database."""
    keywords = load_keywords()
    if not keywords:
        print("No keywords found to process")
        return

    print(f"Found {len(keywords)} keywords to process")
    models = load_models()

    # Validate schema before processing
    if not validate_and_update_schema(models):
        print("Error: Database schema validation failed")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for keyword in keywords:
        print(f"\nProcessing keyword: '{keyword}'")

        # Check if there's an existing entry for today
        cursor.execute("""
            SELECT 1 FROM keyword_rankings
            WHERE keyword = ? AND check_date = ?
        """, (keyword, datetime.now().strftime('%Y-%m-%d')))
        if cursor.fetchone():
            print(f"Skipping '{keyword}' as it already has an entry for today")
            continue

        responses = {}
        for model_id, model_name in models:
            response = call_openrouter_api(model_id, model_name, prompt=keyword)
            safe_name = sanitize_column_name(model_name)

            if response:
                responses[safe_name] = {
                    'answer': response,
                    'atlan_mention': 'atlan' in response.lower()
                }

        if responses:
            columns = ['keyword', 'check_date']
            values = [keyword, datetime.now().strftime('%Y-%m-%d')]

            for safe_name, data in responses.items():
                columns.extend([f"{safe_name}_answer", f"{safe_name}_atlan_mention"])
                values.extend([data['answer'], data['atlan_mention']])

            placeholders = ','.join(['?' for _ in values])
            insert_sql = f"""
            INSERT OR REPLACE INTO keyword_rankings ({','.join(columns)})
            VALUES ({placeholders})
            """

            cursor.execute(insert_sql, values)
            print(f"Successfully saved responses for keyword '{keyword}'")

    conn.commit()
    conn.close()
    print("Completed keyword processing")

if __name__ == "__main__":
    print("Starting LLM response collection...")
    
    # Check if keywords file exists
    if not os.path.exists(KEYWORDS_CSV):
        print(f"Error: Keywords file '{KEYWORDS_CSV}' not found")
        exit(1)
    
    # Load models first
    print("Loading models from models.json...")
    models = load_models()
    print(f"Loaded {len(models)} models")
    
    # Ensure database exists and has correct schema
    if not ensure_database():
        print("Error: Failed to ensure database exists")
        exit(1)
        
    if not validate_and_update_schema(models):
        print("Error: Failed to validate and update database schema")
        exit(1)
    
    # Process keywords
    print("Starting keyword processing...")
    process_keywords()
    print("Completed keyword processing")