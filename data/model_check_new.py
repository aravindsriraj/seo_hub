import os
import json
import sqlite3
import requests
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import re
from typing import Dict, Optional, Tuple
import logging
from pathlib import Path
from tqdm import tqdm
import sys

# Setup logging
logging.basicConfig(
    filename='aimodels_check.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

# Constants
DB_PATH = "aimodels.db"
MODELS_JSON = "models.json"
KEYWORDS_CSV = "keywords.csv"
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
OUTPUT_DIR = "responses"
PROGRESS_FILE = "aimodels_progress.json"

def sanitize_column_name(name):
    """Convert a string to a safe SQL column name."""
    safe_name = re.sub(r'[^a-zA-Z0-9]', '_', name)
    if safe_name[0].isdigit():
        safe_name = 'n_' + safe_name
    return safe_name.lower()

def load_progress() -> Dict:
    """Load progress from checkpoint file."""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def save_progress(keyword: str, completed_models: list):
    """Save progress checkpoint."""
    progress = load_progress()
    progress[keyword] = completed_models
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)

def get_last_processed_state() -> Tuple[Optional[str], list]:
    """Get the last processed keyword and its completed models."""
    progress = load_progress()
    if not progress:
        return None, []
    last_keyword = list(progress.keys())[-1]
    return last_keyword, progress.get(last_keyword, [])

def load_models():
    """Load models from JSON file."""
    try:
        with open(MODELS_JSON, "r") as f:
            models_data = json.load(f)
            if not isinstance(models_data, dict) or "models" not in models_data:
                raise ValueError("Invalid models.json format")
            return [(model["id"], model["name"]) for model in models_data["models"]]
    except Exception as e:
        logging.error(f"Error loading models: {str(e)}")
        return []

def get_required_columns(models):
    """Get list of required columns based on models."""
    columns = {'keyword', 'check_date'}
    for _, model_name in models:
        safe_name = sanitize_column_name(model_name)
        columns.add(f"{safe_name}_answer")
        columns.add(f"{safe_name}_atlan_mention")
    return columns

def create_table_sql(models):
    """Generate SQL for creating table with dynamic columns."""
    columns = ["keyword TEXT", "check_date DATE"]
    for _, model_name in models:
        safe_name = sanitize_column_name(model_name)
        columns.extend([
            f"{safe_name}_answer TEXT",
            f"{safe_name}_atlan_mention BOOLEAN"
        ])
    columns.append("PRIMARY KEY (keyword, check_date)")
    return f"CREATE TABLE IF NOT EXISTS keyword_rankings ({', '.join(columns)})"

def initialize_database(models):
    """Initialize database with schema."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    create_sql = create_table_sql(models)
    cursor.execute(create_sql)
    conn.commit()
    conn.close()
    logging.info("Database initialized")

def validate_and_update_schema(models):
    """Check if database schema matches models and update if necessary."""
    if not os.path.exists(DB_PATH):
        initialize_database(models)
        return True

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        required_columns = get_required_columns(models)
        cursor.execute("PRAGMA table_info(keyword_rankings)")
        existing_columns = {info[1] for info in cursor.fetchall()}
        
        missing_columns = required_columns - existing_columns
        if missing_columns:
            for column in missing_columns:
                column_type = "BOOLEAN" if column.endswith("_atlan_mention") else "TEXT"
                cursor.execute(f"ALTER TABLE keyword_rankings ADD COLUMN {column} {column_type}")
            conn.commit()
            logging.info("Schema updated successfully")
        return True
    except Exception as e:
        logging.error(f"Schema validation failed: {str(e)}")
        return False
    finally:
        conn.close()

def load_keywords():
    """Load keywords from CSV file."""
    try:
        df = pd.read_csv(KEYWORDS_CSV)
        if 'keyword' not in df.columns:
            raise ValueError("CSV must contain 'keyword' column")
        return df['keyword'].unique().tolist()
    except Exception as e:
        logging.error(f"Error loading keywords: {str(e)}")
        return []

def call_openrouter_api(model_id, model_name, prompt):
    """Call OpenRouter API and return the response."""
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
        
        log_api_response(model_name, response_json, prompt)
        return response_json.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        logging.error(f"API error for {model_name}: {str(e)}")
        return ""

def log_api_response(model_name, response_data, keyword):
    """Log API response to a file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{OUTPUT_DIR}/{timestamp}_{sanitize_column_name(model_name)}_{keyword.replace(' ', '_')}.txt"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Model: {model_name}\n")
        f.write(f"Keyword: {keyword}\n")
        f.write("-" * 80 + "\n")
        json.dump(response_data, f, indent=2)
    return filename

def save_to_database(keyword: str, responses: Dict):
    """Save responses to database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
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
        conn.commit()
    except Exception as e:
        logging.error(f"Database error for {keyword}: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

def process_keywords():
    """Process keywords with progress tracking and resume capability."""
    keywords = load_keywords()
    if not keywords:
        logging.error("No keywords found to process")
        return

    models = load_models()
    if not validate_and_update_schema(models):
        logging.error("Database schema validation failed")
        return

    last_keyword, completed_models = get_last_processed_state()
    start_index = keywords.index(last_keyword) + 1 if last_keyword in keywords else 0

    # Main progress bar for keywords
    with tqdm(total=len(keywords), initial=start_index, desc="Processing Keywords") as pbar:
        for keyword in keywords[start_index:]:
            responses = {}
            current_completed = []
            
            # Nested progress bar for models
            model_pbar = tqdm(models, desc=f"Models for '{keyword}'", leave=False)
            for model_id, model_name in model_pbar:
                safe_name = sanitize_column_name(model_name)
                if safe_name in completed_models:
                    model_pbar.update(1)
                    continue

                try:
                    model_pbar.set_description(f"Processing {model_name}")
                    response = call_openrouter_api(model_id, model_name, keyword)
                    if response:
                        responses[safe_name] = {
                            'answer': response,
                            'atlan_mention': 'atlan' in response.lower()
                        }
                        current_completed.append(safe_name)
                        save_progress(keyword, current_completed)
                except Exception as e:
                    logging.error(f"Error processing {model_name} for {keyword}: {str(e)}")
                
                model_pbar.update(1)

            if responses:
                save_to_database(keyword, responses)
            
            pbar.update(1)
            model_pbar.close()

if __name__ == "__main__":
    try:
        # Check required files
        required_files = [KEYWORDS_CSV, MODELS_JSON]
        for file in required_files:
            if not os.path.exists(file):
                print(f"Error: Required file '{file}' not found")
                exit(1)
        
        print("Starting LLM response collection...")
        logging.info("Starting LLM response collection...")
        
        process_keywords()
        
        print("\nCompleted keyword processing")
        logging.info("Completed keyword processing")
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Progress has been saved.")
        print("Run the script again to resume from the last processed keyword.")
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        logging.error(f"Fatal error: {str(e)}")
        print("Check aimodels_check.log for details.")