import sqlite3
from datetime import datetime
import os

def get_schema(db_path):
    """Connect to a SQLite database and retrieve its schema."""
    schema_details = []
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        schema_details.append(f"Schema for database: {db_path}")
        schema_details.append("-" * 50)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        if not tables:
            schema_details.append("No tables found in the database.")
        else:
            for table in tables:
                table_name = table[0]
                schema_details.append(f"\nTable: {table_name}")
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                for column in columns:
                    schema_details.append(f"  - {column[1]} ({column[2]})")
    except sqlite3.Error as e:
        schema_details.append(f"Error reading database {db_path}: {e}")
    finally:
        if conn:
            conn.close()
    return "\n".join(schema_details)

def save_schema_to_file(schema_text, output_file):
    """Save the schema details to a file."""
    with open(output_file, "w") as f:
        f.write(schema_text)

def main():
    # List of SQLite database files
    db_files = [
        "aimodels.db",
        "rankings.db",
        "url_tracker.db",
        "urls_analysis.db"
    ]
    
    # Generate schema details for all databases
    all_schemas = []
    for db_file in db_files:
        all_schemas.append(get_schema(db_file))
    
    # Combine schemas with timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"Database Schema Details (Generated on {timestamp})\n\n"
    full_schema = header + "\n\n".join(all_schemas)
    
    # Save to file
    output_file = f"schema_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    save_schema_to_file(full_schema, output_file)
    print(f"Schema details saved to: {output_file}")

if __name__ == "__main__":
    main()
