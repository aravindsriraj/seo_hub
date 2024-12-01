import sqlite3
import requests
from bs4 import BeautifulSoup
import json

# Function to fetch all rows from the database where estimated_word_count is not present
def fetch_urls_without_estimated_count():
    conn = sqlite3.connect('urls_analysis.db')
    cursor = conn.cursor()
    cursor.execute('SELECT url, word_count FROM urls WHERE estimated_word_count IS NULL')
    rows = cursor.fetchall()
    conn.close()
    return rows

# Function to scrape the content and estimate word count
def scrape_and_estimate_word_count(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        # Remove unwanted tags
        for script in soup(['script', 'style']):
            script.decompose()  # Remove all script and style elements
        # Extract all text from the page
        text = soup.get_text(separator=' ')
        words = text.split()
        estimated_word_count = len(words)
        return estimated_word_count  # Return only the estimated word count
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return 0  # Return 0 if there's an error

# Function to check and create the estimated word count column if it doesn't exist
def check_and_create_column():
    conn = sqlite3.connect('urls_analysis.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(urls)")
    columns = [column[1] for column in cursor.fetchall()]
    if "estimated_word_count" not in columns:
        cursor.execute("ALTER TABLE urls ADD COLUMN estimated_word_count INTEGER")
        print ("New Col added")
    conn.commit()
    conn.close()

# Function to check and create the word_count column if it doesn't exist
def check_and_create_word_count_column():
    conn = sqlite3.connect('urls_analysis.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(urls)")
    columns = [column[1] for column in cursor.fetchall()]
    if "word_count" not in columns:
        cursor.execute("ALTER TABLE urls ADD COLUMN word_count INTEGER")
        print("New word_count column added")
    conn.commit()
    conn.close()

# Main function to run the script
def main():
    check_and_create_column()  # Check and create the estimated_word_count column if it doesn't exist
    check_and_create_word_count_column()  # Check and create the word_count column if it doesn't exist
    # Step 1: Fetch all rows from the database where estimated_word_count is not present
    urls = fetch_urls_without_estimated_count()
    
    total_urls = len(urls)  # Get the total number of URLs to process
    results = []
    
    for index, (url, word_count) in enumerate(urls):
        estimated_word_count = scrape_and_estimate_word_count(url)  # Step 3: Scrape using Beautiful Soup
        results.append((url, word_count, estimated_word_count))  # Store URL, actual word count, and estimated word count
        
        # Update the database with the estimated word count
        conn = sqlite3.connect('urls_analysis.db')
        cursor = conn.cursor()
        cursor.execute("UPDATE urls SET estimated_word_count = ? WHERE url = ?", (estimated_word_count, url))
        print(f"{url} has an estimated word count of {estimated_word_count}. {index+1} of {total_urls} processed")
        conn.commit()
        conn.close()
        
        # Print progress
        # print(f"Processed {index + 1} of {total_urls} URLs.")  # Update the user on progress
    
    # Step 4: Display results
    for url, actual_word_count, estimated_word_count in results:
        print(f"URL: {url}, Actual Word Count: {actual_word_count}, Estimated Word Count: {estimated_word_count}")

# Call the main function
if __name__ == "__main__":
    main()
