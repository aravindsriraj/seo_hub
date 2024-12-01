import sqlite3
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime

# Function to fetch 50 random URLs from the database
def fetch_random_urls():
    conn = sqlite3.connect('urls_analysis.db')
    cursor = conn.cursor()
    cursor.execute('SELECT url FROM urls')
    urls = cursor.fetchall()
    conn.close()
    return [url[0] for url in urls]

# Function to format date to YYYY-MM-DD
def format_date(date_str):
    try:
        date_obj = datetime.fromisoformat(date_str) if 'T' in date_str else datetime.strptime(date_str, '%B %d, %Y')
        return date_obj.strftime('%Y-%m-%d')
    except ValueError:
        return date_str  # Return the original string if parsing fails

# Function to extract dates from HTML content
def extract_dates_from_html(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        # Look for the <script> tag with type "application/ld+json"
        json_ld_script = soup.find('script', type='application/ld+json')
        if json_ld_script:
            json_data = json.loads(json_ld_script.string)
            date_published = json_data.get('datePublished')
            date_modified = json_data.get('dateModified')
            return format_date(date_published) if date_published else None, format_date(date_modified) if date_modified else None
        
        # If not found in the script tag, check the specific <div>
        blog_info_div = soup.find('div', class_='blog-hero_content-info')
        if blog_info_div:
            text_content = blog_info_div.get_text()
            date_patterns = [
                r'\b\d{1,2}[-/]\d{1,2}[-/]\d{2,4}\b',  # Matches dates like 12-31-2020 or 12/31/20
                r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b',    # Matches dates like 2020-12-31
                r'\b\w{3,9} \d{1,2}, \d{4}\b',          # Matches dates like January 1, 2020
                r'\b\d{1,2} \w{3,9} \d{4}\b'            # Matches dates like 1 January 2020
            ]
            for pattern in date_patterns:
                found_dates = re.findall(pattern, text_content)
                if found_dates:
                    return format_date(found_dates[0]), None  # Return the first found date and None for modified

        return None, None  # Return None if no dates are found
    except Exception as e:
        print(f"Error fetching or parsing {url}: {e}")
        return None, None

# Function to update the database with the extracted dates
def update_dates_in_database(url, date_published, date_modified):
    conn = sqlite3.connect('urls_analysis.db')
    cursor = conn.cursor()
    
    # Update the datePublished and dateModified columns for the given URL
    cursor.execute('''
        UPDATE urls
        SET datePublished = ?, dateModified = ?
        WHERE url = ?
    ''', (date_published, date_modified, url))
    
    conn.commit()
    conn.close()

# Function to display dates for fetched URLs and update the database
def display_dates_for_urls():
    urls = fetch_random_urls()
    for url in urls:
        print(f"URL: {url}")
        date_published, date_modified = extract_dates_from_html(url)

        # Apply the logic for date assignment
        if date_modified and not date_published:
            date_published = date_modified  # Use dateModified as datePublished if datePublished is not found
        elif date_published and not date_modified:
            date_modified = ''  # Leave dateModified blank if only datePublished is found
        elif not date_published and not date_modified:
            date_published = ''  # Leave both blank if no dates are found
            date_modified = ''

        # Update the database with the extracted dates
        update_dates_in_database(url, date_published, date_modified)

        # Print the results
        if date_published or date_modified:
            print("Dates found:")
            if date_published:
                print(f" - Date Published: {date_published}")
            if date_modified:
                print(f" - Date Modified: {date_modified}")
        else:
            print("No dates found.")
        print()  # New line for better readability

# Main function to run the script
def main():
    display_dates_for_urls()

if __name__ == "__main__":
    main()
