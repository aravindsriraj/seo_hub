from seo_hub.data.url_tracker_db import URLTrackerDB

def __init__(self):
    self.db_path = URL_TRACKER_DB_PATH
    print(f"Database path: {self.db_path}")  # Added print statement
    self._init_db()

db = URLTrackerDB()

# Test adding a sitemap
print("Adding sitemap...")
db.add_sitemap("https://www.example.com/sitemap.xml")

# Test updating a URL
print("Updating URL...")
db.update_url("https://www.example.com/page", "https://www.example.com/sitemap.xml", 100, "2023-01-01", "2023-01-02")

# Test getting URL info
url_info = db.get_url_info("https://www.example.com/page")
print("URL Info:", url_info)

# Test getting all URLs
all_urls = db.get_all_urls()
print("All URLs:", all_urls)