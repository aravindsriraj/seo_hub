import requests
import xml.etree.ElementTree as ET

def extract_urls_from_xml(xml_url):
    """
    Parses an XML file and extracts all URLs (loc elements) from it.
    Handles XML namespaces if present.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(xml_url.strip(), headers=headers)
        if response.status_code == 200:
            xml_content = response.content
            tree = ET.ElementTree(ET.fromstring(xml_content))
            root = tree.getroot()
            
            # Handle namespaces
            namespace = {'ns': root.tag.split('}')[0].strip('{')} if '}' in root.tag else {}
            urls = []
            
            # Extract and clean URLs
            for elem in root.findall(".//ns:loc", namespace):
                if elem.text:
                    clean_url = elem.text.strip()
                    if clean_url:
                        urls.append(clean_url)
            
            return urls
        else:
            st.error(f"Failed to fetch XML. HTTP status code: {response.status_code}")
            return []
    except Exception as e:
        st.error(f"Error parsing XML: {e}")
        return []