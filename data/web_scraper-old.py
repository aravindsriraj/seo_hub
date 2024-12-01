import requests
import os
import google.generativeai as genai
from dotenv import load_dotenv
import time

# Load environment variables from .env
load_dotenv()

# Configure Gemini API
api_key= os.getenv("GEMINI_API_KEY")
genai.configure(api_key=api_key)

# Create the model
generation_config = {
    "temperature": 1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
    "response_mime_type": "text/plain",
}

model = genai.GenerativeModel(
    model_name="gemini-1.5-flash",
    generation_config=generation_config,
)


def summarize_url_with_gemini(url, retries=3, delay=1):
    """
    Directly pass the URL to Gemini API for summarization and categorization.
    Includes retry logic for 429 errors and a delay between retries.
    
    Args:
        url (str): The URL to summarize.
        retries (int): Number of times to retry on 429 errors.
        delay (int): Delay in seconds between retries.
        
    Returns:
        tuple: Summary, category, primary keyword (if educational), and word count.
    """
    try:
        # Retry logic for Gemini API
        for attempt in range(1, retries + 1):
            try:
                # Prepare the prompt for Gemini
                chat_session = model.start_chat(history=[])
                prompt = (
                    f"Analyze the content of the following URL: {url}\n\n"
                    f"Provide the output in the following structured format:\n"
                    f"Summary: <A concise summary of the webpage content. Don't make up answers without reading the page fully first.>\n"
                    f"Category: <A single category that best describes the content. The category should be type of the page & not the website itself. For example: Product Page, Comparison Page, Integration Page, Educational Page etc>\n"
                    f"Primary Keyword: <For educational pages, provide the primary keyword or term discussed. Pick only one keyword and it is likely to be present in the URL, Title and repeated many times in the content>\n"
                    f"Word Count: <Provide the total word count of the article>"
                )
                gemini_response = chat_session.send_message(prompt)

                # Parse the response
                response_text = gemini_response.text.strip()
                summary, category, primary_keyword, word_count = parse_gemini_response(response_text)
                return summary, category, primary_keyword, word_count

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    print(f"429 Error: Retry {attempt}/{retries} after a delay...")
                    time.sleep(delay * attempt)  # Exponential backoff delay
                else:
                    raise
        raise Exception("Exceeded maximum retries for Gemini API.")
    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return "Error", "Error", "N/A", 0  # Return 0 for word count on error


def parse_gemini_response(response_text):
    """
    Parse the Gemini API response to extract the summary, category, primary keyword, and word count.
    
    Args:
        response_text (str): The full response text from the Gemini API.
        
    Returns:
        tuple: Extracted summary, category, primary keyword (if any), and word count.
    """
    try:
        lines = response_text.split("\n")
        summary = next((line.split(": ", 1)[1] for line in lines if line.startswith("Summary")), "N/A")
        category = next((line.split(": ", 1)[1] for line in lines if line.startswith("Category")), "Uncategorized")
        primary_keyword = next(
            (line.split(": ", 1)[1] for line in lines if line.startswith("Primary Keyword")), "N/A"
        )
        word_count = next(
            (line.split(": ", 1)[1] for line in lines if line.startswith("Word Count")), 0
        )
        return summary.strip(), category.strip(), primary_keyword.strip(), int(word_count.strip())
    except Exception as e:
        print(f"Error parsing Gemini response: {e}")
        return "N/A", "Uncategorized", "N/A", 0  # Return 0 for word count on error
