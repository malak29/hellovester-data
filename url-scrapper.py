import requests
from bs4 import BeautifulSoup
import json
import psycopg2
import openai
import re
import concurrent.futures


# Function to get OpenAI embedding for a text input
def get_openai_embedding(text):
    try:
        response = openai.embeddings.create(
            input=text,
            model="text-embedding-ada-002"
        )
        embedding = response.data[0].embedding  # Correctly accessing the embedding
        return embedding
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

# Function to extract main text from a webpage
def scrape_text(url):
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Extract text from paragraphs
        paragraphs = soup.find_all("p")
        paragraph_text = " ".join([p.get_text() for p in paragraphs])
        
        # Extract text from headings (h1-h6)
        headings = soup.find_all(re.compile("h[1-6]", re.IGNORECASE))  # Use regex to match h1-h6 tags
        heading_text = " ".join([h.get_text() for h in headings])
        
        # Extract text from lists (ul, ol, li)
        lists = soup.find_all(["ul", "ol", "li"])
        list_text = " ".join([li.get_text() for li in lists])
        
        # Extract text from tables (table, tr, td, th)
        tables = soup.find_all("table")
        table_text = ""
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cols = row.find_all(["td", "th"])
                table_text += " ".join([col.get_text() for col in cols])
        
        # Combine all text elements into one string
        full_text = " ".join([paragraph_text, heading_text, list_text, table_text])
        return full_text.strip()
    
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None

# Load URLs from JSON file
with open("./data/urls.json", "r") as f:
    url_data = json.load(f)

# Extract the URLs (excluding the first one)
urls = [item["url"] for item in url_data]

# Function to store embeddings in PostgreSQL with PGVector
def store_embeddings_for_url(url, text, embedding):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="hellovester",
            user="malak",
            password="postgres",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Insert the URL, content, and embedding into the database
        cursor.execute("""
        INSERT INTO crypto_embeddings (url, content, embedding)
        VALUES (%s, %s, %s)
        """, (url, text, embedding))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Successfully stored embedding for {url}")
    except Exception as e:
        print(f"Error storing embedding for {url}: {e}")

# Function to process each URL: scrape text, generate embedding, store in DB
def process_url(url):
    text = scrape_text(url)
    if text:
        embedding = get_openai_embedding(text)
        if embedding:
            store_embeddings_for_url(url, text, embedding)
        else:
            print(f"Failed to generate embedding for {url}")
    else:
        print(f"Failed to scrape text from {url}")

# Main function to process URLs concurrently
def main():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(process_url, urls)

if __name__ == "__main__":
    main()
