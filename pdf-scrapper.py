import os
import fitz  # PyMuPDF
import psycopg2
import openai
import tiktoken  # For accurate token counting


# Function to get OpenAI embedding for a text input
def get_openai_embedding(text):
    # Truncate text to fit within token limit
    enc = tiktoken.get_encoding("cl100k_base")
    tokens = enc.encode(text)
    truncated_tokens = tokens[:8000]  # Limit to 8000 tokens
    truncated_text = enc.decode(truncated_tokens)
    
    response = openai.embeddings.create(
        input=truncated_text,
        model="text-embedding-ada-002"
    )
    embedding = response.data[0].embedding
    return embedding

# Function to extract text from PDF using PyMuPDF
def extract_text_from_pdf(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            text += page.get_text("text")
        return text.strip()
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return None

# Function to estimate the number of tokens in a text using tiktoken
def count_tokens(text):
    enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text))

# Function to split the text into smaller chunks based on token count
def split_text_into_chunks(text, max_tokens=2000):
    enc = tiktoken.get_encoding("cl100k_base")
    tokens = enc.encode(text)
    
    chunks = []
    current_chunk = []
    current_token_count = 0

    for token in tokens:
        if current_token_count + 1 > max_tokens:
            # Decode and add the current chunk
            chunks.append(enc.decode(current_chunk))
            current_chunk = [token]
            current_token_count = 1
        else:
            current_chunk.append(token)
            current_token_count += 1

    # Add the last chunk if not empty
    if current_chunk:
        chunks.append(enc.decode(current_chunk))

    return chunks

# Function to store PDF embeddings in PostgreSQL
def store_pdf_embeddings():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="hellovester",
        user="malak",
        password="postgres",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pdf_embeddings (
        id SERIAL PRIMARY KEY,
        file_name TEXT,
        content TEXT,
        embedding vector(1536)
    )
    """)
    conn.commit()

    # Directory containing the PDF files
    pdf_folder = "./data/knowledge_base/"
    pdf_files = [f for f in os.listdir(pdf_folder) if f.endswith(".pdf")]

    # Loop over PDFs, extract text, generate embeddings, and store in the DB
    for pdf_file in pdf_files:
        pdf_path = os.path.join(pdf_folder, pdf_file)
        text = extract_text_from_pdf(pdf_path)
        
        if text:
            # Split the text into chunks
            chunks = split_text_into_chunks(text)
            
            # For each chunk, generate embeddings and store in the database
            for chunk in chunks:
                try:
                    embedding = get_openai_embedding(chunk)
                    print(f"Generated embedding for {pdf_file} chunk: {embedding[:5]}...")

                    # Store the PDF file name, content, and embedding in the database
                    cursor.execute("""
                    INSERT INTO pdf_embeddings (file_name, content, embedding)
                    VALUES (%s, %s, %s)
                    """, (pdf_file, chunk, embedding))
                except Exception as e:
                    print(f"Error processing chunk: {e}")

    # Commit and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Embeddings for PDFs stored in PGVector")

# Run the function to store PDF embeddings
if __name__ == "__main__":
    store_pdf_embeddings()