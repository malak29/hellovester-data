import openai
import psycopg2


# Function to generate embeddings for a given query
def get_openai_embedding(query_text):
    try:
        response = openai.embeddings.create(
            model="text-embedding-ada-002",  # You can use other models as needed
            input=[query_text]
        )
        # Correctly extract the embedding from the new response structure
        embedding = response.data[0].embedding
        return embedding
    except Exception as e:
        print(f"Error generating embedding: {e}")
        return None

# Function to query the PDF embeddings table
def query_pdf_embeddings(query_embedding):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="hellovester",  # Your DB name
            user="malak",  # Your DB user
            password="postgres",  # Your DB password
            host="localhost",  # Your DB host (localhost if local)
            port="5432"  # Your DB port (default is 5432)
        )
        cursor = conn.cursor()

        # Convert embedding to PostgreSQL vector format
        embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'

        # Perform similarity search on pdf_embeddings table using PGVector
        cursor.execute("""
            SELECT file_name, content, embedding <=> %s::vector AS similarity_score
            FROM pdf_embeddings
            ORDER BY embedding <=> %s::vector
            LIMIT 5
        """, (embedding_str, embedding_str))

        results = cursor.fetchall()

    except psycopg2.Error as e:
        print(f"Error querying pdf_embeddings: {e}")
        return []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

    return results

# Function to query the Crypto embeddings table
def query_crypto_embeddings(query_embedding):
    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname="hellovester",  # Your DB name
            user="malak",  # Your DB user
            password="postgres",  # Your DB password
            host="localhost",  # Your DB host
            port="5432"  # Your DB port
        )
        cursor = conn.cursor()

        # Convert embedding to PostgreSQL vector format
        embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'

        # Perform similarity search on crypto_embeddings table using PGVector
        cursor.execute("""
            SELECT url, content, embedding <=> %s::vector AS similarity_score
            FROM crypto_embeddings
            ORDER BY embedding <=> %s::vector
            LIMIT 5
        """, (embedding_str, embedding_str))

        results = cursor.fetchall()

    except psycopg2.Error as e:
        print(f"Error querying crypto_embeddings: {e}")
        return []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

    return results

# Function to combine results from both tables and display them
def query_both_tables(query_text):
    query_embedding = get_openai_embedding(query_text)

    if query_embedding is None:
        return []

    # Query both tables
    pdf_results = query_pdf_embeddings(query_embedding)
    crypto_results = query_crypto_embeddings(query_embedding)

    # Combine the results from both tables
    combined_results = pdf_results + crypto_results

    # Sort by similarity score (descending order)
    combined_results = sorted(combined_results, key=lambda x: x[2], reverse=True)

    return combined_results

# Main function to run the similarity search based on user query
if __name__ == "__main__":
    query_text = input("Enter your query: ")  # Example: "What is Bitcoin?"

    # Get results from both PDF and Crypto embeddings tables
    results = query_both_tables(query_text)

    # Display the results
    if results:
        print("\nTop 5 similar results:")
        for result in results:
            print(f"Source: {result[0]}")
            print(f"Content: {result[1][:300]}...")  # Show the first 300 characters of content
            print(f"Similarity Score: {result[2]}")
            print("------")
    else:
        print("No similar results found.")