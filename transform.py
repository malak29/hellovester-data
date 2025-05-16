from sentence_transformers import SentenceTransformer
import psycopg2

# Load the model
model = SentenceTransformer('all-MiniLM-L6-v2')

# Database connection setup
def get_db_connection():
    conn = psycopg2.connect(
        dbname="hellovester",  # Update with your database name
        user="malak",  # Update with your username
        password="postgres",  # Update with your password
        host="localhost",
        port="5432"
    )
    return conn

# Re-insert the embeddings with 384 dimensions
def update_embeddings():
    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch all the data from the table
    cur.execute("SELECT id, content FROM crypto_embeddings")
    rows = cur.fetchall()

    # Update embeddings
    for row in rows:
        id, content = row
        embedding = model.encode([content], convert_to_numpy=True).tolist()[0]  # 384 dimensions
        cur.execute(
            "UPDATE crypto_embeddings SET embedding = %s WHERE id = %s",
            (embedding, id)
        )

    conn.commit()
    cur.close()
    conn.close()

update_embeddings()
