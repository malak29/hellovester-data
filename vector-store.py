import psycopg2
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from pgvector.psycopg2 import register_vector

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="hellovester",
    user="your_macbook_username",
    password="your_password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Enable PGVector
cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
conn.commit()

# Register PGVector
register_vector(conn)

# Load SentenceTransformer model
model = SentenceTransformer("all-MiniLM-L6-v2")

# Load CSV from hellovester-data repo
df = pd.read_csv("../hellovester-data/data/crypto_data.csv")

# Convert text descriptions into embeddings
embeddings = model.encode(df["description"].tolist(), convert_to_numpy=True)

# Insert data into PGVector table
for i, row in df.iterrows():
    cursor.execute(
        "INSERT INTO crypto_embeddings (name, description, embedding) VALUES (%s, %s, %s);",
        (row["name"], row["description"], embeddings[i].tolist())
    )

conn.commit()
cursor.close()
conn.close()
