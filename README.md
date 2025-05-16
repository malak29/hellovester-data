# HelloVester: AI-Powered Cryptocurrency Analytics Platform

HelloVester is an AI-driven cryptocurrency analytics platform designed to simplify crypto data interpretation. It integrates **Retrieval-Augmented Generation (RAG)**, **temporal pattern recognition**, and **interactive flowchart generation** to empower users with actionable market insights and conceptual understanding of crypto systems.

---

## 🔍 Key Features

* **Temporal-Aware Search**: Analyze historical cryptocurrency data over custom date ranges.
* **Performance Scoring**: Highlights volatile and liquid assets using a custom metric.
* **AI-Powered Insights**: Converts raw data into human-readable narratives with DeepSeek/GPT.
* **Interactive Charts**: Real-time data exploration using Plotly.
* **Professor Model**: Generates visual step-by-step flowcharts using React Flow based on user queries.

---

## 📦 Repositories in This Project

| Repository                                                              | Description                                                                                              |
| ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| [hellovester-frontend](https://github.com/malak29/hellovester-frontend) | React and Streamlit-based frontend for user interaction, visualization, and flowchart rendering.         |
| [hellovester-backend](https://github.com/malak29/hellovester-backend)   | FastAPI backend handling queries, embeddings retrieval, and response generation.                         |
| [hellovester-data](https://github.com/malak29/hellovester-data)         | Data ingestion, vectorization, and processing pipeline including whitepapers, PDFs, and historical CSVs. |

---

## 🧠 Architecture Overview

### 🔹 Analytics Model

| Component       | Tech Stack         | Purpose                             |
| --------------- | ------------------ | ----------------------------------- |
| Data Ingestion  | Pandas             | Process CSVs into structured format |
| Vectorization   | Sentence-BERT      | Convert data to embeddings          |
| Vector Database | ChromaDB           | Semantic search & hybrid filtering  |
| Language Model  | DeepSeek API       | Human-readable insights             |
| Visualization   | Streamlit + Plotly | Interactive time-series dashboards  |

### 🔹 Professor Model

| Component    | Tech Stack                      | Purpose                                 |
| ------------ | ------------------------------- | --------------------------------------- |
| Data Sources | PyMuPDF, BeautifulSoup          | Extract text from URLs and PDFs         |
| Embeddings   | OpenAI GPT-4 Turbo Embeddings   | Create vector representations           |
| Database     | PostgreSQL + pgvector           | Efficient vector indexing and search    |
| Backend API  | FastAPI                         | Async query handling                    |
| Flowcharts   | React Flow + Prompt Engineering | Visual step-by-step explanations        |
| Frontend     | React + TailwindCSS             | User interface for queries and diagrams |

---

## ⚙️ Setup Instructions

Each repo has its own setup instructions. See the `README.md` in:

* [`hellovester-frontend`](https://github.com/malak29/hellovester-frontend#readme)
* [`hellovester-backend`](https://github.com/malak29/hellovester-backend#readme)
* [`hellovester-data`](https://github.com/malak29/hellovester-data#readme)

---

## ✅ Evaluation Metrics

| Metric            | Value     |
| ----------------- | --------- |
| Precision\@5      | 82%       |
| MRR               | 0.89      |
| ROUGE-L F1        | 0.76      |
| Avg. Latency      | 1.8s      |
| User Satisfaction | 4.2 / 5.0 |

---

## 🚧 Challenges Tackled

* Handled inconsistent PDF formats and URLs using regex-based preprocessing.
* Reduced LLM hallucinations by embedding metadata validation.
* Improved retrieval speed and precision with hybrid search and pgvector.
* Overcame memory limitations with batch processing and efficient indexing.

---

## 🔮 Future Enhancements

* Live data streaming from CoinGecko and Binance.
* LSTM-based price forecasting models.
* NFT and DeFi analytics expansion.
* UI upgrades with advanced charting and dynamic flow interactions.
* Enhanced backend scalability and caching strategies.

---

## 👨‍💻 Contributors

* **Malak Parmar** — [malak29](https://github.com/malak29)
* **Nikhil Doye**

---
