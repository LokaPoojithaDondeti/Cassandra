# Sales Data Processing with Medallion Architecture

This project implements a Medallion Architecture for sales data processing using Cassandra Astra DB and Python. It ingests raw sales data, cleans and transforms it, performs aggregations, and stores insights in structured layers (Bronze → Silver → Gold).

## Architecture Overview

The pipeline follows a three-layer Medallion approach:

1. 🥉 Bronze Layer – Raw data ingestion from CSV.
2. 🥈 Silver Layer – Data cleaning and transformation.
3. 🥇 Gold Layer – Aggregation, analytics, and insights.

## Getting Started

### Prerequisites

- Python 3.7 or higher installed.
- Astra DB account and database created.

### Astra DB Setup

To run this project with your own Cassandra Astra DB:

1. Create a free account on DataStax Astra: https://www.datastax.com/astra
2. Create a new database called sales_db.
3. Once the database is active:
   - Navigate to Database Settings > Connect.
   - Select “Data API” and copy:
     - API Endpoint
     - Application Token (use Database Admin role)
4. Update the db_config in MedallionPipeline.py with your:
   - ASTRA_DB_API_ENDPOINT
   - ASTRA_DB_APPLICATION_TOKEN
   - KEYSPACE_NAME (usually "sales_db" or "default_keyspace")

### Installation

Clone this repository:

```bash
git clone https://github.com/LokaPoojithaDondeti/Cassandra.git
cd Cassandra
```
Create a .env file in the project root:
```bash
ASTRA_DB_APPLICATION_TOKEN=your_token_here
ASTRA_DB_API_ENDPOINT=your_endpoint_here
KEYSPACE_NAME=sales_keyspace
```

Install dependencies:

```bash
pip install --upgrade astrapy pandas numpy
```

### Running the Code

Run the Medallion pipeline:

```bash
python MedallionPipeline.py
```

This will:

- Load raw data into the bronze_sales collection.
- Clean and process data into silver_sales.
- Aggregate insights into gold_sales_by_region, gold_sales_by_category, and gold_top_performers.