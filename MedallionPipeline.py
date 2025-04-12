import pandas as pd
import numpy as np
import uuid
from astrapy import DataAPIClient
import os
from dotenv import load_dotenv

load_dotenv()
class MedallionPipeline:
    def __init__(self, db_config, input_file):
        """
        Initializes the MedallionPipeline with database configuration and input file.
        """
        self.db_config = db_config
        self.input_file = input_file
        self.client = DataAPIClient(db_config["ASTRA_DB_APPLICATION_TOKEN"])
        self.db = self.client.get_database_by_api_endpoint(
            db_config["ASTRA_DB_API_ENDPOINT"], keyspace=db_config["KEYSPACE_NAME"]
        )

    def ensure_collection_exists(self, collection_name):
        """Creates the collection if it does not exist."""
        try:
            if collection_name not in self.db.list_collections():
                self.db.create_collection(collection_name)
        except Exception as e:
            print(f"Error ensuring collection {collection_name} exists: {e}")

    def load_raw_data(self):
        """
        Loads raw data into the Bronze table.
        """
        df = pd.read_csv(self.input_file)
        self.ensure_collection_exists("bronze_sales")
        collection = self.db.get_collection("bronze_sales")

        for _, row in df.iterrows():
            doc = row.to_dict()
            doc["_id"] = str(uuid.uuid4())
            collection.insert_one(doc)
        print("Raw data loaded into Bronze table.")

    def clean_data(self):
        """
        Cleans and standardizes data, then inserts into the Silver table.
        """
        collection = self.db.get_collection("bronze_sales")
        data = collection.find()
        df = pd.DataFrame(data)

        df.drop_duplicates(subset=["Order ID"], keep="first", inplace=True)
        df.fillna({"Units Sold": 0, "Total Revenue": 0.0, "Total Profit": 0.0}, inplace=True)
        df["Order Date"] = pd.to_datetime(df["Order Date"], errors="coerce", utc=True)

        self.ensure_collection_exists("silver_sales")
        silver_collection = self.db.get_collection("silver_sales")

        for _, row in df.iterrows():
            doc = row.to_dict()
            doc["_id"] = str(uuid.uuid4())
            silver_collection.insert_one(doc)
        print("Data cleaned and stored in Silver table.")

    def aggregate_data(self):
        """
        Creates Gold tables with aggregated insights.
        """
        collection = self.db.get_collection("silver_sales")
        data = collection.find()
        df = pd.DataFrame(data)

        self.ensure_collection_exists("gold_sales_by_region")
        self.ensure_collection_exists("gold_sales_by_category")
        self.ensure_collection_exists("gold_top_performers")

        # Aggregation 1: Sales by Region
        gold_sales_by_region = df.groupby("Region")["TotalRevenue"].sum().reset_index()
        collection_region = self.db.get_collection("gold_sales_by_region")
        for _, row in gold_sales_by_region.iterrows():
            doc = row.to_dict()
            doc["_id"] = str(uuid.uuid4())
            collection_region.insert_one(doc)

        # Aggregation 2: Sales by Category
        gold_sales_by_category = df.groupby("Item Type")["TotalProfit"].sum().reset_index()
        collection_category = self.db.get_collection("gold_sales_by_category")
        for _, row in gold_sales_by_category.iterrows():
            doc = row.to_dict()
            doc["_id"] = str(uuid.uuid4())
            collection_category.insert_one(doc)

        # Aggregation 3: Top Performers (Highest Orders)
        gold_top_performers = df.groupby("Country")["UnitsSold"].sum().reset_index().nlargest(10, "UnitsSold")
        collection_performers = self.db.get_collection("gold_top_performers")
        for _, row in gold_top_performers.iterrows():
            doc = row.to_dict()
            doc["_id"] = str(uuid.uuid4())
            collection_performers.insert_one(doc)

        print("Aggregated insights stored in Gold tables.")

    def run_pipeline(self):
        """
        Runs the complete Medallion Architecture pipeline.
        """
        self.load_raw_data()
        self.clean_data()
        self.aggregate_data()
        print("Medallion Architecture pipeline completed.")


# Database Configuration
db_config = {
    "ASTRA_DB_APPLICATION_TOKEN": os.getenv(ASTRA_DB_APPLICATION_TOKEN),
    "ASTRA_DB_API_ENDPOINT": os.getenv(ASTRA_DB_API_ENDPOINT),
    "KEYSPACE_NAME": os.getenv(KEYSPACE_NAME)
}
DATASET_URL = "https://raw.githubusercontent.com/gchandra10/filestorage/main/sales_100.csv"

# Run the pipeline
pipeline = MedallionPipeline(db_config=db_config, input_file=DATASET_URL)
pipeline.run_pipeline()
