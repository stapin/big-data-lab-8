import os
import time
import urllib.request
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from config import GreenplumConfig, SparkConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataSeeder")

class DataSeeder:
    def __init__(self):
        self.jar_path = SparkConfig.JDBC_DRIVER_PATH
        self.download_jdbc_driver()

        logger.info("Initializing SparkSession for data seeding")
        self.spark = SparkSession.builder \
            .appName("Greenplum_Data_Seeder") \
            .master(SparkConfig.MASTER) \
            .config("spark.jars", self.jar_path) \
            .config("spark.driver.extraClassPath", self.jar_path) \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")
        self.file_path = "openfoodfacts.csv.gz"
        self.url = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"

    def download_jdbc_driver(self):
        if not os.path.exists(self.jar_path):
            logger.info("Downloading PostgreSQL JDBC driver...")
            driver_url = "https://jdbc.postgresql.org/download/postgresql-42.5.4.jar"
            urllib.request.urlretrieve(driver_url, self.jar_path)
            logger.info("JDBC driver successfully downloaded")
        else:
            logger.info("JDBC driver already exists, skipping download")

    def download_data(self):
        if not os.path.exists(self.file_path):
            logger.info(f"Downloading dataset from {self.url} (This may take a few minutes)...")
            urllib.request.urlretrieve(self.url, self.file_path)
            logger.info("Dataset successfully downloaded")
        else:
            logger.info("Dataset file already exists, skipping download")

    def process_and_load(self):
        logger.info("Reading raw data into Spark DataFrame")
        raw_df = self.spark.read.csv(self.file_path, sep='\t', header=True)

        target_columns = [
            "product_name", "energy-kcal_100g", "proteins_100g", 
            "fat_100g", "carbohydrates_100g"
        ]
        df = raw_df.select(*target_columns).dropna()

        for c in target_columns[1:]:
            df = df.withColumn(c, col(c).cast("float"))
        
        df = df.withColumn("id", monotonically_increasing_id())

        df_sample = df.sample(fraction=0.01, seed=42)
        row_count = df_sample.count()
        logger.info(f"Prepared {row_count} rows for insertion into Greenplum")
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to write data to {GreenplumConfig.RAW_TABLE} (Attempt {attempt + 1}/{max_retries})")
                df_sample.write \
                    .mode("overwrite") \
                    .option("createTableOptions", "DISTRIBUTED BY (id)") \
                    .jdbc(url=GreenplumConfig.JDBC_URL,
                          table=GreenplumConfig.RAW_TABLE,
                          properties=GreenplumConfig.PROPERTIES)
                
                logger.info(f"Data successfully loaded into {GreenplumConfig.RAW_TABLE}")
                break
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed. Database might still be initializing.")
                if attempt < max_retries - 1:
                    logger.info("Waiting 15 seconds before retrying...")
                    time.sleep(15)
                else:
                    logger.error(f"Failed to load data after {max_retries} attempts. Error: {str(e)}")
                    raise

    def stop(self):
        if self.spark:
            logger.info("Stopping SparkSession")
            self.spark.stop()

if __name__ == "__main__":
    seeder = DataSeeder()
    try:
        seeder.download_data()
        seeder.process_and_load()
    except Exception as err:
        logger.error(f"Seeder process failed: {err}")
    finally:
        seeder.stop()