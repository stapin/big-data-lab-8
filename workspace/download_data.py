import os
import time
import urllib.request
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from config import GreenplumConfig, SparkConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        # Теперь мы читаем локальный запеченный файл!
        self.file_path = "/workspace/sample_data.csv" 

    def download_jdbc_driver(self):
        if not os.path.exists(self.jar_path):
            logger.info("Downloading PostgreSQL JDBC driver...")
            driver_url = "https://jdbc.postgresql.org/download/postgresql-42.5.4.jar"
            urllib.request.urlretrieve(driver_url, self.jar_path)
        else:
            logger.info("JDBC driver already exists")

    def process_and_load(self):
        logger.info("Reading local sample data into Spark DataFrame")
        # Читаем обычный CSV (с заголовками и автоматическим определением типов)
        df = self.spark.read.csv(self.file_path, header=True, inferSchema=True)
        df = df.withColumn("id", monotonically_increasing_id())

        row_count = df.count()
        logger.info(f"Prepared {row_count} rows for insertion into Greenplum")
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to write data to db:{GreenplumConfig.DB_NAME}\n{GreenplumConfig.RAW_TABLE} (Attempt {attempt + 1}/{max_retries})")
                df.write \
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
                    time.sleep(15)
                else:
                    logger.error(f"Failed to load data. Error: {str(e)}")
                    raise

    def stop(self):
        if self.spark:
            self.spark.stop()

if __name__ == "__main__":
    seeder = DataSeeder()
    try:
        seeder.process_and_load() # Убрали метод download_data()
    except Exception as err:
        logger.error(f"Seeder process failed: {err}")
    finally:
        seeder.stop()