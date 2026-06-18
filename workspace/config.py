import os
from dotenv import load_dotenv


load_dotenv()

class GreenplumConfig:
    HOST = os.getenv("HOST", "localhost")
    PORT = os.getenv("PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "postgres") 
    USER = os.getenv("USER", "gpadmin")
    PASSWORD = os.getenv("PASSWORD") 
    
    JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB_NAME}?sslmode=disable&stringtype=unspecified"
    
    PROPERTIES = {
        "user": USER,
        "password": PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    RAW_TABLE = "products_raw"
    CLUSTERED_TABLE = "products_clustered"

class SparkConfig:
    APP_NAME = "Greenplum_ML_Pipeline"
    MASTER = "local[*]"
    
    JDBC_DRIVER_PATH = os.path.abspath("postgresql-42.5.4.jar")
    
    SETTINGS = {
        "spark.driver.memory": "2g",
        "spark.executor.memory": "4g",
        "spark.memory.fraction": "0.8",
        "spark.sql.shuffle.partitions": "10",
        "spark.default.parallelism": "10",
        
        "spark.jars": JDBC_DRIVER_PATH,
        "spark.driver.extraClassPath": JDBC_DRIVER_PATH
    }