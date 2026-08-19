import os
from dotenv import load_dotenv
import yaml

load_dotenv()

class GreenplumConfig:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    PORT = os.getenv("PORT", "5432")
    DB_NAME = os.getenv("DB_NAME") 
    USER = os.getenv("POSTGRES_USER", "gpadmin")
    PASSWORD = os.getenv("POSTGRES_PASSWORD") 
    
    JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{PORT}/{DB_NAME}?sslmode=disable&stringtype=unspecified"
    
    PROPERTIES = {
        "user": USER,
        "password": PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    
    RAW_TABLE = "products_raw"
    FEATURES_TABLE = "features"
    PREDICTIONS_TABLE = "predictions"
    CLUSTERED_TABLE = "products_clustered"


class SparkConfigManager:
    def __init__(self, config_file="spark_config.yaml"):
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file {config_file} not found!")

        with open(config_file, 'r', encoding='utf-8') as f:
            yaml_content = f.read()

        self.JDBC_DRIVER_PATH = os.path.abspath("postgresql-42.5.4.jar")
        
        self._config = yaml.safe_load(yaml_content)

    def get_spark_config(self, profile_name: str) -> dict:
        profiles = self._config.get("spark_profiles", {})
        if profile_name not in profiles:
            raise ValueError(f"Spark profile '{profile_name}' not found in config.yaml")

        profile = profiles[profile_name]
        profile["spark.jars"] = self.JDBC_DRIVER_PATH
        profile["spark.driver.extraClassPath"] = self.JDBC_DRIVER_PATH
        
        return profile

spark_conf_manager = SparkConfigManager()