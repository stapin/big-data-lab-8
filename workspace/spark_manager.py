import logging
from pyspark.sql import SparkSession
from config import spark_conf_manager

logger = logging.getLogger(__name__)

class SparkProfile:
    SEEDER = "seeder"
    ML_PIPELINE = "ml_pipeline"
    DATA_MART = "data_mart"

class SparkManager:
    def __init__(self):
        self._spark = None

    def get_session(self, profile: SparkProfile) -> SparkSession:
        if self._spark is None:
            logger.info("Initializing SparkSession with custom configurations")
            spark_conf = spark_conf_manager.get_spark_config(profile)
            builder = SparkSession.builder \
                .appName(spark_conf["app_name"]) \
                .master(spark_conf["master"])
            
            for key, value in spark_conf["settings"].items():
                builder = builder.config(key, value)
                
            self._spark = builder.getOrCreate()
            
            self._spark.sparkContext.setLogLevel("ERROR")
            logger.info("SparkSession successfully created")
            
        return self._spark

    def stop(self):
        if self._spark:
            logger.info("Stopping SparkSession")
            self._spark.stop()
            logger.info("SparkSession stopped")