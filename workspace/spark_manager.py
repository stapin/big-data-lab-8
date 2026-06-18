import logging
from pyspark.sql import SparkSession
from config import SparkConfig

logger = logging.getLogger(__name__)

class SparkManager:
    def __init__(self):
        self._spark = None

    def get_session(self) -> SparkSession:
        if self._spark is None:
            logger.info("Initializing SparkSession with custom configurations")
            builder = SparkSession.builder \
                .appName(SparkConfig.APP_NAME) \
                .master(SparkConfig.MASTER)
            
            for key, value in SparkConfig.SETTINGS.items():
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