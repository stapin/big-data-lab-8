import logging
from spark_manager import SparkManager, SparkProfile
from config import GreenplumConfig
from model import FoodClusteringModel
from pyspark.ml.functions import array_to_vector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ML_Orchestrator")

class MLPipeline:
    
    def __init__(self):
        self.spark_manager = SparkManager()
        self.spark = self.spark_manager.get_session(SparkProfile.ML_PIPELINE)
        self.ml_model = FoodClusteringModel(k_clusters=5)
        
        self.jdbc_url = GreenplumConfig.JDBC_URL
        self.conn_properties = GreenplumConfig.PROPERTIES
        
        self.features_table = GreenplumConfig.FEATURES_TABLE
        self.predictions_table = GreenplumConfig.PREDICTIONS_TABLE

    def extract_data(self):
        logger.info(f"Extract stage: Reading preprocessed features from table '{self.features_table}'")
        
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=self.features_table,
            properties=self.conn_properties
        )
        
        df = df.withColumn("features", array_to_vector("features"))
        
        df.cache()
        row_count = df.count()
        logger.info(f"Extract stage completed. Rows extracted: {row_count}")
        return df

    def transform_and_model(self, df):
        logger.info("Transform stage: Starting machine learning pipeline")
        result_df = self.ml_model.fit_predict(df)
        
        final_df = result_df.select("id", "prediction")
        return final_df

    def load_data(self, df):
        logger.info(f"Load stage: Writing results to table '{self.predictions_table}'")
        
        df.write \
            .mode("overwrite") \
            .option("createTableOptions", "DISTRIBUTED BY (id)") \
            .jdbc(self.jdbc_url, self.predictions_table, properties=self.conn_properties)
            
        logger.info("Load stage completed successfully")

    def run(self):
        logger.info("Starting ML pipeline execution")
        try:
            features_data = self.extract_data()
            clustered_data = self.transform_and_model(features_data)
            self.load_data(clustered_data)
            logger.info("ML pipeline successfully finished. Results are waiting in DB for Data Mart.")
        except Exception as e:
            logger.error(f"Critical error occurred during ML pipeline execution: {e}")
        finally:
            self.spark_manager.stop()

if __name__ == "__main__":
    pipeline = MLPipeline()
    pipeline.run()