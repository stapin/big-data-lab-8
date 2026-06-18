import logging
from spark_manager import SparkManager
from model import FoodClusteringModel

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ML_Orchestrator")

class MLPipeline:
    
    def __init__(self):
        self.spark_manager = SparkManager()
        self.spark = self.spark_manager.get_session()
        self.ml_model = FoodClusteringModel(k_clusters=5)
        
        # Пути к общей директории Volume
        self.features_path = "/workspace/shared_data/features.parquet"
        self.results_path = "/workspace/shared_data/results.parquet"

    def extract_data(self):
        logger.info(f"Extract stage: Reading preprocessed features from '{self.features_path}'")
        # Читаем уже готовые фичи, которые подготовила Scala-витрина
        df = self.spark.read.parquet(self.features_path)
        
        df.cache()
        row_count = df.count()
        logger.info(f"Extract stage completed. Rows extracted: {row_count}")
        return df

    def transform_and_model(self, df):
        logger.info("Transform stage: Starting machine learning pipeline")
        # Передаем датафрейм в K-Means
        result_df = self.ml_model.fit_predict(df)
        
        # Оставляем только нужные колонки для возврата в витрину
        # Колонка 'prediction' содержит номер кластера
        final_df = result_df.select("id", "prediction")
        return final_df

    def load_data(self, df):
        logger.info(f"Load stage: Writing results to '{self.results_path}'")
        # Сохраняем результат работы модели в общую папку для Scala-витрины
        df.write \
            .mode("overwrite") \
            .parquet(self.results_path)
            
        logger.info("Load stage completed successfully")

    def run(self):
        logger.info("Starting ML pipeline execution")
        try:
            features_data = self.extract_data()
            clustered_data = self.transform_and_model(features_data)
            self.load_data(clustered_data)
            logger.info("ML pipeline successfully finished. Results are waiting for Data Mart.")
        except Exception as e:
            logger.error(f"Critical error occurred during ML pipeline execution: {e}")
        finally:
            self.spark_manager.stop()

if __name__ == "__main__":
    pipeline = MLPipeline()
    pipeline.run()