import os
import sys
import argparse
import logging
from config import GreenplumConfig, spark_conf_manager
from spark_manager import SparkManager, SparkProfile
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.functions import vector_to_array

# Настройка логирования для DataMart
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataMart")

class DataMart:
    def __init__(self):
        logger.info("Initializing SparkSession for DataMart")
        self.spark = SparkManager().get_session(SparkProfile.DATA_MART)
        
        self.jdbc_url = GreenplumConfig.JDBC_URL
        self.conn_properties = GreenplumConfig.PROPERTIES

        # Берем названия таблиц из единого конфигуратора
        self.raw_table = GreenplumConfig.RAW_TABLE
        self.features_table = GreenplumConfig.FEATURES_TABLE
        self.predictions_table = GreenplumConfig.PREDICTIONS_TABLE
        self.clustered_table = GreenplumConfig.CLUSTERED_TABLE

    def extract_and_transform(self):
        logger.info("=== [DataMart] Запуск извлечения и предобработки данных ===")
        logger.info(f">>> Подключение к Greenplum по адресу: {self.jdbc_url}")

        try:
            raw_df = self.spark.read.jdbc(self.jdbc_url, self.raw_table, properties=self.conn_properties)
        except Exception as e:
            logger.error(f"Ошибка чтения из БД: {e}")
            sys.exit(1)

        logger.info(">>> Очистка данных от пропусков (NaN)...")
        clean_df = raw_df.dropna()

        logger.info(">>> Векторизация признаков...")
        assembler = VectorAssembler(
            inputCols=["energy-kcal_100g", "proteins_100g", "fat_100g"],
            outputCol="rawFeatures",
            handleInvalid="skip"
        )
        vectorized_df = assembler.transform(clean_df)

        logger.info(">>> Масштабирование признаков...")
        scaler = StandardScaler(
            inputCol="rawFeatures",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        scaled_df = scaler.fit(vectorized_df).transform(vectorized_df)

        # Преобразуем VectorUDT в обычный массив (ArrayType) для сохранения в Greenplum
        final_features_df = scaled_df.select("id", vector_to_array("features").alias("features"))

        # Load to Database
        logger.info(f">>> Сохранение предобработанных данных в таблицу {self.features_table}...")
        final_features_df.write \
            .mode("overwrite") \
            .option("createTableOptions", "DISTRIBUTED BY (id)") \
            .jdbc(self.jdbc_url, self.features_table, properties=self.conn_properties)
            
        logger.info("=== [DataMart] Данные успешно подготовлены и выгружены в БД! ===")

    def load_results(self):
        logger.info("=== [DataMart] Запуск загрузки результатов в Greenplum ===")
        logger.info(f">>> Чтение результатов кластеризации из таблицы {self.predictions_table}...")
        
        try:
            results_df = self.spark.read.jdbc(self.jdbc_url, self.predictions_table, properties=self.conn_properties)
        except Exception as e:
            logger.error(f"Ошибка чтения результатов модели из БД: {e}")
            sys.exit(1)
        
        logger.info(">>> Предпросмотр полученных данных от модели:")
        results_df.show(5)

        logger.info(f">>> Запись итоговых данных в таблицу {self.clustered_table}...")
        
        # Пишем в Greenplum с учетом DISTRIBUTED BY (id)
        results_df.write \
            .mode("overwrite") \
            .option("createTableOptions", "DISTRIBUTED BY (id)") \
            .jdbc(self.jdbc_url, self.clustered_table, properties=self.conn_properties)

        logger.info("=== [DataMart] Данные успешно загружены! Контур завершил работу. ===")

    def stop(self):
        self.spark.stop()

def main():
    parser = argparse.ArgumentParser(description="DataMart ETL Controller")
    parser.add_argument("--mode", choices=["extract", "load"], required=True, 
                        help="Режим работы: 'extract' для выгрузки из БД, 'load' для записи результатов")
    args = parser.parse_args()

    datamart = DataMart()
    if args.mode == "extract":
        datamart.extract_and_transform()
    elif args.mode == "load":
        datamart.load_results()
        
    datamart.stop()

if __name__ == "__main__":
    main()