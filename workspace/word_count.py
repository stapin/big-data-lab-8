import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WordCountJob")

class WordCountJob:
    def __init__(self, app_name: str = "WordCountScript", master: str = "local[*]"):
        logger.info(f"Initializing SparkSession: {app_name}")
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info(f"SparkSession successfully created. Spark version: {self.spark.version}")

    def load_data(self) -> list[str]:
        logger.info("Generating sample text data")
        return [
            "Hello Spark",
            "Hello Docker",
            "Spark is awesome and Docker is awesome too"
        ]

    def count_words(self, text_data: list[str]) -> list[tuple[str, int]]:
        logger.info("Parallelizing data into RDD")
        rdd = self.spark.sparkContext.parallelize(text_data)
        logger.info(f"Number of RDD partitions: {rdd.getNumPartitions()}")

        logger.info("Executing MapReduce word count logic")
        word_counts = rdd.flatMap(lambda line: line.split(" ")) \
                         .map(lambda word: (word, 1)) \
                         .reduceByKey(lambda a, b: a + b)

        logger.info("Collecting results to the driver node")
        return word_counts.collect()

    def display_results(self, results: list[tuple[str, int]]):
        logger.info("--- WordCount Results ---")
        for word, count in results:
            logger.info(f"'{word}': {count}")
        logger.info("-------------------------")

    def run(self):
        logger.info("Starting WordCount pipeline")
        try:
            data = self.load_data()
            results = self.count_words(data)
            self.display_results(results)
            logger.info("WordCount job completed successfully")
        except Exception as e:
            logger.error(f"Error occurred during WordCount execution: {e}")
        finally:
            self.stop()

    def stop(self):
        if self.spark:
            logger.info("Stopping SparkSession")
            self.spark.stop()
            logger.info("SparkSession stopped")

if __name__ == "__main__":
    job = WordCountJob()
    job.run()