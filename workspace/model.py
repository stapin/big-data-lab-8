import logging
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

logger = logging.getLogger("ml_clustering")

class FoodClusteringModel:
    def __init__(self, k_clusters=5):
        self.k_clusters = k_clusters
        # Scala-витрина уже собрала данные в колонку "features"
        # Нам остается только инициализировать K-Means
        self.kmeans = KMeans(
            k=self.k_clusters, 
            featuresCol="features", 
            predictionCol="prediction"
        )

    def fit_predict(self, df):
        logger.info(f"Training K-Means model with k={self.k_clusters}")
        
        # Обучаем модель на готовых фичах
        model = self.kmeans.fit(df)
        
        # Получаем предсказания (номера кластеров)
        predictions = model.transform(df)

        # Оцениваем качество кластеризации
        evaluator = ClusteringEvaluator(
            featuresCol="features", 
            metricName="silhouette", 
            distanceMeasure="squaredEuclidean"
        )
        silhouette = evaluator.evaluate(predictions)
        logger.info(f"Model evaluation completed. Silhouette Score: {silhouette:.4f}")

        return predictions