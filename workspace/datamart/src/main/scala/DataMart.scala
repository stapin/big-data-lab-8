import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.functions.col

import java.util.Properties
import java.io.File
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

object DataMart {
  def main(args: Array[String]): Unit = {
    System.setProperty("java.net.preferIPv4Stack", "true")

    if (args.length < 2 || args(0) != "--mode") {
      println("[ERROR] Использование: DataMart --mode <extract|load>")
      sys.exit(1)
    }
    val mode = args(1)

    println(">>> Чтение конфигурации из spark.conf...")
    val rootConfig = ConfigFactory.parseFile(new File("spark.conf")).resolve()
    val config = rootConfig.getConfig("datamart")

    // Извлекаем настройки БД
    val dbConfig = config.getConfig("db")
    val jdbcUrl = s"jdbc:postgresql://${dbConfig.getString("host")}:${dbConfig.getString("port")}/${dbConfig.getString("db_name")}?sslmode=disable"
    
    val connectionProperties = new Properties()
    connectionProperties.put("user", dbConfig.getString("user"))
    connectionProperties.put("password", dbConfig.getString("password"))
    connectionProperties.put("driver", "org.postgresql.Driver")

    val tables = config.getConfig("tables")
    val rawTable = tables.getString("raw")
    val featuresTable = tables.getString("features")
    val predictionsTable = tables.getString("predictions")
    val clusteredTable = tables.getString("clustered")

    // 2. Инициализация сессии Spark
    val sparkConfig = config.getConfig("spark")
    val builder = SparkSession.builder()
      .appName(s"${sparkConfig.getString("app_name")}_${mode.capitalize}")
      .master(sparkConfig.getString("master"))
      .config("spark.driver.bindAddress", "0.0.0.0")
      .config("spark.driver.host", "127.0.0.1")

    if (sparkConfig.hasPath("settings")) {
      val settings = sparkConfig.getConfig("settings")
      settings.entrySet().asScala.foreach { entry =>
        // Ключи в HOCON парсятся в кавычках, unwrapped() снимает их
        builder.config(entry.getKey, entry.getValue.unwrapped().toString)
      }
    }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    mode match {
      case "extract" => 
        extractAndTransform(spark, jdbcUrl, connectionProperties, rawTable, featuresTable)
      case "load" => 
        loadResults(spark, jdbcUrl, connectionProperties, predictionsTable, clusteredTable)
      case _ =>
        println(s"[ERROR] Неизвестный режим работы: $mode. Используйте 'extract' или 'load'.")
        sys.exit(1)
    }

    spark.stop()
  }

  // РЕЖИМ 1: Извлечение и Предобработка (EXTRACT)
  def extractAndTransform(spark: SparkSession, url: String, props: Properties, rawTable: String, featuresTable: String): Unit = {
    println("=== [DataMart] Запуск извлечения и предобработки данных (Scala) ===")
    println(s">>> Чтение сырых данных из Greenplum (таблица $rawTable)...")
    
    val rawDf = spark.read.jdbc(url, rawTable, props)
    val cleanDf = rawDf.na.drop()

    println(">>> Векторизация и масштабирование признаков...")
    val assembler = new VectorAssembler()
      .setInputCols(Array("energy-kcal_100g", "proteins_100g", "fat_100g"))
      .setOutputCol("rawFeatures")
      .setHandleInvalid("skip")
    
    val vectorizedDf = assembler.transform(cleanDf)

    val scaler = new StandardScaler()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)
      
    val scalerModel = scaler.fit(vectorizedDf)
    val scaledDf = scalerModel.transform(vectorizedDf)

    val finalFeaturesDf = scaledDf.select(col("id"), vector_to_array(col("features")).alias("features"))

    println(s">>> Сохранение фичей для ML-модели в таблицу $featuresTable (Greenplum)...")
    finalFeaturesDf.write
      .mode(SaveMode.Overwrite)
      .option("createTableOptions", "DISTRIBUTED BY (id)")
      .jdbc(url, featuresTable, props)

    println("=== [DataMart] Данные успешно подготовлены и выгружены в БД! ===")
  }

  // РЕЖИМ 2: Загрузка результатов кластеризации (LOAD)
  def loadResults(spark: SparkSession, url: String, props: Properties, predictionsTable: String, clusteredTable: String): Unit = {
    println("=== [DataMart] Запуск загрузки результатов в Greenplum (Scala) ===")
    println(s">>> Чтение предсказаний модели из промежуточной таблицы $predictionsTable...")
    
    val resultsDf = spark.read.jdbc(url, predictionsTable, props)
    
    println(">>> Предпросмотр полученных данных от модели:")
    resultsDf.show(5)

    println(s">>> Запись финальных результатов в итоговую таблицу $clusteredTable...")
    resultsDf.write
      .mode(SaveMode.Overwrite)
      .option("createTableOptions", "DISTRIBUTED BY (id)")
      .jdbc(url, clusteredTable, props)

    println("=== [DataMart] Данные успешно загружены! Контур завершил работу. ===")
  }
}