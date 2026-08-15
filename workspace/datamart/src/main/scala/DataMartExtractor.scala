import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.sql.SaveMode
import java.util.Properties

object DataMartExtractor {
  def main(args: Array[String]): Unit = {
    // 0. Жестко отключаем IPv6 на уровне Java до загрузки сетевых классов Spark
    System.setProperty("java.net.preferIPv4Stack", "true")

    // 1. Инициализация сессии Spark
    val spark = SparkSession.builder()
      .appName("Scala_DataMart_Extractor")
      .master("local[*]") // Используем все ядра
      .config("spark.driver.bindAddress", "0.0.0.0") // Универсальный адрес (Слушать везде)
      .config("spark.driver.host", "127.0.0.1")      // Представляться как локалхост
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println("=== [DataMart] Запуск извлечения и предобработки данных ===")

    // 2. Чтение файла конфигурации (application.properties из ресурсов)
    val configProps = new Properties()
    val configStream = getClass.getResourceAsStream("/application.properties")
    if (configStream != null) {
      configProps.load(configStream)
    } else {
      println("[ERROR] Файл application.properties не найден в src/main/resources/")
      sys.exit(1)
    }

    val dbHost = sys.env.getOrElse("DB_HOST", "localhost")
    val dbUser = sys.env.getOrElse(
      "POSTGRES_USER",
      throw new IllegalArgumentException("Environment variable POSTGRES_USER is not set")
    )
    val dbPassword = sys.env.getOrElse(
      "POSTGRES_PASSWORD",
      throw new IllegalArgumentException("Environment variable POSTGRES_PASSWORD is not set")
    )
    val jdbcUrl = s"jdbc:postgresql://$dbHost:5432/gpadmin?sslmode=disable"
    val connectionProperties = new Properties()
    connectionProperties.put("user", dbUser)
    connectionProperties.put("password", dbPassword)
    connectionProperties.put("driver", "org.postgresql.Driver")

    // 3. Извлечение данных (Extract) из Greenplum
    println(">>> Чтение сырых данных из Greenplum (таблица products_raw)...")
    val rawDf = spark.read.jdbc(jdbcUrl, "products_raw", connectionProperties)
    
    // 4. Предобработка данных (Transform)
    println(">>> Очистка данных от пропусков (NaN)...")
    val cleanDf = rawDf.na.drop()

    println(">>> Векторизация признаков (VectorAssembler)...")
    val assembler = new VectorAssembler()
      // ВАЖНО: Проверь, что эти названия колонок совпадают с колонками в products_raw
      .setInputCols(Array("energy-kcal_100g", "proteins_100g", "fat_100g"))
      .setOutputCol("rawFeatures")
      .setHandleInvalid("skip")

    val vectorizedDf = assembler.transform(cleanDf)

    println(">>> Масштабирование признаков (StandardScaler)...")
    val scaler = new StandardScaler()
      .setInputCol("rawFeatures")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(vectorizedDf)
    val scaledDf = scalerModel.transform(vectorizedDf)

    // Оставляем только id продукта и итоговый вектор для модели
    val finalFeaturesDf = scaledDf.select("id", "features")

    // 5. Выгрузка данных в общую директорию (Load into Volume)
    println(">>> Сохранение предобработанных данных в Parquet (shared_data)...")
    val outputPath = "/workspace/shared_data/features.parquet"

    finalFeaturesDf.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)

    println("=== [DataMart] Данные успешно подготовлены и выгружены в Volume! ===")
    spark.stop()
  }
}