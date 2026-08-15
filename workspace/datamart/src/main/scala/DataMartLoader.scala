import org.apache.spark.sql.{SparkSession, SaveMode}
import java.util.Properties

object DataMartLoader {
  def main(args: Array[String]): Unit = {
    // 0. Обход бага с сетью (как мы делали в Extractor)
    System.setProperty("java.net.preferIPv4Stack", "true")

    // 1. Инициализация сессии Spark
    val spark = SparkSession.builder()
      .appName("Scala_DataMart_Loader")
      .master("local[*]")
      .config("spark.driver.bindAddress", "0.0.0.0")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println("=== [DataMart] Запуск загрузки результатов в Greenplum ===")

    // 2. Чтение конфигурации (используем тот же файл application.properties)
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
    println(s">>> Подключение к базе данных Greenplum по адресу: $jdbcUrl")
    val connectionProperties = new Properties()
    connectionProperties.put("user", dbUser)
    connectionProperties.put("password", dbPassword)
    connectionProperties.put("driver", "org.postgresql.Driver")

    // 3. Чтение результатов работы Python-модели из Volume
    val inputPath = "/workspace/shared_data/results.parquet"
    println(s">>> Чтение результатов кластеризации из $inputPath ...")
    
    val resultsDf = spark.read.parquet(inputPath)
    
    println(">>> Предпросмотр полученных данных от модели:")
    resultsDf.show(5)

    // 4. Загрузка данных в Greenplum (Load)
    println(">>> Запись данных в таблицу products_clustered (Greenplum)...")
    
    // ВАЖНО: Указываем ключ распределения DISTRIBUTED BY (id) для Greenplum
    resultsDf.write
      .mode(SaveMode.Overwrite)
      .option("createTableOptions", "DISTRIBUTED BY (id)")
      .jdbc(jdbcUrl, "products_clustered", connectionProperties)

    println("=== [DataMart] Данные успешно загружены в базу данных! Контур завершил работу. ===")
    spark.stop()
  }
}