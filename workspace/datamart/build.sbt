name := "DataMart"

version := "1.0"

// Версия Scala, совместимая со Spark 3.5.0
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

// Подключаем Spark и драйвер PostgreSQL
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.5.4"
)

// Изолируем выполнение от процесса сборки sbt
fork in run := true