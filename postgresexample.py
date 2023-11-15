from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://103.67.78.159:7077") \
    .config("spark.driver.memory", "1g") \
    .config("spark.jars", "postgresql-42.6.0.jar") \
    .appName("PostgreSQLExample") \
    .getOrCreate()

data = [("John", 25), ("Jane", 30), ("Doe", 22)]


columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

jdbc_url = "jdbc:postgresql://localhost:5432/scooter"
connection_properties = {
    "user": "ilhamkwr",
    "password": "ilham051198",
    "driver": "org.postgresql.Driver"
}

df.write.jdbc(url=jdbc_url, table="table_test", mode="append", properties=connection_properties)

spark.stop()
