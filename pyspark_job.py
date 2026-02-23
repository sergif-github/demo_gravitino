from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Crear Spark session
spark = SparkSession.builder \
    .appName("Embalses High Volume") \
    .config("spark.jars.packages",
            "mysql:mysql-connector-java:8.1.0,org.postgresql:postgresql:42.5.4") \
    .getOrCreate()

# Conexión a MySQL (raw)
mysql_url = "jdbc:mysql://mysql:3306/embalses_db"
mysql_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Leer datos raw
df_raw = spark.read.jdbc(url=mysql_url, table="embalses", properties=mysql_properties)

# Filtrar embalses con porcentaje_volumen_embalsado > 90%
df_high = df_raw.filter(col("porcentaje_volumen_embalsado") > 90)

# Contar cuántos días ha estado cada embalse > 90%
df_count = df_high.groupBy("embalse").agg(count("*").alias("dias_alto_volumen"))

# Conexión a PostgreSQL (processed)
postgres_url = "jdbc:postgresql://postgres:5432/processed_db"
postgres_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

# Escribir resultados en PostgreSQL
df_count.write.jdbc(url=postgres_url, table="embalses_high_volume", mode="overwrite", properties=postgres_properties)

print("PySpark job finalizado: resultados insertados en embalses_high_volume")
spark.stop()
