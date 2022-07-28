import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession,functions as F

from pyspark.sql.types import *

spark = SparkSession.builder \
.appName("Clean Dirty Data") \
.master("Local[2]") \
.getOrCreate()

df = spark.read.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.load("file:///tmp/dirty_store_transactions.csv")

df.limit(5).toPandas()
.
.
.
df.printSchema()
|-- STORE_ID: string (nullable = true)
|-- STORE_LOCATION: string (nullable = true)
|-- PRODUCT_CATEGORY: string (nullable = true)
|-- PRODUCT_ID: string (nullable = true)
|-- MRP: string (nullable = true)
|-- CP: string (nullable = true)
|-- DISCOUNT: string (nullable = true)
|-- SP: string (nullable = true)
|-- Date: string (nullable = true)

# STORE_LOCATION
df1 = df.withColumn("STORE_LOCATION", F.regexp_replace(F.col("STORE_LOCATION"),
																											"[^A-Zaz0-9]", ""))

df1.limit(5).toPandas()
.
.
.
.
# CLEAN DOLLAR SIGN
df2 = df1.withColumn("MRP", F.regexp_replace(F.col("MRP"), "\$", "").cast(FloatType())) \
.withColumn("CP", F.regexp_replace(F.col("CP"), "\$", "").cast(FloatType())) \
.withColumn("DISCOUNT", F.regexp_replace(F.col("DISCOUNT"), "\$", "").cast(FloatType())) \
.withColumn("SP", F.regexp_replace(F.col("SP"), "\$", "").cast(FloatType()))


df2.limit(5).toPandas()
.
.
.
.
df2.printSchema()
root
|-- STORE_ID: string (nullable = true)
|-- STORE_LOCATION: string (nullable = true)
|-- PRODUCT_CATEGORY: string (nullable = true)
|-- PRODUCT_ID: string (nullable = true)
|-- MRP: float (nullable = true)
|-- CP: float (nullable = true)
|-- DISCOUNT: float (nullable = true)
|-- SP: float (nullable = true)
|-- Date: string (nullable = true)

# DATE

df3 = df2.withColumn("sales_date", F.col("Date").cast(DateType()))

df2.limit(5).toPandas()

df2.printSchema()
root
|-- STORE_ID: string (nullable = true)
|-- STORE_LOCATION: string (nullable = true)
|-- PRODUCT_CATEGORY: string (nullable = true)
|-- PRODUCT_ID: string (nullable = true)
|-- MRP: float (nullable = true)
|-- CP: float (nullable = true)
|-- DISCOUNT: float (nullable = true)
|-- SP: float (nullable = true)
|-- Date: string (nullable = true)
|-- sales_date: date (nullable = true)

df3.count()
37853

df4 = df3.dropna()

df4.count()
37853

df5 = df4.dropna("Date")

# Write to PostgreSQL

psql_conn = "jdbc:postgresql://localhost:5432/traindb?user=train&password=Ankara06"

df5.wirte.mode("overwrite") \
.jdbc(url=psql_conn,
			table='clean_transaction',
			properties={"driver":"org.postgresql.Driver"})

! psql -U train -d traindb -c "select * from clean_transactions limit 5;"


import findspark

findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession,functions as F

from pyspark.sql.types import *

spark = SparkSession.builder \
.appName("Clean Dirty Data") \
.master("Local[2]") \
.getOrCreate()

df = spark.read.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.load("file:///tmp/dirty_store_transactions.csv")

df.limit(5).toPandas()
.
.
.
df.printSchema()
|-- STORE_ID: string (nullable = true)
|-- STORE_LOCATION: string (nullable = true)
|-- PRODUCT_CATEGORY: string (nullable = true)
|-- PRODUCT_ID: string (nullable = true)
|-- MRP: string (nullable = true)
|-- CP: string (nullable = true)
|-- DISCOUNT: string (nullable = true)
|-- SP: string (nullable = true)
|-- Date: string (nullable = true)

# STORE_LOCATION
df1 = df.withColumn("STORE_LOCATION", F.regexp_replace(F.col("STORE_LOCATION"),
																											"[^A-Zaz0-9]", ""))

df1.limit(5).toPandas()
.
.
.
.
# CLEAN DOLLAR SIGN
df2 = df1.withColumn("MRP", F.regexp_replace(F.col("MRP"), "\$", "").cast(FloatType())) \
.withColumn("CP", F.regexp_replace(F.col("CP"), "\$", "").cast(FloatType())) \
.withColumn("DISCOUNT", F.regexp_replace(F.col("DISCOUNT"), "\$", "").cast(FloatType())) \
.withColumn("SP", F.regexp_replace(F.col("SP"), "\$", "").cast(FloatType()))


df2.limit(5).toPandas()
.
.
.
.
df2.printSchema()
root
|-- STORE_ID: string (nullable = true)
|-- STORE_LOCATION: string (nullable = true)
|-- PRODUCT_CATEGORY: string (nullable = true)
|-- PRODUCT_ID: string (nullable = true)
|-- MRP: float (nullable = true)
|-- CP: float (nullable = true)
|-- DISCOUNT: float (nullable = true)
|-- SP: float (nullable = true)
|-- Date: string (nullable = true)

# DATE

df3 = df2.withColumn("sales_date", F.col("Date").cast(DateType()))

df2.limit(5).toPandas()

df2.printSchema()
root
|-- STORE_ID: string (nullable = true)
|-- STORE_LOCATION: string (nullable = true)
|-- PRODUCT_CATEGORY: string (nullable = true)
|-- PRODUCT_ID: string (nullable = true)
|-- MRP: float (nullable = true)
|-- CP: float (nullable = true)
|-- DISCOUNT: float (nullable = true)
|-- SP: float (nullable = true)
|-- Date: string (nullable = true)
|-- sales_date: date (nullable = true)

df3.count()
37853

df4 = df3.dropna()

df4.count()
37853

df5 = df4.dropna("Date")

# Write to PostgreSQL

psql_conn = "jdbc:postgresql://localhost:5432/traindb?user=train&password=Ankara06"
print("writing to postgresql")
df5.wirte.mode("overwrite") \
.jdbc(url=psql_conn,
			table='clean_transaction',
			properties={"driver":"org.postgresql.Driver"})

print("writing completed.")

#! psql -U train -d traindb -c "select * from clean_transactions limit 5;"
