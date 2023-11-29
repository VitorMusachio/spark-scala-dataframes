import org.apache.spark.sql.functions.{col, column, expr}

// spark session
val spark = SparkSession.builder()
                        .getOrCreate()

// load data
val df = spark.read
              .option("header","true")
              .option("inferSchema","true")
              .option("delimiter",";")
              .format("csv")
              .load("data/bank-additional-full.csv")

""" SELECT COLUMNS """"

// different ways to select a column 
df.select("age").show(5)
df.select($"age").show(5)
df.select('age).show(5)
df.select(dados.col("age")).show(5)
df.select(col("age")).show(5)
df.select(column("age")).show(5)
df.select(expr("age")).show(5)

""" CREATE, REMOVE AND ALTER COLUMNS """

// create a new columns with a constant value using "lit" function
val data_1 = dados.withColumn("nova_coluna", lit(1)) 

// add new column
val age_filter = expr("age < 40")
dados.select("age", "y").withColumn("age_under_40", age_filter)
     .show(5)

// rename column
dados.select(expr("age as idade")).show(5)
dados.select(col("age").alias("idade")).show(5)
dados.select($"age").withColumnRenamed("age", "idade").show(5)

// remove column
val df_1 = dados.drop("age")
df_1.columns
