import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row

// spark session
val spark = SparkSession.builder()
                        .getOrCreate()

// spark session
val spark = SparkSession.builder()
                        .getOrCreate()
val df = spark.read
              .option("header","true")
              .option("inferSchema","true")
              .option("delimiter",";")
              .format("csv")
              .load("data/bank-additional-full.csv")

""" FILTER AND ORDER BY """

// filter age > 55 and order by age desc
df.select($"age", $"job")
  .filter($"age" > 55)
  .orderBy($"age".desc).show(2)

// filter marital = married
df.select($"age", $"marital")
  .filter($"marital" === "married")
  .show(5)

df.select($"age", $"marital")
  .filter($"marital".equalTo("married"))
  .show(5)

// filter marital <> married
df.select($"age", $"marital")
  .where($"marital" =!= "married")
  .show(5) // scala stype

df.select($"age", $"marital")
  .where("marital <> 'married' ")
  .show(5) // SQL stype

// show unique values
df.select($"marital")
  .distinct()
  .show()

// using multiple filters
val age_filter = col("age") > 40
val marital_filter = col("marital").contains("married")

df.select($"age", $"marital", $"job")
  .where(col("job").isin("unemployed", "retired"))
  .where(marital_filter.or(age_filter))
  .show(5)

// filters can be used in select too
df.select($"age", $"marital", $"job")
  .where(col("job").isin("unemployed", "retired"))
  .where(marital_filter.or(age_filter))
  .withColumn("marital_filter", marital_filter)
  .show(5)

""" DATA CONVERSIONS """

// set age as a string
val df_1 = dados.withColumn("idade_string", col("age").cast("string"))
df_1.select($"idade_string")

""" FUNCTIONS """

df.select(upper($"poutcome")).show(5) // uppercase string
df.select(lower($"poutcome")).show(5) // lowercase string