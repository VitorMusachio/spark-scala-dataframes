import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType, DoubleType, IntegerType}
import org.apache.spark.sql.functions.{col, column, expr, upper, lower, desc, asc, split, udf}
import org.apache.spark.sql.Row

// spark session
val spark = SparkSession.builder()
                        .getOrCreate()

// load data using schema
// val schema = df.schema
val schema = new StructType(Array(
new StructField("age", IntegerType, true),
new StructField("job", StringType, true)))

// create rows
val newRows = Seq(
Row(30, "Data Scientist"),
Row(20, "Java Dev"),
Row(10, null)
)

// create a RDD
val parallelizedRows = spark.sparkContext.parallelize(newRows)

// create dataframe from RDD
val manual_data = spark.createDataFrame(parallelizedRows, schema)

// show dataframe info
manual_data.show()

// load data
val df = spark.read
              .option("header","true")
              .option("inferSchema","true")
              .option("delimiter",";")
              .format("csv")
              .load("data/bank-additional-full.csv")

""" MISSING VALUES """

// drop the row if one value is null
df.select($"age", $"marital").na.drop("any")

// drop the row if all values are null
df.select($"age", $"marital").na.drop("all")

// fill
manual_data.na.fill("Unknown").show()

// specifying values for each column
val values_to_fill = Map("age" -> 0, "job" -> "Unknown")
manual_data.na.fill(values_to_fill).show()

// replace values
manual_data.na.replace("job", Map("Dev Java" -> "Desenvolvedor")).show()