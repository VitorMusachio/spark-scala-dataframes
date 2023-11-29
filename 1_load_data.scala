import org.apache.spark.sql.types.{StructField, StructType, StringType, DoubleType, IntegerType}
import org.apache.spark.sql.SparkSession

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

""" DATA EXPLORATION """

df.printSchema() 						// shows data schema
df.show() 								// shows the first dataframe records
df.head(5) 								// shows the first five dataframe records
df.select($"age").describe().show() 	// shows column statistics (column "age" in the example)
df.columns 								// shows the dataframe columns

""" LOAD DATA USING SCHEMA """"

// get schema
val myManualSchema = new StructType(Array(
new StructField("age", IntegerType, true),
new StructField("job", StringType, true),
new StructField("marital", StringType, true),
new StructField("education", StringType, true),
new StructField("default", StringType, true),
new StructField("housing", StringType, true),
new StructField("loan", StringType, true),
new StructField("contact", StringType, true),
new StructField("month", StringType, true),
new StructField("day_of_week", StringType, true),
new StructField("duration", StringType, true),
new StructField("campaign", IntegerType, true),
new StructField("pdays", IntegerType, true),
new StructField("previous", IntegerType, true),
new StructField("poutcome", StringType, true),
new StructField("emp.var.rate", DoubleType, true),
new StructField("cons.price.idx", DoubleType, true),
new StructField("cons.conf.idx", DoubleType, true),
new StructField("euribor3m", DoubleType, true),
new StructField("nr.employe", DoubleType, true),
new StructField("y", StringType, true)
))

// load data using schema
val df_schema = spark.read
					 .option("header","true")
					 .option("delimiter",";")
					 .option("mode", "FAILFAST")
					 .schema(myManualSchema) // schema variable
					 .format("csv")
					 .load("data/bank-additional-full.csv")

// data exploration from "df_schema"
df_schema.printSchema()
df_schema.show()
df_schema.head(5)
df_schema.select($"age").describe().show()
df_schema.columns

""" WRITE DATAFRAME IN A FILE """

df.write
  .format("csv")
  .mode("overwrite")
  .option("sep", "\t")
  .save("/tmp/my-tsv-file.csv")
