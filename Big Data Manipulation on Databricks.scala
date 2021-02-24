// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.

// ENTER THE CODE BELOW
// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema1 = StructType(Array(StructField("LocationID", IntegerType, true), StructField("Borough", StringType, true), StructField("Zone", StringType, true), StructField("service_zone", StringType, true)))

val df1 = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema1)
   .load("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with


// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
df_filter.show(5)

// COMMAND ----------

// PART 1a: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val df_f1 = df_filter.groupBy("DOLocationID").count()
val df_f2 = df_f1.orderBy($"count".desc, $"DOLocationID".asc)
val df_f3 = df_f2.withColumnRenamed("count", "number_of_dropoffs")
df_f3.show(5)


// COMMAND ----------

// PART 1b: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
val pu_f1 = df_filter.groupBy("PULocationID").count()
val pu_f2 = pu_f1.orderBy($"count".desc, $"PULocationID".asc)
val pu_f3 = pu_f2.withColumnRenamed("count", "number_of_pickups")
pu_f3.show(5)

// COMMAND ----------

// PART 2: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW
val dopu = df_f3.join(pu_f3, $"DOLocationID" === $"PULocationID","full_outer")

val dopu0 = dopu.na.fill(0.0, Seq("number_of_pickups"))
val dopu00 = dopu0.na.fill(0.0, Seq("number_of_dropoffs"))


val dopu1 = dopu00.withColumn("number_activities", $"number_of_dropoffs" + $"number_of_pickups")
val dopu2 = dopu1.orderBy($"number_activities".desc, $"DOLocationID".asc)
val dopu3 = dopu2.withColumnRenamed("DOLocationID", "LocationID").select("LocationID","number_activities")
dopu3.show(3)


// COMMAND ----------

// PART 3: List all the boroughs in the order of having the highest to lowest number of activities (i.e. sum of all pickups and all dropoffs at that LocationID), along with the total number of activity counts for each borough in NYC during that entire period of time.
// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW

// dopu.count()
// df_f3.count()
// pu_f3.count()
// dopu2.filter($"number_of_dropoffs" <4 ).show()
val df2 = df1.join(dopu3, df1("LocationID") === dopu3("LocationID"),"left").drop(dopu3("LocationID"))

val df3 = df2.na.fill(0.0, Seq("number_activities"))

val df4 = df3.groupBy("Borough").agg(sum("number_activities"))

val df5 = df4.withColumnRenamed("sum(number_activities)", "total_number_activities")

val df6 = df5.orderBy($"total_number_activities".desc)
// df2.filter($"number_activities" <4 ).show()
df6.show()



// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of (daily) average pickups, along with the values of average number of pickups on each of the two days. The day of week should be a string with its full name, for example, "Monday" - not a number 1 or "Mon" instead.
// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW

val df_date = df_filter.withColumn("pickup_date", to_date(col("pickup_datetime"))) 
val df_date1 = df_date.groupBy("pickup_date").count()
val df_date2 = df_date1.withColumn("day_of_week", date_format(col("pickup_date"), "EEEE"))
val df_date3 = df_date2.groupBy("day_of_week").agg(avg("count"))

val df_date4 = df_date3.orderBy($"avg(count)".desc)
val df_date5 = df_date4.withColumnRenamed("avg(count)", "avg_count")

df_date5.show(2)

// COMMAND ----------

// PART 5: For each particular hour of a day (0 to 23, 0 being midnight) - in their order from 0 to 23, find the zone in Brooklyn borough with the LARGEST number of pickups. 
// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW
val df_hour = df_filter.withColumn("pickup_hour", hour(col("pickup_datetime"))) 
val df_hour1 = df_hour.join(df1, df_hour("PULocationID") === df1("LocationID"),"left") 

val df_hour2 =df_hour1.filter($"Borough" === "Brooklyn").select("Zone","pickup_hour")

val df_hour3 = df_hour2
  .withColumn("cnt",count("*").over(Window.partitionBy($"pickup_hour",$"Zone")))
  .withColumn("rnb",row_number().over(Window.partitionBy($"pickup_hour").orderBy($"cnt".desc)))
  .where($"rnb"===1)
  .select($"pickup_hour",$"Zone",$"cnt".as("max"))

val df_hour4 = df_hour3.withColumnRenamed("pickup_hour", "hour_of_day").withColumnRenamed("Zone", "zone").withColumnRenamed("max", "max_count").orderBy($"hour_of_day".asc)


df_hour4.show(24, false)
// df_hour4.display()
// df_hour2.filter($"Zone" === "DUMBO/Vinegar Hill" && $"pickup_hour" === 12).count()


// COMMAND ----------

// PART 6 - Find which 3 different days of the January, in Manhattan, saw the largest percentage increment in pickups compared to previous day, in the order from largest increment % to smallest increment %. 
// Print the day of month along with the percent CHANGE (can be negative), rounded to 2 decimal places, in number of pickups compared to previous day.
// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW
val df_month = df_filter.withColumn("day", dayofmonth(to_date(col("pickup_datetime"))))
                        .withColumn("month", month(to_date(col("pickup_datetime")))) 


val df_month1 = df_month.join(df1, df_month("PULocationID") === df1("LocationID"),"left") 

val df_month2 = df_month1.filter($"Borough" === "Manhattan" && $"month" === 1).select("day","month", "Borough")

val df_month3 = df_month2.groupBy("day").count()

val df_month4 = df_month3.orderBy($"day".asc)

val window = Window.orderBy("day")
val df_month5 = df_month4.withColumn("prev_count", lag(col("count"),1).over(window))
                         .withColumn("percent_change", round(lit(100)*($"count"-$"prev_count")/$"prev_count",2))
                         .orderBy($"percent_change".desc).select("day","percent_change")
// val df_month3 = df_month3.withColumn("diff", when(isnull(df_month3.value - df_month3.prev_value), 0).otherwise(100*(df_month3.value - df_month3.prev_value)/df_month3.prev_value))
df_month5.show(3)

