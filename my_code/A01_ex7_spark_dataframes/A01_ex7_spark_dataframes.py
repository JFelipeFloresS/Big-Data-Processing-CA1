# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------


# ------------------------------------------
# IMPORTS
# ------------------------------------------
import pyspark
import pyspark.sql.functions
import sys

# ------------------------------------------
# CONSTANTS
# ------------------------------------------
DATE = "date" # pyspark.sql.types.StringType
BUS_LINE_ID = "busLineID" # pyspark.sql.types.IntegerType
BUS_LINE_PATTERN_ID = "busLinePatternID" # pyspark.sql.types.StringType
CONGESTION = "congestion" # pyspark.sql.types.IntegerType
LONGITUDE = "longitude" # pyspark.sql.types.FloatType
LATITUDE = "latitude" # pyspark.sql.types.FloatType
DELAY = "delay" # pyspark.sql.types.IntegerType
VEHICLE_ID = "vehicleID" # pyspark.sql.types.IntegerType
CLOSER_STOP_ID = "closerStopID" # pyspark.sql.types.IntegerType
AT_STOP = "atStop" # pyspark.sql.types.IntegerType

# new columns we create along the way
PREV_DATE = "prevDate" # pyspark.sql.types.StringType
PREV_LATITUDE = "prevLatitude" # pyspark.sql.types.FloatType
PREV_LONGITUDE = "prevLongitude" # pyspark.sql.types.FloatType
SECONDS = "seconds" # pyspark.sql.types.LongType
SPEED_MS = "speedMS" # pyspark.sql.types.DoubleType
AVG_SPEED_KMH = "avgSpeedKMH" # pyspark.sql.types.DoubleType
BUCKET_ID = "bucketID" # pyspark.sql.types.IntegerType
BUCKET_RANGE = "bucketRange" # pyspark.sql.types.StringType
NUM_VEHICLES = "numVehicles" # pyspark.sql.types.LongType

EARTH_RADIUS_KM = 6371.0

# ------------------------------------------
# FUNCTION haversine_km
# ------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    # same formula as the core version, but built with Spark columns so it runs inside Spark

    # 1. We convert the degrees to radians
    lat1 = pyspark.sql.functions.radians(lat1)
    lon1 = pyspark.sql.functions.radians(lon1)
    lat2 = pyspark.sql.functions.radians(lat2)
    lon2 = pyspark.sql.functions.radians(lon2)

    # 2. Haversine formula: h = sin^2((lat2 - lat1) / 2) + cos(lat1) * cos(lat2) * sin^2((lon2 - lon1) / 2)
    h = pyspark.sql.functions.pow(pyspark.sql.functions.sin((lat2 - lat1) / 2), 2) + \
        pyspark.sql.functions.cos(lat1) * pyspark.sql.functions.cos(lat2) * pyspark.sql.functions.pow(pyspark.sql.functions.sin((lon2 - lon1) / 2), 2)

    # 3. We turn h into the angle between the 2 points (2 * arcsin(sqrt(h))) and multiply it by the radius of the earth
    #    edge case: h can go a tiny bit over 1 because of rounding, which would make asin give null, so we cap it at 1
    return 2 * EARTH_RADIUS_KM * pyspark.sql.functions.asin(pyspark.sql.functions.sqrt(pyspark.sql.functions.least(pyspark.sql.functions.lit(1.0), h)))

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            bucket_size_kmh,
            max_speed_accepted,
            day_picked
           ):

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("busLineID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("busLinePatternID", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("congestion", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("delay", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("vehicleID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("closerStopID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("atStop", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C1: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation T1: 'filter' to keep only the rows of day_picked
    #    the date always starts with "yyyy-mm-dd", so startswith is enough to check the day
    filteredDF = inputDF.filter(inputDF[DATE].startswith(day_picked))

    # 4. Operation T2: 'select' to keep only the vehicle, date and position
    selectedDF = filteredDF.select(VEHICLE_ID, DATE, LATITUDE, LONGITUDE)

    # 5. Operation T3: 'withColumn' + 'lag' over a window per vehicle (sorted by date) to bring the previous date and position of the same vehicle into each row
    #    the window keeps each vehicle separate, so we never pair up measurements of two different buses
    #    edge case: the first row of each vehicle has no previous row, so lag gives null there
    vehicleWindow = pyspark.sql.Window.partitionBy(VEHICLE_ID).orderBy(DATE)
    lagDF = selectedDF.withColumn(PREV_DATE, pyspark.sql.functions.lag(DATE).over(vehicleWindow)) \
                      .withColumn(PREV_LATITUDE, pyspark.sql.functions.lag(LATITUDE).over(vehicleWindow)) \
                      .withColumn(PREV_LONGITUDE, pyspark.sql.functions.lag(LONGITUDE).over(vehicleWindow))

    # 6. Operation T4: 'withColumn' to work out the seconds between the previous row and this one
    secondsDF = lagDF.withColumn(SECONDS,
                                 pyspark.sql.functions.unix_timestamp(lagDF[DATE]) - pyspark.sql.functions.unix_timestamp(lagDF[PREV_DATE]))

    # 7. Operation T5: 'filter' to keep only the rows that make a proper pair with the previous one
    #    edge case: the first row of each vehicle has null seconds, and null > 0 is never true, so it gets dropped here
    #    edge case: two measurements with the same time would mean dividing by 0, so we can't get a speed out of them and we skip the pair
    pairsDF = secondsDF.filter(secondsDF[SECONDS] > 0)

    # 8. Operation T6: 'withColumn' to work out the speed (m/s) of each pair with the haversine formula
    #    we use Spark's own math functions instead of a python udf, so it all runs inside Spark and keeps full (double) precision
    speedDF = pairsDF.withColumn(SPEED_MS, haversine_km(pairsDF[PREV_LATITUDE], pairsDF[PREV_LONGITUDE], pairsDF[LATITUDE], pairsDF[LONGITUDE]) * 1000 / pairsDF[SECONDS])

    # 9. Operation T7: 'filter' to drop the GPS noise, any pair faster than max_speed_accepted (m/s). A speed of exactly the max is still fine
    validDF = speedDF.filter(speedDF[SPEED_MS] <= max_speed_accepted)

    # 10. Operation T8: 'groupBy' vehicle + 'agg' to get the average speed of each vehicle in km/h
    #     edge case: a vehicle with no valid pairs (only one measurement, or all its pairs were GPS noise) has no rows left, so it gets dropped
    avgSpeedDF = validDF.groupBy(VEHICLE_ID).agg(pyspark.sql.functions.avg(validDF[SPEED_MS] * 3.6).alias(AVG_SPEED_KMH))

    # 11. Operation T9: 'withColumn' to work out the bucket of each vehicle
    #     bucket i covers [i * bucket_size_kmh, (i+1) * bucket_size_kmh), so an average of exactly 10.0 with size 5.0 goes in bucket 2, not 1
    bucketDF = avgSpeedDF.withColumn(BUCKET_ID, pyspark.sql.functions.floor(avgSpeedDF[AVG_SPEED_KMH] / bucket_size_kmh).cast("int"))

    # 12. Operation T10: 'groupBy' bucket + 'agg' to count the vehicles in each bucket
    #     edge case: empty buckets never get a group, so they don't show up in the result (which is what we want)
    countDF = bucketDF.groupBy(BUCKET_ID).agg(pyspark.sql.functions.count("*").alias(NUM_VEHICLES))

    # 13. Operation T11: 'withColumn' to build the "lo_hi" range string
    #     casting a double to string gives "5.0" (same as python's str), so the format matches the core version
    rangeDF = countDF.withColumn(BUCKET_RANGE,
                                 pyspark.sql.functions.concat((countDF[BUCKET_ID] * bucket_size_kmh).cast("string"),
                                                              pyspark.sql.functions.lit("_"),
                                                              ((countDF[BUCKET_ID] + 1) * bucket_size_kmh).cast("string")))

    # 14. Operation T12: 'select' to get the (bucket id, range, vehicles) columns + 'orderBy' bucket id
    solutionDF = rangeDF.select(BUCKET_ID, BUCKET_RANGE, NUM_VEHICLES).orderBy(BUCKET_ID)

    # ---------------------------------------

    # Operation A1: 'collect'
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)


# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed

    # 1.1. We get the dataset directory
    my_dataset_dir = "../../FileStore/tables/my_datasets/A01_micro_dataset_7/"
    #my_dataset_dir = "../../FileStore/tables/my_datasets/my_dataset_complete/"

    # 1.2. We get the bucket_size_kmh
    bucket_size_kmh = 5.0

    # 1.3. We get the max_speed_accepted
    max_speed_accepted = 28.0

    # 1.4. We get the day_picked
    day_picked = "2013-01-07"

    # 1.5. We call to the program from terminal
    if (len(sys.argv) > 4):
       my_dataset_dir = sys.argv[1]
       bucket_size_kmh = float(sys.argv[2])
       max_speed_accepted = float(sys.argv[3])
       day_picked = sys.argv[4]

    # 2. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(spark,
            my_dataset_dir,
            bucket_size_kmh,
            max_speed_accepted,
            day_picked
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex7_spark_dataframes.py "../../FileStore/tables/my_datasets/A01_micro_dataset_7/" 5.0 28.0 "2013-01-07" > "../../my_results/Student_Solutions/A01_ex7_spark_dataframes/A01_ex7_spark_dataframes_small_dataset.txt"
# python3 ./A01_ex7_spark_dataframes.py "../../FileStore/tables/my_datasets/my_dataset_complete/" 5.0 28.0 "2013-01-07" > "../../my_results/Student_Solutions/A01_ex7_spark_dataframes/A01_ex7_spark_dataframes_entire_dataset.txt"
