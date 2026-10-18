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
import math
from pyspark.sql.functions import col, unix_timestamp, array_sort, lag, avg, udf, count, format_string

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

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            bucket_size_kmh,
            max_speed_accepted,
            day_picked
           ):

    # convert max speed accepted from m/s to km/h
    max_speed_accepted_kmh = max_speed_accepted * 3.6

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField(DATE, pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField(BUS_LINE_ID, pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField(BUS_LINE_PATTERN_ID, pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField(CONGESTION, pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField(LONGITUDE, pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField(LATITUDE, pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField(DELAY, pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField(VEHICLE_ID, pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField(CLOSER_STOP_ID, pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField(AT_STOP, pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C1: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    selectedDF = inputDF.select(DATE, LONGITUDE, LATITUDE, VEHICLE_ID)

    # 3. Operation T1: 'filter' only get the rows of the day picked
    filteredDF = selectedDF.filter(col(DATE).startswith(day_picked))

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    EARTH_RADIUS = 6371.0

    def hav(theta):
        return math.sin(theta / 2) ** 2

    def calculate_central_angle(lat1, lon1, lat2, lon2):
        # Convert degrees to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        # Haversine formula: hav(central_angle) = hav(lat2 - lat1) + (cos(lat1) * cos(lat2) * hav(lon2 - lon1))
        h = hav(lat2_rad - lat1_rad) + (math.cos(lat1_rad) * math.cos(lat2_rad) * hav(lon2_rad - lon1_rad))
        # invert the haversine to get the central angle: central_angle = 2 * arcsin(sqrt(h))
        return 2 * math.asin(math.sqrt(h))

    # 4. Operation T2: 'calculate speed' using the haversine formula to calculate the distance between two points on the Earth
    def calculate_speed(lat1, lon1, lat2, lon2, time_diff_seconds):
        if lat1 is None or time_diff_seconds is None or time_diff_seconds <= 0:
            return None
        central_angle = calculate_central_angle(lat1, lon1, lat2, lon2)
        distance_km = EARTH_RADIUS * central_angle
        speed_kmh = (distance_km / time_diff_seconds) * 3600.
        return speed_kmh

    vehicle_window = pyspark.sql.Window.partitionBy(VEHICLE_ID).orderBy(DATE)

    # calculate the window's speed for each vehicle based on that vehicle's previous row's latitude, longitude, and date
    speedDF = filteredDF.withColumn("prev_latitude", lag(LATITUDE).over(vehicle_window)) \
        .withColumn("prev_longitude", lag(LONGITUDE).over(vehicle_window)) \
        .withColumn("prev_date", lag(DATE).over(vehicle_window)) \
        .withColumn("time_diff_seconds", (unix_timestamp(DATE) - unix_timestamp("prev_date")))

    pairsDF = speedDF.filter(col("time_diff_seconds").isNotNull() & (col("time_diff_seconds") > 0))

    # calculate the speed using the haversine formula
    speedDF = pairsDF.withColumn("speed_kmh", udf(calculate_speed, pyspark.sql.types.FloatType())(col("prev_latitude"), col("prev_longitude"), col(LATITUDE), col(LONGITUDE), col("time_diff_seconds")))

    # 5. Operation T3: 'filter' only get the rows with speed less than or equal to max_speed_accepted_kmh
    filtered_speedDF = speedDF.filter(col("speed_kmh") <= max_speed_accepted_kmh)

    # calculate the average speed for each vehicle
    avg_speedDF = filtered_speedDF.groupBy(VEHICLE_ID).agg(avg("speed_kmh").alias("avg_speed_kmh"))

    # 6. Operation T4: 'bucketize' the average speed into buckets of size bucket_size_kmh in format bucketID, vehicle in bucket count
    bucketizedDF = avg_speedDF.withColumn("bucketID", (col("avg_speed_kmh") / bucket_size_kmh).cast("int")) \
        .groupBy("bucketID").agg(count(VEHICLE_ID).alias("numVehicles"))

    # 7. Operation T5: 'sort' the bucketizedDF by bucketID and format the output as bucketID, bucketSpeedStart_bucketSpeedEnd, numVehicles
    solutionDF = bucketizedDF.withColumn("bucketRange",
                                         format_string("%.1f_%.1f",
                                                   (col("bucketID") * bucket_size_kmh),
                                                   ((col("bucketID") + 1) * bucket_size_kmh))) \
        .select("bucketID", "bucketRange", "numVehicles") \
        .orderBy("bucketID")


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
