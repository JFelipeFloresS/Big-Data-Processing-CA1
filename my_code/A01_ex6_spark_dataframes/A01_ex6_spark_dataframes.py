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
from pyspark.sql.functions import col, unix_timestamp, array_sort

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
            current_time,
            seconds_horizon
           ):

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

    # 3. Operation T1: 'filter' AT_STOP = 1 and  current time within the horizon
    current_time_ts = unix_timestamp(pyspark.sql.functions.lit(current_time), "yyyy-MM-dd HH:mm:ss")
    end_time_ts = current_time_ts + seconds_horizon
    filteredDF = inputDF.filter((col(AT_STOP) == 1) &
                                (unix_timestamp(col(DATE), "yyyy-MM-dd HH:mm:ss") >= current_time_ts) &
                                (unix_timestamp(col(DATE), "yyyy-MM-dd HH:mm:ss") <= end_time_ts))

    # new constant column name for unique bus stops
    UNIQUE_BUS_STOPS = "stops"

    # 4. Operation T2: 'groupBy' bus line and aggregate unique bus stops
    grouppedDF = filteredDF.groupBy(BUS_LINE_ID).agg(pyspark.sql.functions.collect_set(CLOSER_STOP_ID).alias(UNIQUE_BUS_STOPS))

    # 5. Operation T3: 'sort' by number of unique bus stops (descending) and then by bus line id (ascending)
    sortedDF = grouppedDF.orderBy(pyspark.sql.functions.size(col(UNIQUE_BUS_STOPS)).desc(), col(BUS_LINE_ID).asc())

    # 6. Operation T4: 'filter' to keep only bus lines with the max number of unique bus stops
    max_unique_stops = sortedDF.select(pyspark.sql.functions.size(col(UNIQUE_BUS_STOPS)).alias("num_unique_stops")).agg(pyspark.sql.functions.max("num_unique_stops")).collect()[0][0]
    solutionDF = sortedDF.filter(pyspark.sql.functions.size(col(UNIQUE_BUS_STOPS)) == max_unique_stops)

    # 7. Operation T5: 'sort' the bus stops for each bus line
    solutionDF = solutionDF.withColumn("sorted_unique_bus_stops", pyspark.sql.functions.array_sort(col(UNIQUE_BUS_STOPS))).drop(UNIQUE_BUS_STOPS).withColumnRenamed("sorted_unique_bus_stops", UNIQUE_BUS_STOPS)

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
    my_dataset_dir = "../../FileStore/tables/my_datasets/A01_micro_dataset_6/"
    #my_dataset_dir = "../../FileStore/tables/my_datasets/my_dataset_complete/"

    # 1.2. We get the current_time
    current_time = "2013-01-09 06:00:00"

    # 1.3. We get the seconds_horizon
    seconds_horizon = 1800

    # 1.4. We call to the program from terminal
    if (len(sys.argv) > 3):
       my_dataset_dir = sys.argv[1]
       current_time = sys.argv[2]
       seconds_horizon = int(sys.argv[3])

    # 2. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(spark,
            my_dataset_dir,
            current_time,
            seconds_horizon
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex6_spark_dataframes.py "../../FileStore/tables/my_datasets/A01_micro_dataset_6/" "2013-01-09 06:00:00" 1800 > "../../my_results/Student_Solutions/A01_ex6_spark_dataframes/A01_ex6_spark_dataframes_small_dataset.txt"
# python3 ./A01_ex6_spark_dataframes.py "../../FileStore/tables/my_datasets/my_dataset_complete/" "2013-01-09 06:00:00" 1800 > "../../my_results/Student_Solutions/A01_ex6_spark_dataframes/A01_ex6_spark_dataframes_entire_dataset.txt"
