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
GAP_START = "gapStart" # pyspark.sql.types.StringType
GAP_END = "gapEnd" # pyspark.sql.types.StringType
GAP_SECONDS = "gapSeconds" # pyspark.sql.types.LongType

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            bus_line_id,
            day_picked,
            gap_limit_seconds
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

    # 3. Operation T1: 'filter' to keep only the rows of bus_line_id on day_picked
    #    the date always starts with "yyyy-mm-dd", so startswith is enough to check the day
    filteredDF = inputDF.filter((inputDF[BUS_LINE_ID] == bus_line_id) & (inputDF[DATE].startswith(day_picked)))

    # 4. Operation T2: 'withColumn' + 'lag' over a window per vehicle (sorted by date) to bring the previous date of the same vehicle into each row
    #    the window keeps each vehicle separate, so we never compare dates of two different buses
    #    edge case: the first row of each vehicle has no previous date, so lag gives null there
    vehicleWindow = pyspark.sql.Window.partitionBy(VEHICLE_ID).orderBy(DATE)
    lagDF = filteredDF.withColumn(GAP_START, pyspark.sql.functions.lag(DATE).over(vehicleWindow)) \
                      .withColumnRenamed(DATE, GAP_END)

    # 5. Operation T3: 'withColumn' to work out the gap in seconds between the previous date and this one
    #    edge case: when the start is null (first row of a vehicle) the gap is null too
    gapDF = lagDF.withColumn(GAP_SECONDS,
                             pyspark.sql.functions.unix_timestamp(lagDF[GAP_END]) - pyspark.sql.functions.unix_timestamp(lagDF[GAP_START]))

    # 6. Operation T4: 'filter' to keep only the gaps strictly bigger than gap_limit_seconds (a gap of exactly the limit is fine)
    #    null > x is never true, so the first row of each vehicle gets dropped here too
    ghostDF = gapDF.filter(gapDF[GAP_SECONDS] > gap_limit_seconds)

    # 7. Operation T5: 'select' to keep only the (vehicle, start, end, seconds) columns
    selectedDF = ghostDF.select(VEHICLE_ID, GAP_START, GAP_END, GAP_SECONDS)

    # 8. Operation T6: 'orderBy' gap start time
    #    edge case: if two gaps start at the exact same time we use the vehicle as a tie breaker, so the output is always in the same order
    solutionDF = selectedDF.orderBy(GAP_START, VEHICLE_ID)

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
    my_dataset_dir = "../../FileStore/tables/my_datasets/A01_micro_dataset_5/"
    #my_dataset_dir = "../../FileStore/tables/my_datasets/my_dataset_complete/"

    # 1.2. We get the bus_line_id
    bus_line_id = 40

    # 1.3. We get the day_picked
    day_picked = "2013-01-07"

    # 1.4. We get the gap_limit_seconds
    gap_limit_seconds = 600

    # 1.5. We call to the program from terminal
    if (len(sys.argv) > 4):
       my_dataset_dir = sys.argv[1]
       bus_line_id = int(sys.argv[2])
       day_picked = sys.argv[3]
       gap_limit_seconds = int(sys.argv[4])

    # 2. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(spark,
            my_dataset_dir,
            bus_line_id,
            day_picked,
            gap_limit_seconds
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex5_spark_dataframes.py "../../FileStore/tables/my_datasets/A01_micro_dataset_5/" 40 "2013-01-07" 600 > "../../my_results/Student_Solutions/A01_ex5_spark_dataframes/A01_ex5_spark_dataframes_small_dataset.txt"
# python3 ./A01_ex5_spark_dataframes.py "../../FileStore/tables/my_datasets/my_dataset_complete/" 40 "2013-01-07" 600 > "../../my_results/Student_Solutions/A01_ex5_spark_dataframes/A01_ex5_spark_dataframes_entire_dataset.txt"
