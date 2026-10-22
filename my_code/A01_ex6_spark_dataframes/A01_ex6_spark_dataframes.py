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
import datetime

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
STOPS = "stops" # pyspark.sql.types.ArrayType(IntegerType)
NUM_STOPS = "numStops" # pyspark.sql.types.IntegerType
MAX_STOPS = "maxStops" # pyspark.sql.types.IntegerType

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

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

    # 3. Operation T1: 'filter' to keep only the rows where the bus is stopped (atStop == 1) inside the time window
    #    the window is [start, end), so a row at exactly the end time doesn't count
    #    we work out the end time once here instead of for every row. The dates all have the same "yyyy-mm-dd hh:mm:ss" format,
    #    so we can compare them as plain strings and skip parsing every row
    #    edge case: the window can cross midnight (e.g. 23:50 + 1800 seconds), timedelta takes care of moving to the next day
    end_time = (datetime.datetime.strptime(current_time, DATE_FORMAT) + datetime.timedelta(seconds=seconds_horizon)).strftime(DATE_FORMAT)
    filteredDF = inputDF.filter((inputDF[AT_STOP] == 1) &
                                (inputDF[DATE] >= current_time) &
                                (inputDF[DATE] < end_time))

    # 4. Operation T2: 'groupBy' bus line + 'agg' to get the sorted list of different stops of each line
    #    collect_set already gets rid of repeated stops, so a line stopping twice at the same stop only counts it once
    stopsDF = filteredDF.groupBy(BUS_LINE_ID) \
                        .agg(pyspark.sql.functions.array_sort(pyspark.sql.functions.collect_set(CLOSER_STOP_ID)).alias(STOPS))

    # 5. Operation T3: 'withColumn' to add how many stops each line has
    sizeDF = stopsDF.withColumn(NUM_STOPS, pyspark.sql.functions.size(stopsDF[STOPS]))

    # 6. Operation T4: 'agg' to get the max number of stops of any line (a DF with a single row)
    maxDF = sizeDF.agg(pyspark.sql.functions.max(NUM_STOPS).alias(MAX_STOPS))

    # 7. Operation T5: 'join' to keep only the line(s) whose number of stops is that max
    #    doing it with a join keeps it all inside Spark, so we don't need an extra action to get the max first
    #    edge case: if two or more lines tie on the max they all match the join, so we keep all of them
    bestDF = sizeDF.join(maxDF, sizeDF[NUM_STOPS] == maxDF[MAX_STOPS])

    # 8. Operation T6: 'select' to keep only the (line, stops) columns
    selectedDF = bestDF.select(BUS_LINE_ID, STOPS)

    # 9. Operation T7: 'orderBy' bus line, so ties come out in increasing order
    solutionDF = selectedDF.orderBy(BUS_LINE_ID)

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
