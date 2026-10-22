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
IS_ON_SCHEDULE = "isOnSchedule" # pyspark.sql.types.IntegerType
PERCENTAGE = "percentage" # pyspark.sql.types.DoubleType
TOTAL = "total" # pyspark.sql.types.LongType
ON_TIME = "onTime" # pyspark.sql.types.LongType

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            line_list,
            hours_list,
            delay_limit
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

    # 3. Operation T1: 'filter' to keep only the rows of the lines we want, in the hours we want, on a saturday or sunday
    #    the date looks like "2013-01-05 08:10:00", so the hour is always 2 characters starting at position 12 (substr counts from 1)
    #    dayofweek gives 1 for sunday up to 7 for saturday, so 1 and 7 are the weekend
    filteredDF = inputDF.filter((inputDF[BUS_LINE_ID].isin(line_list)) &
                                (inputDF[DATE].substr(12, 2).isin(hours_list)) &
                                (pyspark.sql.functions.dayofweek(inputDF[DATE]).isin([1, 7])))

    # 4. Operation T2: 'withColumn' to flag each row as on schedule (1) or not (0)
    #    both ends count as on schedule (e.g. with a limit of 60, a delay of exactly 60 or -60 is still fine)
    flaggedDF = filteredDF.withColumn(IS_ON_SCHEDULE,
                                      pyspark.sql.functions.when((filteredDF[DELAY] >= (-1) * delay_limit) &
                                                                 (filteredDF[DELAY] <= delay_limit), 1).otherwise(0))

    # 5. Operation T3: 'groupBy' bus line + 'agg' to count the total and on time rows per line
    #    edge case: a line from line_list with no rows never gets a group, so it won't show up in the result (which is what we want)
    countsDF = flaggedDF.groupBy(BUS_LINE_ID).agg(pyspark.sql.functions.count("*").alias(TOTAL),
                                                  pyspark.sql.functions.sum(IS_ON_SCHEDULE).alias(ON_TIME))

    # 6. Operation T4: 'select' to work out the percentage (rounded to 2 decimals) and get the (line, percentage, total, on time) format
    #    total is always at least 1 here, so no division by zero
    formattedDF = countsDF.select(BUS_LINE_ID,
                                  pyspark.sql.functions.round(countsDF[ON_TIME] / countsDF[TOTAL] * 100, 2).alias(PERCENTAGE),
                                  TOTAL,
                                  ON_TIME)

    # 7. Operation T5: 'orderBy' percentage (biggest first), and on a tie by bus line (smallest first)
    #    we sort on the rounded percentage, as that's the value we actually print
    solutionDF = formattedDF.orderBy(pyspark.sql.functions.desc(PERCENTAGE), pyspark.sql.functions.asc(BUS_LINE_ID))

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
    my_dataset_dir = "../../FileStore/tables/my_datasets/A01_micro_dataset_4/"
    #my_dataset_dir = "../../FileStore/tables/my_datasets/my_dataset_complete/"

    # 1.2. We get the line_list
    line_list = [40, 72]

    # 1.3. We get the hours_list
    hours_list = ["08", "09"]

    # 1.4. We get the delay_limit
    delay_limit = 60

    # 1.5. We call to the program from terminal
    if (len(sys.argv) > 4):
       my_dataset_dir = sys.argv[1]
       line_list = [int(x) for x in sys.argv[2].split(",")]
       hours_list = sys.argv[3].split(",")
       delay_limit = int(sys.argv[4])

    # 2. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(spark,
            my_dataset_dir,
            line_list,
            hours_list,
            delay_limit
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex4_spark_dataframes.py "../../FileStore/tables/my_datasets/A01_micro_dataset_4/" "40,72" "08,09" 60 > "../../my_results/Student_Solutions/A01_ex4_spark_dataframes/A01_ex4_spark_dataframes_small_dataset.txt"
# python3 ./A01_ex4_spark_dataframes.py "../../FileStore/tables/my_datasets/my_dataset_complete/" "40,72" "08,09" 60 > "../../my_results/Student_Solutions/A01_ex4_spark_dataframes/A01_ex4_spark_dataframes_entire_dataset.txt"
