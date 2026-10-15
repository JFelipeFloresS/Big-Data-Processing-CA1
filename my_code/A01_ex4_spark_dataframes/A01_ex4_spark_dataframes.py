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

    # 3. Operation T1: 'select' keep date, bus line, and delay. add is on schedule column based on delay limit
    infoDF = inputDF.select("date", "busLineID", "delay") \
        .withColumn("isOnSchedule", pyspark.sql.functions.when(
            (inputDF["delay"] >= -delay_limit) & (inputDF["delay"] <= delay_limit), 1).otherwise(0))

    # 4. Operation T2: 'filter' to keep only the records that match the specified bus lines and hours only on saturday or sunday
    filteredDF = infoDF.filter(
        (infoDF["busLineID"].isin(line_list)) &
        (infoDF["date"].substr(12, 2).isin(hours_list)) &
        (pyspark.sql.functions.dayofweek(infoDF["date"]).isin([7, 1]))
    )

    # 5. Operation T3: 'aggregation' to calculate the percentage of buses on schedule
    solutionDF = filteredDF.groupBy("busLineID").agg(
        (pyspark.sql.functions.sum("isOnSchedule") / pyspark.sql.functions.count("*") * 100).alias("percentage"),
        pyspark.sql.functions.count("*").alias("total"),
        pyspark.sql.functions.sum("isOnSchedule").alias("onTime")
    )

    # 6. Operation T4: 'select' to format the output as (Bus_Line, Percentage on schedule, Total number o measurements of the bus line, Total number of measurement of the bus line)
    solutionDF = solutionDF.select(
        "busLineID",
        pyspark.sql.functions.round("percentage", 2).alias("percentage"),
        "total",
        "onTime"
    ).orderBy("busLineID")

    # 7. Operation T5: 'orderBy' to sort the results by percentage in descending order. and then by busLineID in ascending order
    solutionDF = solutionDF.orderBy(pyspark.sql.functions.desc("percentage"), "busLineID")

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
