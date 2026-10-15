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
            bus_line_id
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

    # 2. Operation C1: We apply 'read' to produce a new DF (e.g., inputDF) containing the entire dataset.
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation T1: We apply 'filter' to obtain a new DF (e.g., filteredDF) containing only the Rows of the bus line 'bus_line_id' (field "busLineID") whose field "busLinePatternID" is not the empty string "". Rows of other lines, or with an empty pattern, must be discarded.
    filteredDF = inputDF.filter((inputDF["busLineID"] == bus_line_id) & (inputDF["busLinePatternID"] != ""))

    # 4. Operation T2: We apply 'select' to obtain a new DF (e.g., infoDF) keeping only the field "busLinePatternID" from filteredDF.
    infoDF = filteredDF.select("busLinePatternID")

    # 5. Operation T3: We apply 'distinct' to obtain a new DF (e.g., distinctDF) with only as many Rows as different values for "busLinePatternID" were in infoDF.
    distinctDF = infoDF.distinct()

    # 6. Operation T4: We apply 'orderBy' to obtain a new DF (e.g., solutionDF) with the Rows of distinctDF sorted by "busLinePatternID" in increasing (string) order.
    solutionDF = distinctDF.orderBy("busLinePatternID")

    # 7. Operation A1: We apply 'collect' to collect the items with the result.
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
    my_dataset_dir = "../../FileStore/tables/my_datasets/A01_micro_dataset_2/"
    #my_dataset_dir = "../../FileStore/tables/my_datasets/my_dataset_complete/"

    # 1.2. We get the bus_line_id
    bus_line_id = 40

    # 1.3. We call to the program from terminal
    if (len(sys.argv) > 2):
       my_dataset_dir = sys.argv[1]
       bus_line_id = int(sys.argv[2])

    # 2. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(spark,
            my_dataset_dir,
            bus_line_id
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex2_spark_dataframes.py "../../FileStore/tables/my_datasets/A01_micro_dataset_2/" 40 > "../../my_results/Student_Solutions/A01_ex2_spark_dataframes/A01_ex2_spark_dataframes_small_dataset.txt"
# python3 ./A01_ex2_spark_dataframes.py "../../FileStore/tables/my_datasets/my_dataset_complete/" 40 > "../../my_results/Student_Solutions/A01_ex2_spark_dataframes/A01_ex2_spark_dataframes_entire_dataset.txt"
