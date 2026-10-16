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
import sys

# ------------------------------------------
# CONSTANTS
# ------------------------------------------
DATE = 0 # (00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
BUS_LINE = 1 # (01) Bus_Line => The bus line. Int (e.g., 120).
BUS_LINE_PATTERN = 2 # (02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
CONGESTION = 3 # (03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
LONGITUDE = 4 # (04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
LATITUDE = 5 # (05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
DELAY = 6 # (06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
VEHICLE = 7 # (07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
CLOSER_STOP = 8 # (08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
AT_STOP = 9 # (09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    #(00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
    #(01) Bus_Line => The bus line. Int (e.g., 120).
    #(02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
    #(03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
    #(04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
    #(05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
    #(06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
    #(07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
    #(08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
    #(09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

    # 3. If the list contains the right amount of parameters
    if (len(params_list) == 10):
        # 3.1. We set the right type for the parameters
        params_list[BUS_LINE] = int(params_list[BUS_LINE])
        params_list[CONGESTION] = int(params_list[CONGESTION])
        params_list[LONGITUDE] = float(params_list[LONGITUDE])
        params_list[LATITUDE] = float(params_list[LATITUDE])
        params_list[DELAY] = int(params_list[DELAY])
        params_list[VEHICLE] = int(params_list[VEHICLE])
        params_list[CLOSER_STOP] = int(params_list[CLOSER_STOP])
        params_list[AT_STOP] = int(params_list[AT_STOP])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            bus_line_id
           ):

    # 1. Operation C1: We apply 'textFile' to produce a new RDD (e.g., inputRDD) containing the entire dataset.
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: We apply 'map' to produce a new RDD (e.g., formattedRDD) where each item of the original inputRDD is displayed as a tuple
    formattedRDD = inputRDD.map(process_line)

    # 3. Operation T2: We apply 'filter' to produce a new RDD (e.g., filteredRDD) containing only the items of the bus line 'bus_line_id' (field "Bus_Line") whose field "Bus_Line_Pattern" is not the empty string "". Measurements of other lines, or with an empty pattern, must be discarded.
    filteredRDD = formattedRDD.filter(lambda x: x[BUS_LINE] == bus_line_id and x[BUS_LINE_PATTERN] != "")

    # 4. Operation T3: We apply 'map' to produce a new RDD (e.g., infoRDD) keeping only the field "Bus_Line_Pattern" from filteredRDD.
    infoRDD = filteredRDD.map(lambda x: x[BUS_LINE_PATTERN])

    # 5. Operation T4: We apply 'distinct' to produce a new RDD (e.g., distinctRDD) with only as many items as different values for "Bus_Line_Pattern" were in infoRDD.
    distinctRDD = infoRDD.distinct()

    # 6. Operation T5: We apply 'sortBy' to produce a new RDD (e.g., solutionRDD) with the items of distinctRDD sorted in increasing (string) order.
    solutionRDD = distinctRDD.sortBy(lambda x: x)

    # 7. Operation A1: We apply 'collect' to collect the items with the result.
    resVAL = solutionRDD.collect()
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

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(sc,
            my_dataset_dir,
            bus_line_id
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex2_spark_core.py "../../FileStore/tables/my_datasets/A01_micro_dataset_2/" 40 > "../../my_results/Student_Solutions/A01_ex2_spark_core/A01_ex2_spark_core_small_dataset.txt"
# python3 ./A01_ex2_spark_core.py "../../FileStore/tables/my_datasets/my_dataset_complete/" 40 > "../../my_results/Student_Solutions/A01_ex2_spark_core/A01_ex2_spark_core_entire_dataset.txt"
