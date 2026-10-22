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
import datetime


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

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

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
        params_list[1] = int(params_list[1])
        params_list[3] = int(params_list[3])
        params_list[4] = float(params_list[4])
        params_list[5] = float(params_list[5])
        params_list[6] = int(params_list[6])
        params_list[7] = int(params_list[7])
        params_list[8] = int(params_list[8])
        params_list[9] = int(params_list[9])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION keep_most_stops
# ------------------------------------------
def keep_most_stops(a, b):
    # a and b are lists of (bus line, stops). Inside each list all lines have the same number of stops,
    # so we only need to look at the first one of each to compare them
    a_size = len(a[0][1])
    b_size = len(b[0][1])

    # 1. If one side has more stops it wins
    if a_size > b_size:
        return a
    if b_size > a_size:
        return b

    # 2. If it's a tie we keep both
    return a + b


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            current_time,
            seconds_horizon
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map' to turn each line into a tuple with the right types
    tupleRDD = inputRDD.map(process_line)

    # 3. Operation T2: 'filter' to keep only the measurements where the bus is stopped (At_Stop == 1) inside the time window
    #    the window is [start, end), so a measurement at exactly the end time doesn't count
    #    we work out the end time once here instead of for every row. The dates all have the same "yyyy-mm-dd hh:mm:ss" format,
    #    so we can compare them as plain strings and skip parsing every row
    #    edge case: the window can cross midnight (e.g. 23:50 + 1800 seconds), timedelta takes care of moving to the next day
    #    edge case: process_line gives back an empty tuple for lines that don't have 10 fields (e.g. a blank line), so we skip those first
    end_time = (datetime.datetime.strptime(current_time, DATE_FORMAT) + datetime.timedelta(seconds=seconds_horizon)).strftime(DATE_FORMAT)
    filteredRDD = tupleRDD.filter(lambda x: len(x) == 10 and x[AT_STOP] == 1 and current_time <= x[DATE] < end_time)

    # 4. Operation T3: 'map' to (bus line, stop)
    pairRDD = filteredRDD.map(lambda x: (x[BUS_LINE], x[CLOSER_STOP]))

    # 5. Operation T4: 'distinct' so a line stopping more than once at the same stop only counts it once
    distinctRDD = pairRDD.distinct()

    # 6. Operation T5: 'groupByKey' + 'mapValues' to get the sorted list of stops of each bus line
    stopsRDD = distinctRDD.groupByKey().mapValues(lambda x: sorted(x))

    # 7. Operation T6: 'map' to put every bus line under the same key ("ALL"), so we can find the max inside Spark without an extra action
    allRDD = stopsRDD.map(lambda x: ("ALL", [x]))

    # 8. Operation T7: 'reduceByKey' to keep only the bus line(s) with the most stops
    #    edge case: if two or more lines tie on the max, keep_most_stops keeps all of them
    bestRDD = allRDD.reduceByKey(keep_most_stops)

    # 9. Operation T8: 'flatMap' to drop the "ALL" key and get one item per bus line
    bestLinesRDD = bestRDD.flatMap(lambda x: x[1])

    # 10. Operation T9: 'sortBy' bus line, so ties come out in increasing order
    solutionRDD = bestLinesRDD.sortBy(lambda x: x[0])

    # ---------------------------------------

    # Operation A1: 'collect'
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

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(sc,
            my_dataset_dir,
            current_time,
            seconds_horizon
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex6_spark_core.py "../../FileStore/tables/my_datasets/A01_micro_dataset_6/" "2013-01-09 06:00:00" 1800 > "../../my_results/Student_Solutions/A01_ex6_spark_core/A01_ex6_spark_core_small_dataset.txt"
# python3 ./A01_ex6_spark_core.py "../../FileStore/tables/my_datasets/my_dataset_complete/" "2013-01-09 06:00:00" 1800 > "../../my_results/Student_Solutions/A01_ex6_spark_core/A01_ex6_spark_core_entire_dataset.txt"
