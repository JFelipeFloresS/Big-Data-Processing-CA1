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
# FUNCTION get_ghost_gaps
# ------------------------------------------
def get_ghost_gaps(vehicle, dates, gap_limit_seconds):
    # 1. We create the output variable
    res = []

    # 2. We sort the dates. They all have the same "yyyy-mm-dd hh:mm:ss" format, so sorting the strings sorts them by time
    dates = sorted(dates)

    # 3. We compare each date with the next one
    for start, end in zip(dates, dates[1:]):
        gap_seconds = int((datetime.datetime.strptime(end, DATE_FORMAT) - datetime.datetime.strptime(start, DATE_FORMAT)).total_seconds())

        # 3.1. It only counts as a ghost gap if it's strictly bigger than the limit (a gap of exactly gap_limit_seconds is fine)
        #      edge case: two measurements with the same time give a gap of 0, so they're never reported
        if gap_seconds > gap_limit_seconds:
            res.append((vehicle, start, end, gap_seconds))

    # 4. We return res
    return res


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            bus_line_id,
            day_picked,
            gap_limit_seconds
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map' to turn each line into a tuple with the right types
    tupleRDD = inputRDD.map(process_line)

    # 3. Operation T2: 'filter' to keep only the measurements of bus_line_id on day_picked
    #    the date always starts with "yyyy-mm-dd", so startswith is enough to check the day
    #    edge case: process_line gives back an empty tuple for lines that don't have 10 fields (e.g. a blank line), so we skip those first
    filteredRDD = tupleRDD.filter(lambda x: len(x) == 10 and x[BUS_LINE] == bus_line_id and x[DATE].startswith(day_picked))

    # 4. Operation T3: 'map' to (vehicle, date), that's all we need to find the gaps
    vehicleRDD = filteredRDD.map(lambda x: (x[VEHICLE], x[DATE]))

    # 5. Operation T4: 'groupByKey' to get all the dates of each vehicle together
    #    we can't just sort the whole RDD and walk through it, because the measurements of one vehicle could end up split
    #    across two partitions, and then we'd miss the gap right at the split. grouping makes sure each vehicle is in one place
    groupedRDD = vehicleRDD.groupByKey()

    # 6. Operation T5: 'flatMap' to sort each vehicle's dates and turn every gap bigger than gap_limit_seconds into a (vehicle, start, end, seconds) item
    #    edge case: a vehicle with a single measurement has no pairs, so it gives no gaps and doesn't show up
    gapRDD = groupedRDD.flatMap(lambda x: get_ghost_gaps(x[0], x[1], gap_limit_seconds))

    # 7. Operation T6: 'sortBy' gap start time
    #    edge case: if two gaps start at the exact same time we use the vehicle as a tie breaker, so the output is always in the same order
    solutionRDD = gapRDD.sortBy(lambda x: (x[1], x[0]))

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

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(sc,
            my_dataset_dir,
            bus_line_id,
            day_picked,
            gap_limit_seconds
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex5_spark_core.py "../../FileStore/tables/my_datasets/A01_micro_dataset_5/" 40 "2013-01-07" 600 > "../../my_results/Student_Solutions/A01_ex5_spark_core/A01_ex5_spark_core_small_dataset.txt"
# python3 ./A01_ex5_spark_core.py "../../FileStore/tables/my_datasets/my_dataset_complete/" 40 "2013-01-07" 600 > "../../my_results/Student_Solutions/A01_ex5_spark_core/A01_ex5_spark_core_entire_dataset.txt"
