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
import time
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
            bus_line_id,
            day_picked,
            gap_limit_seconds
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map' to convert each line into a tuple of parameters
    tupleRDD = inputRDD.map(process_line)

    # 3. Operation T2: 'filter' to keep only the tuples with the specified bus_line_id and day_picked
    filteredRDD = tupleRDD.filter(lambda x: x[BUS_LINE] == bus_line_id and x[DATE].startswith(day_picked))

    # 4. Operation T3: 'sortBy' to have consecutive records for the same vehicle
    sortedRDD = filteredRDD.sortBy(lambda x: (x[VEHICLE], x[DATE]))

    PREVIOUS_DATE = AT_STOP + 1  # New column to store the previous date for each vehicle
    GHOST_GAP = PREVIOUS_DATE + 1  # New column to indicate ghost gaps

    # 5. Operation T4: 'map' to calculate the time difference between consecutive records for the same vehicle
    def calculate_time_diff(records):
        """
        :param records:
        :return: records with the additional information of the previous date and the time difference between consecutive records for the same vehicle
        """
        previous_record = None
        for record in records:
            # when looping through all records we can't assume that the previous record is from the same vehicle, so we need to check if the vehicle has changed
            if previous_record is not None and record[VEHICLE] != previous_record[VEHICLE]:
                previous_record = None

            if previous_record is not None:
                previous_date = datetime.datetime.strptime(previous_record[DATE], "%Y-%m-%d %H:%M:%S")
                current_date = datetime.datetime.strptime(record[DATE], "%Y-%m-%d %H:%M:%S")
                time_diff = int((current_date - previous_date).total_seconds())
                yield record + (previous_record[DATE], time_diff)
            else:
                yield record + (None, 0)  # For the first record of each vehicle, we set the previous date to None and time difference to 0
            previous_record = record


    infoRDD = sortedRDD.mapPartitions(calculate_time_diff)

    # 6. Operation T5: 'filter' to keep only the records where the time difference exceeds the gap_limit_seconds
    ghost_gapRDD = infoRDD.filter(lambda x: x[GHOST_GAP] > gap_limit_seconds)

    # 7. Operation T6: 'sortBy' to sort the final RDD by the starting time of the ghost gap
    solutionRDD = ghost_gapRDD.sortBy(lambda x: x[PREVIOUS_DATE])

    # 8. Operation T7: 'map' to format the output as (Bus_Line, Vehicle, Previous_Date, Current_Date, Time_Difference)
    solutionRDD = solutionRDD.map(lambda x: (x[VEHICLE], x[PREVIOUS_DATE], x[DATE], x[GHOST_GAP]))

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
