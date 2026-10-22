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
# FUNCTION get_hour
# ------------------------------------------
def get_hour(date):
    # the date looks like "2013-01-05 08:10:00", so the hour is always characters 11 and 12 (e.g. "08")
    return date[11:13]


# ------------------------------------------
# FUNCTION is_weekend
# ------------------------------------------
def is_weekend(date):
    # weekday() gives 0 for monday up to 6 for sunday, so 5 and 6 are saturday and sunday
    return datetime.datetime.strptime(date, DATE_FORMAT).weekday() in [5, 6]


# ------------------------------------------
# FUNCTION is_on_schedule
# ------------------------------------------
def is_on_schedule(delay, delay_limit):
    # both ends count as on schedule (e.g. with a limit of 60, a delay of exactly 60 or -60 is still fine)
    return (-1) * delay_limit <= delay <= delay_limit


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            line_list,
            hours_list,
            delay_limit
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map' to turn each line into a tuple with the right types
    tupleRDD = inputRDD.map(process_line)

    # 3. Operation T2: 'filter' to keep only the measurements of the lines we want, in the hours we want, on a saturday or sunday
    #    edge case: process_line gives back an empty tuple for lines that don't have 10 fields (e.g. a blank line), so we skip those first
    filteredRDD = tupleRDD.filter(lambda x: len(x) == 10 and
                                            x[BUS_LINE] in line_list and
                                            get_hour(x[DATE]) in hours_list and
                                            is_weekend(x[DATE]))

    # 4. Operation T3: 'map' to (bus line, (on schedule, 1)) so we can count both things in one go
    #    on schedule is 1 if the delay is within [-delay_limit, delay_limit] (both ends included) and 0 otherwise
    countRDD = filteredRDD.map(lambda x: (x[BUS_LINE], (1 if is_on_schedule(x[DELAY], delay_limit) else 0, 1)))

    # 5. Operation T4: 'reduceByKey' to add up the on schedule and total counts per bus line
    #    edge case: a line from line_list with no measurements never gets here, so it won't show up in the result (which is what we want)
    reducedRDD = countRDD.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # 6. Operation T5: 'map' to work out the percentage and get the (line, percentage, total, on time) format
    #    total is always at least 1 here, so no division by zero
    formattedRDD = reducedRDD.map(lambda x: (x[0], round((x[1][0] / x[1][1]) * 100, 2), x[1][1], x[1][0]))

    # 7. Operation T6: 'sortBy' percentage (biggest first), and on a tie by bus line (smallest first)
    #    we sort on the rounded percentage, as that's the value we actually print
    solutionRDD = formattedRDD.sortBy(lambda x: (-x[1], x[0]))

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

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(sc,
            my_dataset_dir,
            line_list,
            hours_list,
            delay_limit
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex4_spark_core.py "../../FileStore/tables/my_datasets/A01_micro_dataset_4/" "40,72" "08,09" 60 > "../../my_results/Student_Solutions/A01_ex4_spark_core/A01_ex4_spark_core_small_dataset.txt"
# python3 ./A01_ex4_spark_core.py "../../FileStore/tables/my_datasets/my_dataset_complete/" "40,72" "08,09" 60 > "../../my_results/Student_Solutions/A01_ex4_spark_core/A01_ex4_spark_core_entire_dataset.txt"
