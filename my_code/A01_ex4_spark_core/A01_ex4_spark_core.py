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
            line_list,
            hours_list,
            delay_limit
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map'
    processedRDD = inputRDD.map(process_line)

    # 3. Operation T2: 'filter' to keep only the records that match the specified bus lines and hours only on saturday or sunday
    filteredRDD = processedRDD.filter(lambda x: x[BUS_LINE] in line_list and
                                      x[DATE].split()[1][:2] in hours_list and
                                      datetime.datetime.strptime(x[DATE], "%Y-%m-%d %H:%M:%S").weekday() in [5, 6])

    # 4. Operation T3: 'map' to create a new RDD with the percentage of buses on schedule for each bus line and hour
    def is_bus_on_schedule(delay, delay_limit):
        return (-1) * delay_limit <= delay <= delay_limit

    def map_to_percentage(x):
        on_schedule = 1 if is_bus_on_schedule(x[DELAY], delay_limit) else 0
        return (x[BUS_LINE], (on_schedule, 1))

    mappedRDD = filteredRDD.map(map_to_percentage)

    # 5. Operation T4: 'reduceByKey' to sum the on_schedule counts and total counts for each bus line and hour
    reducedRDD = mappedRDD.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # 6. Operation T6: 'map' to calculate the percentage of buses on schedule and format the output as (Bus_Line, Percentage on schedule, Total number o measurements of the bus line, Total number of measurement of the bus line)
    def format_output(x):
        bus_line, (on_schedule, total) = x
        percentage = round((on_schedule / total) * 100, 2)
        return (bus_line, percentage, total, on_schedule)

    solutionRDD = reducedRDD.map(format_output)

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
