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
import math
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
            bucket_size_kmh,
            max_speed_accepted,
            day_picked
           ):

    # convert max speed accepted from m/s to km/h
    max_speed_accepted = max_speed_accepted * 3.6

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map' + 'process_line'
    tupleRDD = inputRDD.map(process_line)

    # 3. Operation T2: 'filter' + 'lambda' selecting only the lines with the right day
    filteredRDD = tupleRDD.filter(lambda x: x[DATE].startswith(day_picked))

    # new column constants for the distance and speed
    DISTANCE = AT_STOP + 1
    SPEED = DISTANCE + 1
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    EARTH_RADIUS = 6371.0

    def hav(theta):
        return math.sin(theta / 2) ** 2

    def calculate_central_angle(lat1, lon1, lat2, lon2):
        # Convert degrees to radians
        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)

        # Haversine formula: hav(central_angle) = hav(lat2 - lat1) + (cos(lat1) * cos(lat2) * hav(lon2 - lon1))
        h = hav(lat2_rad - lat1_rad) + (math.cos(lat1_rad) * math.cos(lat2_rad) * hav(lon2_rad - lon1_rad))
        # invert the haversine to get the central angle: central_angle = 2 * arcsin(sqrt(h))
        return 2 * math.asin(math.sqrt(h))

    # 5. Operation T4: 'map' + 'lambda' to compute the distance travelled from last measurement and the speed in km/h, and then bucketize the speed
    def calculate_speeds_for_vehicle(record):
        vehicle, records = record
        records = sorted(records, key=lambda x: x[DATE])

        # compare each record with the previous one to calculate distance and speed for each vehicle
        for prev, curr in zip(records, records[1:]):
            # We calculate the distance using the Haversine formula
            # formula: distance = radius_of_earth * central_angle_between_two_points
            # hav(central_angle) = sin^2(central_angle / 2)
            # central_angle = 2 * arcsin(sqrt(hav(lat2 - lat1) + (cos(lat1) * cos(lat2) * hav(lon2 - lon1))))
            r = EARTH_RADIUS # Radius of earth in kilometers
            central_angle = calculate_central_angle(prev[LATITUDE], prev[LONGITUDE], curr[LATITUDE], curr[LONGITUDE])
            distance = central_angle * r

            # We calculate the time difference in hours
            time_diff = (datetime.datetime.strptime(curr[DATE], DATE_FORMAT) - datetime.datetime.strptime(prev[DATE], DATE_FORMAT)).total_seconds() / 3600.0

            # We calculate the speed in km/h
            if time_diff > 0:
                yield vehicle, distance / time_diff

    vehicleRDD = filteredRDD.map(lambda x: (x[VEHICLE], x)).groupByKey()

    filtered_speed_RDD = vehicleRDD.flatMap(calculate_speeds_for_vehicle)

    valid_speed_RDD = filtered_speed_RDD.filter(lambda x: x[1] <= max_speed_accepted)

    def get_speed_bucket_id(speed):
        return math.floor(speed / bucket_size_kmh)

    def get_speed_bucket(bucket_id):
        bucket_start = bucket_id * bucket_size_kmh
        bucket_end = bucket_start + bucket_size_kmh
        return f"{bucket_start}_{bucket_end}"

    average_speed_RDD = valid_speed_RDD.groupByKey().mapValues(lambda speeds: sum(speeds) / len(speeds))

    bucket_count_RDD = (average_speed_RDD
                        .map(lambda x: (get_speed_bucket_id(x[1]), 1))
                        .reduceByKey(lambda a, b: a + b))

    solutionRDD = (bucket_count_RDD
                   .sortByKey()
                   .map(lambda x: (x[0], get_speed_bucket(x[0]), x[1])))

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
    my_dataset_dir = "../../FileStore/tables/my_datasets/A01_micro_dataset_7/"
    #my_dataset_dir = "../../FileStore/tables/my_datasets/my_dataset_complete/"

    # 1.2. We get the bucket_size_kmh
    bucket_size_kmh = 5.0

    # 1.3. We get the max_speed_accepted
    max_speed_accepted = 28.0

    # 1.4. We get the day_picked
    day_picked = "2013-01-07"

    # 1.5. We call to the program from terminal
    if (len(sys.argv) > 4):
       my_dataset_dir = sys.argv[1]
       bucket_size_kmh = float(sys.argv[2])
       max_speed_accepted = float(sys.argv[3])
       day_picked = sys.argv[4]

    # 2. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')

    # 3. We call to our main function
    my_main(sc,
            my_dataset_dir,
            bucket_size_kmh,
            max_speed_accepted,
            day_picked
           )

    print("\n\n\n--- Completed ---")


# python3 ./A01_ex7_spark_core.py "../../FileStore/tables/my_datasets/A01_micro_dataset_7/" 5.0 28.0 "2013-01-07" > "../../my_results/Student_Solutions/A01_ex7_spark_core/A01_ex7_spark_core_small_dataset.txt"
# python3 ./A01_ex7_spark_core.py "../../FileStore/tables/my_datasets/my_dataset_complete/" 5.0 28.0 "2013-01-07" > "../../my_results/Student_Solutions/A01_ex7_spark_core/A01_ex7_spark_core_entire_dataset.txt"
