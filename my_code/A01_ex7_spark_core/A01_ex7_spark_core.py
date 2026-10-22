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
EARTH_RADIUS_KM = 6371.0

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
# FUNCTION haversine_km
# ------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    # 1. We convert the degrees to radians
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # 2. Haversine formula: h = sin^2((lat2 - lat1) / 2) + cos(lat1) * cos(lat2) * sin^2((lon2 - lon1) / 2)
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2

    # 3. We turn h into the angle between the 2 points (2 * arcsin(sqrt(h))) and multiply it by the radius of the earth
    #    edge case: h can go a tiny bit over 1 because of rounding, which would crash asin, so we cap it at 1
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, h)))


# ------------------------------------------
# FUNCTION get_average_speed
# ------------------------------------------
def get_average_speed(vehicle, measurements, max_speed_accepted):
    # 1. We create the output variable
    res = []

    # 2. We sort the measurements by date. They all have the same "yyyy-mm-dd hh:mm:ss" format, so sorting the strings sorts them by time
    measurements = sorted(measurements)

    # 3. We work out the speed of each pair of consecutive measurements
    speeds_kmh = []
    for (date1, lat1, lon1), (date2, lat2, lon2) in zip(measurements, measurements[1:]):
        seconds = (datetime.datetime.strptime(date2, DATE_FORMAT) - datetime.datetime.strptime(date1, DATE_FORMAT)).total_seconds()

        # 3.1. edge case: two measurements with the same time would mean dividing by 0, so we can't get a speed out of them and we skip the pair
        if seconds > 0:
            speed_ms = haversine_km(lat1, lon1, lat2, lon2) * 1000 / seconds

            # 3.2. If the speed is above max_speed_accepted (m/s) it's GPS noise, so we skip the pair. A speed of exactly the max is still fine
            if speed_ms <= max_speed_accepted:
                speeds_kmh.append(speed_ms * 3.6)

    # 4. If the vehicle has at least one valid pair we return its average speed
    if len(speeds_kmh) > 0:
        res.append((vehicle, sum(speeds_kmh) / len(speeds_kmh)))

    # 5. We return res
    return res


# ------------------------------------------
# FUNCTION get_bucket_range
# ------------------------------------------
def get_bucket_range(bucket_id, bucket_size_kmh):
    # we use (i+1) * size for the end (instead of start + size) so it matches the [i * size, (i+1) * size) definition exactly
    return str(bucket_id * bucket_size_kmh) + "_" + str((bucket_id + 1) * bucket_size_kmh)


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            bucket_size_kmh,
            max_speed_accepted,
            day_picked
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # 2. Operation T1: 'map' to turn each line into a tuple with the right types
    tupleRDD = inputRDD.map(process_line)

    # 3. Operation T2: 'filter' to keep only the measurements of day_picked
    #    the date always starts with "yyyy-mm-dd", so startswith is enough to check the day
    #    edge case: process_line gives back an empty tuple for lines that don't have 10 fields (e.g. a blank line), so we skip those first
    filteredRDD = tupleRDD.filter(lambda x: len(x) == 10 and x[DATE].startswith(day_picked))

    # 4. Operation T3: 'map' to (vehicle, (date, latitude, longitude)), that's all we need to work out the speeds
    vehicleRDD = filteredRDD.map(lambda x: (x[VEHICLE], (x[DATE], x[LATITUDE], x[LONGITUDE])))

    # 5. Operation T4: 'groupByKey' to get all the measurements of each vehicle together
    #    we need them in one place so we can sort them by time and compare each one with the next
    groupedRDD = vehicleRDD.groupByKey()

    # 6. Operation T5: 'flatMap' to get the average speed (km/h) of each vehicle using only its valid pairs
    #    edge case: a vehicle with no valid pairs (only one measurement, or all its pairs were GPS noise) gives nothing back, so it gets dropped
    avgSpeedRDD = groupedRDD.flatMap(lambda x: get_average_speed(x[0], x[1], max_speed_accepted))

    # 7. Operation T6: 'map' to (bucket id, 1)
    #    bucket i covers [i * bucket_size_kmh, (i+1) * bucket_size_kmh), so an average of exactly 10.0 with size 5.0 goes in bucket 2, not 1
    bucketRDD = avgSpeedRDD.map(lambda x: (int(math.floor(x[1] / bucket_size_kmh)), 1))

    # 8. Operation T7: 'reduceByKey' to count the vehicles in each bucket
    #    edge case: empty buckets never get a key, so they don't show up in the result (which is what we want)
    countRDD = bucketRDD.reduceByKey(lambda a, b: a + b)

    # 9. Operation T8: 'sortByKey' to sort by bucket id
    sortedRDD = countRDD.sortByKey()

    # 10. Operation T9: 'map' to get the (bucket id, "lo_hi", vehicles) format
    solutionRDD = sortedRDD.map(lambda x: (x[0], get_bucket_range(x[0], bucket_size_kmh), x[1]))

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
