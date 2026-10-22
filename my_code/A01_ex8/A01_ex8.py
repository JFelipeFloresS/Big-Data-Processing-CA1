# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# A01_ex8.py
#
# Exercise 8: novel use case over a novel dataset.
#
# Dataset: dublinbikes station status data (Dublin City Council, GBFS format)
#          https://data.gov.ie/en_GB/dataset/dublinbikes-api
#
# Use case: optimal stations to pick up and drop off a bike.
# Take into account the time of day, the day of the week, and the average speed of a cyclist.
# Display the 5 closest stations to the origin and destination along with the expected number of available bikes and docks at the time of arrival.
#
# Technology: Spark DataFrames (see the report in my_report for the
# justification).
#
# --------------------------------------------------------

import sys

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import types as T
import datetime

# ------------------------------------------
# CONSTANTS
# ------------------------------------------
# (00) system_id           => The bike-share system. String (e.g., "dublin_bikes").
# (01) last_reported       => Time of the snapshot. String <%Y-%m-%d %H:%M:%S> (e.g., "2025-01-01 00:05:00").
# (02) station_id          => The station identifier. Int (e.g., 10).
# (03) num_bikes_available => Bikes available to rent. Int (e.g., 9).
# (04) num_docks_available => Free docks to return a bike. Int (e.g., 7).
# (05) is_installed        => Whether the station is installed. Boolean (e.g., true).
# (06) is_renting          => Whether the station allows renting. Boolean (e.g., true).
# (07) is_returning        => Whether the station allows returning. Boolean (e.g., true).
# (08) name                => The station name. String (e.g., "DAME STREET").
# (09) short_name          => Short station name. String (empty in this dataset).
# (10) address             => The station address. String (e.g., "Dame Street").
# (11) lat                 => Latitude of the station. Float (e.g., 53.344006).
# (12) lon                 => Longitude of the station. Float (e.g., -6.266802).
# (13) region_id           => Region identifier. String (empty in this dataset).
# (14) capacity            => Total docks at the station. Int (e.g., 16).
SYSTEM_ID = "system_id"
LAST_REPORTED = "last_reported"
STATION_ID = "station_id"
NUM_BIKES_AVAILABLE = "num_bikes_available"
NUM_DOCKS_AVAILABLE = "num_docks_available"
IS_INSTALLED = "is_installed"
IS_RENTING = "is_renting"
IS_RETURNING = "is_returning"
NAME = "name"
SHORT_NAME = "short_name"
ADDRESS = "address"
LATITUDE = "lat"
LONGITUDE = "lon"
REGION_ID = "region_id"
CAPACITY = "capacity"

# Columns created by the program
HOUR = "hour"
DAY_OF_WEEK = "dayOfWeek"
DISTANCE_ORIGIN = "distanceOrigin"
DISTANCE_DROPOFF = "distanceDropOff"
DISTANCE_DESTINATION = "distanceDestination"
TIME_AT_PICK_UP = "timeAtPickUp"
TIME_AT_DROP_OFF = "timeAtDropOff"
TIME_AT_DESTINATION = "timeAtDestination"
WALKING_TIME_TO_PICK_UP = "walkingTimeToPickUp"
WALKING_TIME_TO_DESTINATION = "walkingTimeToDestination"
PICKUP_ID = "pickup_id"
DROPOFF_ID = "dropoff_id"
PICKUP_LATITUDE = "pickup_latitude"
PICKUP_LONGITUDE = "pickup_longitude"
DROPOFF_LATITUDE = "dropoff_latitude"
DROPOFF_LONGITUDE = "dropoff_longitude"
RELATIVE_MINUTE_OF_DAY = "relativeMinuteOfDay"
CHANCE_OF_BIKE = "chanceOfBike"
CHANCE_OF_DOCK = "chanceOfDock"
CHANCE_OF_AVAILABILITY = "totalChance"
AVG_BIKES_AVAILABLE = "avgBikesAvailable"
AVG_DOCKS_AVAILABLE = "avgDocksAvailable"
PICKUP_SAMPLES = "pickupSamples"
DROPOFF_SAMPLES = "dropoffSamples"
AVAILABLE = "available"
PICKUP_NAME = "pickup_name"
DROPOFF_NAME = "dropoff_name"

WALKING_SPEED_KMH = 5.0 # km/h
EARTH_RADIUS_KM = 6371.0
MINUTES_PER_DAY = 24 * 60
TOLERANCE = 5 # minutes

# ------------------------------------------
# FUNCTION haversine_km
# ------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    # same formula as the core version, but built with Spark columns so it runs inside Spark

    # 1. We convert the degrees to radians
    lat1 = pyspark.sql.functions.radians(lat1)
    lon1 = pyspark.sql.functions.radians(lon1)
    lat2 = pyspark.sql.functions.radians(lat2)
    lon2 = pyspark.sql.functions.radians(lon2)

    # 2. Haversine formula: h = sin^2((lat2 - lat1) / 2) + cos(lat1) * cos(lat2) * sin^2((lon2 - lon1) / 2)
    h = pyspark.sql.functions.pow(pyspark.sql.functions.sin((lat2 - lat1) / 2), 2) + \
        pyspark.sql.functions.cos(lat1) * pyspark.sql.functions.cos(lat2) * pyspark.sql.functions.pow(pyspark.sql.functions.sin((lon2 - lon1) / 2), 2)

    # 3. We turn h into the angle between the 2 points (2 * arcsin(sqrt(h))) and multiply it by the radius of the earth
    #    edge case: h can go a tiny bit over 1 because of rounding, which would make asin give null, so we cap it at 1
    return 2 * EARTH_RADIUS_KM * pyspark.sql.functions.asin(pyspark.sql.functions.sqrt(pyspark.sql.functions.least(pyspark.sql.functions.lit(1.0), h)))


# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            leaving_at,
            day_of_week,
            leaving_from,
            destination,
            avg_speed,
            min_total_availability
           ):

    # 1. We define the Schema of our DF.
    my_schema = T.StructType(
        [T.StructField(SYSTEM_ID, T.StringType(), True),
         T.StructField(LAST_REPORTED, T.TimestampType(), True),
         T.StructField(STATION_ID, T.IntegerType(), True),
         T.StructField(NUM_BIKES_AVAILABLE, T.IntegerType(), True),
         T.StructField(NUM_DOCKS_AVAILABLE, T.IntegerType(), True),
         T.StructField(IS_INSTALLED, T.BooleanType(), True),
         T.StructField(IS_RENTING, T.BooleanType(), True),
         T.StructField(IS_RETURNING, T.BooleanType(), True),
         T.StructField(NAME, T.StringType(), True),
         T.StructField(SHORT_NAME, T.StringType(), True),
         T.StructField(ADDRESS, T.StringType(), True),
         T.StructField(LATITUDE, T.DoubleType(), True),
         T.StructField(LONGITUDE, T.DoubleType(), True),
         T.StructField(REGION_ID, T.StringType(), True),
         T.StructField(CAPACITY, T.IntegerType(), True)
         ])

    # 2. Operation C1: 'read' the CSV files (they have a header row and quoted strings)
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "\"") \
        .option("header", "true") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # 3. Operation A1: 'filter' all unique stations
    stationsDF = (inputDF
                    .groupBy(STATION_ID)
                    .agg(F.max_by(NAME, LAST_REPORTED).alias(NAME),
                         F.max_by(LATITUDE, LAST_REPORTED).alias(LATITUDE),
                         F.max_by(LONGITUDE, LAST_REPORTED).alias(LONGITUDE),
                         F.max_by(CAPACITY, LAST_REPORTED).alias(CAPACITY))
                         .withColumn(DISTANCE_ORIGIN, haversine_km(F.col(LATITUDE), F.col(LONGITUDE), F.lit(leaving_from[0]), F.lit(leaving_from[1])))
                         .withColumn(DISTANCE_DESTINATION, haversine_km(F.col(LATITUDE), F.col(LONGITUDE), F.lit(destination[0]), F.lit(destination[1])))
                         .withColumn(WALKING_TIME_TO_PICK_UP, F.col(DISTANCE_ORIGIN) / (WALKING_SPEED_KMH / 60))
                         .withColumn(WALKING_TIME_TO_DESTINATION, F.col(DISTANCE_DESTINATION) / (WALKING_SPEED_KMH / 60)))

    # 4. Operation A2: 'select' to keep only the relevant columns for the next steps
    originDF = (stationsDF
                .orderBy(DISTANCE_ORIGIN)
                .limit(5)
                .select(F.col(STATION_ID).alias(PICKUP_ID),
                        F.col(NAME).alias(PICKUP_NAME),
                        F.col(LATITUDE).alias(PICKUP_LATITUDE),
                        F.col(LONGITUDE).alias(PICKUP_LONGITUDE),
                        F.col(WALKING_TIME_TO_PICK_UP)))

    destinationDF = (stationsDF
                     .orderBy(DISTANCE_DESTINATION)
                     .limit(5)
                     .select(F.col(STATION_ID).alias(DROPOFF_ID),
                             F.col(NAME).alias(DROPOFF_NAME),
                             F.col(LATITUDE).alias(DROPOFF_LATITUDE),
                             F.col(LONGITUDE).alias(DROPOFF_LONGITUDE),
                             F.col(WALKING_TIME_TO_DESTINATION)))

    # 5. Operation A3: 'filter' every pickup/dropoff pairing with timings in minutes since midnight of the leaving day
    leave_hour, leave_minute = [int(part) for part in leaving_at.split(":")]
    leaving_minute = leave_hour * 60 + leave_minute

    routesDF = (originDF.crossJoin(destinationDF)
                .filter(F.col(PICKUP_ID) != F.col(DROPOFF_ID)) # we don't want to pick up and drop off at the same station
                .withColumn(DISTANCE_DROPOFF, # calculate the distance between the pickup and dropoff stations using the haversine formula
                            haversine_km(
                                F.col(PICKUP_LATITUDE),
                                F.col(PICKUP_LONGITUDE),
                                F.col(DROPOFF_LATITUDE),
                                F.col(DROPOFF_LONGITUDE)))
                .withColumn(TIME_AT_PICK_UP,  # calculate the time the user will arrive at the pickup station
                            F.lit(leaving_minute) + F.col(WALKING_TIME_TO_PICK_UP))
                .withColumn(TIME_AT_DROP_OFF, # calculate the time the user will arrive at the dropoff station
                            F.col(TIME_AT_PICK_UP) + F.col(DISTANCE_DROPOFF) / (avg_speed / 60))
                .withColumn(TIME_AT_DESTINATION, # calculate the time the user will arrive at the destination
                            F.col(TIME_AT_DROP_OFF) + F.col(WALKING_TIME_TO_DESTINATION))
                .cache())

    # 6. Operation A4: 'select' to keep only the relevant columns for the next steps
    next_day = day_of_week % 7 + 1 # if it's Sunday (7), the next day is Monday (1)

    minute_of_day = (F.hour(F.col(LAST_REPORTED)) * 60 # calculate the minute of the day for each station status report
                     + F.minute(F.col(LAST_REPORTED))
                     + F.second(F.col(LAST_REPORTED)) / 60)

    snapshotDF = (inputDF # filter the station status reports to only include those that are relevant for the pickup and dropoff times
                  .select(STATION_ID, LAST_REPORTED, NUM_BIKES_AVAILABLE, NUM_DOCKS_AVAILABLE, IS_RENTING, IS_RETURNING)
                  .withColumn(RELATIVE_MINUTE_OF_DAY,
                              F.when(F.dayofweek(F.col(LAST_REPORTED)) == day_of_week, minute_of_day)
                              .when(F.dayofweek(F.col(LAST_REPORTED)) == next_day, minute_of_day + MINUTES_PER_DAY))
                  .filter(F.col(RELATIVE_MINUTE_OF_DAY).isNotNull()))

    # 7. Operation A5: 'select' to get distinct pickup ids and minute
    pickup_timesDF = routesDF.select(PICKUP_ID, TIME_AT_PICK_UP).distinct()

    # 8. Operation A6: 'join' to get the chance of a bike at each pickup station when we get there
    pickup_chanceDF = (snapshotDF
                       .join(F.broadcast(pickup_timesDF),
                             snapshotDF[STATION_ID] == pickup_timesDF[PICKUP_ID])
                       .filter(F.abs(F.col(RELATIVE_MINUTE_OF_DAY) - F.col(TIME_AT_PICK_UP)) <= TOLERANCE)
                       .groupBy(PICKUP_ID)
                       .agg(F.avg(((F.col(NUM_BIKES_AVAILABLE) > 0) & F.col(IS_RENTING)).cast("int"))
                            .alias(CHANCE_OF_BIKE),
                            F.round(F.avg(((F.col(NUM_BIKES_AVAILABLE) > 0) & F.col(IS_RENTING)).cast("int")), 2).alias(AVG_BIKES_AVAILABLE),
                            F.count("*").alias(PICKUP_SAMPLES)))

    # 9. Operation A7: 'select' to get distinct dropoff ids and minute
    dropoff_timesDF = routesDF.select(PICKUP_ID, DROPOFF_ID, TIME_AT_DROP_OFF)

    # 10. Operation A8: 'join' to get the chance of a dock at each dropoff station when we get there
    dropoff_chanceDF = (snapshotDF
                        .join(F.broadcast(dropoff_timesDF),
                              snapshotDF[STATION_ID] == dropoff_timesDF[DROPOFF_ID])
                        .filter(F.abs(F.col(RELATIVE_MINUTE_OF_DAY) - F.col(TIME_AT_DROP_OFF)) <= TOLERANCE)
                        .groupBy(PICKUP_ID, DROPOFF_ID)
                        .agg(F.avg(((F.col(NUM_DOCKS_AVAILABLE) > 0) & F.col(IS_RETURNING)).cast("int"))
                             .alias(CHANCE_OF_DOCK),
                             F.round(F.avg(((F.col(NUM_DOCKS_AVAILABLE) > 0) & F.col(IS_RETURNING)).cast("int")), 2).alias(AVG_DOCKS_AVAILABLE),
                             F.count("*").alias(DROPOFF_SAMPLES)))

    # 11. Operation A9: 'join' to get the chance of a bike and a dock for each route
    def clock(col_name):
        m = F.floor(F.col(col_name))
        return F.format_string("%02d:%02d", ((m / 60).cast("int") % 24), (m % 60).cast("int"))

    allSolutionsDF = (routesDF
                  .join(pickup_chanceDF, PICKUP_ID, "left")
                  .join(dropoff_chanceDF, [PICKUP_ID, DROPOFF_ID], "left")
                  .withColumn(CHANCE_OF_AVAILABILITY,
                              F.coalesce(F.col(CHANCE_OF_BIKE), F.lit(0.0)) *
                              F.coalesce(F.col(CHANCE_OF_DOCK), F.lit(0.0)))
                  .orderBy(F.desc(CHANCE_OF_AVAILABILITY), F.asc(TIME_AT_DESTINATION), F.asc(DISTANCE_DROPOFF))
                  .select(PICKUP_NAME, DROPOFF_NAME,
                          clock(TIME_AT_PICK_UP).alias(TIME_AT_PICK_UP),
                          clock(TIME_AT_DROP_OFF).alias(TIME_AT_DROP_OFF),
                          clock(TIME_AT_DESTINATION).alias(TIME_AT_DESTINATION),
                          F.round(CHANCE_OF_BIKE, 2).alias(CHANCE_OF_BIKE),
                          F.round(CHANCE_OF_DOCK, 2).alias(CHANCE_OF_DOCK),
                          F.round(CHANCE_OF_AVAILABILITY, 2).alias(CHANCE_OF_AVAILABILITY),
                          PICKUP_SAMPLES, DROPOFF_SAMPLES))

    # Operation A10: 'filter' to get only the solutions with a total chance of availability above the threshold
    accetableSolutionsDF = (allSolutionsDF
                            .filter(F.col(CHANCE_OF_AVAILABILITY) >= min_total_availability)
                            .orderBy(F.asc(TIME_AT_DESTINATION), F.desc(CHANCE_OF_AVAILABILITY), F.asc(DISTANCE_DROPOFF)))

    earliest_time_at_destination = accetableSolutionsDF.agg(F.min(TIME_AT_DESTINATION)).collect()[0][0]
    latest_time_at_destination = accetableSolutionsDF.agg(F.max(TIME_AT_DESTINATION)).collect()[0][0]

    # in case there are no solutions above the threshold, we will return the best available solutions
    if accetableSolutionsDF.count() > 0:
        # if a solution with a total chance of availability of 1 exists and it's the earliest arrival, we will return only that solution
        if accetableSolutionsDF.filter(F.col(CHANCE_OF_AVAILABILITY) == 1).count() > 0 and earliest_time_at_destination == accetableSolutionsDF.filter(F.col(CHANCE_OF_AVAILABILITY) == 1).agg(F.min(TIME_AT_DESTINATION)).collect()[0][0]:
            solutionDF = accetableSolutionsDF.filter(F.col(CHANCE_OF_AVAILABILITY) == 1).orderBy(F.asc(TIME_AT_DESTINATION)).limit(1)
        else:
            solutionDF = accetableSolutionsDF.limit(5)
    else:
        solutionDF = allSolutionsDF.limit(5)

    print(f"\nSolutions with total chance of availability >= {min_total_availability}: {accetableSolutionsDF.count()}\nAll solutions: {allSolutionsDF.count()}\n")
    print(f"Earliest time at destination: {earliest_time_at_destination}\nLatest time at destination: {latest_time_at_destination}\n")

    # ---------------------------------------

    # Operation A1: 'collect'
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)


# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Usage:
#   python3 ./A01_ex8.py <dataset_dir> <leaving_at> <day_of_week> <leaving_from> <destination> <average_speed>
#
# Example (ten most stressed stations on weekday mornings):
#   python3 ./A01_ex8.py "../../FileStore/tables/my_datasets/A01_micro_dataset_8/" 08:00 1 53.366346,-6.218066 53.341677,-6.260252 15
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    my_dataset_dir = "../../FileStore/tables/my_datasets/A01_micro_dataset_8/"
    # my_dataset_dir = "../../FileStore/tables/my_datasets/my_ex8_dataset_complete/"
    leaving_at = "08:00"
    day_of_week = 1
    leaving_from = [53.366346, -6.218066]
    destination = [53.341677, -6.260252]
    avg_speed = 15 # km/h
    min_total_availability = 0.8 # minimum total availability to consider a route

    if len(sys.argv) > 1:
        my_dataset_dir = sys.argv[1]
    if len(sys.argv) > 2:
        leaving_at = sys.argv[2]
    if len(sys.argv) > 3:
        day_of_week = int(sys.argv[3])
    if len(sys.argv) > 4:
        leaving_from = [float(coord) for coord in sys.argv[4].split(",")]
    if len(sys.argv) > 5:
        destination = [float(coord) for coord in sys.argv[5].split(",")]
    if len(sys.argv) > 6:
        avg_speed = float(sys.argv[6])
    if len(sys.argv) > 7:
        min_total_availability = float(sys.argv[7])

    # 2. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    # ensure UTC timezone for timestamp parsing
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # 3. We call to our main function
    my_main(spark,
            my_dataset_dir,
            leaving_at,
            day_of_week,
            leaving_from,
            destination,
            avg_speed,
            min_total_availability
           )