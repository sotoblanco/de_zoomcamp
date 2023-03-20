import pyspark.sql.types as T

GREEN_INPUT_DATA_PATH = '../resources/green_tripdata_2019-01.csv.gz'
FHV_INPUT_DATA_PATH = '../resources/fhv_tripdata_2019-01.csv.gz'

BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_PICKUP_LOCATION = 'pickup_location_windowed'

PRODUCE_TOPIC_RIDES_FHV = 'rides_fhv'
CONSUME_TOPIC_RIDES_FHV = 'rides_fhv'
PRODUCE_TOPIC_RIDES_GREEN = 'rides_green'
CONSUME_TOPIC_RIDES_GREEN = 'rides_green'
PRODUCE_TOPIC_RIDES_ALL = "rides_all"

RIDE_SCHEMA = T.StructType(
    [T.StructField("vendor_id", T.IntegerType()),
     T.StructField('tpep_pickup_datetime', T.TimestampType()),
     T.StructField('tpep_dropoff_datetime', T.TimestampType()),
     T.StructField("passenger_count", T.IntegerType()),
     T.StructField("trip_distance", T.FloatType()),
     T.StructField("payment_type", T.IntegerType()),
     T.StructField("PUlocationID", T.IntegerType()),
     T.StructField("total_amount", T.FloatType()),
     ])