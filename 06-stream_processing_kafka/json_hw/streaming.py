from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import RIDE_SCHEMA, CONSUME_TOPIC_RIDES_GREEN, CONSUME_TOPIC_RIDES_FHV, TOPIC_WINDOWED_PICKUP_LOCATION, PRODUCE_TOPIC_RIDES_GREEN, PRODUCE_TOPIC_RIDES_FHV, PRODUCE_TOPIC_RIDES_ALL


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery


def sink_memory(df, query_name, query_template):
    query_df = df \
        .writeStream \
        .queryName(query_name) \
        .format("memory") \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


def sink_kafka(df, topic):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .outputMode('append') \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    columns = df.columns

    df = df.withColumn("value", F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast('string'))
    return df.select(['key', 'value'])


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation


def op_windowed_groupby(df, window_duration, slide_duration):
    df_windowed_aggregation = df.groupBy(
        F.window(timeColumn=df.tpep_pickup_datetime, windowDuration=window_duration, slideDuration=slide_duration),
        df.PUlocationID
    ).count()
    return df_windowed_aggregation

def sink_console_popular_pickup(df):
    write_query = df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()
    return write_query

if __name__ == "__main__":
    spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # read streaming data from both green and fhv rides kafka topics
    df_consume_stream_green = read_from_kafka(consume_topic=CONSUME_TOPIC_RIDES_GREEN)
    print(df_consume_stream_green.printSchema())

    df_consume_stream_fhv = read_from_kafka(consume_topic=CONSUME_TOPIC_RIDES_FHV)
    print(df_consume_stream_fhv.printSchema())

    # parse streaming data
    df_rides_green = parse_ride_from_kafka_message(df_consume_stream_green, RIDE_SCHEMA)
    print(df_rides_green.printSchema())

    df_rides_fhv = parse_ride_from_kafka_message(df_consume_stream_fhv, RIDE_SCHEMA)
    print(df_rides_fhv.printSchema())

    df_rides_all = df_rides_green.union(df_rides_fhv)
    df_rides_all_messages = prepare_df_to_kafka_sink(df=df_rides_all, 
                                                    value_columns=[field.name for field in RIDE_SCHEMA], 
                                                    key_column='PUlocationID')

    kafka_sink_query = sink_kafka(df=df_rides_all_messages, topic=PRODUCE_TOPIC_RIDES_ALL)

    df_trip_count_by_pickup_location = op_groupby(df_rides_all, ['PUlocationID'])
    df_most_popular_pickup_location = df_trip_count_by_pickup_location.orderBy(F.desc('count')).limit(1)

    # print the DataFrame
    #sink_console(df_most_popular_pickup_location)

    
    # Apply the op_windowed_groupby function
    window_duration = "1 hour"
    slide_duration = "10 minutes"
    df_popular_pickup = op_windowed_groupby(df_rides_all, window_duration, slide_duration)

    # Output the most popular pickup locations with a readable format
    console_query_popular_pickup = sink_console_popular_pickup(df_popular_pickup)

    spark.streams.awaitAnyTermination()
    #sink_console(df_rides_green, output_mode='append')
    #sink_console(df_rides_fhv, output_mode='append')

    #df_trip_count_by_pickup_location = op_windowed_groupby(df_rides_green.union(df_rides_fhv),
    #                                                       window_duration="10 minutes",
    #                                                       slide_duration='5 minutes')

    # print the DataFrame
    #df_trip_count_by_pickup_location.writeStream \
    #    .outputMode("append") \
    #    .format("console") \
    #    .option("truncate", False) \
    #    .start() \
    # write the output to the kafka topic
    #df_trip_count_messages = prepare_df_to_kafka_sink(df=df_trip_count_by_pickup_location,
    #                                                  value_columns=['count'], key_column='PUlocationID')
    #kafka_sink_query = sink_kafka(df=df_trip_count_messages, topic=TOPIC_WINDOWED_PICKUP_LOCATION)

    #spark.streams.awaitAnyTermination()