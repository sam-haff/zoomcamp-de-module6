from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration


def create_events_aggregated_by_pickup_sink(t_env, table_name):
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            locationid INT,
            trips_n BIGINT,
            PRIMARY KEY (window_start, locationid) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "taxi_events"
    pattern = "yyyy-MM-dd HH:mm:ss"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            dropoff_timestamp AS TO_TIMESTAMP(lpep_dropoff_datetime, '{pattern}'),
            WATERMARK FOR  dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips-3',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_aggregation(dst_table_name, by_location_field):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
# Removed watermark yada yada because it was unnused variable(sure it shouldn't be te case).
# Watermarks are set through sql in create_source function, so that variable was indeed redundant
# https://medium.com/@ipolyzos_/understanding-watermarks-in-apache-flink-c8793a50fbb8
    try:
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_by_pickup_sink(t_env, dst_table_name)
        
        t_env.execute_sql(f"""
        INSERT INTO {aggregated_table}
        SELECT 
            SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTES) as window_start,
            SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTES) as window_end,
            {by_location_field} as locationid, 
            COUNT(*) AS trips_n
        FROM {source_table}
        GROUP BY SESSION(dropoff_timestamp, INTERVAL '5' MINUTES), {by_location_field};
        """)

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))




