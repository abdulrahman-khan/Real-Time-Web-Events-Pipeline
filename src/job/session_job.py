# Job 3 sessions and aggregation
# relies on Job 1's kafka sink process_events_kafka

# Pulls data from process_events_kafka kafka table
    # StreamTableEnvironment in streaming mode
# Sessionizes the data by IP address and host
    # a session window is dymamic: starts hen the first session arrives, closing after 1 minute of inactivity
    # a user browing the site is multiple events, but one session
    # uses watermarks to handle event time procession up to 15 seconds late
# Stores results in PostgresSQL table

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session 

def create_aggregated_sessions_sink_postgress(t_env):
    table_name = 'session_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            ip VARCHAR,
            host VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    # Execute DDL to create the table
    t_env.execute_sql(sink_ddl)
    return table_name


def session_input(t_env):
    """
    Create a PostgreSQL sink table for processed events.
    Stores event_hour, host, and aggregated hit count.
    """
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    # Execute DDL to create the table
    t_env.execute_sql(sink_ddl)
    return table_name


# kafka source table
def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


# flink session_job
# reads kafka input
# applies session window by IP and HOST, COUNT ON site hits
# writes to postgres table
def log_aggregation():
    """
    Main job definition:
    - Configures environments
    - Reads Kafka input
    - Applies session windowing by IP and host
    - Aggregates event counts
    - Writes results to PostgreSQL
    """
    # Set up the streaming execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)  # Enables checkpointing every 10ms (example config)
    env.set_parallelism(3)        # Set parallelism for better throughput

    # Set up the Table API environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # kafka source - postgres sink
        source_table = create_processed_events_source_kafka(t_env)
        aggregated_table = create_aggregated_sessions_sink_postgress(t_env)
        
        # session with 1 minute gap on timestamp
        t_env.from_path(source_table)\
            .window(
                Session.with_gap(lit(1).minutes).on(col("window_timestamp")).alias("w")
            ).group_by(
                col("w"),
                col("ip"),
                col("host")
            )\
            .select(
                col("w").start.alias("event_hour"),  # Start time of the session window
                col("ip"),
                col("host"),
                col("ip").count.alias("num_hits")   # Count number of hits per session
            )\
            .execute_insert(aggregated_table)

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
