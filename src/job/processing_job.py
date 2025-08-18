# Job 1 - processing and enrichment
# the upstream job that produces the enriched data that other jobs consume

# Pulls data from kafka source
# Processes each event, enriching it with Geo Location data API
# Sinks data into PostgresSQL table processed_events
# Sinks data into Kafka topic process_events_kafka

import os
import json
import requests

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.udf import ScalarFunction, udf

# gets location from ip address providing more information
# using ip2location.io API
class GetLocation(ScalarFunction):
    def eval(self, ip_address):
        url = "https://api.ip2location.io"
        response = requests.get(url, params={
            'ip': ip_address,
            'key': os.environ.get("IP_CODING_KEY")
        })

        if response.status_code != 200:
            return json.dumps({})  # Return empty dict if request failed

        data = json.loads(response.text)
        country = data.get('country_code', '')
        state = data.get('region_name', '')
        city = data.get('city_name', '')

        return json.dumps({'country': country, 'state': state, 'city': city})


get_location = udf(GetLocation(), result_type=DataTypes.STRING())


# source tabke from kafka
def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"

    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}')
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' =
                'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule '
                'required username="{kafka_key}" password="{kafka_secret}";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """

    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name


# sink table to postgres
def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp TIMESTAMP(3),
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """

    t_env.execute_sql(sink_ddl)
    return table_name



def create_processed_events_sink_kafka(t_env):
    table_name = "process_events_kafka"
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")

    sasl_config = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="{kafka_key}" password="{kafka_secret}";'
    )

    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_timestamp VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_GROUP').split('.')[0] + '.' + table_name}',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.jaas.config' = '{sasl_config}',
            'format' = 'json'
        );
    """

    print(sink_ddl)
    t_env.execute_sql(sink_ddl)
    return table_name


def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # enable checkpointing every 10 seconds 10 * 1000 ms
    # allows Flink to recover state in case of failure
    env.enable_checkpointing(10 * 1000)
    
    # defines how many concurrent tasks (subtasks) execute. 1 = single thread
    env.set_parallelism(1)


    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Register UDF
    t_env.create_temporary_function("get_location", get_location)

    try:
        # Create Kafka source and Postgres sink
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)

        print('loading into postgres')

        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                ip,
                event_timestamp,
                referrer,
                host,
                url,
                get_location(ip) as geodata
            FROM {source_table}
            """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
