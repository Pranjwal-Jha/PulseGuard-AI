from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Kafka Source Table (Raw Telemetry)
    t_env.execute_sql("""
        CREATE TABLE telemetry (
            tenant_id STRING,
            service_name STRING,
            metric_name STRING,
            current_value DOUBLE,
            baseline_value DOUBLE,
            `timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'telemetry_raw',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_processor',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset'
        )
    """)

    # Kafka Sink Table (Anomalies)
    t_env.execute_sql("""
        CREATE TABLE anomalies (
            tenant_id STRING,
            service_name STRING,
            metric_name STRING,
            spike_percentage DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'telemetry_anomalies',
            'properties.bootstrap.servers' = 'kafka:9092',
            'format' = 'json'
        )
    """)

    # Tumbling Window Aggregation (e.g., 1 minute)
    # If the average current_value is significantly higher than baseline
    t_env.execute_sql("""
        INSERT INTO anomalies
        SELECT
            tenant_id,
            service_name,
            metric_name,
            ((AVG(current_value) - AVG(baseline_value)) / AVG(baseline_value)) * 100 AS spike_percentage
        FROM TABLE(
            TUMBLE(TABLE telemetry, DESCRIPTOR(`timestamp`), INTERVAL '1' MINUTE)
        )
        GROUP BY
            tenant_id,
            service_name,
            metric_name,
            window_start,
            window_end
        HAVING ((AVG(current_value) - AVG(baseline_value)) / AVG(baseline_value)) > 0.2
    """)

if __name__ == '__main__':
    main()
