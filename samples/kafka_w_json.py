
import datetime
import logging
import sys
import os

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import (
    JsonRowSerializationSchema,
    JsonRowDeserializationSchema,
)


# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.
def write_to_kafka(se_env: StreamExecutionEnvironment):
    type_info = Types.ROW([Types.INT(), Types.STRING(), Types.SQL_TIME()])
    ct = datetime.datetime.now()
    ds = se_env.from_collection(
        [
            (1, "hi", ct),
            (2, "hello", ct),
            (3, "hi", ct),
            (4, "hello", ct),
            (5, "hi", ct),
            (6, "hello", ct),
            (6, "hello", ct),
        ],
        type_info=type_info,
    )

    serialization_schema = (
        JsonRowSerializationSchema.Builder().with_type_info(type_info).build()
    )
    kafka_producer = FlinkKafkaProducer(
        topic="test_json_topic",
        serialization_schema=serialization_schema,
        producer_config={
            "bootstrap.servers": "localhost:9092",
            "group.id": "test_group",
        },
    )

    # note that the output type of ds must be RowTypeInfo
    ds.add_sink(kafka_producer)
    se_env.execute()


def read_from_kafka(se_env: StreamExecutionEnvironment):
    deserialization_schema = (
        JsonRowDeserializationSchema.Builder()
        .type_info(Types.ROW([Types.INT(), Types.STRING(), Types.SQL_TIME()]))
        .build()
    )
    kafka_consumer = FlinkKafkaConsumer(
        topics="test_json_topic",
        deserialization_schema=deserialization_schema,
        properties={"bootstrap.servers": "localhost:9092", "group.id": "test_group_1"},
    )
    kafka_consumer.set_start_from_earliest()

    se_env.add_source(kafka_consumer).print()
    se_env.execute()


def get_file_path(relative_path):
    cwd = os.path.dirname(__file__)
    return os.path.join(cwd, relative_path)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    kafka_jar_path = f'file:///{get_file_path("../dependency/jars/flink-sql-connector-kafka-1.15.0.jar")}'
    print(kafka_jar_path)
    env.add_jars(kafka_jar_path)

    print("start writing data to kafka")
    write_to_kafka(env)

    print("start reading data from kafka")
    read_from_kafka(env)
