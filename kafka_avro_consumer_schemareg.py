from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
import io
from avro.io import DatumReader, BinaryDecoder
import avro.schema
import configparser


def load_config(file,header):
    """Load config from config file"""

    config = configparser.ConfigParser()
    config.read(file)
    dct = dict(config.items(header))
    return dct

def pop_schema_registry_params(conf):
    """Remove potential Schema Registry related configurations from dictionary"""

    conf.pop('schema.registry.url', None)
    conf.pop('basic.auth.user.info', None)
    conf.pop('basic.auth.credentials.source', None)

    return conf

def decode(msg_value, reg_client, topic):
    """ Decode kafka message and returns a python dictionary """
    schema_from_reg = avro.schema.parse(
        reg_client.get_latest_version(topic+'-value').schema.schema_str)
    reader = DatumReader(schema_from_reg)

    message_bytes = io.BytesIO(msg_value)
    # ignoring first bytes
    #   https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
    message_bytes.seek(5)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)

    return event_dict

def on_assign(c, partitions):
    """
    When the consumer group is created at the first time, auto.offset.reset determines what to do when there is no
     initial offset value or if the current offset doesn't exist anymore on the server.
     on_assign function is responsible to reassign the offset policy even if the consumer group and offset value
     already exists, forcing the offset to latest( partition.offset = -1 ) or earliest ( partition.offset = -2 )
    """
    for p in partitions:
        p.offset = -2
        print(p.offset, "set on -" , p.topic, "  - partition = ", p.partition)
    c.assign(partitions)



consumer_conf = load_config("config_file.config","kafka")
topic_name = ['topic-name']

schema_registry_conf = {
    'url': consumer_conf['schema.registry.url'],
    'basic.auth.user.info': consumer_conf['basic.auth.user.info']}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

consumer_conf = pop_schema_registry_params(consumer_conf)
consumer_conf['group.id'] = 'consumer_group_id_name'
consumer_conf['auto.offset.reset'] = 'earliest'

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(topic_name, on_assign=on_assign)



while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            msg_value = msg.value()
            event_dict = decode(msg_value,schema_registry_client,msg.topic())
            print("\n \n Processing *** - ",msg.topic())
            event_dict['kafka_topic_name'] = msg.topic()
            event_dict['kafka_offset'] = msg.offset()
            #kafka timestamp
            #   https://docs.confluent.io/3.3.1/clients/confluent-kafka-python/index.html#confluent_kafka.Message.timestamp
            event_dict['kafka_timestamp'] = msg.timestamp()[1]
            print("Processed parition - ",msg.partition())
            print(event_dict)
    except KeyboardInterrupt:
        print("break streaming")
        break
    except SerializerError as e:
        # Report malformed record, discard results, continue polling
        print("Message deserialization failed {}".format(e))
        pass

# Leave group and commit final offsets
consumer.close()
