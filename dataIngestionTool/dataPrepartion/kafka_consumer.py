import sys
import json
import datetime
import traceback
from kafka import KafkaConsumer
from configparser import ConfigParser

# instantiate config Parser
config = ConfigParser()

def consumeKafka(topic,kafkaBrokers,autoOffsetReset,enableAutoCommit,groupId):
    try:
        consumer = KafkaConsumer(topic, bootstrap_servers=kafkaBrokers,auto_offset_reset=autoOffsetReset,enable_auto_commit=enableAutoCommit,group_id=groupId)
        for message in consumer:
            if message.key is not None and message.value is not None:
                logFile = str(message.key.decode('utf-8'))
                content=str(json.loads(message.value.decode('utf-8')))+"\n"
                file_path = config.get('DIT_Kafka_config', 'LOG_DIR') + logFile + ".log"
                with open(file_path,mode = 'a',encoding = 'utf-8') as filePtr:
                    filePtr.write(content)
        print("End consuming :")
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in consumeKafka() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())


def main(configPath):
    try:
        config.read(configPath)
        print("Starting kafka consumer with following details::")
        topic = config.get('DIT_Kafka_config', 'TOPIC')
        kafkaBrokers=config.get('DIT_Kafka_config', 'KAFKA_BROKERS').split(',')
        autoOffsetReset=config.get('DIT_Kafka_config', 'AUTO_OFFSET_RESET')
        enableAutoCommit=eval(config.get('DIT_Kafka_config', 'ENABLE_AUTO_COMMIT'))
        groupId=config.get('DIT_Kafka_config', 'GROUP_ID')
        print("Topic :: "+ topic +" groupId :: "+groupId)
        consumeKafka(topic,kafkaBrokers,autoOffsetReset,enableAutoCommit,groupId)
    except Exception as e:
        print(str(datetime.datetime.now()) + "____________ Exception occurred in main() ________________")
        print("Exception::msg %s" % str(e))
        print(traceback.format_exc())

if __name__ == "__main__":
    sys.exit(main('C:\\Users\\sk250102\\Documents\\Teradata\\DIT\\DataIngestionTool\\config\\config.cnf'))