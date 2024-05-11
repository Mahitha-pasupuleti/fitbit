import logging  # Import the logging module for logging events
import json  # Import the json module for JSON handling
import time  # Import the time module for time-related functions
from confluent_kafka import Producer, KafkaException  # Import necessary Kafka modules

class test:
    def __init__(self):
        # Initialize Kafka producer configuration and topic
        self.topic = "user_info"
        self.conf = {
            'bootstrap.servers': '',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': '',
            'sasl.password': '',
            'client.id': "Mahitha's-Laptop"
        }
        # Initialize logger for logging producer events
        self.logger = logging.getLogger('KafkaProducer')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def delivery_callback(self, err, msg):
        # Callback function for message delivery status
        if err:
            self.logger.error('Message failed delivery: %s', err.str())
        else:
            # Extract key and value from the delivered message
            key = msg.key().decode('utf-8')
            user_id = json.loads(msg.value().decode('utf-8'))["user_id"]
            self.logger.info("Produced event to : key = %s value = %s", key, user_id)

    def produce_invoices(self, producer, batch_size=1000):
        # Method to produce messages to Kafka topic in batches
        try:
            with open("data/user.json") as lines:
                file_info = lines.read()
                user_info = json.loads(file_info)
                batch = []
                for item in user_info:
                    user_id = item.get("user_id")
                    # Produce message to Kafka with callback for delivery status
                    producer.produce(
                        self.topic,
                        key=str(user_id).encode("UTF-8"),
                        value=json.dumps(item).encode("UTF-8"),
                        callback=self.delivery_callback
                    )
                    batch.append(item)
                    if len(batch) >= batch_size:
                        self.logger.info("Flushing batch...")
                        producer.flush()
                        batch = []
                # Flush any remaining messages
                if batch:
                    self.logger.info("Flushing remaining messages...")
                    producer.flush()
        except FileNotFoundError:
            self.logger.error("File 'bpm.json' not found.")
        except json.JSONDecodeError as e:
            self.logger.error("Error decoding JSON data: %s", str(e))

    def start(self):
        # Method to start the Kafka producer
        kafka_producer = Producer(self.conf)
        self.produce_invoices(kafka_producer)
        kafka_producer.flush()
        time.sleep(100)  # Wait for 5 seconds (adjust as needed)
        kafka_producer.close()

# Entry point of the program
if __name__ == "__main__":
    kafka_producer = test()  # Create an instance of the Kafka producer class
    kafka_producer.start()  # Start producing messages to Kafka
