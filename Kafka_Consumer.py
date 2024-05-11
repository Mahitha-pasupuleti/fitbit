from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class Kafka_Consumer:
    def __init__(self):
        # Initialize Kafka configuration
        self.Conf = Configuration()
        self.checkpoint_base = self.Conf.kafkaTopic + "/user_topic_test"
        self.BOOTSTRAP_SERVER = ""
        self.JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
        self.CLUSTER_API_KEY = ""
        self.CLUSTER_API_SECRET = ""

    def ingestFromKafka(self, startingTime=1, maxOffsetsPerTrigger=10000):
        # Start a Spark session
        spark = SparkSession.builder.appName("BronzeStream").getOrCreate()
        # Read from Kafka stream
        data = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.BOOTSTRAP_SERVER)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", f"{self.JAAS_MODULE} required username='{self.CLUSTER_API_KEY}' password='{self.CLUSTER_API_SECRET}';")
                .option("subscribe", "user_info")
                .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
                .option("startingTimestamp", startingTime)
                .load())
        return data
    
    def getSchema(self):
        # Define schema for Kafka data
        return """
            user_id LONG, update_type STRING, timestamp LONG, 
            dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, 
            address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>
            """

    def getKafkaData(self, kafka_df):
        # Extract and transform Kafka data
        from pyspark.sql.functions import cast, from_json

        print("Schema Entered")

        data = (kafka_df.select(kafka_df.key.cast("string").alias("key"),
                            from_json(kafka_df.value.cast("string"), self.getSchema()).alias("value"),
                            "topic", "partition", "offset", kafka_df.timestamp.cast("LONG").alias("timestamp"))
                )
        # data.display()
        print(data)
        print("Schema Out")
        return data
    
    def waitForMicroBatch(self, sleep=3000):
        # Wait for micro-batch processing
        import time
        print(f"\tWaiting for {sleep} milliseconds...", end='')
        time.sleep(sleep / 1000)  # Convert milliseconds to seconds
        print("Done.")

    def mergeOutputFiles(self):
        # Merge output JSON files
        # Note: This code assumes dbutils and spark are globally accessible
        # Consider passing these objects as parameters if possible
        # List all JSON files in the ADLS directory
        json_files = dbutils.fs.ls(self.checkpoint_base)
        json_files = [file.path for file in json_files if file.path.endswith(".json")]
        self.waitForMicroBatch(sleep=120)

        # Initialize an empty list to hold all data
        merged_data = []

        # Read each JSON file and append its contents to the merged_data list
        for json_file in json_files:
            df = spark.read.json(json_file)
            data = df.collect()
            merged_data.extend(data)

        # Write the merged data to a single JSON file
        merged_output_path = f"{self.checkpoint_base}/merged_output_json"
        # Ensure the output path is correct and includes the file name
        spark.createDataFrame(merged_data).write.mode("overwrite").json(merged_output_path)

        print(f"Merged output file saved: {merged_output_path}")
        return 0

    def process(self, startingTime=1, maxOffsetsPerTrigger=10000, triggerInterval="30 seconds"):
        # Process Kafka data using Spark structured streaming
        spark = SparkSession.builder.appName("KafkaData").getOrCreate()
        rawDF = self.ingestFromKafka(startingTime, maxOffsetsPerTrigger)
        KafkaDF = self.getKafkaData(rawDF)
        sQuery = (KafkaDF.writeStream
                  .format("json")
                  .option("checkpointLocation", f"{self.checkpoint_base}/checkpoint")
                  .option("path", self.checkpoint_base)
                  .outputMode("append")
                  .queryName("user_info_Kafka_Data")
                  .trigger(processingTime=triggerInterval)
                  .start())
        print("Streaming query started.")
        self.waitForMicroBatch(sleep=60000)  # Wait for the query to finish
        sQuery.stop()
        print("Streaming query finished.")
        return sQuery

# Example usage:
bzQuery = Kafka_Consumer()
bz = bzQuery.process(maxOffsetsPerTrigger=500000, triggerInterval="60 seconds")  
# Increased maxOffsetsPerTrigger
bzQuery.mergeOutputFiles()
