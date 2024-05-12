class Bronze():
    def __init__(self, env):
        """
        Initialize the Bronze layer with environment settings.
        
        Parameters:
        - env (str): Environment name
        
        Actions:
        - Initializes necessary configurations for the Bronze layer.
        """
        Conf = Configuration()
        self.landing_zone = Conf.data + "/data"  # Set the landing zone path
        self.checkpoint_base = Conf.checkpoint + "/checkpoints"  # Set the checkpoint base path
        self.catalog = env  # Set the catalog name
        self.db_name = Conf.db_name  # Set the database name
        spark.sql(f"USE {self.catalog}.{self.db_name}")  # Set the active Spark catalog and database

    def consume_user_registration(self, once=True, processing_time="5 seconds"):
        """
        Consume user registration data and write it to a Delta table.
        
        Parameters:
        - once (bool, optional): Whether to run the stream once. Default is True.
        - processing_time (str, optional): Processing time interval. Default is "5 seconds".
        
        Returns:
        - StreamingQuery: The streaming query object.
        """
        from pyspark.sql import functions as F
        schema = "userId long, deviceId long, mac_address string, timestamp double"  # Define schema
        
        # Read user registration data from cloud source into a streaming DataFrame
        df_stream = (spark.readStream
                        .format("cloudFiles")
                        .schema(schema)
                        .option("cloudFiles.format", "csv")
                        .option("header", "true")
                        .load(self.landing_zone + "/users_registered")
                    )
                        
        # Use append mode to insert from source
        stream_writer = df_stream.writeStream \
                                 .format("delta") \
                                 .option("checkpointLocation", self.checkpoint_base + "/users_registered") \
                                 .outputMode("append")
        
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bronze_p2")  # Set scheduler pool
        
        # Write data to the Delta table and return the streaming query
        return stream_writer.toTable(f"{self.catalog}.{self.db_name}.users_registered")
          
    # Other methods like consume_gym_logins, consume_kafka_dump, etc., follow a similar structure

    def consume(self, once=True):
        """
        Consume data from various sources in the Bronze layer.
        
        Parameters:
        - once (bool, optional): Whether to run the stream once. Default is True.
        
        Actions:
        - Calls individual consume methods for different data sources.
        - If 'once' is True, waits for all streams to complete.
        """
        import time
        start = int(time.time())
        self.consume_user_registration(once) 
        self.consume_gym_logins(once) 
        self.consume_kafka_dump(once)
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        
        
    def assert_count(self, table_name, expected_count, filter="true"):
        """
        Asserts the count of records in a table against an expected count.
        
        Parameters:
        - table_name (str): Name of the table to check.
        - expected_count (int): Expected number of records.
        - filter (str, optional): Filter condition for the count. Default is "true".
        
        Actions:
        - Reads the table and compares the count against the expected count.
        - Raises AssertionError if counts do not match.
        """
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}"        
        
    def validate(self, sets):
        """
        Validates counts of records in various tables.
        
        Parameters:
        - sets (int): Number of sets to validate against.
        
        Actions:
        - Calls assert_count method for different tables with expected counts based on 'sets'.
        """
        self.assert_count("users_registered", 5 if sets == 1 else 1000)
        self.assert_count("gym_logins", 8 if sets == 1 else 1502)
        self.assert_count("kafka_dump_bz", 7 if sets == 1 else 1000, "topic='user_info'")
        self.assert_count("kafka_dump_bz", 16 if sets == 1 else 3004, "topic='workout'")
        self.assert_count("kafka_dump_bz", 778746, "topic='bpm'")
