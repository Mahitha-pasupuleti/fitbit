class Gold():
    def __init__(self, env):
        """
        Initialize the Gold layer with environment settings.
        
        Parameters:
        - env (str): Environment name
        
        Actions:
        - Initializes necessary configurations for the Gold layer.
        """
        Conf = Configuration()
        self.landing_zone = Conf.data + "/data"  # Set the landing zone path
        self.checkpoint_base = Conf.checkpoint + "/checkpoints"  # Set the checkpoint base path
        self.catalog = env  # Set the catalog name
        self.db_name = Conf.db_name  # Set the database name
        spark.sql(f"USE {self.catalog}.{self.db_name}")  # Set the active Spark catalog and database

    def upsert_workout_bpm_summary(self, once=True, processing_time="15 seconds", startingVersion=0):
        """
        Upsert workout BPM summary data into the Gold layer.
        
        Parameters:
        - once (bool, optional): Whether to run the stream once. Default is True.
        - processing_time (str, optional): Processing time interval. Default is "15 seconds".
        - startingVersion (int, optional): Starting version for the stream. Default is 0.
        
        Returns:
        - StreamingQuery: The streaming query object.
        """
        from pyspark.sql import functions as F
        
        # Define the upsert query for workout BPM summary data
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.workout_bpm_summary a
        USING workout_bpm_summary_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter = Upserter(query, "workout_bpm_summary_delta")
        
        # Read user bins data for joining
        df_users = spark.read.table(f"{self.catalog}.{self.db_name}.user_bins")
        
        # Read streaming data from source and perform aggregations
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .table(f"{self.catalog}.{self.db_name}.workout_bpm")
                         .groupBy("user_id", "workout_id", "session_id", "end_time")
                         .agg(F.min("heartrate").alias("min_bpm"), F.mean("heartrate").alias("avg_bpm"), 
                              F.max("heartrate").alias("max_bpm"), F.count("heartrate").alias("num_recordings"))                         
                         .join(df_users, ["user_id"])
                         .select("workout_id", "session_id", "user_id", "age", "gender", "city", "state", "min_bpm", "avg_bpm", "max_bpm", "num_recordings")
                     )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm_summary")
                        )
        
        # Start the stream based on 'once' and return the streaming query
        return stream_writer.trigger(availableNow=True).start()
    
    
    def upsert(self, once=True, processing_time="5 seconds"):
        """
        Upsert data into the Gold layer.
        
        Parameters:
        - once (bool, optional): Whether to run the stream once. Default is True.
        
        Actions:
        - Calls individual upsert methods for different data sources.
        - Waits for streams to complete if 'once' is True.
        """
        self.upsert_workout_bpm_summary(once, processing_time)
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
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success") 
        
    def assert_rows(self, location, table_name, sets):
        """
        Asserts rows in a table match the expected rows from a file.
        
        Parameters:
        - location (str): Location of the file with expected rows.
        - table_name (str): Name of the table to check.
        - sets (int): Number of sets to validate against.
        
        Actions:
        - Reads expected rows from a file and compares them with actual rows in the table.
        - Raises AssertionError if rows do not match.
        """
        print(f"Validating records in {table_name}...", end='')
        expected_rows = spark.read.format("parquet").load(f"{self.test_data_dir}/{location}_{sets}.parquet").collect()
        actual_rows = spark.table(table_name).collect()
        assert expected_rows == actual_rows, f"Expected data mismatches with the actual data in {table_name}"
        print(f"Expected data matches with the actual data in {table_name}: Success")
        
        
    def validate(self, sets):    
        """
        Validates counts of records in various tables.
        
        Parameters:
        - sets (int): Number of sets to validate against.
        
        Actions:
        - Calls assert_count method for different tables with expected counts based on 'sets'.
        """
        self.assert_count("gym_summary", 1502)       
        if sets>1:
            self.assert_count("workout_bpm_summary", 1000)
        print(f"Gold layer validation completed")
