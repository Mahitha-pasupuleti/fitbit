class Silver():
    def __init__(self, env):
        """
        Initialize the Silver layer with environment settings.
        
        Parameters:
        - env (str): Environment name
        
        Actions:
        - Initializes necessary configurations for the Silver layer.
        """
        Conf = Configuration()
        self.landing_zone = Conf.data + "/data"  # Set the landing zone path
        self.checkpoint_base = Conf.checkpoint + "/checkpoints"  # Set the checkpoint base path
        self.catalog = env  # Set the catalog name
        self.db_name = Conf.db_name  # Set the database name
        spark.sql(f"USE {self.catalog}.{self.db_name}")  # Set the active Spark catalog and database

    def upsert_users(self, once=True, processing_time="15 seconds", startingVersion=0):
        """
        Upsert user data into the Silver layer.
        
        Parameters:
        - once (bool, optional): Whether to run the stream once. Default is True.
        - processing_time (str, optional): Processing time interval. Default is "15 seconds".
        - startingVersion (int, optional): Starting version for the stream. Default is 0.
        
        Returns:
        - StreamingQuery: The streaming query object.
        """
        from pyspark.sql import functions as F
        
        # Define the upsert query for user data
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.users a
            USING users_delta b
            ON a.user_id=b.user_id
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter = Upserter(query, "users_delta")
        
        # Read user delta data from the source
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         .table(f"{self.catalog}.{self.db_name}.users_registered")
                         .selectExpr("user_id", "device_id", "mac_address", "cast(timestamp as timestamp)")
                         .dropDuplicates(["user_id", "device_id"])
                   )
        
        # Define the streaming writer
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/users")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")  # Set scheduler pool
        
        # Start the stream based on 'once' and return the streaming query
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
          
    # Other upsert methods like upsert_gym_logs, upsert_user_profile, etc., follow a similar structure

    def _await_queries(self, once):
        """
        Await the completion of streaming queries.
        
        Parameters:
        - once (bool): Whether to run the stream once.
        
        Actions:
        - Waits for active streaming queries to terminate if 'once' is True.
        """
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
                
    def upsert(self, once=True):
        """
        Upsert data into the Silver layer.
        
        Parameters:
        - once (bool, optional): Whether to run the stream once. Default is True.
        
        Actions:
        - Calls individual upsert methods for different data sources.
        - Waits for streams to complete if 'once' is True.
        """
        self.upsert_users(once, processing_time)
        self.upsert_gym_logs(once, processing_time)
        self.upsert_user_profile(once, processing_time)
        self.upsert_workouts(once, processing_time)
        self.upsert_heart_rate(once, processing_time)        
        self._await_queries(once)
        self.upsert_user_bins(once, processing_time)
        self.upsert_completed_workouts(once, processing_time)        
        self._await_queries(once)
        self.upsert_workout_bpm(once, processing_time)
        self._await_queries(once)
        
        
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
        self.assert_count("users", 5 if sets == 1 else 1000)
        self.assert_count("gym_logs", 8 if sets == 1 else 1502)
        self.assert_count("user_profile", 5 if sets == 1 else 1000)
        self.assert_count("workouts", 16 if sets == 1 else 3004)
        self.assert_count("heart_rate", 778746)
        self.assert_count("user_bins", 5 if sets == 1 else 1000)
        self.assert_count("completed_workouts", 8 if sets == 1 else 1502)
        self.assert_count("workout_bpm", 3968 if sets == 1 else 772364)
