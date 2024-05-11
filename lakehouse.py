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

class Silver():
    def __init__(self, env):
        Conf = Configuration()
        self.landing_zone = Conf.data + "/data"
        self.checkpoint_base = Conf.checkpoint + "/checkpoints"        
        self.catalog = env
        self.db_name = Conf.db_name
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def upsert_users(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        #Idempotent - User cannot register again so ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.users a
            USING users_delta b
            ON a.user_id=b.user_id
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "users_delta")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         .table(f"{self.catalog}.{self.db_name}.users_registered")
                         .selectExpr("user_id", "device_id", "mac_address", "cast(timestamp as timestamp)")
                         .dropDuplicates(["user_id", "device_id"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/users")
                        )

        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "silver_p2")
        
        if once == True:
            return stream_writer.trigger(availableNow=True).start()
        else:
            return stream_writer.trigger(processingTime=processing_time).start()
        
          
    def upsert_gym_logs(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.gym_logs a
            USING gym_logs_delta b
            ON a.mac_address=b.mac_address AND a.gym=b.gym AND a.login=b.login
            WHEN MATCHED AND b.logout > a.login AND b.logout > a.logout
              THEN UPDATE SET logout = b.logout
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "gym_logs_delta")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         .table(f"{self.catalog}.{self.db_name}.gym_logins_bz")
                         .selectExpr("mac_address", "gym", "cast(login as timestamp)", "cast(logout as timestamp)")
                         .dropDuplicates(["mac_address", "gym", "login"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/gym_logs")
                        )
        
        return stream_writer.trigger(availableNow=True).start()
        
    
    def upsert_user_profile(self, once=False, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F

        schema = """
            user_id bigint, update_type STRING, timestamp FLOAT, 
            dob STRING, sex STRING, gender STRING, first_name STRING, last_name STRING, 
            address STRUCT<street_address: STRING, city: STRING, state: STRING, zip: INT>
            """
        
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.user_profile a
            USING user_profile_cdc b
            ON a.user_id=b.user_id
            WHEN MATCHED AND a.updated < b.updated
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
            """
        
        df_cdc = (spark.readStream
                       .option("startingVersion", startingVersion)
                       .option("ignoreDeletes", True)
                       .table(f"{self.catalog}.{self.db_name}.kafka_dumps")
                       .filter("topic = 'user_info'")
                       .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                       .select("v.*")
                       .select("user_id", F.to_date('dob','MM/dd/yyyy').alias('dob'),
                               'sex', 'gender','first_name','last_name', 'address.*',
                               F.col('timestamp').cast("timestamp").alias("updated"),
                               "update_type")
                       .dropDuplicates(["user_id", "updated"])
                 )
    
        stream_writer = (df_cdc.writeStream
                               .foreachBatch(data_upserter.upsert) 
                               .outputMode("update") 
                               .option("checkpointLocation", f"{self.checkpoint_base}/user_profile") 
                        )
        
        return stream_writer.trigger(availableNow=True).start()
        
    
    def upsert_workouts(self, once=False, processing_time="10 seconds", startingVersion=0):
        from pyspark.sql import functions as F        
        schema = "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"
        
        #Idempotent - User cannot have two workout sessions at the same time. 
        #So ignore the duplicates and insert the new records
        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.workouts a
            USING workouts_delta b
            ON a.user_id=b.user_id AND a.time=b.time
            WHEN NOT MATCHED THEN INSERT *
            """

        data_upserter=Upserter(query, "workouts_delta")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         .table(f"{self.catalog}.{self.db_name}.kafka_dumps")
                         .filter("topic = 'workout'")
                         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                         .select("v.*")
                         .select("user_id", "workout_id", 
                                 F.col("timestamp").cast("timestamp").alias("time"), 
                                 "action", "session_id")
                         .dropDuplicates(["user_id", "time"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation",f"{self.checkpoint_base}/workouts")
                                 .queryName("workouts_upsert_stream")
                        )
        
        return stream_writer.trigger(availableNow=True).start()
        
        
    def upsert_heart_rate(self, once=False, startingVersion=0):
        from pyspark.sql import functions as F
        
        schema = "device_id LONG, time TIMESTAMP, heartrate DOUBLE"
        
        #Idempotent - Only one BPM signal is allowed at a timestamp. So ignore the duplicates and insert the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.heart_rate a
        USING heart_rate_delta b
        ON a.device_id=b.device_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "heart_rate_delta")
    
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         .table(f"{self.catalog}.{self.db_name}.kafka_dump")
                         .filter("topic = 'bpm'")
                         .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                         .select("v.*", F.when(F.col("v.heartrate") <= 0, False).otherwise(True).alias("valid"))
                         .withWatermark("time", "30 seconds")
                         .dropDuplicates(["device_id", "time"])
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/heart_rate")
                                 .queryName("heart_rate_upsert_stream")
                        )
        
        return stream_writer.trigger(availableNow=True).start()

    
    def upsert_user_bins(self, once=True, startingVersion=0):
        from pyspark.sql import functions as F
        
        # Idempotent - This table is maintained as SCD Type 1 dimension
        #            - Insert new user_id records 
        #            - Update old records using the user_id

        query = f"""
            MERGE INTO {self.catalog}.{self.db_name}.user_bins a
            USING user_bins_delta b
            ON a.user_id=b.user_id
            WHEN MATCHED 
              THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        
        data_upserter=Upserter(query, "user_bins_delta")
        
        df_user = spark.table(f"{self.catalog}.{self.db_name}.users").select("user_id")
        
        # Running stream on silver table requires ignoreChanges
        # No watermark required - Stream to staic join is stateless
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreChanges", True)
                         .table(f"{self.catalog}.{self.db_name}.user_profile")
                         .join(df_user, ["user_id"], "left")
                         .select("user_id", self.age_bins(F.col("dob")),"gender", "city", "state")
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/user_bins")
                        )

        return stream_writer.trigger(availableNow=True).start()

        
                  
            
    def upsert_completed_workouts(self, once=True, startingVersion=0):
        from pyspark.sql import functions as F
              
        #Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.completed_workouts a
        USING completed_workouts_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "completed_workouts_delta")
    
        df_start = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         .table(f"{self.catalog}.{self.db_name}.workouts")
                         .filter("action = 'start'")                         
                         .selectExpr("user_id", "workout_id", "session_id", "start_time")
                   )
        
        # df_start.display()
        
        
        df_stop = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .option("ignoreDeletes", True)
                         .table(f"{self.catalog}.{self.db_name}.workouts")
                         .filter("action = 'stop'")                         
                         .selectExpr("user_id", "workout_id", "session_id", "end_time")
                   )
        
        # df_stop.display()

        join_condition = [df_start.user_id == df_stop.user_id, df_start.workout_id==df_stop.workout_id, df_start.session_id==df_stop.session_id, 
                          df_stop.end_time < df_start.start_time + F.expr('interval 3 hour')]         
        
        df_delta = (df_start.join(df_stop, join_condition)
                            .select(df_start.user_id, df_start.workout_id, df_start.session_id, df_start.start_time, df_stop.end_time)
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("append")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/completed_workouts")
                        )
        
        return stream_writer.trigger(availableNow=True).start()
    
    
    
    def upsert_workout_bpm(self, once=True, startingVersion=0):
        from pyspark.sql import functions as F
              
        #Idempotent - Only one user workout session completes. So ignore the duplicates and insert the new records
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.workout_bpm a
        USING workout_bpm_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id AND a.time=b.time
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "workout_bpm_delta")        
        
        df_users = spark.read.table("users")
        
        df_completed_workouts = (spark.readStream
                                      .option("startingVersion", startingVersion)
                                      .option("ignoreDeletes", True)
                                      .table(f"{self.catalog}.{self.db_name}.completed_workouts")
                                      .join(df_users, "user_id")
                                      .selectExpr("user_id", "device_id", "workout_id", "session_id", "start_time", "end_time")
                                 )
        
        df_bpm = (spark.readStream
                       .option("startingVersion", startingVersion)
                       .option("ignoreDeletes", True)
                       .table(f"{self.catalog}.{self.db_name}.heart_rate")
                       .filter("valid = True")                         
                       .selectExpr("device_id", "time", "heartrate")
                   )
        
        join_condition = [df_completed_workouts.device_id == df_bpm.device_id, 
                          df_bpm.time > df_completed_workouts.start_time, df_bpm.time <= df_completed_workouts.end_time,
                          df_completed_workouts.end_time < df_bpm.time + F.expr('interval 3 hour')] 
        
        df_delta = (df_bpm.join(df_completed_workouts, join_condition)
                          .select("user_id", "workout_id","session_id", "start_time", "end_time", "time", "heartrate")
                   )
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("append")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm")
                        )
        
        return stream_writer.trigger(availableNow=True).start()
       
    def _await_queries(self, once):
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
                
    def upsert(self, once=True):
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
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}"  
        
    def validate(self, sets):
        self.assert_count("users", 5 if sets == 1 else 1000)
        self.assert_count("gym_logs", 8 if sets == 1 else 1502)
        self.assert_count("user_profile", 5 if sets == 1 else 1000)
        self.assert_count("workouts", 16 if sets == 1 else 3004)
        self.assert_count("heart_rate", 778746)
        self.assert_count("user_bins", 5 if sets == 1 else 1000)
        self.assert_count("completed_workouts", 8 if sets == 1 else 1502)
        self.assert_count("workout_bpm", 3968 if sets == 1 else 772364)

class Gold():
    def __init__(self, env):
        Conf = Configuration()
        self.landing_zone = Conf.data + "/data"
        self.checkpoint_base = Conf.checkpoint + "/checkpoints"        
        self.catalog = env
        self.db_name = Conf.db_name
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        
    def upsert_workout_bpm_summary(self, once=True, processing_time="15 seconds", startingVersion=0):
        from pyspark.sql import functions as F
        
        query = f"""
        MERGE INTO {self.catalog}.{self.db_name}.workout_bpm_summary a
        USING workout_bpm_summary_delta b
        ON a.user_id=b.user_id AND a.workout_id = b.workout_id AND a.session_id=b.session_id
        WHEN NOT MATCHED THEN INSERT *
        """
        
        data_upserter=Upserter(query, "workout_bpm_summary_delta")
        
        df_users = spark.read.table(f"{self.catalog}.{self.db_name}.user_bins")
        
        df_delta = (spark.readStream
                         .option("startingVersion", startingVersion)
                         .table(f"{self.catalog}.{self.db_name}.workout_bpm")
                         .groupBy("user_id", "workout_id", "session_id", "end_time")
                         .agg(F.min("heartrate").alias("min_bpm"), F.mean("heartrate").alias("avg_bpm"), 
                              F.max("heartrate").alias("max_bpm"), F.count("heartrate").alias("num_recordings"))                         
                         .join(df_users, ["user_id"])
                         .select("workout_id", "session_id", "user_id", "age", "gender", "city", "state", "min_bpm", "avg_bpm", "max_bpm", "num_recordings")
                     )
        
        # df_delta.display()
        
        stream_writer = (df_delta.writeStream
                                 .foreachBatch(data_upserter.upsert)
                                 .outputMode("update")
                                 .option("checkpointLocation", f"{self.checkpoint_base}/workout_bpm_summary")
                        )
        
        return stream_writer.trigger(availableNow=True).start()
    
    
    def upsert(self, once=True, processing_time="5 seconds"):
        self.upsert_workout_bpm_summary(once, processing_time)
        if once:
            for stream in spark.streams.active:
                stream.awaitTermination()
        
        
    def assert_count(self, table_name, expected_count, filter="true"):
        print(f"Validating record counts in {table_name}...", end='')
        actual_count = spark.read.table(f"{self.catalog}.{self.db_name}.{table_name}").where(filter).count()
        assert actual_count == expected_count, f"Expected {expected_count:,} records, found {actual_count:,} in {table_name} where {filter}" 
        print(f"Found {actual_count:,} / Expected {expected_count:,} records where {filter}: Success") 
        
    def assert_rows(self, location, table_name, sets):
        print(f"Validating records in {table_name}...", end='')
        expected_rows = spark.read.format("parquet").load(f"{self.test_data_dir}/{location}_{sets}.parquet").collect()
        actual_rows = spark.table(table_name).collect()
        assert expected_rows == actual_rows, f"Expected data mismatches with the actual data in {table_name}"
        print(f"Expected data matches with the actual data in {table_name}: Success")
        
        
    def validate(self, sets):    
        self.assert_count("gym_summary", 1502)       
        if sets>1:
            self.assert_count("workout_bpm_summary", 1000)
        print(f"Gold layer validation completed")        
