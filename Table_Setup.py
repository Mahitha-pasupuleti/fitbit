class SetupHelper():   
    def __init__(self, env):
        Conf = Configuration()
        self.landing_zone = Conf.data + "/data"
        self.checkpoint_base = Conf.checkpoint + "/checkpoints"        
        self.catalog = env
        self.db_name = Conf.db_name
        self.started = true
        
    def new_DB(self):
        print(f"Creating the database {self.catalog}.{self.db_name}...", end='')
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.db_name}")
        spark.sql(f"USE {self.catalog}.{self.db_name}")
        print("New DB created")
        
    def create_users_registered(self):
        if(self.started):
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.users_registered(
                    user_id long,
                    device_id long, 
                    mac_address string, 
                    registration_timestamp double,
                    load_time timestamp,
                    source_file string                    
                    )
                  """) 
            print("Table created")
        else:
            raise ReferenceError("Table creation failed!")
            
    
    def create_gym_logs(self):
        if(self.started):
            print(f"Creating gym_logs table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                    mac_address string,
                    gym_address long,
                    logged_in double,                      
                    logged_out double,                    
                    timestamp timestamp
                    )
                  """) 
            print("Table created")
        else:
            raise ReferenceError("startedTable creation failed!")
            
            
    def create_kafka_dump(self):
        if(self.started):
            print(f"Creating kafka_dump table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.kafka_dump(
                  key string, 
                  value string, 
                  topic string, 
                  partition bigint, 
                  offset bigint, 
                  timestamp bigint,                  
                  date date, 
                  week_part string,                  
                  load_time timestamp,
                  source_file string)
                  PARTITIONED BY (topic, week_part)
                  """) 
            print("Table created")
        else:
            raise ReferenceError("startedTable creation failed!")       
    
            
    def create_users(self):
        if(self.started):
            print(f"Creating users table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.users(
                    userId bigint, 
                    deviceId bigint, 
                    mac_address string,
                    timestamp timestamp
                    )
                  """)  
            print("Table created")
        else:
            raise ReferenceError("startedTable creation failed!")            
    
    def create_gym_logs(self):
        if(self.started):
            print(f"Creating gym_logs table...", end='')
            spark.sql(f"""CREATE OR REPLACE TABLE {self.catalog}.{self.db_name}.gym_logs(
                    mac_address string,
                    gymId bigint,
                    login timestamp,                      
                    logout timestamp
                    )
                  """) 
            print("Table created")
        else:
            raise ReferenceError("startedTable creation failed!")
            
    def create_user_profile(self):
        if(self.started):
            print(f"Creating user_profile table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_profile(
                    userId long, 
                    dob DATE, 
                    sex STRING, 
                    gender STRING, 
                    first_name STRING, 
                    last_name STRING, 
                    street_address STRING, 
                    city STRING, 
                    state STRING, 
                    zip INT, 
                    timestamp TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("startedTable creation failed!")

    def create_heart_rate(self):
        if(self.started):
            print(f"Creating heart_rate table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.heart_rate(
                    deviceId LONG, 
                    timestamp TIMESTAMP, 
                    heartrate DOUBLE, 
                    valid BOOLEAN)
                  """)
            print("Done")
        else:
            raise ReferenceError("startedTable creation failed!")

            
    def create_user_bins(self):
        if(self.started):
            print(f"Creating user_bins table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.user_bins(
                    user_id BIGINT, 
                    age STRING, 
                    gender STRING, 
                    city STRING, 
                    state STRING)
                  """)  
            print("Done")
        else:
            raise ReferenceError("startedTable creation failed!")
            
            
    def create_workouts(self):
        if(self.started):
            print(f"Creating workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workouts(
                    user_id INT, 
                    workout_id INT, 
                    time TIMESTAMP, 
                    action STRING, 
                    session_id INT)
                  """)  
            print("Done")
        else:
            raise ReferenceError("startedTable creation failed!")
            
            
    def create_completed_workouts(self):
        if(self.started):
            print(f"Creating completed_workouts table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.completed_workouts(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT, 
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP)
                  """)  
            print("Done")
        else:
            raise ReferenceError("startedTable creation failed!")
            
            
    def create_workout_bpm(self):
        if(self.started):
            print(f"Creating workout_bpm table...", end='')
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm(
                    user_id INT, 
                    workout_id INT, 
                    session_id INT,
                    start_time TIMESTAMP, 
                    end_time TIMESTAMP,
                    time TIMESTAMP, 
                    heartrate DOUBLE)
                  """)  
            print("Done")
        else:
            raise ReferenceError("startedTable creation failed!")
            
    def create_workout_bpm_summary(self):
        if(self.started):
            spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.workout_bpm_summary(
                    workout_id INT, 
                    session_id INT, 
                    user_id BIGINT,
                    min_bpm DOUBLE, 
                    avg_bpm DOUBLE, 
                    max_bpm DOUBLE, 
                    num_recordings BIGINT)
                  """)
            print("Done")
        else:
            raise ReferenceError("started Table creation failed!")
            
    def create_gym_summary(self):
        if(self.started):
            spark.sql(f"""CREATE OR REPLACE VIEW {self.catalog}.{self.db_name}.gym_summary AS
                            SELECT to_date(login::timestamp) date,
                            gym, l.mac_address, workout_id, session_id, 
                            round((logout::long - login::long)/60,2) minutes_in_gym,
                            round((end_time::long - start_time::long)/60,2) minutes_exercising
                            FROM gym_logs l 
                            JOIN (
                            SELECT mac_address, workout_id, session_id, start_time, end_time
                            FROM completed_workouts w INNER JOIN users u ON w.user_id = u.user_id) w
                            ON l.mac_address = w.mac_address 
                            AND w. start_time BETWEEN l.login AND l.logout
                            order by date, gym, l.mac_address, session_id
                        """)
            print("Done")
        else:
            raise ReferenceError("startedTable creation failed!")
            
    def setup(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup ...")
        self.new_DB()       
        self.create_users_registered()
        self.create_gym_logs() 
        self.create_kafka_dump()        
        self.create_users()
        self.create_gym_logs()
        self.create_user_profile()
        self.create_heart_rate()
        self.create_workouts()
        self.create_completed_workouts()
        self.create_workout_bpm()
        self.create_user_bins()
        self.create_workout_bpm_summary()  
        self.create_gym_summary()
        print(f"Setup completed in {int(time.time()) - start} seconds")
        
    def validate(self):
        import time
        start = int(time.time())
        print(f"\nStarting setup validation ...")
        assert spark.sql(f"SHOW DATABASES IN {self.catalog}") \
                    .filter(f"databaseName == '{self.db_name}'") \
                    .count() == 1, f"The database '{self.catalog}.{self.db_name}' is missing"
        print(f"Found database {self.catalog}.{self.db_name}: Success")
        self.assert_table("users_registered")   
        self.assert_table("gym_logs")
        self.assert_table("kafka_dump")
        self.assert_table("users")
        self.assert_table("gym_logs")
        self.assert_table("user_profile")
        self.assert_table("heart_rate")
        self.assert_table("workouts")
        self.assert_table("completed_workouts")
        self.assert_table("workout_bpm")
        self.assert_table("workout_bpm_summary") 
        self.assert_table("gym_summary")
        print(f"Setup validation completed in {int(time.time()) - start} seconds")
        
    def cleanup(self): 
        if spark.sql(f"SHOW DATABASES IN {self.catalog}").filter(f"databaseName == '{self.db_name}'").count() == 1:
            print(f"Dropping the database {self.catalog}.{self.db_name}...", end='')
            spark.sql(f"DROP DATABASE {self.catalog}.{self.db_name} CASCADE")
            print("Done")
        print(f"Deleting {self.landing_zone}...", end='')
        dbutils.fs.rm(self.landing_zone, True)
        print("Done")
        print(f"Deleting {self.checkpoint_base}...", end='')
        dbutils.fs.rm(self.checkpoint_base, True)
        print("Done")    
