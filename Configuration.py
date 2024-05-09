class Configuration():    
    def __init__(self):      
        self.data = spark.sql("`data_zone`").select("url").collect()[0][0]
        self.checkpoint = spark.sql("`checkpoint`").select("url").collect()[0][0]
        self.kafkaTopic = spark.sql("`kafkaTopic`").select("url").collect()[0][0]
        self.db_name = "fitbit_db"
        self.maxFilesPerTrigger = 1000
