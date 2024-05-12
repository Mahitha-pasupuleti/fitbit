dbutils.widgets.text("Environment", "dev", "Environment")
dbutils.widgets.text("RunType", "once", "Batch mode")

env = dbutils.widgets.get("Environment")
once = dbutils.widgets.get("RunType")
processing_time = dbutils.widgets.get("ProcessingTime")
print(f"Starting fitbit in batch mode.")

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

%run ./Configuration.py

%run ./Table_Setup.py

TS = SetupHelper(env)
TS.setup()
TS.validate()

%run ./Bronze.py
%run ./Silver.py
%run ./Gold.py

BR = Bronze(env)
SV = Silver(env)
GD = Gold(env)

BR.consume(once)
SV.upsert(once)
GD.upsert(once)

TS.cleanup()
