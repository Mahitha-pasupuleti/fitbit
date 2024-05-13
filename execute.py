# Define widgets to capture environment, run type
dbutils.widgets.text("Environment", "dev", "Environment")  # Widget to capture environment (default: dev)
dbutils.widgets.text("RunType", "once", "Batch mode")  # Widget to capture run type (default: once, batch mode)

# Get values of widgets
env = dbutils.widgets.get("Environment")  # Get the value of the environment widget
once = dbutils.widgets.get("RunType")  # Get the value of the run type widget

# Print starting message based on run type
print(f"Starting fitbit in {once} mode.")  # Print a message indicating the start of the process in the specified mode

# Set Spark configurations for optimization and Delta Lake
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)  # Set the number of shuffle partitions
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)  # Enable Delta Lake optimization for write operations
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)  # Enable Delta Lake auto-compaction
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")  # Specify the state store provider for Spark streaming

# Run configuration setup script
%run ./Configuration.py  # Run the script to set up configurations

# Run table setup script
%run ./Table_Setup.py  # Run the script to set up tables

# Instantiate and setup SetupHelper for environment setup
TS = SetupHelper(env)  # Instantiate SetupHelper class with the specified environment
TS.setup()  # Perform setup tasks based on the environment
TS.validate()  # Validate the setup

# Run scripts for processing data through Bronze, Silver, and Gold layers
%run ./Bronze.py
%run ./Silver.py
%run ./Gold.py

# Instantiate Bronze, Silver, and Gold classes
BR = Bronze(env)
SV = Silver(env)
GD = Gold(env)

# Process data through each layer based on run type
BR.consume(once)
SV.upsert(once)
GD.upsert(once)  

# Clean up resources after processing
TS.cleanup()
