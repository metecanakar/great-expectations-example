from pyspark.sql import SparkSession
import great_expectations as gx


# ----------------------------
# Initialize GX context & Spark
# ----------------------------
context = gx.get_context()

spark = SparkSession.builder.appName("GE_Schema_Test").getOrCreate()

# Sample data
data = [
    ("Alice", 25, "F"),
    ("Bob", 32, "M"),
    ("Charlie", 29, "M"),
    ("Diana", 900, "F")
]
df = spark.createDataFrame(data, ["name", "age", "gender"])

# ----------------------------
# Create Data Source
# ----------------------------
data_source_name = "my_spark_data_source"
data_source = context.data_sources.add_spark(name=data_source_name)

# ----------------------------
# Create Data Asset
# ----------------------------
data_asset_name = "my_dataframe_asset"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# ----------------------------
# Create Batch Definition
# ----------------------------
batch_definition_name = "my_batch"
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# ----------------------------
# Provide runtime batch parameters
# ----------------------------
batch_parameters = {"dataframe": df}

# ----------------------------
# Get the batch and validate
# ----------------------------
batch = batch_definition.get_batch(batch_parameters=batch_parameters)

age_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="age",
    min_value=0,
    max_value=200)

validation_results = batch.validate(age_expectation)

print(validation_results)

