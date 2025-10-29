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
    ("Diana", 900, "F"),
    ("Mete", None, "F")
]
df = spark.createDataFrame(data, ["name", "age", "gender"])

# ----------------------------
# Create Data Source
# - Registers a data source in GX. This is how GX knows where your data comes from.
# - add_spark tells GX the data source is a Spark DataFrame.
# - After this, GX can manage and validate batches of data from Spark.
# ----------------------------
data_source_name = "my_spark_data_source"
data_source = context.data_sources.add_spark(name=data_source_name)

# ----------------------------
# Create Data Asset
# - A dataframe Data Assset is used to group your validation results.
# - If you have a pipeline with three stages and you want the validation results for each stage to be grouped together, you would create a Data Asset with a unique name representing each stage
# - A Data Asset represents a dataset in GX.
# - Here, you’re creating an asset corresponding to the Spark DataFrame you want to validate.
# ----------------------------
data_asset_name = "my_dataframe_asset"
data_asset = data_source.add_dataframe_asset(name=data_asset_name)

# ----------------------------
# Create Batch Definition
# - GX works with batches of data (think snapshots or subsets of a dataset).
# - add_batch_definition_whole_dataframe tells GX you want to validate the entire DataFrame as one batch.
# - batch_definition_name is just an identifier for this batch.
# - Typically, a Batch Definition is used to describe how the data within a Data Asset should be retrieved. With dataframes, all of the data in a given dataframe will always be retrieved as a Batch.
# ----------------------------
batch_definition_name = "my_batch"
batch_definition = data_asset.add_batch_definition_whole_dataframe(batch_definition_name)

# ----------------------------
# Provide runtime batch parameters
# - GX allows you to pass runtime data instead of reading from a file.
# - Here, you pass the Spark DataFrame df as a batch parameter.
# - Because dataframes exist in memory and cease to exist when a Python session ends the dataframe itself is not saved as part of a Data Assset or Batch Definition.
# - Instead, a dataframe created in the current Python session is passed in at runtime as a Batch Parameter dictionary.
# ----------------------------
batch_parameters = {"dataframe": df}


# ----------------------------
# Get the batch and validate
# ----------------------------

# Get the batch using the DataFrame you provided.
batch = batch_definition.get_batch(batch_parameters=batch_parameters)

# 1. Define an expectation: age column should have values between 0 and 200
age_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="age",
    min_value=0,
    max_value=200)

# 2. Age should not be null
age_not_null_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
    column="age"
)

lst_expectations = [age_expectation, age_not_null_expectation]

for expectation in lst_expectations:
    # checks the batch against the expectation
    validation_results = batch.validate(expect=expectation)

    # outputs the validation report.
    print(f"Validation Results for expectation: {expectation}")
    print(validation_results)

