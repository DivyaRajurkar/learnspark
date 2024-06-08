from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder \
    .appName('SparkByExamples.com') \
    .getOrCreate()

# Define the schema with the correct field names and types
schema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ]), True),
    StructField('dob', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('salary', IntegerType(), True)
])

# Sample data
dataDF = [
    (('James', '', 'Smith'), '1991-04-01', 'M', 3000),
    (('Michael', 'Rose', ''), '2000-05-19', 'M', 4000),
    (('Robert', '', 'Williams'), '1978-09-05', 'M', 4000),
    (('Maria', 'Anne', 'Jones'), '1967-12-01', 'F', 4000),
    (('Jen', 'Mary', 'Brown'), '1980-02-17', 'F', -1)
]

# Create DataFrame
df = spark.createDataFrame(data=dataDF, schema=schema)

# Print the schema
df.printSchema()

# Show the DataFrame (optional)
df.show()

