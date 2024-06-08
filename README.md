# learnspark
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


### Explanation

1. **Schema Definition**:
    - `StructType`: This is used to define the schema of a DataFrame. It can hold multiple `StructField` objects.
    - `StructField`: This defines a field in the DataFrame. It includes the field name, data type, and a boolean indicating if null values are allowed.
    - The `name` field is a nested structure with three sub-fields: `firstname`, `middlename`, and `lastname`.

2. **Sample Data**:
    - The data is structured to match the defined schema. Each record is a tuple containing:
        - Another tuple for the `name` field (nested structure).
        - A string for the `dob` field.
        - A string for the `gender` field.
        - An integer for the `salary` field.

3. **Creating DataFrame**:
    - The `spark.createDataFrame` method is used to create a DataFrame from the provided data and schema.

4. **Printing Schema**:
    - The printSchema() method in PySpark prints the schema of the DataFrame in a readable format, showing the hierarchical structure of the 
      nested fields. Below is the output you can expect when running the printSchema() method on the DataFrame with the given schema:
** Expected Schema Output**:
root

 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- dob: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)
 
** Explanation **
Root Level:

root: This is the top level of the schema.
Nested Field (name):

**1.name:** struct (nullable = true): The name field is a struct (i.e., a nested structure) and can contain null values.
Under name, there are three nested fields:
**I)firstname:** string (nullable = true): The first name, which is a string and can be null.

**II)middlename:** string (nullable = true): The middle name, which is a string and can be null.

**III)lastname:** string (nullable = true): The last name, which is a string and can be null.
Other Fields:

**2.dob:** string (nullable = true): The date of birth, which is a string and can be null.

**3.gender:** string (nullable = true): The gender, which is a string and can be null.

**4.salary:** integer (nullable = true): The salary, which is an integer and can be null.

**output:-**

+--------------------+----------+------+------+
|                name|       dob|gender|salary|
+--------------------+----------+------+------+
|   {James, , Smith}|1991-04-01|     M|  3000|
| {Michael, Rose, }|2000-05-19|     M|  4000|
|{Robert, , Willi...|1978-09-05|     M|  4000|
|{Maria, Anne, Jo...|1967-12-01|     F|  4000|
| {Jen, Mary, Brown}|1980-02-17|     F|    -1|
+--------------------+----------+------+------+
https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e23
