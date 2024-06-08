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

**use case1:-**
**1. PySpark withColumnRenamed –** To rename DataFrame column name
PySpark has a withColumnRenamed() function on DataFrame to change a column name. This is the most straight forward approach; this function takes two parameters; the first is your existing column name and the second is the new column name you wish for.

**PySpark withColumnRenamed() Syntax:** 
withColumnRenamed(existingName, newName)
df2 = df.withColumnRenamed("dob","DateOfBirth")

**use case2:-**
**2. PySpark withColumnRenamed** – To rename multiple columns
To change multiple column names, we should chain withColumnRenamed functions as shown below. You can also store all columns to rename in a list and loop through to rename all columns, I will leave this to you to explore.


df2 = df.withColumnRenamed("dob","DateOfBirth") \
    .withColumnRenamed("salary","salary_amount")
df2.printSchema()
This creates a new DataFrame “df2” after renaming dob and salary columns.
**use case3:-**
**3. Using PySpark StructType** – To rename a nested column in Dataframe
Changing a column name on nested data is not straight forward and we can do this by creating a new schema with new DataFrame columns using StructType and use it using cast function 

# Define the new schema for the 'name' column with renamed fields
new_name_schema = StructType([
    StructField('fname', StringType(), True),
    StructField('middlename', StringType(), True),
    StructField('lname', StringType(), True)
])

# Select columns and cast the 'name' column to the new schema
new_df = df.select(
    col('name').cast(new_name_schema).alias('name'),
    col('dob'),
    col('gender'),
    col('salary')
)
![image](https://github.com/DivyaRajurkar/learnspark/assets/75663254/889f13a4-eb73-4e27-8c6e-1288a72ef01a)
**use case4:-**
**4. Using Select – To rename nested elements**
# Select and rename the nested columns
new_df = df.select(
    struct(
        col('name.firstname').alias('fname'),
        col('name.middlename').alias('middlename'),
        col('name.lastname').alias('lname')
    ).alias('name'),
    col('dob'),
    col('gender'),
    col('salary')
)
![image](https://github.com/DivyaRajurkar/learnspark/assets/75663254/b98f1623-aa34-4bce-86c4-03369536914a)
**use case4:-**
# Example 5
5. Using PySpark DataFrame withColumn – To rename nested columns
When you have nested columns on PySpark DatFrame and if you want to rename it, use withColumn on a data frame object to create a new column from an existing and we will need to drop the existing column. Below example creates a “fname” column from “name.firstname” and drops the “name” column
df4 = df.withColumn("fname",col("name.firstname")) \
      .withColumn("mname",col("name.middlename")) \
      .withColumn("lname",col("name.lastname")) \
      .drop("name")
df4.printSchema()
**use case4:-**
**Example 6**
**. Using toDF() – To change all columns in a PySpark DataFrame**
When we have data in a flat structure (without nested) , use toDF() with a new schema to change all column names.
newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(*newColumns).printSchema()
