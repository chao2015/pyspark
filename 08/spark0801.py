from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def basic(spark):
    df = spark.read.json("file:///Users/chao/Documents/app/spark-2.3.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()


# Spark SQL支持两种不同的方法将现有RDD转换为数据集。 第一种方法使用反射来推断包含特定类型对象的RDD的模式。
# 这种基于反射的方法可以提供更简洁的代码，并且在编写Spark应用程序时已经了解模式时可以很好地工作。
def schema_inference_example(spark):
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("file:///Users/chao/Documents/app/spark-2.3.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people)
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    # The results of SQL queries are Dataframe objects.
    # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)
        # Name: Justin


# 创建数据集的第二种方法是通过编程接口，允许您构建模式，然后将其应用于现有RDD。
# 虽然此方法更繁琐，但它允许您在直到运行时才知道列及其类型时构造数据集。
def programmatic_schema_example(spark):
    sc = spark.sparkContext

    # Load a text file and convert each line to a Row.
    lines = sc.textFile("file:///Users/chao/Documents/app/spark-2.3.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    parts = lines.map(lambda l: l.split(","))
    # Each line is converted to a tuple.
    people = parts.map(lambda p: (p[0], p[1].strip()))

    # The schema is encoded in a string.
    schemaString = "name age"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaPeople = spark.createDataFrame(people, schema)

    # Creates a temporary view using the DataFrame
    schemaPeople.createOrReplaceTempView("people")

    # SQL can be run over DataFrames that have been registered as a table.
    results = spark.sql("SELECT name FROM people")

    results.show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+


if __name__ == '__main__':
    spark = SparkSession.builder.appName("spark0801").getOrCreate()

    # basic(spark)
    # schema_inference_example(spark)
    programmatic_schema_example(spark)

    spark.stop()
