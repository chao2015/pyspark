from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf


# spark与elasticsearch的整合

def get_grade(value):
    if value <= 50 and value >= 0:
        return "健康"
    elif value <= 100:
        return "中等"
    elif value <= 150:
        return "对敏感人群不健康"
    elif value <= 200:
        return "不健康"
    elif value <= 300:
        return "非常不健康"
    elif value <= 500:
        return "危险"
    elif value > 500:
        return "爆表"
    else:
        return None


if __name__ == '__main__':
    spark = SparkSession.builder.appName("project").getOrCreate()

    data2015 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/Beijing_2015_HourlyPM25_created20160201.csv").select("Year", "Month", "Day", "Hour", "Value", "QC Name")
    data2016 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/Beijing_2016_HourlyPM25_created20170201.csv").select("Year", "Month", "Day", "Hour", "Value", "QC Name")
    data2017 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/data/Beijing_2017_HourlyPM25_created20170803.csv").select("Year", "Month", "Day", "Hour", "Value", "QC Name")

    grade_function_udf = udf(get_grade, StringType())

    group2015 = data2015.withColumn("Grade", grade_function_udf(data2015['Value'])).groupBy("Grade").count()
    group2016 = data2016.withColumn("Grade", grade_function_udf(data2016['Value'])).groupBy("Grade").count()
    group2017 = data2017.withColumn("Grade", grade_function_udf(data2017['Value'])).groupBy("Grade").count()

    result2015 = group2015.select("Grade", "count").withColumn("percent", group2015['count'] / data2015.count() * 100)
    result2016 = group2016.select("Grade", "count").withColumn("percent", group2016['count'] / data2016.count() * 100)
    result2017 = group2017.select("Grade", "count").withColumn("percent", group2017['count'] / data2017.count() * 100)

    result2015.selectExpr("Grade as grade", "count", "percent").write.format("org.elasticsearch.spark.sql").option("es.nodes","chao2:9200").mode("overwrite").save("weather2015/pm")
    result2016.selectExpr("Grade as grade", "count", "percent").write.format("org.elasticsearch.spark.sql").option("es.nodes","chao2:9200").mode("overwrite").save("weather2016/pm")
    result2017.selectExpr("Grade as grade", "count", "percent").write.format("org.elasticsearch.spark.sql").option("es.nodes","chao2:9200").mode("overwrite").save("weather2017/pm")

    spark.stop()
