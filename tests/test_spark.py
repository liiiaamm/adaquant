from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("local-bronze-test")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.hadoop:hadoop-aws:3.4.1",
            "com.amazonaws:aws-java-sdk-bundle:1.12.696"
        ])
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)

path = "s3a://adaquant-data-bucket/bronze/ada/"

df = spark.read.option("header", True).csv(path)

df.printSchema()
df.show(5)
