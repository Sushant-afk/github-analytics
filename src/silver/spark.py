from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, BooleanType
from dotenv import load_dotenv
import os

load_dotenv()
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")


def createSparkSession():
    return SparkSession.builder\
    .appName("GitHub Analytics")\
    .config("spark.jars.ivy", "/tmp/.ivy2")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)\
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()


def main():
    spark = createSparkSession()
    actorType = StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True)
    ])
    orgType = StructType([
        StructField("id", LongType(), True),
        StructField("login", StructField(), True)
    ])
    repoType = StructType([
        StructField("id", LongType(), True),
        StructField("login", StructField(), True)
    ])
    df_schema = StructType([
        StructField("id", LongType(), True),
        StructField("created_at", DateType(), True),
        StructField("org", orgType, True),
        StructField("actor", actorType, True),
        StructField("repo", repoType, True),
        StructField("type", StringType(), True),
        StructField("payload", StringType()),
        StructField("public", BooleanType(), True)
    ])


