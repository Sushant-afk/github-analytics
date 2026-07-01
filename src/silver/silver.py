from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, BooleanType
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as f
from pyspark.sql.types import *

load_dotenv()
aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")


def createSparkSession():
    return SparkSession.builder\
    .appName("GitHub Analytics")\
    .master("local[*]") \
    .config("spark.jars.ivy", "/tmp/.ivy2")\
    .config("spark.jars.packages"   , "org.apache.hadoop:hadoop-aws:3.4.2")\
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
        StructField("login", StringType(), True)
    ])
    repoType = StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True)
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

    dt = datetime.now()
    fetch_time = datetime.now(timezone.utc) - timedelta(hours=6)
    # file_path = (
    # f"s3a://github-analytics90416-669003566676-ap-southeast-2-an/"
    # f"archives/{dt.strftime('%Y')}/{dt.strftime('%b')}/"
    # f"{dt.strftime('%d')}/{fetch_time.strftime('%H')}.gz"
    # )

    try:
        output_file_path = "s3a://github-analytics90416-669003566676-ap-southeast-2-an/parquet1/2026/Jun/28/03.gz"
        file_path = "s3a://github-analytics90416-669003566676-ap-southeast-2-an/archives/2026/Jun/28/03.gz"

        df_raw = spark.read.option("header", "true").json(file_path, schema=df_schema)
        print(df_raw.printSchema())
        print("Fetch complete")
        df_raw.write.mode('overwrite').parquet(output_file_path)

    except Exception as e:
        # Catches general python, network, or OS-level file system failures
        print(f"Unexpected Pipeline Error: {str(e)}")


def addDatePartitions(df):
    return df.withColumn("year", f.year("created_at")).withColumn("month", f.month("created_at")).withColumn("day", f.dayofmonth("created_at"))

def parsePushEvent(df):
    push_schema = StructType([
        StructField("push_id", LongType()),
        StructField("size", IntegerType()),
        StructField("distinct_size", IntegerType()),
        StructField("ref", StringType()),
        StructField(
            "commits",
            ArrayType(
                StructType([
                    StructField("sha", StringType()),
                    StructField("message", StringType())
                ])
            )
        )
    ])
    df = df.withColumn("push_payload", f.when(f.col("type") == "PushEvent", f.from_json("payload", push_schema)))
    df = (
        df.withColumn("push_size", f.col("push_payload.size")).withColumn("distinct_size", f.col("push_payload.distinct_size"))
        .withColumn("branch", f.col("push_payload.ref")).withColumn("commit_count", f.size("push_payload.commits"))
    )
    return df

def parsePullEvent(df):
    pr_schema = StructType([
        StructField("action", StringType()),
        StructField(
            "pull_request",
            StructType([
                StructField("number", IntegerType()),
                StructField("state", StringType()),
                StructField("merged", BooleanType())
            ])
        )
    ])
    df = df.withColumn("pr_payload", f.when(f.col("type") == "PullRequestEvent", f.from_json("payload", pr_schema)))
    df = ( df.withColumn("pr_action", f.col("pr_payload.action")).withColumn("pr_number", f.col("pr_payload.pull_request.number"))
          .withColumn("pr_state", f.col("pr_payload.pull_request.state")).withColumn("merged", f.col("pr_payload.pull_request.merged"))
    )
    return df

if __name__ == "__main__":
    main()


