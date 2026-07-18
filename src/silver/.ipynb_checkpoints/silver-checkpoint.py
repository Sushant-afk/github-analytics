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
        StructField("login", StringType(), True)
    ])
    repoType = StructType([
        StructField("id", LongType(), True),
        # StructField("login", StringType(), True),   ### to be removed, no field as login
        StructField("name", StringType(), True)
    ])
    df_schema = StructType([
        StructField("id", StringType(), True),
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
        # file_path = "s3a://github-analytics90416-669003566676-ap-southeast-2-an/archives/2026/Jun/28/03.gz"
        output_file_path = "s3a://github-analytics90416-669003566676-ap-southeast-2-an/parquet1/2026/Jun/28/03/"

        # df_raw = spark.read.option("header", "true").json(file_path, schema=df_schema)
        # print(df_raw.printSchema())
        # print("Fetch complete")
        # df_raw.write.mode('overwrite').parquet(output_file_path)

        silver_path = "s3a://github-analytics90416-669003566676-ap-southeast-2-an/silver/2026/Jun/28/03/"
        df = spark.read.parquet(output_file_path)
        df = addDatePartitions(df)
        print("addDatePartitions completed")
        df = parsePushEvent(df)
        print("parsePushEvent completed")
        df = parsePullEvent(df)
        print("parsePullEvent completed")
        df = parseIssueEvent(df)
        print("parseIssueEvent completed")
        df = addForkTag(df)
        print("addForkTag completed")
        df = df.drop(f.col("payload"))

        df.coalesce(1).write.mode("overwrite").parquet(silver_path)

    except Exception as e:
        # Catches general python, network, or OS-level file system failures
        print(f"Unexpected Pipeline Error: {str(e)}")
    


def addDatePartitions(df):
    return df.withColumn("year", f.year("created_at")).withColumn("month", f.month("created_at")).withColumn("day", f.dayofmonth("created_at"))

def parsePushEvent(df):
    pushSchema = StructType([
        StructField("push_id", LongType()),
        StructField("repository_id", StringType()),
        StructField("ref", StringType())
    ])
    df = df.withColumn("push_payload", f.when(f.col("type")=="PushEvent", f.from_json("payload", pushSchema)))
    df = df.withColumn("push_id", f.col("push_payload.push_id")).withColumn("push_repo_id", f.col("push_payload.repository_id")).withColumn("push_ref", f.col("push_payload.ref"))
    df = df.drop("push_payload")
    return df

def parsePullEvent(df):
    prSchema = StructType([
    StructField("action", StringType()),
            StructField("number", IntegerType()),
            StructField("pull_request", StructType([
            StructField("id", LongType())
        ]))
    ])
    df = df.withColumn("pr_payload", f.when(f.col("type") == "PullRequestEvent", f.from_json("payload", prSchema)))\
        .withColumn("pr_repo", f.when(f.col("type") == "PullRequestEvent", f.col("repo.name")))
    df = df.withColumn("pr_action", f.col("pr_payload.action")).withColumn("pr_number", f.col("pr_payload.number"))\
              .withColumn("pr_id", f.col("pr_payload.pull_request.id"))
    return df

def addForkTag(df):
    df = df.withColumn("is_fork", f.when(f.col("type") == "ForkEvent", f.lit("Y")))\
    .withColumn("fork_repo", f.when(f.col("type") == "ForkEvent", f.col("repo.name")))
    return df


def parseIssueEvent(df):
    issues_schema = StructType([
    StructField("action", StringType()),
    StructField("issue", StructType([
            StructField("id", LongType()),
            StructField("number", IntegerType()),
            StructField("created_at", DateType()),
            StructField("closed_at", DateType()),
            StructField("state", StringType())
        ]))
    ])
    df = df.withColumn("issue_payload", f.when(f.col("type")=="IssuesEvent", f.from_json("payload", issues_schema)))
    df = df.withColumn("issue_action", f.col("issue_payload.action")).withColumn("issue_id", f.col("issue_payload.issue.id"))\
                       .withColumn("issue_number", f.col("issue_payload.issue.number")).withColumn("issue_created_at", f.col("issue_payload.issue.created_at"))\
                       .withColumn("issue_closed_at", f.col("issue_payload.issue.closed_at")).withColumn("issue_state", f.col("issue_payload.issue.state"))
    df = df.drop("issue_payload")
    return df


if __name__ == "__main__":
    main()


