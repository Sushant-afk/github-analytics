from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
from pyspark.sql import functions as f

aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

PG_HOST = "postgres_db"
DB_NAME = os.environ.get("MART_DB")
DB_USER = os.environ.get("MART_DB_USERNAME")
DB_PASS = os.environ.get("MART_DB_PASSWORD")

def createSparkSession():
    return SparkSession.builder\
    .appName("GitHub Analytics")\
    .master("local[*]") \
    .config("spark.jars.ivy", "/tmp/.ivy2")\
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.3")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)\
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)\
    .config("spark.driver.host", "127.0.0.1")\
    .config("spark.driver.bindAddress", "127.0.0.1")\
    .getOrCreate()


def read_for_staging(spark, path):
    try:
        df = spark.read.parquet(path)
    except Exception as e:
        print(f"Failed to read in read_for_staging {e}")
        raise
    return df

def staging_main(df):
    main_tbl_df = df.select(f.col("id"), 
                            f.col("actor.login").alias("event_actor"),
                            f.col("repo.id").alias("repo_uid"), 
                            f.col("repo.name").substr(1, 50).alias("repo_name"),
                            f.col("org.id").alias("org_uid"), 
                            f.col("org.login").substr(1, 50).alias("org_name"), 
                            f.col("created_at").cast("date").alias("creation_date"), 
                            f.current_timestamp().alias("insert_time"),
                            f.col("type").alias("event_type")                          
                            )
    try:
        JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{DB_NAME}"
        print(JDBC_URL)
        main_tbl_df.write\
            .format("jdbc")\
            .option("url", JDBC_URL)\
            .option("dbtable", "staging.main_event_dtl")\
            .option("user", DB_USER)\
            .option("password", DB_PASS)\
            .option("driver", "org.postgresql.Driver")\
            .option("batchsize", 50000)\
            .option("numPartitions", 8)\
            .mode("append")\
            .save()

        print("Insert into staging.main_event_dtl completed.")
    except Exception as e:
        print(f"Failed to insert in staging_main {e}")
        raise


def staging_push(df):
    push_tbl_df = df.filter(f.col("type") == "PushEvent").select(
        f.col("id").cast("long"),
        f.col("push_repo_id").cast("string").alias("repo_id"),  # Forces clean string mapping
        f.col("push_id").cast("string").alias("push_id"),        # Forces clean string mapping
        f.col("push_ref").substr(1, 50).cast("string").alias("ref"),
        f.current_timestamp().alias("insert_time")
    )
    try:
        JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{DB_NAME}"
        print(JDBC_URL)
        push_tbl_df.write\
            .format("jdbc")\
            .option("url", JDBC_URL)\
            .option("dbtable", "staging.push_event_dtl")\
            .option("user", DB_USER)\
            .option("password", DB_PASS)\
            .option("driver", "org.postgresql.Driver")\
            .option("batchsize", 50000)\
            .option("numPartitions", 8)\
            .mode("append")\
            .save()

        print("Insert into staging.staging_push completed.")
    except Exception as e:
        print(f"Failed to insert in staging_main {e}")
        raise

def staging_pull(df):
    pull_tbl_df = df.filter(f.col("type") == "PullRequestEvent").select(f.col("id").cast("long"), 
                            f.col("pr_action").alias("action_type"),
                            f.col("pr_id").alias("pull_id"),
                            f.col("pr_number").cast("int").alias("pull_num"),
                            f.col("pr_repo").substr(1,50).alias("repo_name"),
                            f.current_timestamp().alias("insert_time")
                            )
    try:
        JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{DB_NAME}"
        print(JDBC_URL)
        pull_tbl_df.write\
            .format("jdbc")\
            .option("url", JDBC_URL)\
            .option("dbtable", "staging.PULL_EVENT_DTL")\
            .option("user", DB_USER)\
            .option("password", DB_PASS)\
            .option("driver", "org.postgresql.Driver")\
            .option("batchsize", 50000)\
            .option("numPartitions", 8)\
            .mode("append")\
            .save()

        print("Insert into staging.PULL_EVENT_DTL completed.")
    except Exception as e:
        print(f"Failed to insert in staging_pull {e}")
        raise

def staging_issue(df):
    issue_tbl_df = df.filter(f.col("type")=="IssuesEvent").select(f.col("id").cast("long"), 
                            f.col("issue_action").substr(1, 50).alias("ACTION_TYPE"),
                            f.col("issue_id").alias("ISSUE_ID"),
                            f.col("issue_state").alias("STATUS"),
                            f.col("issue_number").cast("int").alias("ISSUE_NUM"),
                            f.col("issue_created_at").cast("date").alias("CREATION_DATE"),
                            f.col("issue_closed_at").cast("date").alias("CLOSURE_DATE"),
                            f.current_timestamp().alias("insert_time")
                            )
    try:
        JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/{DB_NAME}"
        print(JDBC_URL)
        issue_tbl_df.write\
            .format("jdbc")\
            .option("url", JDBC_URL)\
            .option("dbtable", "staging.ISSUE_EVENT_DTL")\
            .option("user", DB_USER)\
            .option("password", DB_PASS)\
            .option("driver", "org.postgresql.Driver")\
            .option("batchsize", 50000)\
            .option("numPartitions", 8)\
            .mode("append")\
            .save()

        print("Insert into staging.ISSUE_EVENT_DTL completed.")
    except Exception as e:
        print(f"Failed to insert in staging_issue {e}")
        raise


def main():
    # print(PG_HOST)
    spark = createSparkSession()
    path = "s3a://github-analytics90416-669003566676-ap-southeast-2-an/silver/2026/Jun/28/03/"
    df = read_for_staging(spark, path)
    # print(DB_NAME, DB_PASS, DB_USER)
    # staging_main(df)
    # staging_push(df)
    # staging_pull(df)
    # staging_issue(df)
    # print(aws_secret_key, aws_access_key, DB_USER, BUCKET_NAME)




if __name__ == '__main__':
    main()
