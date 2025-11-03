#!/usr/bin/env python3
import argparse
import os
import shutil
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, count, lower


# -------------------------------
# Spark session builder
# -------------------------------
def build_spark(app_name: str, master_url: str | None = None) -> SparkSession:
    """Build a SparkSession configured for AWS EC2 + S3A."""
    builder = SparkSession.builder.appName(app_name)

    if master_url:
        builder = builder.master(master_url)

    # EC2 instance credentials for S3
    builder = builder.config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    )

    # S3 options
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.fast.upload", "true")
    builder = builder.config("spark.sql.shuffle.partitions", "200")

    return builder.getOrCreate()


# -------------------------------
# Helper: write single CSV (robust)
# -------------------------------
def write_single_csv(df, final_path: str, header: bool = True) -> None:
    """Safely write a DataFrame as a single local CSV file."""
    if df.rdd.isEmpty():
        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        with open(final_path, "w") as f:
            if header:
                f.write(",".join(df.columns) + "\n")
        print(f"⚠️ Empty DataFrame: wrote empty CSV to {final_path}")
        return

    tmp_dir = final_path + "_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", header).csv(f"file://{tmp_dir}")

    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        print(f"[DEBUG] tmp_dir exists? {os.path.exists(tmp_dir)}")
        print(f"[DEBUG] Files under tmp_dir: {os.listdir(tmp_dir) if os.path.exists(tmp_dir) else 'N/A'}")
        raise RuntimeError(f"Could not find part file under {tmp_dir}")

    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    shutil.move(part_files[0], final_path)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"✅ Wrote single CSV: {final_path}")


# -------------------------------
# Input path resolution
# -------------------------------
def resolve_input_path(net_id: str,
                       bucket_override: str | None = None,
                       local_sample: str | None = None) -> str:
    """Determine input path for Spark."""
    if local_sample:
        return local_sample.rstrip("/") + "/"

    if bucket_override:
        bucket_uri = bucket_override if bucket_override.startswith("s3a://") else f"s3a://{bucket_override}"
        return f"{bucket_uri.rstrip('/')}/data/"

    env_bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if env_bucket:
        bucket_uri = env_bucket if env_bucket.startswith("s3a://") else f"s3a://{env_bucket}"
        return f"{bucket_uri.rstrip('/')}/data/"

    return f"s3a://{net_id}-assignment-spark-cluster-logs/data/"


# -------------------------------
# Main
# -------------------------------
def main():
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution")
    parser.add_argument("master", nargs="?", help="Spark master URL, e.g. spark://10.0.0.10:7077")
    parser.add_argument("--net-id", required=True, help="Your NET ID, e.g. abc123")
    parser.add_argument("--bucket", help="Override S3 bucket name or URI")
    parser.add_argument("--local-sample", help="Use a local directory instead of S3")
    parser.add_argument("--output-dir", default="/home/ubuntu/spark-cluster",
                        help="Output directory on the master node")
    args = parser.parse_args()

    input_path = resolve_input_path(args.net_id, args.bucket, args.local_sample).rstrip("/*")
    output_dir = args.output_dir

    spark = build_spark("Problem1-LogLevel-Distribution", args.master)
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

    # Read logs
    df = spark.read.option("recursiveFileLookup", "true").text(input_path)
    print(f"✅ Loaded {df.count():,} log lines from {input_path}")

    # Parse key fields with lowercase normalization (case-insensitive)
    parsed = df.select(
        regexp_extract('value', r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias('timestamp'),
        regexp_extract(lower('value'), r'(info|warn|error|debug)', 1).alias('log_level'),
        regexp_extract(lower('value'), r'(info|warn|error|debug)\s+([^:]+):', 2).alias('component'),
        col('value').alias('log_entry')
    )

    # Count distribution
    counts = (
        parsed
        .where(col("log_level") != "")
        .groupBy("log_level")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )

    # Random sample with retries
    sample10 = None
    for attempt in range(3):
        temp = (
            parsed
            .where(col("log_level") != "")
            .orderBy(rand())
            .limit(10)
            .select("log_entry", "log_level")
        )
        if not temp.rdd.isEmpty():
            sample10 = temp
            print(f"✅ Successfully got random sample on attempt {attempt + 1}")
            break
        else:
            print(f"⚠️ Sample attempt {attempt + 1} returned empty, retrying...")

    if sample10 is None:
        print("⚠️ All 3 attempts returned empty sample; writing empty CSV.")
        sample10 = parsed.where(col("log_level") != "").limit(0).select("log_entry", "log_level")

    # Summary stats
    total_lines = df.count()
    total_with_levels = parsed.where(col("log_level") != "").count()
    unique_levels = counts.count()

    # Output paths
    counts_path = os.path.join(output_dir, "problem1_counts.csv")
    sample_path = os.path.join(output_dir, "problem1_sample.csv")
    summary_path = os.path.join(output_dir, "problem1_summary.txt")

    # Write results
    write_single_csv(counts, counts_path, header=True)
    write_single_csv(sample10, sample_path, header=True)

    # Write summary
    os.makedirs(output_dir, exist_ok=True)
    lines = [
        f"Input path: {input_path}",
        f"Total log lines processed: {total_lines:,}",
        f"Total lines with log levels: {total_with_levels:,}",
        f"Unique log levels found: {unique_levels}",
        "",
        "Log level distribution:"
    ]
    for row in counts.collect():
        pct = (row['count'] / total_with_levels * 100.0) if total_with_levels else 0.0
        lines.append(f"  {row['log_level']:<5}: {row['count']:>10,} ({pct:6.2f}%)")

    with open(summary_path, "w") as f:
        f.write("\n".join(lines))

    print("\n✅ All results written successfully to:")
    print(f"   {counts_path}")
    print(f"   {sample_path}")
    print(f"   {summary_path}\n")

    spark.stop()


if __name__ == "__main__":
    main()