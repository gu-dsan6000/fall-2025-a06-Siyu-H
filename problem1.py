#!/usr/bin/env python3
import argparse
import os
import shutil
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, count, lower, when


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
    """Safely write a DataFrame as a single local CSV file, even if empty."""
    try:
        if df is None or df.rdd.isEmpty():
            print(f"⚠️ Warning: DataFrame is empty — writing empty CSV to {final_path}")
            os.makedirs(os.path.dirname(final_path), exist_ok=True)
            with open(final_path, "w") as f:
                if header:
                    f.write(",".join(df.columns) + "\n")
            return

        tmp_dir = final_path + "_tmp"
        df.coalesce(1).write.mode("overwrite").option("header", header).csv(tmp_dir)

        part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        if not part_files:
            print(f"[DEBUG] tmp_dir exists? {os.path.exists(tmp_dir)}")
            print(f"[DEBUG] Files under tmp_dir: {os.listdir(tmp_dir) if os.path.exists(tmp_dir) else 'N/A'}")
            print(f"⚠️ Warning: No CSV part files found — writing empty file to {final_path}")
            os.makedirs(os.path.dirname(final_path), exist_ok=True)
            with open(final_path, "w") as f:
                if header:
                    f.write(",".join(df.columns) + "\n")
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return

        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        shutil.move(part_files[0], final_path)
        shutil.rmtree(tmp_dir, ignore_errors=True)
        print(f"✅ Wrote single CSV: {final_path}")

    except Exception as e:
        print(f"❌ Error while writing {final_path}: {e}")
        import traceback
        traceback.print_exc()


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
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution (robust version)")
    parser.add_argument("master", nargs="?", help="Spark master URL, e.g. spark://10.0.0.10:7077")
    parser.add_argument("--net-id", required=True, help="Your NET ID, e.g. abc123")
    parser.add_argument("--bucket", help="Override S3 bucket name or URI")
    parser.add_argument("--local-sample", help="Use a local directory instead of S3")
    parser.add_argument("--output-dir", default="/home/ubuntu/spark-cluster",
                        help="Output directory on the master node")
    args = parser.parse_args()

    input_path = resolve_input_path(args.net_id, args.bucket, args.local_sample).rstrip("/*")
    output_dir = args.output_dir

    print(f"🔍 Input path resolved to: {input_path}")

    spark = build_spark("Problem1-LogLevel-Distribution", args.master)
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

    print("🚀 Reading log files ...")
    df = spark.read.option("recursiveFileLookup", "true").text(input_path)
    total_lines = df.count()
    print(f"✅ Loaded {total_lines:,} log lines from {input_path}")

    print("🔹 Parsing log levels (case-insensitive & multiple patterns)...")
    parsed = df.select(
        # 时间戳提取
        regexp_extract('value', r'^(\d{2,4}[-/]\d{2}[-/]\d{2,4}[ ,:0-9]*)', 1).alias('timestamp'),

        # log level 提取，兼容多种格式
        when(
            regexp_extract(lower('value'),
                           r'(info|warn|error|debug)', 1) != "",
            regexp_extract(lower('value'), r'(info|warn|error|debug)', 1)
        ).otherwise(
            when(
                regexp_extract('value',
                               r'(INFO|WARN|ERROR|DEBUG)', 1) != "",
                lower(regexp_extract('value', r'(INFO|WARN|ERROR|DEBUG)', 1))
            ).otherwise(
                lower(regexp_extract('value',
                                     r'(?<="level"[:=]\s*["\']?)(info|warn|error|debug)', 1))
            )
        ).alias('log_level'),

        regexp_extract(lower('value'), r'(info|warn|error|debug)\s+([^:]+):', 2).alias('component'),
        col('value').alias('log_entry')
    ).cache()

    parsed_count = parsed.count()
    print(f"📊 Parsed total rows: {parsed_count:,}")

    # Count distribution
    counts = (
        parsed
        .where(col("log_level") != "")
        .groupBy("log_level")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )

    print("🔹 Generating random sample ...")
    sample10 = (
        parsed
        .where(col("log_level") != "")
        .orderBy(rand())
        .limit(10)
        .select("log_entry", "log_level")
    )

    sample_count = sample10.count()
    print(f"✅ Random sample rows: {sample_count}")

    total_with_levels = parsed.where(col("log_level") != "").count()
    unique_levels = counts.count()

    # Output paths
    counts_path = os.path.join(output_dir, "problem1_counts.csv")
    sample_path = os.path.join(output_dir, "problem1_sample.csv")
    summary_path = os.path.join(output_dir, "problem1_summary.txt")

    print("💾 Writing CSV outputs ...")
    write_single_csv(counts, counts_path, header=True)
    write_single_csv(sample10, sample_path, header=True)

    print("📝 Writing summary text ...")
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
    print("🏁 Spark session stopped. Job completed successfully.")


if __name__ == "__main__":
    main()
