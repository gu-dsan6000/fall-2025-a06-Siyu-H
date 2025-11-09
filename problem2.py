#!/usr/bin/env python3
import argparse, os, glob, shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, input_file_name,
    expr, min as smin, max as smax
)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


# ======================
# Spark Initialization
# ======================
def build_spark(app_name: str, master_url: str | None = None) -> SparkSession:
    b = SparkSession.builder.appName(app_name)
    if master_url:
        b = b.master(master_url)
    b = (
        b.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "com.amazonaws.auth.InstanceProfileCredentialsProvider")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.fast.upload", "true")
         .config("spark.sql.shuffle.partitions", "200")
    )
    return b.getOrCreate()


# ======================
# Resolve Input Directory
# ======================
def resolve_input_dir(bucket_override: str | None) -> str:
    if bucket_override:
        uri = bucket_override if bucket_override.startswith("s3a://") else f"s3a://{bucket_override}"
        return f"{uri}/data"
    env = os.environ.get("SPARK_LOGS_BUCKET")
    if env:
        uri = env if env.startswith("s3a://") else f"s3a://{env}"
        return f"{uri}/data"
    raise SystemExit("ERROR: No bucket provided. Set --bucket or $SPARK_LOGS_BUCKET (must start with s3a://)")


# ======================
# Main Function
# ======================
def main():
    p = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    p.add_argument("master", nargs="?", help="Spark master URL, e.g. spark://10.0.0.10:7077")
    p.add_argument("--net-id", required=True)
    p.add_argument("--bucket", required=True, help="Your S3 bucket name or URI")
    p.add_argument("--output-dir", default="/home/ubuntu/spark-cluster")
    p.add_argument("--skip-spark", action="store_true", help="Skip Spark and only rebuild charts")
    p.add_argument("--debug", action="store_true", help="Print debug info about data counts and samples")
    args = p.parse_args()

    outdir = args.output_dir
    os.makedirs(outdir, exist_ok=True)

    # Clean old Spark outputs
    for d in ["problem2_timeline", "problem2_cluster_summary"]:
        path = os.path.join(outdir, d)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)

    if args.skip_spark:
        import pandas as pd
        timeline = pd.read_csv(os.path.join(outdir, "problem2_timeline/part-00000.csv"))
        cluster = pd.read_csv(os.path.join(outdir, "problem2_cluster_summary/part-00000.csv"))
    else:
        input_dir = resolve_input_dir(args.bucket)
        spark = build_spark("Problem2-Cluster-Usage", args.master)
        spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
        spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

        df = spark.read.option("recursiveFileLookup", "true").text(input_dir)
        if args.debug:
            print(f"✅ Raw log lines: {df.count()}")
            df.show(5, truncate=False)

        df = df.withColumn("file_path", input_file_name())
        df = df.withColumn("application_id", regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1))
        df = df.withColumn("cluster_id", regexp_extract(col("application_id"), r"application_(\d+)_\d+", 1))
        df = df.withColumn("app_number", regexp_extract(col("application_id"), r"_(\d+)$", 1))

        df = df.withColumn("ts_str", regexp_extract(col("value"), r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1))
        df = df.withColumn("timestamp", expr("try_to_timestamp(ts_str, 'yy/MM/dd HH:mm:ss')"))

        df2 = df.where((col("application_id") != "") & (col("timestamp").isNotNull()))
        if args.debug:
            print(f"✅ Valid timestamp lines: {df2.count()}")
            df2.select("value", "ts_str", "timestamp").show(5, truncate=False)

        # === Timeline ===
        timeline = (
            df2.groupBy("cluster_id", "application_id", "app_number")
               .agg(smin("timestamp").alias("start_time"),
                    smax("timestamp").alias("end_time"))
        )

        if args.debug:
            print(f"✅ Timeline records: {timeline.count()}")

        timeline_path = os.path.join(outdir, "problem2_timeline")
        (
            timeline.orderBy("cluster_id", "app_number")
                    .coalesce(1)
                    .write.mode("overwrite").option("header", True).csv(timeline_path)
        )

        # === Cluster Summary ===
        cluster = (
            timeline.groupBy("cluster_id")
                    .agg({"application_id": "count",
                          "start_time": "min",
                          "end_time": "max"})
                    .withColumnRenamed("count(application_id)", "num_applications")
                    .withColumnRenamed("min(start_time)", "cluster_first_app")
                    .withColumnRenamed("max(end_time)", "cluster_last_app")
        )

        if args.debug:
            print(f"✅ Cluster count: {cluster.count()}")

        cluster_path = os.path.join(outdir, "problem2_cluster_summary")
        (
            cluster.orderBy(col("num_applications").desc())
                   .coalesce(1)
                   .write.mode("overwrite").option("header", True).csv(cluster_path)
        )

        def _pick_single_csv(folder):
            if os.path.isdir(folder):
                files = glob.glob(os.path.join(folder, "part-*.csv"))
                if files:
                    return files[0]
                else:
                    print(f"⚠️ Warning: No CSV found in {folder}, only _SUCCESS?")
            return None

        timeline_csv = _pick_single_csv(timeline_path)
        cluster_csv  = _pick_single_csv(cluster_path)

        if not timeline_csv or not cluster_csv:
            print("⚠️ Detected empty Spark output folders. Retrying local fallback write...")
            import pandas as pd
            timeline_pd = timeline.toPandas()
            cluster_pd = cluster.toPandas()
            timeline_fallback = os.path.join(outdir, "problem2_timeline_local.csv")
            cluster_fallback = os.path.join(outdir, "problem2_cluster_summary_local.csv")
            timeline_pd.to_csv(timeline_fallback, index=False)
            cluster_pd.to_csv(cluster_fallback, index=False)
            print(f"✅ Local fallback write complete:")
            print(f"   {timeline_fallback}")
            print(f"   {cluster_fallback}")
            timeline_csv = timeline_fallback
            cluster_csv = cluster_fallback
        else:
            print("✅ Files written successfully:")
            print(f"   Timeline CSV: {timeline_csv}")
            print(f"   Cluster CSV:  {cluster_csv}")

        spark.stop()

        import pandas as pd
        timeline = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
        cluster  = pd.read_csv(cluster_csv)

    # === Visualization ===
    fig = plt.figure(figsize=(10, 5))
    xs = cluster["cluster_id"].astype(str)
    ys = cluster["num_applications"].astype(int)
    plt.bar(xs, ys)
    for i, v in enumerate(ys):
        plt.text(i, v, str(v), ha="center", va="bottom", fontsize=9)
    plt.title("Applications per Cluster")
    plt.xlabel("Cluster ID")
    plt.ylabel("# Applications")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "problem2_bar_chart.png"), dpi=150)
    plt.close()

    import numpy as np
    timeline["duration_sec"] = (timeline["end_time"] - timeline["start_time"]).dt.total_seconds().clip(lower=0)
    if len(cluster) > 0:
        largest_cluster = cluster.sort_values("num_applications", ascending=False)["cluster_id"].astype(str).iloc[0]
        d = timeline[timeline["cluster_id"].astype(str) == str(largest_cluster)]["duration_sec"].dropna()
        if len(d) > 0:
            fig = plt.figure(figsize=(10, 5))
            plt.hist(d, bins=50, density=True)
            plt.xscale("log")
            plt.title(f"Duration Distribution (cluster {largest_cluster}, n={len(d)})")
            plt.xlabel("Duration (sec, log scale)")
            plt.ylabel("Density")
            plt.tight_layout()
            plt.savefig(os.path.join(outdir, "problem2_density_plot.png"), dpi=150)
            plt.close()


if __name__ == "__main__":
    main()
