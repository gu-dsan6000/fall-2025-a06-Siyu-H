# #!/usr/bin/env python
# # -*- coding: utf-8 -*-
# """
# Problem 2: Cluster Usage Analysis
# Usage:
#   # Full run (connects to cluster)
#   uv run python problem2.py spark://<MASTER_PRIVATE_IP>:7077 --net-id <YOUR_NET_ID>

#   # Visualization only (skip Spark execution)
#   uv run python problem2.py --skip-spark
# """
# import argparse
# import os
# import re
# from datetime import datetime

# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, regexp_extract, min as spark_min, max as spark_max
# from pyspark.sql.types import TimestampType, StringType

# # Parsing rules:
# # - cluster_id comes from the application directory name: application_<clusterid>_<appnum>
# # - application_id is the directory name itself: application_<clusterid>_<appnum>
# # - start_time / end_time are inferred from timestamps in AM / executor logs:
# #   take the min and max timestamps per application
# # Example timestamp in logs: 17/03/29 10:04:41
# TS_PATTERN = r"(\d{2})/(\d{2})/(\d{2})\s(\d{2}):(\d{2}):(\d{2})"

# def parse_ts(s: str):
#     """Parse timestamp string like '17/03/29 10:04:41' into datetime."""
#     m = re.search(TS_PATTERN, s)
#     if not m:
#         return None
#     yy, MM, dd, hh, mm, ss = map(int, m.groups())
#     # Year estimation: dataset covers 2015–2017
#     year = 2000 + yy
#     try:
#         return datetime(year, MM, dd, hh, mm, ss)
#     except ValueError:
#         return None

# def build_spark(app_name: str, master: str | None):
#     """Initialize SparkSession with suitable configuration for cluster processing."""
#     builder = SparkSession.builder.appName(app_name)
#     if master:
#         builder = builder.master(master)
#     builder = (
#         builder
#         .config("spark.sql.shuffle.partitions", "400")
#         .config("spark.executor.memory", "3g")
#         .config("spark.driver.memory", "3g")
#     )
#     return builder.getOrCreate()

# def read_all_logs(spark: SparkSession, local_sample: bool):
#     """Read all log files either from local sample or from S3 bucket."""
#     if local_sample:
#         base = "data/sample"
#         path = os.path.join(base, "*")
#         df = spark.read.text(path)
#         df = df.withColumn("source_path", col("input_file_name()"))
#         return df

#     bucket = os.environ.get("SPARK_LOGS_BUCKET")
#     if not bucket:
#         raise RuntimeError("SPARK_LOGS_BUCKET not set")
#     s3_path = f"{bucket}/data/application_*/*"
#     # input_file_name() must be imported from pyspark.sql.functions
#     from pyspark.sql.functions import input_file_name
#     df = spark.read.text(s3_path).withColumn("source_path", input_file_name())
#     return df

# def extract_ids_from_path(path_col):
#     """Extract application_id, cluster_id, and app_number from the file path."""
#     # Example path: .../application_1485248649253_0001/container_00000
#     app_dir_pat = r"(application_(\d+)_(\d+))"
#     return (
#         regexp_extract(path_col, app_dir_pat, 1).alias("application_id"),
#         regexp_extract(path_col, app_dir_pat, 2).alias("cluster_id"),
#         regexp_extract(path_col, app_dir_pat, 3).alias("app_number")
#     )

# def make_plots(timeline_csv: str, cluster_summary_csv: str, out_dir: str):
#     """Create bar chart and duration density plot from generated CSV files."""
#     os.makedirs(out_dir, exist_ok=True)
#     tl = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
#     cs = pd.read_csv(cluster_summary_csv, parse_dates=["cluster_first_app", "cluster_last_app"])

#     # Plot 1: Number of applications per cluster (bar chart)
#     plt.figure()
#     order = cs.sort_values("num_applications", ascending=False)
#     ax = sns.barplot(data=order, x="cluster_id", y="num_applications")
#     for p in ax.patches:
#         ax.annotate(f"{int(p.get_height())}", (p.get_x()+p.get_width()/2, p.get_height()),
#                     ha='center', va='bottom', fontsize=9, xytext=(0,3), textcoords='offset points')
#     plt.xticks(rotation=45, ha="right")
#     plt.tight_layout()
#     bar_path = os.path.join(out_dir, "problem2_bar_chart.png")
#     plt.savefig(bar_path, dpi=150)
#     plt.close()

#     # Plot 2: Duration distribution for the most heavily used cluster (log scale on x-axis)
#     top_cluster = order.iloc[0]["cluster_id"]
#     sub = tl[tl["cluster_id"] == str(top_cluster)].copy()
#     sub["duration_sec"] = (sub["end_time"] - sub["start_time"]).dt.total_seconds().clip(lower=1)
#     plt.figure()
#     sns.histplot(sub["duration_sec"], bins=30, kde=True)
#     plt.xscale("log")
#     plt.title(f"Duration distribution (cluster {top_cluster})  (n={len(sub)})")
#     plt.xlabel("duration_sec (log-scale)")
#     plt.tight_layout()
#     den_path = os.path.join(out_dir, "problem2_density_plot.png")
#     plt.savefig(den_path, dpi=150)
#     plt.close()

# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("master", nargs="?", default=None, help="Spark master URL, e.g., spark://<MASTER_PRIVATE_IP>:7077")
#     parser.add_argument("--net-id", type=str, default=None, help="Your NetID (optional, used for labeling outputs)")
#     parser.add_argument("--skip-spark", action="store_true", help="Skip Spark execution and only recreate plots from CSVs")
#     parser.add_argument("--local-sample", action="store_true", help="Use data/sample/ for local testing")
#     args = parser.parse_args()

#     out_dir = "data/output" if args.local_sample or args.skip_spark else "~/spark-cluster"
#     out_dir_expanded = os.path.expanduser(out_dir)
#     os.makedirs(out_dir_expanded, exist_ok=True)

#     timeline_csv = os.path.join(out_dir_expanded, "problem2_timeline.csv")
#     summary_csv  = os.path.join(out_dir_expanded, "problem2_cluster_summary.csv")
#     stats_txt    = os.path.join(out_dir_expanded, "problem2_stats.txt")

#     if not args.skip_spark:
#         spark = build_spark("Problem2-ClusterUsage", args.master)
#         df = read_all_logs(spark, args.local_sample)  # Columns: value (log line), source_path

#         # Extract application_id / cluster_id / app_number
#         app_id, cluster_id, app_number = extract_ids_from_path(col("source_path"))
#         logs = df.select(
#             col("value").alias("log_entry"),
#             col("source_path"),
#             app_id, cluster_id, app_number
#         ).filter(col("application_id") != "")

#         # Parse timestamps using UDF
#         from pyspark.sql.functions import udf
#         parse_ts_udf = udf(lambda s: parse_ts(s), TimestampType())
#         logs = logs.withColumn("ts", parse_ts_udf(col("log_entry")))

#         # Compute min/max timestamps for each application
#         agg = logs.groupBy("cluster_id", "application_id", "app_number").agg(
#             spark_min("ts").alias("start_time"),
#             spark_max("ts").alias("end_time")
#         )

#         # Convert to Pandas for writing CSVs
#         pdf = agg.toPandas().sort_values(["cluster_id", "app_number"])
#         # Drop applications without valid timestamps
#         pdf = pdf.dropna(subset=["start_time", "end_time"])

#         # Write timeline CSV
#         pdf.to_csv(timeline_csv, index=False)

#         # Aggregate cluster-level statistics
#         cs = (pdf.groupby("cluster_id")
#                 .agg(num_applications=("application_id", "nunique"),
#                      cluster_first_app=("start_time", "min"),
#                      cluster_last_app=("end_time", "max"))
#                 .reset_index()
#                 .sort_values("num_applications", ascending=False))
#         cs.to_csv(summary_csv, index=False)

#         # Write overall statistics summary
#         text = []
#         text.append(f"Total unique clusters: {cs['cluster_id'].nunique()}")
#         text.append(f"Total applications: {pdf['application_id'].nunique()}")
#         text.append(f"Average applications per cluster: {pdf.groupby('cluster_id')['application_id'].nunique().mean():.2f}")
#         text.append("")
#         text.append("Most heavily used clusters:")
#         for _, r in cs.head(5).iterrows():
#             text.append(f"  Cluster {r['cluster_id']}: {int(r['num_applications'])} applications")
#         with open(stats_txt, "w", encoding="utf-8") as f:
#             f.write("\n".join(text) + "\n")

#         spark.stop()

#     # Always create visualizations
#     make_plots(timeline_csv, summary_csv, out_dir_expanded)

# if __name__ == "__main__":
#     main()
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

    # 🧹 Clean old Spark outputs
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

        # === 自动检测空输出并修复 ===
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

        # ⚙️ 自动修复逻辑（必须在 spark.stop() 前执行）
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

        # ✅ 在最后再停止 Spark
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
