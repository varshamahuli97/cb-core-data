import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, countDistinct, when, sum, bround, broadcast, coalesce, lit,
    current_timestamp, date_format, from_unixtime, concat_ws
)
import os
import duckdb

# Add parent directory to sys.path for importing project-specific modules
sys.path.append(str(Path(__file__).resolve().parents[2]))

# Import reusable utilities from project
from constants.ParquetFileConstants import ParquetFileConstants
from dfutil.user import userDFUtil
from dfutil.enrolment.acbp import acbpDFUtil
from dfutil.enrolment import enrolmentDFUtil
from dfutil.content import contentDFUtil

# Initialize the Spark Session with tuning configurations
spark = SparkSession.builder \
    .appName("UserReportGenerator") \
    .config("spark.executor.memory", "50g") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def write_csv_per_mdo_id(df, output_dir, groupByAttr, isIndividualWrite=False, threshold=100000):
    """
    Optimized hybrid write strategy: 
    - Small/medium groups: Direct CSV write via Spark partitionBy
    - Large groups: Filter first, then write to parquet for DuckDB processing
    
    Args:
        df (DataFrame): Source DataFrame
        output_dir (str): Output directory path
        groupByAttr (str): Column to group by
        isIndividualWrite (bool): If True, write as parquet instead of CSV
        threshold (int): Max row count per group to consider for fast write
    """
    
    if isIndividualWrite == False:
        print("📊 Step 1: Analyzing group sizes...")
        spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")
        df_clean = df.checkpoint()  # This breaks the lineage completely
        df_clean.repartition(32).cache()  # Cache the DataFrame to avoid recomputation
        df_size = df_clean.count()
        print(f"Total rows in DataFrame: {df_size}")
        # Step 1: Get group counts
        group_counts = df_clean.groupBy(groupByAttr).count()
        group_counts.cache()  # Cache since we'll use it multiple times
        
        # Step 2: Collect IDs for fast + fallback paths
        small_ids = [row[groupByAttr] for row in group_counts.filter(col("count") <= threshold).collect()]
        large_ids = [row[groupByAttr] for row in group_counts.filter(col("count") > threshold).collect()]
        
        print(f"📈 Small groups (≤{threshold} rows, fast write): {len(small_ids)}")
        print(f"📊 Large groups (>{threshold} rows, DuckDB write): {len(large_ids)}")
        
        # Step 3: Fast write for small/medium groups - FILTER FIRST
        if small_ids:
            print("🚀 Writing small groups directly via Spark...")
            small_df = df_clean.filter(col(groupByAttr).isin(small_ids))
            
            small_df \
                .repartition(groupByAttr) \
                .write \
                .mode("overwrite") \
                .partitionBy(groupByAttr) \
                .option("header", True) \
                .csv(output_dir)
            
            print(f"✅ Completed writing {len(small_ids)} small groups")

        # Step 4: DuckDB write for large groups - FILTER FIRST, then write to parquet
        if large_ids:
            print("🦆 Processing large groups via DuckDB...")
            # df.cache().count()  # Ensure the DataFrame is cached for performance
            # OPTIMIZATION: Filter large IDs first, then write only filtered data to parquet
            large_df = df_clean.filter(col(groupByAttr).isin(large_ids))
            
            parquet_tmp_path = output_dir + '_tmp_large_groups'
            write_csv_per_mdo_id_duckdb(large_df, output_dir, groupByAttr, parquet_tmp_path, large_ids)
            
            print(f"✅ Completed writing {len(large_ids)} large groups")
        
        # Cleanup
        group_counts.unpersist()
        
    else:
        # Parquet write mode
        print("📦 Writing as partitioned parquet...")
        df_clean.repartition(groupByAttr) \
            .write \
            .mode("overwrite") \
            .partitionBy(groupByAttr) \
            .option("header", True) \
            .option("compression", "snappy") \
            .parquet(output_dir)

def write_csv_per_mdo_id_duckdb(df, output_dir: str, group_by_attr: str, parquet_tmp_path: str, large_ids):
    """
    Optimized: Writes CSVs per group_by_attr using pre-filtered DataFrame.
    Only processes large groups that were already filtered by Spark.

    Args:
        df (DataFrame): Pre-filtered Spark DataFrame containing only large groups
        output_dir (str): Output directory to write CSVs
        group_by_attr (str): Column to group by (e.g., "mdo_id")
        parquet_tmp_path (str): Temporary Parquet output path
        large_ids (list): List of large group IDs (for validation/logging)
    """
    print(f"📦 Step 1: Writing filtered large groups to Parquet...")
    print(f"    - Processing {len(large_ids)} large groups")
    print(f"    - Parquet path: {parquet_tmp_path}")
    
    # Write only the pre-filtered large groups to parquet
    df \
        .coalesce(4) \
        .write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(parquet_tmp_path)

    print("🦆 Step 2: Loading into DuckDB...")
    con = duckdb.connect()
    
    # DuckDB optimizations
    con.execute("PRAGMA memory_limit='30GB';")
    con.execute("PRAGMA threads=8;")
    con.execute("PRAGMA temp_directory='/tmp/duckdb_spill';")
    con.execute("INSTALL parquet; LOAD parquet;")
    
    # Load the parquet data
    con.execute(f"CREATE TABLE result_df AS SELECT * FROM parquet_scan('{parquet_tmp_path}/**/*.parquet');")

    print("🔍 Step 3: Verifying large group IDs...")
    # Use the provided large_ids directly (since we already filtered)
    group_ids = [(val,) for val in large_ids]
    
    # Optional: Verify that our filtering worked correctly
    actual_groups = con.execute(f"SELECT DISTINCT {group_by_attr} FROM result_df").fetchall()
    print(f"    - Expected groups: {len(group_ids)}")
    print(f"    - Actual groups in parquet: {len(actual_groups)}")

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("📤 Step 4: Writing individual CSV files for large groups...")
    
    for (group_val,) in group_ids:
        safe_val = str(group_val).replace("/", "_") if group_val is not None else "null"
        output_path = Path(output_dir) / f"{group_by_attr}={safe_val}.csv"

        con.execute(f"""
            COPY (
                SELECT * FROM result_df
                WHERE {group_by_attr} = ?
            ) TO '{output_path}' (FORMAT CSV, HEADER, DELIMITER ',');
        """, [group_val])

        print(f"✅ Wrote large group: {output_path}")

    con.close()
    
    # Optional: Cleanup temporary parquet files
    print("🧹 Cleaning up temporary parquet files...")
    try:
        import shutil
        shutil.rmtree(parquet_tmp_path)
        print(f"✅ Cleaned up: {parquet_tmp_path}")
    except Exception as e:
        print(f"⚠️  Could not clean up {parquet_tmp_path}: {e}")
    
    print("🎉 Done writing all large group CSV files.")
