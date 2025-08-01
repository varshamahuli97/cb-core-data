import findspark
findspark.init()
import sys
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import col, lit, expr, date_format, current_timestamp

from datetime import datetime,timedelta
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from constants.ParquetFileConstants import ParquetFileConstants
from jobs.config import get_environment_config
from jobs.default_config import create_config

class InAppReviewModel:    
    def __init__(self):
        self.class_name = "org.ekstep.analytics.dashboard.report.InAppReviewModel"
        
    def name(self):
        return "InAppReviewModel"
    
    @staticmethod
    def get_date():
        return datetime.now().strftime("%Y-%m-%d")
    
    @staticmethod
    def current_date_time():
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def process_data(self, spark,conf):
        try:
            today = self.get_date()
            currentDateTime = date_format(current_timestamp(), ParquetFileConstants.DATE_TIME_WITH_AMPM_FORMAT)
            
            weeklyClapsDF = spark.read.parquet(ParquetFileConstants.CLAPS_PARQUET_FILE)
            
            # calculate end of the week to set an expiry date for the feeds
            def endOfWeek(today_date):
                """Calculate the end of the week (Sunday)"""
                days_until_sunday = 7 - today_date.weekday() - 1  # weekday() returns 0=Monday, 6=Sunday
                if days_until_sunday == 7:  # if today is Sunday
                    days_until_sunday = 0
                return today_date + timedelta(days=days_until_sunday)
            
            def endOfDay(date):
                """Get the end of the current day"""
                # Get the start of the next day and subtract 1 second
                start_of_next_day = datetime.combine(date + timedelta(days=1), datetime.min.time())
                return start_of_next_day - timedelta(microseconds=1)
            
            # Today's date
            today_date = datetime.now().date()
            
            # Calculate expireOn date
            expire_on_date = endOfWeek(today_date)
            print(f"Expire on date {expire_on_date}")
            
            # convert expireOn date to epoch seconds as expected by feed table
            expire_on_datetime = endOfDay(expire_on_date)
            expire_on_epoch_ms = int(expire_on_datetime.timestamp() * 1000)
            print(f"Expire on epochms {expire_on_epoch_ms}")
            
            # fetch the userids based on the condition
            filtered_df = weeklyClapsDF.filter(
                col("claps_updated_this_week") & 
                (expr("date_format(last_claps_updated_on, 'yyyy-MM-dd')") == currentDateTime)
            ).select("userid")
            
            # add required columns for feed data
            result_df = filtered_df \
                .withColumn("expireon", lit(expire_on_epoch_ms)) \
                .withColumn("category", lit("InAppReview")) \
                .withColumn("id", expr("uuid()").cast("string")) \
                .withColumn("createdby", lit("weekly_claps")) \
                .withColumn("createdon", currentDateTime) \
                .withColumn("action", lit("{}")) \
                .withColumn("priority", lit(1)) \
                .withColumn("status", lit("unread")) \
                .withColumn("updatedby", lit(None).cast("string")) \
                .withColumn("updatedon", lit(None).cast("date")) \
                .withColumn("version", lit("v1"))
            
            result_df.printSchema()
            result_df.show(5,truncate=False)
            
            result_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options({
                    "keyspace": conf.cassandraUserFeedKeyspace,
                    "table": conf.cassandraUserFeedTable
                }) \
                .mode("append") \
                .save()
                
        except Exception as e:
            print(f"Error occurred during InAppReviewModel processing: {str(e)}")
            import sys
            sys.exit(1)

def main():
    # Initialize Spark Session with optimized settings for caching
    spark = SparkSession.builder \
        .appName("In App Review Report Model") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.executor.memory", "42g") \
        .config("spark.driver.memory", "10g") \
        .config("spark.executor.memoryFraction", "0.7") \
        .config("spark.storage.memoryFraction", "0.2") \
        .config("spark.storage.unrollFraction", "0.1") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    

    # Create model instance
    start_time = datetime.now()
    print(f"[START] InAppReviewModel processing started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_dict = get_environment_config()
    config = create_config(config_dict)
    model = InAppReviewModel()
    model.process_data(spark,config)
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"[END] InAppReviewModel processing completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[INFO] Total duration: {duration}")
    spark.stop()
# Example usage:
if __name__ == "__main__":
   main()

