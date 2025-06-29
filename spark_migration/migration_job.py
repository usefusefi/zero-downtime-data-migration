from enum import Enum
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, md5, concat_ws
from pyspark.sql.types import StructType
from petabyte_spark_config import PetabyteSparkConfig

class CloudProvider(Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

@dataclass
class DataSource:
    provider: CloudProvider
    connection_string: str

class SparkMigrationJob:
    def __init__(self, spark: SparkSession, source: DataSource, target: DataSource):
        self.spark = spark
        self.source = source
        self.target = target

    def execute_incremental_migration(self, watermark_column: str):
        # Read source data with watermark for incremental processing
        source_df = self.spark.read.format(self._get_source_format())\
            .option("path", self.source.connection_string)\
            .load()\
            .withWatermark(watermark_column, "10 minutes")

        validated_df = self._apply_data_quality_checks(source_df)

        # Repartition for optimal write
        partitioned_df = validated_df.repartition(
            int(self.spark.conf.get("spark.sql.shuffle.partitions"))
        )

        # Write to target with checkpoint for exactly-once semantics
        partitioned_df.write.format(self._get_target_format())\
            .mode("append")\
            .option("checkpointLocation", "/checkpoints/incremental")\
            .option("path", self.target.connection_string)\
            .save()

    def _apply_data_quality_checks(self, df):
        critical_columns = ["id", "timestamp", "customer_id"]
        for column in critical_columns:
            if column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                if null_count > 0:
                    raise ValueError(f"Critical column {column} contains {null_count} null values")
        # Business rule: amount >= 0 if exists
        if "amount" in df.columns:
            df = df.filter(col("amount") >= 0)
        return df

    def _get_source_format(self):
        mapping = {
            CloudProvider.AWS: "parquet",
            CloudProvider.GCP: "bigquery",
            CloudProvider.AZURE: "parquet"
        }
        return mapping.get(self.source.provider, "parquet")

    def _get_target_format(self):
        mapping = {
            CloudProvider.AWS: "delta",
            CloudProvider.GCP: "bigquery",
            CloudProvider.AZURE: "delta"
        }
        return mapping.get(self.target.provider, "delta")

if __name__ == "__main__":
    spark = PetabyteSparkConfig.create_optimized_session("PetabyteMigration", max_executors=100)
    source = DataSource(CloudProvider.AWS, "s3a://source-bucket/data/")
    target = DataSource(CloudProvider.GCP, "gs://target-bucket/data/")
    job = SparkMigrationJob(spark, source, target)
    job.execute_incremental_migration("updated_at")
