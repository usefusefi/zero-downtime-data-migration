from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from enum import Enum
from dataclasses import dataclass
from typing import Optional

class CloudProvider(Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

@dataclass
class DataSource:
    provider: CloudProvider
    connection_string: str

class PetabyteSparkConfig:
    @staticmethod
    def create_optimized_session(app_name: str, max_executors: int = 1000) -> SparkSession:
        conf = SparkConf()
        conf.set("spark.executor.memory", "6g")
        conf.set("spark.executor.cores", "4")
        conf.set("spark.driver.memory", "12g")
        conf.set("spark.sql.shuffle.partitions", str(max_executors * 4))
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.dynamicAllocation.enabled", "true")
        conf.set("spark.dynamicAllocation.maxExecutors", str(max_executors))
        return SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()

class SparkMigrationJob:
    def __init__(self, spark: SparkSession, source: DataSource, target: DataSource):
        self.spark = spark
        self.source = source
        self.target = target

    def execute_incremental_migration(self, watermark_col: str):
        source_df = self.spark.read.format("parquet").load(self.source.connection_string)\
            .withWatermark(watermark_col, "10 minutes")
        validated_df = self._apply_data_quality_checks(source_df)
        validated_df.write.format("delta").mode("append")\
            .option("checkpointLocation", "/tmp/checkpoints")\
            .save(self.target.connection_string)

    def _apply_data_quality_checks(self, df):
        from pyspark.sql.functions import col
        critical_cols = ["id", "timestamp", "customer_id"]
        for c in critical_cols:
            if c in df.columns:
                null_count = df.filter(col(c).isNull()).count()
                if null_count > 0:
                    raise ValueError(f"Nulls found in critical column {c}")
        return df

if __name__ == "__main__":
    spark = PetabyteSparkConfig.create_optimized_session("PetabyteMigration", max_executors=100)
    source = DataSource(CloudProvider.AWS, "s3a://source-bucket/data/")
    target = DataSource(CloudProvider.GCP, "gs://target-bucket/data/")
    job = SparkMigrationJob(spark, source, target)
    job.execute_incremental_migration("updated_at")
