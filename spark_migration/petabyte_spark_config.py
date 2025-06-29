from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

class PetabyteSparkConfig:
    @staticmethod
    def create_optimized_session(app_name: str, max_executors: int = 1000) -> SparkSession:
        conf = SparkConf()
        conf.set("spark.executor.memory", "6g")
        conf.set("spark.executor.cores", "4")
        conf.set("spark.driver.memory", "12g")
        conf.set("spark.driver.maxResultSize", "8g")
        conf.set("spark.sql.shuffle.partitions", str(max_executors * 4))
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.network.timeout", "800s")
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        conf.set("spark.dynamicAllocation.enabled", "true")
        conf.set("spark.dynamicAllocation.minExecutors", "10")
        conf.set("spark.dynamicAllocation.maxExecutors", str(max_executors))
        conf.set("spark.dynamicAllocation.initialExecutors", "50")
        conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")
        conf.set("spark.sql.recovery.checkpointInterval", "10s")
        return SparkSession.builder.appName(app_name).config(conf=conf).getOrCreate()
