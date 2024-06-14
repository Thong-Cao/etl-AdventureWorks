"""File for DAGs related configs"""

from pyspark.conf import SparkConf
from pyspark.sql.types import *


"""Service addresses and URIs"""
class ServiceConfig:
    """Class for service configs"""
    def __init__(self, key: str):
        lookup = {
            "hdfs": ("hdfs", "namenode", "8020"),
            "webhdfs": ("http", "namenode", "9870"),
            "spark": ("spark", "spark-master", "7077"),
            "hive_metastore": ("thrift", "hive-metastore", "9083")
        }
        try:
            self.service, self.hostname, self.port = lookup[key]
        except KeyError:
            raise ValueError("Invalid input key.")

    @property
    def addr(self):
        return f"{self.hostname}:{self.port}"

    @property
    def uri(self):
        return f"{self.service}://{self.addr}"


"""Spark configs and schemas"""
def get_default_SparkConf() -> SparkConf:
    """Get a SparkConf object with some default values"""
    HDFS_CONF = ServiceConfig("hdfs")
    SPARK_CONF = ServiceConfig("spark")
    HIVE_METASTORE_CONF = ServiceConfig("hive_metastore")

    default_configs = [
        ("spark.master", SPARK_CONF.uri),
        ("spark.sql.warehouse.dir", HDFS_CONF.uri + "/data_warehouse"),
        (
            "spark.hadoop.fs.defaultFS",
            f"{HDFS_CONF.service}://{HDFS_CONF.hostname}"
        ),
        ("spark.hadoop.dfs.replication", 1),
        ("spark.hive.exec.dynamic.partition", True),
        ("spark.hive.exec.dynamic.partition.mode", "nonstrict"),  
        ("spark.hive.metastore.uris", HIVE_METASTORE_CONF.uri)  
            # so that Spark can read metadata into catalog
    ]
        
    conf = SparkConf().setAll(default_configs)
    return conf


class SparkSchema:
    """Contains PySpark schemas used in DAGs"""
    def __init__(self):
        # Source schema expected for flights data from OpenSky API
        self.src_customer = StructType(
    [
        StructField("CustomerID", IntegerType(), nullable=False),
        StructField("PersonID", IntegerType(), nullable=True),
        StructField("StoreID", IntegerType(), nullable=True),
        StructField("TerritoryID", IntegerType(), nullable=True),
        StructField("AccountNumber", StringType(), nullable=True),
        StructField("rowguid", StringType(), nullable=False), 
        StructField("ModifiedDate", TimestampType(), nullable=False)
    ])