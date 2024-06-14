from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
"""File for DAGs related configs"""

from pyspark.conf import SparkConf
from pyspark.sql.types import *

from config_services import get_default_SparkConf, SparkSchema
SCHEMAS = SparkSchema()

def main():
    conf = get_default_SparkConf()
    spark = SparkSession.builder \
        .config(conf = conf) \
        .enableHiveSupport() \
        .getOrCreate()


    # Database connection parameters
    db_params = {
        'url': 'jdbc:postgresql://db:5432/Adventureworks',
        'properties': {
            'user': 'postgres',
            'password': 'postgres',
            'driver': 'org.postgresql.Driver'
        }
    }

    # Extract data from PostgreSQL
    customer_df = spark.read \
        .jdbc(db_params['url'], 'Sales.Customer', properties=db_params['properties'])

    # Define the schema for the DataFrame
    customer_schema = StructType([
        StructField("CustomerID", IntegerType(), nullable=False),
        StructField("PersonID", IntegerType(), nullable=True),
        StructField("StoreID", IntegerType(), nullable=True),
        StructField("TerritoryID", IntegerType(), nullable=True),
        StructField("rowguid", StringType(), nullable=False),
        StructField("ModifiedDate", TimestampType(), nullable=False)
    ])

    # Apply the schema directly to the DataFrame
    df_customer = customer_df.selectExpr(
        "cast(customerid as int) CustomerID",
        "cast(personid as int) PersonID",
        "cast(storeid as int) StoreID",
        "cast(territoryid as int) TerritoryID",
        "cast(rowguid as string) rowguid",
        "cast(modifieddate as timestamp) ModifiedDate"
    )

    # Show the DataFrame
    df_customer.show()

    # Write the DataFrame to Hive table
    df_customer.write \
        .mode("overwrite") \
        .format("hive") \
        .saveAsTable("Customer")


if __name__ == "__main__":
    main()
