from pyspark.sql import SparkSession


def create_spark(app_name: str = "HouseholdDEProject") -> SparkSession:
    """
    Creates and returns a SparkSession configured with Delta Lake support.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark