from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    ArrayType,
)
from src.utils.spark_session import create_spark


def get_transaction_schema() -> StructType:
    """
    Defines the explicit nested schema for purchase transactions.
    """

    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("transaction_date", StringType(), False),

        StructField("customer", StructType([
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), True),
        ]), False),

        StructField("store", StructType([
            StructField("store_name", StringType(), False),
            StructField("store_type", StringType(), True),
            StructField("city", StringType(), True),
        ]), False),

        StructField("payment_method", StringType(), True),

        StructField("items", ArrayType(
            StructType([
                StructField("item_name", StringType(), False),
                StructField("category", StringType(), True),
                StructField("quantity", IntegerType(), False),
                StructField("price", DoubleType(), False),
            ])
        ), False),
    ])


def main():

    spark = create_spark("Bronze_Ingest_Purchase")

    schema = get_transaction_schema()

    bronze_df = (
        spark.read
        .schema(schema)
        .option("mode", "FAILFAST")  # fail if malformed
        .json("data/raw/transactions.json")
    )

    bronze_df.write \
        .format("delta") \
        .mode("append") \
        .save("delta/bronze/transactions")

    print("Bronze ingestion completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()