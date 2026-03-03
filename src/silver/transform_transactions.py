from pyspark.sql.functions import col, explode
from src.utils.spark_session import create_spark
from pyspark.sql.functions import to_date


def main():

    spark = create_spark("Silver_Transform")

    # Read Bronze Delta table
    bronze_df = spark.read.format("delta").load("delta/bronze/transactions")

    # Explode items array (one row per item)
    silver_df = (
        bronze_df
        .withColumn("item", explode(col("items")))
        .select(
            col("transaction_id"),
            col("transaction_date"),

            col("customer.customer_id").alias("customer_id"),
            col("customer.name").alias("customer_name"),

            col("store.store_name").alias("store_name"),
            col("store.store_type").alias("store_type"),
            col("store.city").alias("city"),

            col("payment_method"),

            col("item.item_name").alias("item_name"),
            col("item.category").alias("category"),
            col("item.quantity").alias("quantity"),
            col("item.price").alias("price"),
        )
    )

    silver_df = silver_df.withColumn("transaction_date", to_date(col("transaction_date")))

    # Add total amount per row
    silver_df = silver_df.withColumn(
        "total_amount",
        col("quantity") * col("price")
    )

    # Write Silver Delta
    valid_df = silver_df.filter((col("quantity") > 0) & (col("price") > 0))
    invalid_df = silver_df.filter((col("quantity") <= 0) | (col("price") <= 0))

    # Write valid
    valid_df.write.format("delta").mode("overwrite").save("delta/silver/transactions")

    # Write invalid
    invalid_df.write.format("delta").mode("append").save("delta/quarantine/transactions")

    print("Silver transformation completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()