from pyspark.sql.functions import col, monotonically_increasing_id, to_date, upper
from pyspark.sql import SparkSession
from src.utils.spark_session import create_spark

def main():
    spark = create_spark("Silver_Dimensions_Fact")

    # Read Silver transactions
    silver_df = spark.read.format("delta").load("delta/silver/transactions")

    # ---------------------------
    # 1. dim_date
    # ---------------------------
    dim_date = (
        silver_df.select(col("transaction_date").alias("full_date"))
        .distinct()
        .withColumn("date_key", monotonically_increasing_id())
        .withColumn("day", col("full_date").dayofmonth)
        .withColumn("month", col("full_date").month)
        .withColumn("quarter", col("full_date").quarter)
        .withColumn("year", col("full_date").year)
        .select("date_key", "full_date", "day", "month", "quarter", "year")
    )

    dim_date.write.format("delta").mode("overwrite").save("delta/silver/dim_date")

    # ---------------------------
    # 2. dim_member
    # ---------------------------
    dim_member = (
        silver_df.select(col("customer_id"), col("customer_name"))
        .distinct()
        .withColumn("member_key", monotonically_increasing_id())
        .withColumnRenamed("customer_id", "natural_id")
        .withColumnRenamed("customer_name", "member_name")
    )

    dim_member.write.format("delta").mode("overwrite").save("delta/silver/dim_member")

    # ---------------------------
    # 3. dim_store
    # ---------------------------
    dim_store = (
        silver_df.select(col("store_name"), col("store_type"), col("city"))
        .distinct()
        .withColumn("store_key", monotonically_increasing_id())
    )

    dim_store.write.format("delta").mode("overwrite").save("delta/silver/dim_store")

    # ---------------------------
    # 4. dim_item
    # ---------------------------
    dim_item = (
        silver_df.select(
            col("item_name"),
            col("category").alias("category_name")
        )
        .distinct()
        .withColumn("item_key", monotonically_increasing_id())
    )

    dim_item.write.format("delta").mode("overwrite").save("delta/silver/dim_item")

    # ---------------------------
    # 5. dim_category
    # ---------------------------
    dim_category = (
        silver_df.select(col("category").alias("category_name"))
        .distinct()
        .withColumn("category_key", monotonically_increasing_id())
    )

    dim_category.write.format("delta").mode("overwrite").save("delta/silver/dim_category")

    # ---------------------------
    # 6. fact_purchase_items
    # ---------------------------

    # Join Silver with dimensions to get surrogate keys
    fact_df = (
        silver_df
        # join dim_date
        .join(dim_date, silver_df.transaction_date == dim_date.full_date, "left")
        # join dim_member
        .join(dim_member, silver_df.customer_id == dim_member.natural_id, "left")
        # join dim_store
        .join(dim_store, silver_df.store_name == dim_store.store_name, "left")
        # join dim_item
        .join(dim_item, silver_df.item_name == dim_item.item_name, "left")
        # join dim_category
        .join(dim_category, silver_df.category == dim_category.category_name, "left")
        # select fact table columns
        .select(
            col("transaction_id"),
            col("date_key"),
            col("member_key"),
            col("store_key"),
            col("item_key"),
            col("category_key"),
            col("quantity"),
            col("price"),
            col("total_amount"),
            col("payment_method"),
        )
        .withColumn("fact_key", monotonically_increasing_id())
    )

    fact_df.write.format("delta").mode("overwrite").save("delta/silver/fact_purchase_items")

    print("Dimensions and fact table built successfully.")

    spark.stop()


if __name__ == "__main__":
    main()