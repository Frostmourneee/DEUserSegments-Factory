from config import get_db_config
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_date,
    datediff,
    lit,
    max,
    sum,
    to_date,
    when,
)


def main():
    spark = (
        SparkSession.builder.appName("UserSegmentsETL")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
        .getOrCreate()
    )

    db_url, db_user, db_password = get_db_config()

    db_config = {
        "url": db_url,
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }

    users_df = spark.read.format("jdbc").options(**db_config, dbtable="users").load()
    orders_df = spark.read.format("jdbc").options(**db_config, dbtable="orders").load()
    activity_df = (
        spark.read.format("jdbc").options(**db_config, dbtable="user_activity").load()
    )

    user_orders = (
        orders_df.filter(col("status") == "completed")
        .groupBy("user_id")
        .agg(
            count("order_id").alias("total_orders"),
            sum("amount").alias("total_income"),
            max("order_date").alias("last_order_date"),
        )
    )

    user_activity_agg = activity_df.groupBy("user_id").agg(
        max("activity_date").alias("last_activity_date"),
        avg("duration_seconds").alias("avg_session_duration"),
    )

    # Сначала строим полную витрину
    full_mart = (
        users_df.join(user_orders, "user_id", "left")
        .join(user_activity_agg, "user_id", "left")
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("total_income", coalesce(col("total_income"), lit(0)))
        .withColumn("last_activity_date", to_date(col("last_activity_date")))
        .withColumn("calculation_date", current_date())
        .withColumn(
            "segment_name",
            when(col("total_orders") >= 10, "vip")
            .when((col("total_orders") >= 5) & (col("total_income") >= 500), "loyal")
            .when(col("total_orders") >= 3, "active")
            .when(datediff(current_date(), col("registration_date")) <= 30, "new")
            .when(datediff(current_date(), col("last_order_date")) > 90, "churned")
            .otherwise("regular"),
        )
    )

    # Теперь user_segments — просто выборка полей
    user_segments = full_mart.select("user_id", "segment_name", "calculation_date")

    # А marketing_mart — почти весь full_mart
    marketing_mart = full_mart.select(
        "user_id",
        "segment_name",
        "total_orders",
        "total_income",
        "last_activity_date",
        "calculation_date",
    )

    # Записываем результаты
    user_segments.write.format("jdbc").options(
        **db_config, dbtable="analytics.user_segments"
    ).mode("overwrite").save()

    marketing_mart.write.format("jdbc").options(
        **db_config, dbtable="analytics.marketing_mart"
    ).mode("overwrite").save()

    print("ETL completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
