from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, to_date, date_format


def process_sales():
    spark = (
        SparkSession.builder
        .appName("SalesBusinessInsights")
        .master("local[2]")
        .getOrCreate()
    )

    try:
        # 1. read source data
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv("/opt/airflow/data/sales.csv")
        )

        print("SOURCE SALES DATA")
        df.show()

        # 2. clean and validate data
        df = (
            df.dropDuplicates(["order_id"])
            .dropna(
                subset=[
                    "order_id", "product", "region",
                    "quantity", "unit_price", "order_date"
                ]
            )
            .filter(
                (col("quantity") > 0) &
                (col("unit_price") > 0)
            )
        )

        # 3. calculate revenue
        df = (
            df.withColumn("order_date", to_date("order_date"))
            .withColumn(
                "revenue",
                col("quantity") * col("unit_price")
            )
        )

        print("CLEANED SALES DATA")
        df.show()

        # 4. revenue by region
        regional = (
            df.groupBy("region")
            .agg(spark_sum("revenue").alias("total_revenue"))
            .orderBy(col("total_revenue").desc())
        )

        print("REVENUE BY REGION")
        regional.show()

        # 5. revenue by product
        products = (
            df.groupBy("product")
            .agg(spark_sum("revenue").alias("total_revenue"))
            .orderBy(col("total_revenue").desc())
        )

        print("REVENUE BY PRODUCT")
        products.show()

        # 6. revenue by month
        monthly = (
            df.withColumn(
                "month", date_format("order_date", "yyyy-MM")
            )
            .groupBy("month")
            .agg(spark_sum("revenue").alias("total_revenue"))
            .orderBy("month")
        )

        print("MONTHLY REVENUE")
        monthly.show()

        # 7. save results as csv files
        regional.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv("/opt/airflow/output/regional_sales")

        products.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv("/opt/airflow/output/product_sales")

        monthly.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv("/opt/airflow/output/monthly_sales")

        print("SALES ANALYSIS COMPLETED SUCCESSFULLY")

    finally:
        spark.stop()


with DAG(
    dag_id="pyspark_sales_business_insights",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["pyspark", "sales", "business-insights"],
) as dag:

    analyze_sales = PythonOperator(
        task_id="analyze_sales",
        python_callable=process_sales,
    )
