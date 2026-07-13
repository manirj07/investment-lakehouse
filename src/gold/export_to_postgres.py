from common.spark_session import get_spark


POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"


def write_to_postgres(df, table_name):

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"reporting.{table_name}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def run():

    spark = get_spark()

    tables = [
        ("portfolio_weights", "/opt/data/silver/portfolio_weights"),
        ("sector_exposure", "/opt/data/silver/sector_exposure"),
        ("country_exposure", "/opt/data/silver/country_exposure"),
        ("portfolio_esg_score", "/opt/data/silver/portfolio_esg_score"),
        ("fact_portfolio_daily_performance", "/opt/data/gold/fact_portfolio_daily_performance"),
        ("fact_portfolio_performance", "/opt/data/gold/fact_portfolio_performance"),
        ("fact_performance_attribution", "/opt/data/gold/fact_performance_attribution")
    ]

    for table_name, path in tables:

        print(f"Exporting {table_name}")

        df = spark.read.format("delta").load(path)

        write_to_postgres(df, table_name)

    spark.stop()

    print("Export completed successfully")


if __name__ == "__main__":
    run()