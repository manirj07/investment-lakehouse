from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# -------------------------
# Bronze
# -------------------------
from bronze.market_prices_ingestion import run as market_prices_run
from bronze.holdings_ingestion import run as holdings_run
from bronze.security_master_ingestion import run as security_master_run
from bronze.esg_scores_ingestion import run as esg_scores_run

# -------------------------
# Silver
# -------------------------
from silver.daily_returns import run as daily_returns_run
from silver.portfolio_positions import run as portfolio_positions_run
from silver.portfolio_weights import run as portfolio_weights_run
from silver.sector_exposure import run as sector_exposure_run
from silver.country_exposure import run as country_exposure_run
from silver.portfolio_daily_performance import run as portfolio_daily_performance_run
from silver.performance_attribution import run as performance_attribution_run
from silver.portfolio_esg_score import run as portfolio_esg_score_run

# -------------------------
# Gold Dimensions
# -------------------------
from gold.dim_security import run as dim_security_run
from gold.dim_portfolio import run as dim_portfolio_run
from gold.dim_date import run as dim_date_run

# -------------------------
# Gold Facts
# -------------------------
from gold.fact_portfolio_performance import run as fact_portfolio_performance_run
from gold.fact_portfolio_daily_performance import run as fact_portfolio_daily_performance_run
from gold.fact_performance_attribution import run as fact_performance_attribution_run


default_args = {
    "owner": "maniraj",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="investment_platform_dag",
    description="Investment Lakehouse Medallion Architecture Pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["investment", "lakehouse", "delta", "medallion"],
) as dag:

    # =====================================================
    # BRONZE
    # =====================================================

    market_prices = PythonOperator(
        task_id="market_prices_ingestion",
        python_callable=market_prices_run,
    )

    holdings = PythonOperator(
        task_id="holdings_ingestion",
        python_callable=holdings_run,
    )

    security_master = PythonOperator(
        task_id="security_master_ingestion",
        python_callable=security_master_run,
    )

    esg_scores = PythonOperator(
        task_id="esg_scores_ingestion",
        python_callable=esg_scores_run,
    )

    # =====================================================
    # SILVER
    # =====================================================

    daily_returns = PythonOperator(
        task_id="daily_returns",
        python_callable=daily_returns_run,
    )

    portfolio_positions = PythonOperator(
        task_id="portfolio_positions",
        python_callable=portfolio_positions_run,
    )

    portfolio_weights = PythonOperator(
        task_id="portfolio_weights",
        python_callable=portfolio_weights_run,
    )

    sector_exposure = PythonOperator(
        task_id="sector_exposure",
        python_callable=sector_exposure_run,
    )

    country_exposure = PythonOperator(
        task_id="country_exposure",
        python_callable=country_exposure_run,
    )

    portfolio_daily_performance = PythonOperator(
        task_id="portfolio_daily_performance",
        python_callable=portfolio_daily_performance_run,
    )

    performance_attribution = PythonOperator(
        task_id="performance_attribution",
        python_callable=performance_attribution_run,
    )

    portfolio_esg_score = PythonOperator(
        task_id="portfolio_esg_score",
        python_callable=portfolio_esg_score_run,
    )

    # =====================================================
    # GOLD DIMENSIONS
    # =====================================================

    dim_security = PythonOperator(
        task_id="dim_security",
        python_callable=dim_security_run,
    )

    dim_portfolio = PythonOperator(
        task_id="dim_portfolio",
        python_callable=dim_portfolio_run,
    )

    dim_date = PythonOperator(
        task_id="dim_date",
        python_callable=dim_date_run,
    )

    # =====================================================
    # GOLD FACTS
    # =====================================================

    fact_portfolio_performance = PythonOperator(
        task_id="fact_portfolio_performance",
        python_callable=fact_portfolio_performance_run,
    )

    fact_portfolio_daily_performance = PythonOperator(
        task_id="fact_portfolio_daily_performance",
        python_callable=fact_portfolio_daily_performance_run,
    )

    fact_performance_attribution = PythonOperator(
        task_id="fact_performance_attribution",
        python_callable=fact_performance_attribution_run,
    )

    # =====================================================
    # DEPENDENCIES
    # =====================================================

    # Bronze layer runs in parallel
    bronze_complete = [
        market_prices,
        holdings,
        security_master,
        esg_scores,
    ]

    # Silver core
    bronze_complete >> daily_returns

    [daily_returns, holdings] >> portfolio_positions

    portfolio_positions >> portfolio_weights

    # Silver analytics
    portfolio_weights >> sector_exposure
    portfolio_weights >> country_exposure
    portfolio_weights >> portfolio_esg_score

    [daily_returns, portfolio_weights] >> performance_attribution

    [daily_returns, portfolio_weights] >> portfolio_daily_performance

    # Gold dimensions
    portfolio_positions >> dim_security
    portfolio_positions >> dim_portfolio
    portfolio_daily_performance >> dim_date

    # Gold facts
    [dim_security, dim_portfolio, dim_date] >> fact_portfolio_daily_performance

    [dim_security, dim_portfolio, dim_date] >> fact_performance_attribution

    [dim_security, dim_portfolio, dim_date] >> fact_portfolio_performance