from datetime import datetime, UTC

from src.utils import load_config, get_db_engine, setup_logger
from src.extract import fetch_crypto_market_data
from src.transform import transform_crypto_data
from src.load import (
    load_dim_coin,
    load_fact_market_snapshot,
    insert_etl_audit_start,
    update_etl_audit_end,
)


def main():
    config = load_config()

    logger = setup_logger(
        name=config["app"]["name"],
        level=config["app"]["log_level"]
    )

    logger.info("Configuration loaded successfully.")

    engine = get_db_engine(config)
    logger.info("Database engine created successfully.")

    schema = config["postgres"]["schema"]
    pipeline_name = config["app"]["name"]
    run_start_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    run_id = insert_etl_audit_start(
        engine=engine,
        schema=schema,
        pipeline_name=pipeline_name,
        run_start_ts=run_start_ts
    )

    try:
        # Extract
        data, snapshot_ts = fetch_crypto_market_data(config, logger)
        extracted_rows = len(data)

        # Transform
        dim_coin_df, fact_market_snapshot_df = transform_crypto_data(
            data,
            snapshot_ts
        )

        # Load
        dim_rows_loaded = load_dim_coin(
            dim_coin_df,
            engine,
            schema,
            logger
        )

        fact_rows_loaded = load_fact_market_snapshot(
            fact_market_snapshot_df,
            engine,
            schema,
            logger
        )

        run_end_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        update_etl_audit_end(
            engine=engine,
            schema=schema,
            run_id=run_id,
            run_end_ts=run_end_ts,
            status="SUCCESS",
            extracted_rows=extracted_rows,
            dim_coin_rows_loaded=dim_rows_loaded,
            fact_rows_loaded=fact_rows_loaded,
            error_message=None
        )

        logger.info("Pipeline completed successfully.")

    except Exception as e:
        run_end_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

        update_etl_audit_end(
            engine=engine,
            schema=schema,
            run_id=run_id,
            run_end_ts=run_end_ts,
            status="FAILED",
            extracted_rows=0,
            dim_coin_rows_loaded=0,
            fact_rows_loaded=0,
            error_message=str(e)
        )

        logger.exception("Pipeline failed.")
        raise


if __name__ == "__main__":
    main()