from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def load_dim_coin(
    dim_coin_df: pd.DataFrame,
    engine: Engine,
    schema: str,
    logger
):
    """
    Load only new coins into dim_coin.
    """

    logger.info("Loading dim_coin table...")

    existing_df = pd.read_sql(
        f"SELECT coin_id FROM {schema}.dim_coin",
        engine
    )

    existing_ids = set(existing_df["coin_id"])

    new_rows = dim_coin_df[~dim_coin_df["coin_id"].isin(existing_ids)]

    if new_rows.empty:
        logger.info("No new coins to insert.")
        return 0

    new_rows.to_sql(
        name="dim_coin",
        con=engine,
        schema=schema,
        if_exists="append",
        index=False
    )

    logger.info(f"{len(new_rows)} new coins inserted into dim_coin.")
    return len(new_rows)


def load_fact_market_snapshot(
    fact_df: pd.DataFrame,
    engine: Engine,
    schema: str,
    logger
):
    """
    Load only new snapshot rows into fact_market_snapshot.
    """

    logger.info("Loading fact_market_snapshot table...")

    existing_df = pd.read_sql(
        f"SELECT snapshot_ts, coin_id FROM {schema}.fact_market_snapshot",
        engine
    )

    if not existing_df.empty:
        existing_keys = set(
            zip(existing_df["snapshot_ts"].astype(str), existing_df["coin_id"])
        )

        fact_df["snapshot_ts"] = fact_df["snapshot_ts"].astype(str)

        new_rows = fact_df[
            ~fact_df.apply(
                lambda row: (row["snapshot_ts"], row["coin_id"]) in existing_keys,
                axis=1
            )
        ]
    else:
        new_rows = fact_df.copy()

    if new_rows.empty:
        logger.info("No new fact rows to insert.")
        return 0

    new_rows.to_sql(
        name="fact_market_snapshot",
        con=engine,
        schema=schema,
        if_exists="append",
        index=False
    )

    logger.info(f"{len(new_rows)} new rows inserted into fact_market_snapshot.")
    return len(new_rows)


def insert_etl_audit_start(
    engine: Engine,
    schema: str,
    pipeline_name: str,
    run_start_ts: str
) -> int:
    """
    Insert a RUNNING record into the ETL audit table and return run_id.
    """

    query = text(f"""
        INSERT INTO {schema}.etl_run_audit (
            pipeline_name,
            run_start_ts,
            status
        )
        VALUES (
            :pipeline_name,
            :run_start_ts,
            :status
        )
        RETURNING run_id;
    """)

    with engine.begin() as conn:
        result = conn.execute(
            query,
            {
                "pipeline_name": pipeline_name,
                "run_start_ts": run_start_ts,
                "status": "RUNNING"
            }
        )
        run_id = result.scalar()

    return run_id


def update_etl_audit_end(
    engine: Engine,
    schema: str,
    run_id: int,
    run_end_ts: str,
    status: str,
    extracted_rows: int,
    dim_coin_rows_loaded: int,
    fact_rows_loaded: int,
    error_message: str | None = None
) -> None:
    """
    Update the ETL audit record at the end of the pipeline run.
    """

    query = text(f"""
        UPDATE {schema}.etl_run_audit
        SET
            run_end_ts = :run_end_ts,
            status = :status,
            extracted_rows = :extracted_rows,
            dim_coin_rows_loaded = :dim_coin_rows_loaded,
            fact_rows_loaded = :fact_rows_loaded,
            error_message = :error_message
        WHERE run_id = :run_id;
    """)

    with engine.begin() as conn:
        conn.execute(
            query,
            {
                "run_end_ts": run_end_ts,
                "status": status,
                "extracted_rows": extracted_rows,
                "dim_coin_rows_loaded": dim_coin_rows_loaded,
                "fact_rows_loaded": fact_rows_loaded,
                "error_message": error_message,
                "run_id": run_id
            }
        )