from __future__ import annotations

import pandas as pd


def transform_crypto_data(data: list[dict], snapshot_ts: str):
    """
    Transform raw CoinGecko API JSON into structured tables.

    Returns
    -------
    dim_coin_df : pandas.DataFrame
    fact_market_snapshot_df : pandas.DataFrame
    """

    df = pd.DataFrame(data)

    # Dimension table
    dim_coin_df = df[["id", "symbol", "name"]].copy()

    dim_coin_df = dim_coin_df.rename(
        columns={
            "id": "coin_id"
        }
    )

    dim_coin_df = dim_coin_df.drop_duplicates()

    # Fact table
    fact_market_snapshot_df = df[
        [
            "id",
            "current_price",
            "market_cap",
            "total_volume"
        ]
    ].copy()

    fact_market_snapshot_df = fact_market_snapshot_df.rename(
        columns={
            "id": "coin_id",
            "current_price": "price_usd",
            "market_cap": "market_cap_usd",
            "total_volume": "volume_24h_usd"
        }
    )

    fact_market_snapshot_df["snapshot_ts"] = snapshot_ts

    return dim_coin_df, fact_market_snapshot_df