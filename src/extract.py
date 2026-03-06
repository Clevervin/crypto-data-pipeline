from __future__ import annotations

from datetime import datetime, UTC

import requests


def fetch_crypto_market_data(config: dict, logger) -> tuple[list[dict], str]:
    """
    Fetch cryptocurrency market data from CoinGecko API.

    Returns
    -------
    data : list of dict
        Raw API response records
    snapshot_ts : str
        Timestamp when the data was extracted
    """

    api_config = config["api"]

    url = f"{api_config['base_url']}{api_config['endpoint']}"

    params = {
        "vs_currency": api_config["vs_currency"],
        "per_page": api_config["per_page"],
        "page": api_config["page"],
    }

    logger.info("Requesting data from CoinGecko API...")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    snapshot_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Fetched {len(data)} crypto records.")

    return data, snapshot_ts