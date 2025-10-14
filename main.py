import os
from datetime import date
from io import StringIO

import requests
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))


def create_constituents(df):
    tickers = []
    for _i, row in df.iterrows():
        ticker = row["ticker"]
        tickers.append(ticker)

    tickers_str = ",".join(tickers)

    return pd.DataFrame(
        {
            "date": date.today(),
            "tickers": [tickers_str],
        }
    )


def refresh_sp500():
    sp_500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(sp_500_url, headers=headers)
    response.raise_for_status()
    tables = pd.read_html(StringIO(response.text))
    sp_500_constituents = tables[0]

    sp_500_constituents = sp_500_constituents.rename(columns=str.lower)
    sp_500_constituents = sp_500_constituents[["symbol"]]
    sp_500_constituents["date"] = date.today()
    sp_500_constituents.columns = ["ticker", "date"]
    sp_500_constituents.sort_values(by="ticker", ascending=True, inplace=True)

    df = create_constituents(sp_500_constituents)

    sp500_hist = pd.read_csv("sp500_historical_components.csv")
    all_data = pd.concat([sp500_hist, df], ignore_index=True)

    all_data = all_data.drop_duplicates(subset=["tickers"], keep="first")
    all_data.to_csv("sp500_historical_components.csv", index=False)


def refresh_nasdaq100():
    url = "https://en.wikipedia.org/wiki/NASDAQ-100"
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()

    tables = pd.read_html(StringIO(resp.text))
    nasdaq100_constituents = tables[4]

    nasdaq100_constituents = nasdaq100_constituents.rename(columns=str.lower)
    nasdaq100_constituents = nasdaq100_constituents[["ticker"]]
    nasdaq100_constituents["date"] = date.today()
    nasdaq100_constituents.columns = ["ticker", "date"]
    nasdaq100_constituents.sort_values(by="ticker", ascending=True, inplace=True)

    df = create_constituents(nasdaq100_constituents)

    nasdaq100_hist = pd.read_csv("nasdaq100_historical_components.csv")
    all_data = pd.concat([nasdaq100_hist, df], ignore_index=True)

    all_data = all_data.drop_duplicates(subset=["tickers"], keep="first")
    all_data.to_csv("nasdaq100_historical_components.csv", index=False)


def main():
    refresh_sp500()
    refresh_nasdaq100()


if __name__ == "__main__":
    main()
