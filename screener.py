import argparse
import sys
import time
import pandas as pd
from finvizfinance.screener.overview import Overview
from finvizfinance.screener.valuation import Valuation
from finvizfinance.screener.financial import Financial
from finvizfinance.screener.ownership import Ownership
from finvizfinance.screener.performance import Performance
from finvizfinance.screener.technical import Technical


def fetch_group_tickers(filters_dict: dict) -> pd.DataFrame:
    """
    Pulls the screener data for the specified filters across all tabs and merges them.

    Args:
        filters_dict (dict): A dictionary of filters (e.g., {'Sector': 'Technology'})

    Returns:
        pd.DataFrame: A DataFrame containing the combined screener data.
    """
    tabs = [
        ("Overview", Overview),
        ("Valuation", Valuation),
        ("Financial", Financial),
        ("Ownership", Ownership),
        ("Performance", Performance),
        ("Technical", Technical),
    ]

    merged_df = None

    for tab_name, screener_class in tabs:
        try:
            print(f"  -> Fetching {tab_name} tab...")
            screener = screener_class()
            screener.set_filter(filters_dict=filters_dict)
            # Fetch the data. finvizfinance handles pagination automatically.
            df = screener.screener_view()

            if df.empty:
                continue

            if merged_df is None:
                merged_df = df
            else:
                # Only keep columns that aren't already in the merged dataframe, plus 'Ticker' to merge on
                new_cols = df.columns.difference(merged_df.columns).tolist()
                new_cols.append("Ticker")
                merged_df = pd.merge(merged_df, df[new_cols], on="Ticker", how="outer")

            # Add a delay to avoid Finviz's rate limits (HTTP 429 Too Many Requests)
            print(f"     (Waiting 3 seconds to respect rate limits...)")
            time.sleep(3)

        except Exception as e:
            print(f"Error fetching {tab_name} data from Finviz: {e}", file=sys.stderr)

    return merged_df if merged_df is not None else pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(
        description="Find all tickers in a specific Finviz group (Sector, Industry, Country)."
    )
    parser.add_argument(
        "--sector",
        type=str,
        help="Filter by Sector (e.g., 'Technology', 'Basic Materials')",
    )
    parser.add_argument(
        "--industry",
        type=str,
        help="Filter by Industry (e.g., 'Semiconductors', 'Gold')",
    )
    parser.add_argument(
        "--country", type=str, help="Filter by Country (e.g., 'USA', 'China')"
    )
    parser.add_argument(
        "--export",
        type=str,
        help="Optional: Export the full detailed results to a CSV file (e.g., 'tech_stocks.csv')",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Print the full detailed table instead of just the ticker symbols",
    )
    parser.add_argument(
        "--sort",
        type=str,
        help="Column to sort by (e.g., 'Market Cap', 'Float Short', 'Price', 'P/E')",
    )
    parser.add_argument(
        "--asc",
        action="store_true",
        help="Sort in ascending order (default is descending when --sort is used)",
    )

    args = parser.parse_args()

    # Build the filter dictionary based on provided arguments
    filters = {}
    if args.sector:
        filters["Sector"] = args.sector
    if args.industry:
        filters["Industry"] = args.industry
    if args.country:
        filters["Country"] = args.country

    if not filters:
        print(
            "Please provide at least one filter (--sector, --industry, or --country).",
            file=sys.stderr,
        )
        print("Example: python screener.py --sector Technology", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching all data tabs for filters: {filters}...")
    df = fetch_group_tickers(filters)

    if df.empty:
        print("No tickers found for the specified filters, or an error occurred.")
        sys.exit(0)

    # Apply sorting if requested
    if args.sort:
        if args.sort in df.columns:
            # Finviz often returns strings like "1.5B", "500M", or "2.5%" for metrics.
            def parse_finviz_metric(val):
                if pd.isna(val):
                    return 0.0
                s = str(val).strip().replace(",", "")
                if s == "-":
                    return float("inf") if args.asc else -float("inf")

                mult = 1.0
                if s.endswith("B"):
                    mult = 1e9
                    s = s[:-1]
                elif s.endswith("M"):
                    mult = 1e6
                    s = s[:-1]
                elif s.endswith("K"):
                    mult = 1e3
                    s = s[:-1]
                elif s.endswith("%"):
                    mult = 0.01
                    s = s[:-1]

                try:
                    return float(s) * mult
                except ValueError:
                    return 0.0

            df["_sort_val"] = df[args.sort].apply(parse_finviz_metric)
            df = df.sort_values(by="_sort_val", ascending=args.asc).drop(
                columns=["_sort_val"]
            )
            print(
                f"Sorted by {args.sort} ({'ascending' if args.asc else 'descending'})"
            )
        else:
            print(f"Warning: Sort column '{args.sort}' not found.")
            print(f"Available columns: {', '.join(df.columns)}")

    # Extract just the ticker symbols
    tickers = df["Ticker"].tolist()
    print(f"\nFound {len(tickers)} tickers:\n")

    if args.details:
        with pd.option_context(
            "display.max_rows", None, "display.max_columns", None, "display.width", 1000
        ):
            print(df)
    else:
        print(", ".join(tickers))

    if args.export:
        df.to_csv(args.export, index=False)
        print(f"\nExported full details to {args.export}")


if __name__ == "__main__":
    main()
