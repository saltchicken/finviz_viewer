import argparse
import sys
import pandas as pd
from finvizfinance.screener.overview import Overview


def fetch_group_tickers(filters_dict: dict) -> pd.DataFrame:
    """
    Pulls the screener overview data for the specified filters.

    Args:
        filters_dict (dict): A dictionary of filters (e.g., {'Sector': 'Technology'})

    Returns:
        pd.DataFrame: A DataFrame containing the screener overview data.
    """
    try:
        screener = Overview()
        screener.set_filter(filters_dict=filters_dict)
        # Fetch the data. Finviz might paginate; finvizfinance handles this automatically.
        df = screener.screener_view()
        return df
    except Exception as e:
        print(f"Error fetching data from Finviz: {e}", file=sys.stderr)
        return pd.DataFrame()


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
        help="Column to sort by (e.g., 'Market Cap', 'Volume', 'Price', 'P/E')",
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

    print(f"Fetching data for filters: {filters}...")
    df = fetch_group_tickers(filters)

    if df.empty:
        print("No tickers found for the specified filters, or an error occurred.")
        sys.exit(0)

    # Apply sorting if requested
    if args.sort:
        if args.sort in df.columns:
            # Finviz often returns strings like "1.5B", "500M", or "2.5%" for metrics.
            # We need to convert these to floats to sort them accurately.
            def parse_finviz_metric(val):
                if pd.isna(val):
                    return 0.0
                s = str(val).strip().replace(",", "")
                if s == "-":
                    return (
                        float("inf") if args.asc else -float("inf")
                    )  # Push missing data to the bottom

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
            # Default to descending (e.g., highest Market Cap first) unless --asc is passed
            df = df.sort_values(by="_sort_val", ascending=args.asc).drop(
                columns=["_sort_val"]
            )
            print(
                f"Sorted by {args.sort} ({'ascending' if args.asc else 'descending'})"
            )
        else:
            print(
                f"Warning: Sort column '{args.sort}' not found. Available columns: {', '.join(df.columns)}"
            )

    # Extract just the ticker symbols
    tickers = df["Ticker"].tolist()
    print(f"\nFound {len(tickers)} tickers:\n")

    if args.details:
        # Print the entire dataframe with all columns
        with pd.option_context(
            "display.max_rows", None, "display.max_columns", None, "display.width", 1000
        ):
            print(df)
    else:
        # Print just a clean, comma-separated list of tickers
        print(", ".join(tickers))

    # Export to CSV if requested
    if args.export:
        df.to_csv(args.export, index=False)
        print(f"\nExported full details to {args.export}")


if __name__ == "__main__":
    main()
