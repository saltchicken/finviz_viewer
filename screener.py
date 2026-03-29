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

            # Only apply filters if they exist (allows fetching all tickers)
            if filters_dict:
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


def clean_columns_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """Formats column names to be strictly PostgreSQL-friendly."""
    df_db = df.copy()
    df_db.columns = (
        df_db.columns.str.lower()
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("%", "pct")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("-", "_")
    )
    return df_db


def main():
    parser = argparse.ArgumentParser(
        description="Find all tickers in a specific Finviz group (Sector, Industry, Country) or ALL tickers."
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
        "--all",
        action="store_true",
        help="Fetch ALL tickers (Warning: Takes a long time)",
    )

    # --- New Database Arguments ---
    parser.add_argument(
        "--db-url",
        type=str,
        help="PostgreSQL Connection URL (e.g., postgresql://user:pass@localhost:5432/dbname)",
    )
    parser.add_argument(
        "--db-table",
        type=str,
        help="The name of the PostgreSQL table to insert the data into",
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

    if not filters and not args.all:
        print(
            "Please provide at least one filter (--sector, --industry, or --country) OR use the --all flag.",
            file=sys.stderr,
        )
        print("Example 1: python screener.py --sector Technology", file=sys.stderr)
        print("Example 2: python screener.py --all", file=sys.stderr)
        sys.exit(1)

    if args.all:
        print("Fetching ALL data tabs for ALL tickers (no filters applied)...")
    else:
        print(f"Fetching all data tabs for filters: {filters}...")

    df = fetch_group_tickers(filters)

    if df.empty:
        print("No tickers found for the specified filters, or an error occurred.")
        sys.exit(0)

    # Extract just the ticker symbols
    tickers = df["Ticker"].tolist()
    print(f"\nFound {len(tickers)} tickers:\n")

    # --- New Database Export Logic ---
    if args.db_url and args.db_table:
        print(f"\nExporting to PostgreSQL database table: '{args.db_table}'...")
        try:
            from sqlalchemy import create_engine

            # Create SQLAlchemy engine
            engine = create_engine(args.db_url)

            # Clean dataframe column names to be Postgres friendly (e.g. "Market Cap" -> "market_cap")
            df_db = clean_columns_for_db(df)

            # Write to SQL. 'replace' drops the table before inserting new values.
            # Use 'append' if you want to keep historical data.
            df_db.to_sql(args.db_table, engine, if_exists="replace", index=False)

            print(f"Successfully populated PostgreSQL table: {args.db_table}")

        except ImportError:
            print("\nError: Missing database dependencies.", file=sys.stderr)
            print("Please run: pip install sqlalchemy psycopg2-binary", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"\nDatabase export failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
