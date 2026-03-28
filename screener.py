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
        help="Filter by Sector (e.g., 'Technology', 'Basic Materials')"
    )
    parser.add_argument(
        "--industry", 
        type=str, 
        help="Filter by Industry (e.g., 'Semiconductors', 'Gold')"
    )
    parser.add_argument(
        "--country", 
        type=str, 
        help="Filter by Country (e.g., 'USA', 'China')"
    )
    parser.add_argument(
        "--export", 
        type=str, 
        help="Optional: Export the full detailed results to a CSV file (e.g., 'tech_stocks.csv')"
    )
    parser.add_argument(
        "--details", 
        action="store_true", 
        help="Print the full detailed table instead of just the ticker symbols"
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
        print("Please provide at least one filter (--sector, --industry, or --country).", file=sys.stderr)
        print("Example: python screener.py --sector Technology", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching data for filters: {filters}...")
    df = fetch_group_tickers(filters)

    if df.empty:
        print("No tickers found for the specified filters, or an error occurred.")
        sys.exit(0)

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
