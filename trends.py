import argparse
import pandas as pd
from finvizfinance.group.performance import Performance


def fetch_performance_data(group: str = "Sector") -> pd.DataFrame:
    """Pulls the performance table for a given group."""
    fg_perf = Performance()
    return fg_perf.screener_view(group=group)


def clean_performance_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans, formats, and restructures the performance DataFrame."""
    # Work on a copy to avoid SettingWithCopy warnings
    df_clean = df.copy()

    # Move 'Change' to the front (right after 'Name') if it exists
    if "Change" in df_clean.columns:
        cols = df_clean.columns.tolist()
        cols.insert(1, cols.pop(cols.index("Change")))
        df_clean = df_clean[cols]

    # Clean up the data, converting string columns that contain a % to decimal
    for col in df_clean.columns:
        if pd.api.types.is_string_dtype(df_clean[col]) and col != "Name":
            if df_clean[col].astype(str).str.contains("%").any():
                df_clean[col] = df_clean[col].str.rstrip("%").astype(float) / 100.0

    # Ensure Rel Volume is numeric (Finviz sometimes leaves it as a string like "1.25")
    if "Rel Volume" in df_clean.columns:
        df_clean["Rel Volume"] = pd.to_numeric(df_clean["Rel Volume"], errors="coerce")

    # Drop unnecessary columns safely
    cols_to_drop = ["Perf YTD", "Avg Volume", "Volume"]
    df_clean.drop(
        columns=[c for c in cols_to_drop if c in df_clean.columns], inplace=True
    )

    # Rename 'Change' to 'Perf Day'
    df_clean.rename(columns={"Change": "Perf Day"}, inplace=True)

    return df_clean


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates a momentum/trend score for each group based on weighted performance."""
    # Define weights. You can easily adjust these to prioritize short vs long-term performance.
    weights = {
        "Perf Day": 0.5,
        "Perf Week": 1.0,
        "Perf Month": 3.0,   # High weight on 1-month momentum
        "Perf Quart": 2.0,   # Moderate weight on quarterly momentum
        "Perf Half": 1.5,
        "Perf Year": 1.0
    }

    df_scored = df.copy()
    df_scored["Score"] = 0.0

    # Apply weights to calculate the total score
    for col, weight in weights.items():
        if col in df_scored.columns:
            # fillna(0) ensures missing data doesn't turn the whole score into NaN
            df_scored["Score"] += df_scored[col].fillna(0) * weight

    # Incorporate Rel Volume as a conviction multiplier into a NEW score
    if "Rel Volume" in df_scored.columns:
        # Multiply the score by Rel Volume for the new column. 
        df_scored["Vol Score"] = df_scored["Score"] * df_scored["Rel Volume"].fillna(1.0)
    else:
        df_scored["Vol Score"] = df_scored["Score"]

    # Round to 4 decimal places for cleaner display
    df_scored["Score"] = df_scored["Score"].round(4)
    df_scored["Vol Score"] = df_scored["Vol Score"].round(4)
    
    # Sort the DataFrame by Vol Score descending so the best performers appear first
    df_scored = df_scored.sort_values(by="Vol Score", ascending=False).reset_index(drop=True)
    
    return df_scored


def analyze_trends(df: pd.DataFrame, time_cols: list) -> pd.DataFrame:
    """Categorizes rows into trends based on their chronological performance and adds a 'Trend' column."""
    df_analyzed = df.copy()
    trends_list = []

    for _, row in df_analyzed.iterrows():
        vals = row[time_cols].values
        
        # Check for constant trends
        if all(v > 0 for v in vals):
            trends_list.append("Constantly Up")
        elif all(v < 0 for v in vals):
            trends_list.append("Constantly Down")
        else:
            # Check for transitions by counting how many times the sign changes
            signs = [1 if v > 0 else -1 for v in vals]
            sign_changes = sum(
                1 for i in range(len(signs) - 1) if signs[i] != signs[i + 1]
            )

            if sign_changes == 1:
                if signs[0] == 1 and signs[-1] == -1:
                    trends_list.append("Positive to Negative")
                elif signs[0] == -1 and signs[-1] == 1:
                    trends_list.append("Negative to Positive")
                else:
                    trends_list.append("Mixed")
            else:
                trends_list.append("Mixed")

    # Add the evaluated trend as a new column to the dataframe
    df_analyzed["Trend"] = trends_list
    return df_analyzed


def main():
    parser = argparse.ArgumentParser(
        description="Extract, clean, and analyze group performance trends using Finviz data."
    )
    parser.add_argument(
        "--group",
        type=str,
        default="Sector",
        help="The Finviz group to analyze (e.g., 'Sector', 'Industry', 'Country'). Default is 'Sector'.",
    )
    args = parser.parse_args()

    # 1. Fetch
    raw_df = fetch_performance_data(group=args.group)
    
    # 2. Clean
    df_perf = clean_performance_data(raw_df)
    
    # 3. Score & Sort
    df_perf = calculate_scores(df_perf)

    # 4. Analyze (Add Trend column)
    time_cols = ["Perf Year", "Perf Half", "Perf Quart", "Perf Month"]
    df_perf = analyze_trends(df_perf, time_cols)

    # Print the DataFrame WITH the newly added Trend column
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 1000
    ):
        print(df_perf)

if __name__ == "__main__":
    main()
