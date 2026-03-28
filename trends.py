import pandas as pd
from finvizfinance.group.performance import Performance

def fetch_performance_data(group: str = 'Sector') -> pd.DataFrame:
    """Pulls the performance table for a given group."""
    fg_perf = Performance()
    return fg_perf.screener_view(group=group)

def clean_performance_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans, formats, and restructures the performance DataFrame."""
    # Work on a copy to avoid SettingWithCopy warnings
    df_clean = df.copy()

    # Move 'Change' to the front (right after 'Name') if it exists
    if 'Change' in df_clean.columns:
        cols = df_clean.columns.tolist()
        cols.insert(1, cols.pop(cols.index('Change'))) 
        df_clean = df_clean[cols]

    # Clean up the data, converting string columns that contain a % to decimal
    for col in df_clean.columns:
        if pd.api.types.is_string_dtype(df_clean[col]) and col != 'Name':
            if df_clean[col].astype(str).str.contains('%').any():
                df_clean[col] = df_clean[col].str.rstrip('%').astype(float) / 100.0

    # Drop unnecessary columns safely
    cols_to_drop = ['Perf YTD', 'Avg Volume', 'Volume']
    df_clean.drop(columns=[c for c in cols_to_drop if c in df_clean.columns], inplace=True)

    # Rename 'Change' to 'Perf Day'
    df_clean.rename(columns={'Change': 'Perf Day'}, inplace=True)
    
    return df_clean

def analyze_trends(df: pd.DataFrame, time_cols: list) -> dict:
    """Categorizes rows into trends based on their chronological performance."""
    trends = {
        'constantly_up': [],
        'constantly_down': [],
        'positive_to_negative': [],
        'negative_to_positive': [],
        'mixed': []
    }

    for _, row in df.iterrows():
        vals = row[time_cols].values
        
        # Check for constant trends
        if all(v > 0 for v in vals):
            trends['constantly_up'].append(row['Name'])
        elif all(v < 0 for v in vals):
            trends['constantly_down'].append(row['Name'])
        else:
            # Check for transitions by counting how many times the sign changes
            signs = [1 if v > 0 else -1 for v in vals]
            sign_changes = sum(1 for i in range(len(signs)-1) if signs[i] != signs[i+1])
                    
            if sign_changes == 1:
                if signs[0] == 1 and signs[-1] == -1:
                    trends['positive_to_negative'].append(row['Name'])
                elif signs[0] == -1 and signs[-1] == 1:
                    trends['negative_to_positive'].append(row['Name'])
                else:
                    trends['mixed'].append(row['Name'])
            else:
                trends['mixed'].append(row['Name'])
                
    return trends

def print_trend_report(trends: dict):
    """Prints the formatted trend results to the console."""
    print("\n--- Sector Trends ---")
    print(f"Constantly Going Up: {', '.join(trends['constantly_up']) if trends['constantly_up'] else 'None'}")
    print(f"Constantly Going Down: {', '.join(trends['constantly_down']) if trends['constantly_down'] else 'None'}")
    print(f"Transitioning Positive to Negative: {', '.join(trends['positive_to_negative']) if trends['positive_to_negative'] else 'None'}")
    print(f"Transitioning Negative to Positive: {', '.join(trends['negative_to_positive']) if trends['negative_to_positive'] else 'None'}")
    print(f"Mixed/Fluctuating: {', '.join(trends['mixed']) if trends['mixed'] else 'None'}")

def main():
    # 1. Fetch
    raw_df = fetch_performance_data(group='Sector')
    
    # 2. Clean & Format
    df_perf = clean_performance_data(raw_df)

    # 3. Print DataFrame Configuration
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
        print(df_perf)

    # 4. Analyze
    time_cols = ['Perf Year', 'Perf Half', 'Perf Quart', 'Perf Month']
    trends = analyze_trends(df_perf, time_cols)

    # 5. Output
    print_trend_report(trends)

if __name__ == "__main__":
    main()
