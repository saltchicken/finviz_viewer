import pandas as pd
from finvizfinance.group.performance import Performance

# 1. Pull the performance table
fg_perf = Performance()
# Options for 'group': 'Sector', 'Industry', 'Industry (Basic Materials)', etc.
df_perf = fg_perf.screener_view(group='Sector')

# 2. Move 'Change' to the front (right after 'Name')
cols = df_perf.columns.tolist()
cols.insert(1, cols.pop(cols.index('Change'))) 
df_perf = df_perf[cols]

# 3. Clean up the data, converting string columns that contain a % to decimal
for col in df_perf.columns:
    if df_perf[col].dtype == 'string' and col != 'Name':
        # Check if any value in the column contains a '%'
        if df_perf[col].astype(str).str.contains('%').any():
            # Strip the '%', convert to float, and divide by 100 to make it a decimal
            df_perf[col] = df_perf[col].str.rstrip('%').astype(float) / 100.0

# 4 Drop the 'Perf YTD', 'Avg Volume', and 'Volume' column
df_perf.drop('Perf YTD', axis=1, inplace=True)
df_perf.drop('Avg Volume', axis=1, inplace=True)
df_perf.drop('Volume', axis=1, inplace=True)

# 5. Rename 'Change' to 'Perf Day'
df_perf.rename(columns={'Change': 'Perf Day'}, inplace=True)

with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
    print(df_perf)


# List the time periods in chronological order (oldest to newest)
time_cols = ['Perf Year', 'Perf Half', 'Perf Quart', 'Perf Month']

trends = {
    'constantly_up': [],
    'constantly_down': [],
    'positive_to_negative': [],
    'negative_to_positive': [],
    'mixed': []
}

for index, row in df_perf.iterrows():
    # Extract the values for the time columns
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

# Print the results
print("\n--- Sector Trends ---")
print(f"Constantly Going Up: {', '.join(trends['constantly_up']) if trends['constantly_up'] else 'None'}")
print(f"Constantly Going Down: {', '.join(trends['constantly_down']) if trends['constantly_down'] else 'None'}")
print(f"Transitioning Positive to Negative: {', '.join(trends['positive_to_negative']) if trends['positive_to_negative'] else 'None'}")
print(f"Transitioning Negative to Positive: {', '.join(trends['negative_to_positive']) if trends['negative_to_positive'] else 'None'}")
print(f"Mixed/Fluctuating: {', '.join(trends['mixed']) if trends['mixed'] else 'None'}")
