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
