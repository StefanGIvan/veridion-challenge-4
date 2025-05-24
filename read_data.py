import pandas as pd

# Configure pandas display
pd.set_option('display.max_columns', None) #don't hide any selected columns
pd.set_option('display.width', None) #auto-detect terminal width
pd.set_option('display.max_colwidth', None) #don't truncate text in cells

# Load the full Parquet dataset
file_path = r"C:\Projects\Python\Veridion Challenge 4\veridion_product_deduplication_challenge.snappy.parquet"
df = pd.read_parquet(file_path)

# Work on a full copy of the DataFrame
full = df.copy()

# Export the raw full dataset
full.to_csv('full_raw.csv', index = False)
print(f"Wrote full_raw.csv ({full.shape[0]} rows) and full.shape[1] columns")

# Define helper first_non_null(function) to pick the first non-null value in each group
def first_non_null(s : pd.Series):
    nonnull = s.dropna()
    return nonnull.iloc[0] if len(nonnull) else None

# Build a dynamic aggregation dict for all columns
agg_dict = {
    col: first_non_null
    for col in full.columns
    if col != 'product_title'
}

# Group by 'product_title' and apply first_non_null to every other field
merged = (
    full
    .groupby('product_title', as_index = False, sort = False)
    .agg(agg_dict)
)

# Fill any remaining blanks with 'Unknown'
merged.fillna('Unknown', inplace = True)

# Export merged view
merged.to_csv('full_dedupe.csv', index = False)
print(f"Wrote full_dedupe.csv ({merged.shape[0]} unique titles and merged.shape[1])")

#Printing raw rows, deduped rows, and rows removed in terminal
print("\nSummary:")
print(f"     Raw rows:    {full.shape[0]}")
print(f"     Deduped rows:{merged.shape[0]}")
print(f"     Removed rows:{full.shape[0] - merged.shape[0]}")

input("\nPress Enter to exit...")