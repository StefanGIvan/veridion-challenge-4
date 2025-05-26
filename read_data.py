import pandas as pd
import csv
from rapidfuzz import process, fuzz

# Configure pandas display
pd.set_option('display.max_columns', None) #don't hide any selected columns
pd.set_option('display.width', None) #auto-detect terminal width
pd.set_option('display.max_colwidth', None) #don't truncate text in cells

# Load the full Parquet dataset
file_path = r"C:\Projects\Python\Veridion Challenge 4\veridion_product_deduplication_challenge.snappy.parquet"
df = pd.read_parquet(file_path)

# Work on a full copy of the DataFrame
full = df.copy()

# Strip out any literal "\n" inside your cells
full = full.replace({r"\n": " "}, regex = True)

# Write full_raw.csv with every field quoted & a clean line terminator
full.to_csv(
    "full_raw.csv",
    index = False,
    quoting = csv.QUOTE_ALL,
    lineterminator = "\n"
)

# Printing out the raw nr of rows and columns
print(f"Wrote full_raw.csv ({full.shape[0]} rows, {full.shape[1]} columns)")

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

# Group by 'product_title' and apply first_non_null to every other field(pandas)
merged = (
    full
    .groupby('product_title', as_index = False, sort = False)
    .agg(agg_dict)
)

# Fill any remaining blanks with 'Unknown'
merged.fillna('Unknown', inplace = True)

# Export merged view
merged.to_csv('full_dedupe.csv', index = False)
print(f"Wrote full_dedupe.csv ({merged.shape[0]} unique titles and {merged.shape[1]}) columns")

# Run ~RapidFuzz~ matching on those titles. Build a mapping title -> canonical title
titles = merged['product_title'].tolist()
canonical = {}
assigned = set()

for t in titles:
    if t in assigned:
        continue
    matches = process.extract(t, titles, scorer=fuzz.token_sort_ratio, score_cutoff=85)
    group = [m[0] for m in matches]
    #choose the longest title as the canonical one
    canon = max(group, key=len)
    for member in group:
        canonical[member] = canon
        assigned.add(member)

# Remap into a new column
merged['canonical_title'] = merged['product_title'].map(canonical)

#Final dedupe on the fuzzy key
final = (
    merged
    .groupby('canonical_title', as_index = False, sort = False)
    .agg(agg_dict)
)
final.fillna('Unknown', inplace = True)

#Export fuzzy deduped output
final.to_csv('fuzzy_dedupe.csv', index = False)
print(f"Wrote fuzzy_dedupe.csv ({final.shape[0]} unique fuzzy titles, {final.shape[1]} columns)")

# Summary
raw_count = full.shape[0]
pandas_count = merged.shape[0]
fuzzy_count = final.shape[0]

print("\nSummary:")
print(f"     Raw rows:      {raw_count}")
print(f"     Pandas-deduped:{pandas_count}")
print(f"     Fuzzy-deduped: {fuzzy_count}")
print(f"     Removed by Pandas: {raw_count - pandas_count}")
print(f"     Removed by Fuzzy:  {pandas_count - fuzzy_count}")

input("\nPress Enter to exit...")