import pandas as pd

pd.set_option('display.max_columns', None) #don't hide any selected columns
pd.set_option('display.width', None) #auto-detect terminal width
pd.set_option('display.max_colwidth', None) #don't truncate text in cells

file_path = r"C:\Projects\Python\Veridion Challenge 4\veridion_product_deduplication_challenge.snappy.parquet"
df = pd.read_parquet(file_path)

cols = ['product_title', 'brand', 'product_name', 'product_identifier']
full = df[cols] #grab every row
full.to_csv('dedupe_full.csv', index = False)
print(f"Wrote dedupe_full.csv ({full.shape[0]} rows)")

#1 exploratory check
counts = full['product_title'].value_counts()
print("\nDuplicates in full:")
print(counts[counts > 1])

#2 define helper first_non_null (function)
def first_non_null(s):
    nonnull = s.dropna()
    return nonnull.iloc[0] if len(nonnull) else None

#3 always merge, regardless of counts
merged = (
    full
    .groupby('product_title', as_index = False, sort = False)
    .agg({
            'brand': first_non_null,
            'product_name': first_non_null,
            'product_identifier': first_non_null
    })
)

#Fill any remaining blanks with 'Unknown'
merged.fillna('Unknown', inplace = True)

#4 export merged view
merged.to_csv('dedupe_merged_full.csv', index = False)
print(f"Wrote dedupe_merged_full.csv ({merged.shape[0]} unique titles)")

print("\nMerged full preview:")
print(merged)

input("\nPress Enter to exit...")