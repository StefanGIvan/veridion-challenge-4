import pandas as pd

pd.set_option('display.max_columns', None) #don't hide any selected columns
pd.set_option('display.width', None) #auto-detect terminal width
pd.set_option('display.max_colwidth', None) #don't truncate text in cells

file_path = r"C:\Projects\Python\Veridion Challenge 4\veridion_product_deduplication_challenge.snappy.parquet"
df = pd.read_parquet(file_path)

print("All columns:\n", df.columns.tolist()) #print all column names

cols = ['product_title', 'brand', 'product_name', 'product_identifier']
sample = df[cols].head(50)

#writing CSV in project folder
sample.to_csv('dedupe_sample.csv', index=False)
print("Wrote dedupe_sample.csv. Opening it in VS Code for a clean view.")

counts = sample['product_title'].value_counts() #count how often each title appears in the sample
dupes = counts[counts > 1] #keep only the titles that appear more than once

print("\nDuplicate titles in sample (product_title -> count):")
print(dupes)

#If there is at least one deduplicate, show its full rows
if not dupes.empty:
    first_dup = dupes.index[0]
    print(f"\nRows for duplicate title: \"{first_dup}\"")
    print(sample[sample['product_title'] == first_dup])

input("\nPress Enter to exit...")