Step 1. I installed fuzzywuzzy and pandas, python's libraries. Initial code to view the parquet file, and to test if it opens & works. Added input "Press Enter to Exit" to see the code clearly.

import pandas as pd

#path of the parquet file
file_path = r"C:\Projects\Python\Veridion Challenge 4\veridion_product_deduplication_challenge.snappy.parquet"

#Load the data
df = pd.read_parquet(file_path)

#Show the first 5 rows
print(df.head())

#Show column names
print("\nColumns in the dataset:")
print(df.columns)

input("\nPress Enter to exit...")


~~~Environment & Setup for Git Bash and Python 3.13~~~
1. Installed Python  3.13.3 on Windows, added to system path
2. Verified in CMD with python --version -> 3.13.3
3. In Git Bash, Python version was 3.12.5 by default
4. Tried to override using ~/.bashrc
alias python='/c/Users/stefa/AppData/Local/Programs/Python/Python313/python.exe'
5.Created ~/bash_profile to source ~/.bashrc (on login shells)
if [ -f ~/.bashrc ]; then
  source ~/.bashrc
fi
Override wasn't consistently applied, so I did a workaround, whenever I executed code in bash I used the command:
py -3.13 script.py


Step 2. Used the following code to see how the parquet file looks like, from multiple kind of angle thinking: rows & columns, column data types, seeing missing values through sum, a sample for 3 specific columns and seeing how those 3 columns look like.

import pandas as pd

file_path = r"C:\Projects\Python\Veridion Challenge 4\veridion_product_deduplication_challenge.snappy.parquet"
df = pd.read_parquet(file_path)

print("\nTotal Rows and Columns:", df.shape)
print("\nColumn Types:\n", df.dtypes)
print("\nMissing Values:\n", df.isnull().sum())
print("\nSample values for 'product_title', 'brand', 'description':")
print(df[['product_title', 'brand', 'description']].head(10))

input("\nPress Enter to exit...")

But, now I ran into the problem of not seeing the whole columns in the bash terminal, so I added the following lines at the top:

pd.set_option('display.max_columns', None) #don't hide any selected columns
pd.set_option('display.width', None) #auto-detect terminal width
pd.set_option('display.max_colwidth', None) #don't truncate text in cells

Decided to see all columns' name first, and then pick 3 important categories, and after that export to a CSV file. Chose 'product_title', 'brand', 'product_name', 'product-identifier'.
Installed Rainbow CSV extension.
Inspected visually the CSV file and noticed that the product_title and product_name seem alike.


Step 3. Merging duplicates on the 50-row sample. I grouped by product_title and for each other field I picked the first non-null value in the group.
So, these lines are no more needed now because I already have the CSV file: 
print("All columns:\n", df.columns.tolist()) #print all column names
#writing CSV in project folder
sample.to_csv('dedupe_sample.csv', index=False)
print("Wrote dedupe_sample.csv. Opening it in VS Code for a clean view.")

I updated the code so I see in the dedupe_merged_sample.csv, showing every column for a duplicated product.
Defined 'first_non_null(s)' -> drops nulls and returns the first reamaining value
Used first_non_null aggregation for every category to pick the first non-missing value in each row:

merged = (
    sample
    .groupby('product_title', as_index = False)
    .agg({
            'brand': first_non_null,
            'product_name': first_non_null,
            'product_identifier': first_non_null
    })
)

Then, exported the deduped sample: merged.to_csv('dedupe_merged_sample.csv', index = False)


Step 4. Recreated CSV files to analyze them with the updated code.
Next is applying identical code to the full dataset(df[cols] instead of sample). I replaced sample with full.
Even though I used 'first_non_null', there were still cell blanks. I used a placeholder like 'Unknown' to make no cell blank.
Also added groupby(..., sort = False) to keep the rows in original order



