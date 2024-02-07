import pandas as pd
import os

def remove_columns(df, columns_to_remove):
    """Remove specified columns from a DataFrame."""
    return df.drop(columns=columns_to_remove, errors='ignore')

def compare_dataframes(df1, df2, unique_key):
    """
    Compare two DataFrames by a unique key and return the differences.

    Parameters:
    - df1: The first DataFrame.
    - df2: The second DataFrame.
    - unique_key: The column name to use as the unique identifier for comparison.

    Returns:
    - A DataFrame containing the differences.
    """
    # Ensure the unique_key column is present in both DataFrames
    if unique_key not in df1.columns or unique_key not in df2.columns:
        raise ValueError(f"Unique key '{unique_key}' not found in one of the DataFrames.")

    # Reorder the DataFrames by the unique key to ensure row alignment
    df1_sorted = df1.sort_values(by=unique_key).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=unique_key).reset_index(drop=True)

    # if unique_key == 'derived_project_name_hash':
    #     print(df1_sorted.head())
    #     print(df2_sorted.head())

    # Compare the sorted DataFrames and get the rows that differ
    df_diff = df1_sorted.compare(df2_sorted)

    return df_diff

initial_data_folder = 'initial'
post_data_folder = 'post'
differences = []

for filename in os.listdir(initial_data_folder):
    if filename.endswith('.csv'):
        base_filename = filename[:-16]

        initial_df_path = os.path.join(initial_data_folder, filename)
        initial_df = pd.read_csv(initial_df_path)
        initial_df = remove_columns(initial_df, ['derived_inserted_at'])
        
        for post_filename in os.listdir(post_data_folder):
            if post_filename.startswith(base_filename) and post_filename.endswith('.csv'):
                post_df_path = os.path.join(post_data_folder, post_filename)
                post_df = pd.read_csv(post_df_path)
                post_df = remove_columns(post_df, ['derived_inserted_at'])

                if 'technology_schedule' in filename:
                    unique_key = 'taskid'
                    post_df = remove_columns(post_df, ['index'])
                    initial_df = remove_columns(post_df, ['index'])
                elif 'early_pipeline' in filename:
                    unique_key = 'derived_project_name_hash'
                    post_df = remove_columns(post_df, ['index'])
                    initial_df = remove_columns(post_df, ['index'])
                elif 'pipeline' in filename:
                    unique_key = 'derived_trial_name_hash'
                    post_df = remove_columns(post_df, ['index'])
                    initial_df = remove_columns(post_df, ['index'])
                
                else:
                    unique_key = 'index'

                print(f"Comparing {filename} and {post_filename}...")
                df_differences = compare_dataframes(initial_df, post_df, unique_key)
                if not df_differences.empty:
                    differences.append(df_differences)
                break
        else:
            print(f"No corresponding file found for {filename} in the post data folder.")

if differences:
    all_differences = pd.concat(differences)
    all_differences.to_csv('differences.csv', index=False)
    print("Diffeeences found and saved to differences.csv")
else:
    all_differences = pd.DataFrame()
    all_differences.to_csv('differences.csv', index=False)
    print("No differences found and differences.csv is empty")


