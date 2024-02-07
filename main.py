import pandas as pd
import os

def remove_columns(df, columns_to_remove):
    """Remove specified columns from a DataFrame."""
    return df.drop(columns=columns_to_remove, errors='ignore')

def compare_dataframes(df1, df2, unique_key):

    # Ensure the unique_key column is present in both DataFrames
    if unique_key not in df1.columns or unique_key not in df2.columns:
        raise ValueError(f"Unique key '{unique_key}' not found in one of the DataFrames.")
    
    if df1[unique_key].nunique() != len(df1) or df2[unique_key].nunique() != len(df2):
        raise ValueError(f"The unique key '{unique_key}' is not unique the logged comparison before this error.")
    
    if len(df1) != len(df2):
        raise ValueError(f"Lengths of the two dataframes are different: {len(df1)} and {len(df2)}")
    # Reorder the DataFrames by the unique key to ensure row alignment
    df1_sorted = df1.sort_values(by=unique_key).reset_index(drop=True)
    df2_sorted = df2.sort_values(by=unique_key).reset_index(drop=True)

    

    # Compare the sorted DataFrames and get the rows that differ
    df_diff = df1_sorted.compare(df2_sorted)

    return df_diff

initial_data_folder = 'initial'
post_data_folder = 'post'
differences = []
unique_id_list = ['technology_status_', 'technology_scope_', 'technology_risk_', 'technology_decision_', 'technology_charter_', 'technology_changelog_'] # not schedule or projects

for filename in os.listdir(initial_data_folder):
    if filename.endswith('.csv'):
        base_filename = filename[:-16]

        initial_df_path = os.path.join(initial_data_folder, filename)
        # read csv which can also have multiline data
        initial_df = pd.read_csv(initial_df_path, quotechar='"', escapechar="\\")
        initial_df = remove_columns(initial_df, ['derived_inserted_at'])
        
        for post_filename in os.listdir(post_data_folder):
            if post_filename.startswith(base_filename) and post_filename.endswith('.csv'):
                post_df_path = os.path.join(post_data_folder, post_filename)
                post_df = pd.read_csv(post_df_path, quotechar='"', escapechar="\\")
                post_df = remove_columns(post_df, ['derived_inserted_at'])

                if 'technology_schedule' in filename:
                    unique_key = 'taskid'
                    post_df = remove_columns(post_df, ['index'])
                    initial_df = remove_columns(initial_df, ['index'])
                elif 'early_pipeline' in filename:
                    unique_key = 'derived_project_name_hash'
                    post_df = remove_columns(post_df, ['index'])
                    initial_df = remove_columns(initial_df, ['index'])
                elif 'pipeline' in filename:
                    unique_key = 'derived_trial_name_hash'
                    post_df = remove_columns(post_df, ['index'])
                    initial_df = remove_columns(initial_df, ['index'])
                elif base_filename in unique_id_list:
                    unique_key = 'uniqueid'
                    post_df = remove_columns(post_df, ['index'])
                    initial_df = remove_columns(initial_df, ['index'])
                else:
                    unique_key = 'index'

                print(f"Comparing {base_filename} ...")
                df_differences = compare_dataframes(initial_df, post_df, unique_key)
                if not df_differences.empty:
                    print(f"Differences found between {filename} and {post_filename}.")
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


