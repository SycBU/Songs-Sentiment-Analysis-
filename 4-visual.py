import pandas as pd
from google.cloud import bigquery

# Load the dataset
data_path = "C:/Users/samin/Documents/Python Scripts/sentiment_analysis_results.tsv"
df = pd.read_csv(data_path, sep='\t')

# Initialize BigQuery client
bigquery_client = bigquery.Client()

# Combine Acoustic Features and Extra Features into one list
combined_features = [
    'acousticness', 'danceability', 'energy', 'instrumentalness', 
    'liveness', 'loudness', 'speechiness', 'valence', 'tempo', 
    'popularity', 'year_end_score', 'weeks_on_chart'
]

# Filter available features from the dataset
available_features = [feature for feature in combined_features if feature in df.columns]

if available_features:
    # Compute correlation for sentiment_score with all features
    combined_corr_matrix = df[['sentiment_score'] + available_features].corr()
    combined_corr_matrix.reset_index(inplace=True)
    
    # Melt the correlation matrix into long format for BigQuery
    combined_corr_matrix = combined_corr_matrix.melt(
        id_vars='index', 
        var_name='Feature', 
        value_name='Correlation'
    ).rename(columns={'index': 'Metric'})
    
    print("Combined correlation matrix computed.")
else:
    combined_corr_matrix = pd.DataFrame()
    print("No features available for correlation.")

# Save combined correlation matrix locally
temp_file = "C:/Users/samin/Documents/Python Scripts/combined_correlation_matrix.csv"
combined_corr_matrix.to_csv(temp_file, index=False)
print(f"Combined correlation matrix saved locally to {temp_file}")

# Upload to BigQuery
dataset_id = "Songs"  # Replace with your BigQuery dataset ID
combined_table_id = "Combined"  # Table for combined features correlation matrix
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

combined_table_ref = bigquery_client.dataset(dataset_id).table(combined_table_id)

# Upload combined correlation data
with open(temp_file, "rb") as source_file:
    job = bigquery_client.load_table_from_file(source_file, combined_table_ref, job_config=job_config)

job.result()  # Wait for the job to complete
print(f"Combined correlation matrix uploaded to BigQuery table {dataset_id}.{combined_table_id}")
