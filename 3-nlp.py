import pandas as pd
import time
import seaborn as sns
import matplotlib.pyplot as plt
from google.cloud import language_v1, storage, bigquery

storage_client = storage.Client()
bigquery_client = bigquery.Client()
bucket_name = "lyrics_sa"

# Function to download file from a folder in GCS
def download_from_gcs(blob_name, local_path):
    full_blob_path = f"cleaned/{blob_name}"  # Specify the 'cleaned' folder
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(full_blob_path)
    blob.download_to_filename(local_path)
    print(f"Downloaded {full_blob_path} to {local_path}")

# Function to upload file to a folder in GCS
def upload_to_gcs(local_path, blob_name):
    full_blob_path = f"results/{blob_name}"  # Specify the 'results' folder
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(full_blob_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to {full_blob_path}")

# Function to upload results to BigQuery
def upload_to_bigquery(df, dataset_id, table_id):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    temp_file = "/tmp/sentiment_analysis_results.csv"
    df.to_csv(temp_file, index=False)

    with open(temp_file, "rb") as source_file:
        job = bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Wait for the job to complete
    print(f"Uploaded results to BigQuery table {dataset_id}.{table_id}")

# Define paths
cleaned_data_blob = "cleaned_music_data.tsv"
local_cleaned_path = r"C:/Users/samin/Documents/Python Scripts/cleaned_data.tsv"
result_blob = "sentiment_analysis_results.tsv"
local_result_path = r"C:/Users/samin/Documents/Python Scripts/sentiment_analysis_results.tsv"

# Step 1: Download cleaned dataset from GCS
download_from_gcs(cleaned_data_blob, local_cleaned_path)

# Step 2: Load the cleaned dataset
df = pd.read_csv(local_cleaned_path, sep='\t')

# Initialize the Google Cloud Natural Language API client
client = language_v1.LanguageServiceClient()

# Function to analyze sentiment
def analyze_sentiment(text):
    if pd.isna(text) or not text.strip():
        return 0.0, 0.0  # Neutral sentiment for empty or invalid text
    document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
    response = client.analyze_sentiment(request={'document': document})
    sentiment = response.document_sentiment
    return sentiment.score, sentiment.magnitude

# Lists to store results
sentiment_scores = []
sentiment_magnitudes = []
predicted_labels = []

# Map sentiment scores to categories: Positive, Neutral, Negative
def categorize_sentiment(score):
    if score > 0.2:
        return "Positive", 1
    elif score < -0.2:
        return "Negative", 0
    else:
        return "Neutral", 2

# Analyze sentiment for each song
print("Analyzing sentiment for lyrics...")
for index, row in df.iterrows():
    text = row['cleaned_lyrics']
    try:
        score, magnitude = analyze_sentiment(text)
        sentiment_scores.append(score)
        sentiment_magnitudes.append(magnitude)
        sentiment_category, predicted_label = categorize_sentiment(score)
        predicted_labels.append(predicted_label)
    except Exception as e:
        print(f"Error processing row {index}: {e}")
        sentiment_scores.append(None)
        sentiment_magnitudes.append(None)
        predicted_labels.append(None)
    time.sleep(0.1)  # Pause to respect API rate limits

# Add sentiment analysis results to the DataFrame
df['sentiment_score'] = sentiment_scores
df['sentiment_magnitude'] = sentiment_magnitudes
df['predicted_label'] = predicted_labels

# Step 3: Save results locally and upload to GCS
df.to_csv(local_result_path, sep='\t', index=False)
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(result_blob)
blob.upload_from_filename(local_result_path)
print(f"Sentiment analysis results saved to {result_blob} in GCS.")

# Step 4: Upload results to BigQuery
dataset_id = "Songs"  # Replace with your BigQuery dataset ID
table_id = "Sentiment"  # Replace with your BigQuery table ID
upload_to_bigquery(df, dataset_id, table_id)
