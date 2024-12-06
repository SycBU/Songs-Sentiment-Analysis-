import os
import pandas as pd
from google.cloud import storage

# Initialize Cloud Storage client
client = storage.Client()
bucket_name = "lyrics_sa"
bucket = client.bucket(bucket_name)

# Ensure the /tmp/ directory exists
tmp_dir = "/tmp/"
if not os.path.exists(tmp_dir):
    os.makedirs(tmp_dir)

# Function to download a file from GCP to local
def download_from_gcs(blob_name, local_path):
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)
    print(f"Downloaded {blob_name} to {local_path}")

# Function to upload a file from local to GCP
def upload_to_gcs(local_path, blob_name):
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to {blob_name}")

# Define GCS paths for datasets
gcs_files = {
    "songs": "songs.tsv",
    "song_chart": "song_chart.tsv",
    "song_pop": "song_pop.tsv",
    "acoustic_features": "acoustic_features.tsv",
    "lyrics": "lyrics.tsv",
}

# Download datasets from GCS
local_files = {}
for key, gcs_path in gcs_files.items():
    local_path = os.path.join(tmp_dir, f"{key}.tsv")
    download_from_gcs(gcs_path, local_path)
    local_files[key] = local_path

# Load datasets
songs = pd.read_csv(local_files["songs"], sep="\t")
song_chart = pd.read_csv(local_files["song_chart"], sep="\t")
song_pop = pd.read_csv(local_files["song_pop"], sep="\t")
acoustic_features = pd.read_csv(local_files["acoustic_features"], sep="\t")
lyrics = pd.read_csv(
    local_files["lyrics"],
    sep="\t",
    names=["song_id", "lyrics"],
    header=None
)

# Step 1: Extract first 100 unique song_ids from the 'songs' dataset
unique_song_ids = songs['song_id'].drop_duplicates().head(100)

# Step 2: Filter each dataset by these song_ids
songs_subset = songs[songs['song_id'].isin(unique_song_ids)]
lyrics_subset = lyrics[lyrics['song_id'].isin(unique_song_ids)]
song_chart_subset = song_chart[song_chart['song_id'].isin(unique_song_ids)]
song_pop_subset = song_pop[song_pop['song_id'].isin(unique_song_ids)]
acoustic_features_subset = acoustic_features[acoustic_features['song_id'].isin(unique_song_ids)]

# Step 3: Merge datasets incrementally
merged_data = pd.merge(songs_subset, lyrics_subset, on="song_id", how="inner")
merged_data = pd.merge(merged_data, song_chart_subset, on="song_id", how="inner")
merged_data = pd.merge(merged_data, song_pop_subset, on="song_id", how="inner")
merged_data = pd.merge(merged_data, acoustic_features_subset, on="song_id", how="inner")

# Step 4: Retain the row with the highest rank_score for each song_id
filtered_data = (
    merged_data.loc[
        merged_data.groupby("song_id")["rank_score"].idxmax()
    ]
)

# Save the filtered dataset locally
filtered_local_path = os.path.join(tmp_dir, r"C:/Users/samin/Documents/Python Scripts/filtered_data.tsv")
filtered_data.to_csv(filtered_local_path, sep="\t", index=False)
print(f"Filtered data saved locally to {filtered_local_path}")

# Upload the filtered dataset back to GCS
filtered_gcs_path = "filtered/filtered_data.tsv"
upload_to_gcs(filtered_local_path, filtered_gcs_path)
print(f"Filtered data uploaded to GCS at {filtered_gcs_path}")
