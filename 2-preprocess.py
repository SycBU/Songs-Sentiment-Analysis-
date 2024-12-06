import pandas as pd
import re
import os
from google.cloud import storage

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

# Download necessary NLTK data
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Custom stopwords
stop_words = stopwords.words('english')
stop_words.extend([
    'verse', 'chorus', 'i"ll', 'intro', 'outro', 'or', 'm', 'ma', 'ours', 'against', 'nor',
    'wasn', 'hasn', 'my', 'had', 'didn', 'isn', 'did', 'aren', 'those', 'than', 'man', 
    'mustn', "you've", 'to', 'she', 'having', "haven't", 'into', 't', 'll', 'himself', 'do', 
    "that'll", 'so', 'of', 'on', 'very', 'for', 'out', 'were', 'should', 'they', 'ain', "should've", 
    'you', "didn't", 'yours', 'was', 'our', 'can', 'myself', "shouldn't", 'have', 'up', 'mightn', 
    "you'll", 'any', 'itself', 'hadn', 'him', 'doesn', 'weren', 'y', 'being', "don't", 'them', 
    'are','and', 'that', 'your', 'yourself', 'their', 'some', 'ourselves', 've', 'doing', 'been', 
    'shouldn', 'yourselves', "mightn't", 'most', 'because', 'few', 'wouldn', "you'd", 'through', 
    "you're", 'themselves', 'an', 'if', "wouldn't", 'its', 'other', "won't", "wasn't", "she's", 'we', 
    'shan', "weren't",'don',"hadn't", 'this', 'off', 'while', 'a', 'haven', 'her', 'theirs', 'all', 
    "hasn't", "doesn't", 'about', 'then', 'by','such', 'but', 'until', 'each', 'there', "aren't", 
    'with', 'not', "shan't", 'hers', 'it', 'too', 'i', 'at', 'is', 'as', 'me', 'herself', 's', 'the', 
    'where', 'am', 'has', 'over', "couldn't", 'when', 'does', 'mustn','re', 'no', 'in', 'who', 'd', 
    'own', 'he', 'be', "isn't", 'his', 'these', 'same', 'whom', 'will', 'needn','couldn', 'from',  
    "it's", 'o', 'yeah','ya','na','wan','uh','gon','ima','mm','uhhuh','bout','em','nigga','niggas', 
    'got','ta','lil','ol','hey','oooh','ooh','oh','youre','dont','im','youve','ive','theres','ill','yaka', 
    'lalalala','la','da','di','yuh', 'shawty','oohooh','shoorah','mmmmmm','ook','bidibambambambam',
    'shh','bro','ho','aint','cant','know','bambam','shitll','tonka'
])

stop_words = set(stop_words)

# Regex pattern for removing escaped new line characters (i.e., '\\n')
newline_pattern = re.compile(r'\\n')

# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

# Initialize Cloud Storage client
client = storage.Client()
bucket_name = "lyrics_sa"
bucket = client.bucket(bucket_name)

# Function to download file from a folder in GCS
def download_from_gcs(blob_name, local_path):
    full_blob_path = f"filtered/{blob_name}"  # Specify the 'filtered' folder
    blob = bucket.blob(full_blob_path)
    blob.download_to_filename(local_path)
    print(f"Downloaded {full_blob_path} to {local_path}")

# Function to upload file to a folder in GCS
def upload_to_gcs(local_path, blob_name):
    full_blob_path = f"cleaned/{blob_name}"  # Specify the 'cleaned' folder
    blob = bucket.blob(full_blob_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to {full_blob_path}")

# Define the preprocessing function for lyrics
def preprocess_lyrics(lyrics):
    # Remove escaped new line characters
    lyrics = newline_pattern.sub(' ', str(lyrics))

    # Tokenize the lyrics
    tokens = word_tokenize(lyrics)

    # Process tokens: remove punctuation, convert to lowercase, remove stopwords, and lemmatize
    processed_tokens = [
        lemmatizer.lemmatize(word.lower())  # Lemmatization to normalize
        for word in tokens if word.isalpha() and word.lower() not in stop_words
    ]

    return ' '.join(processed_tokens)

# Local paths
filtered_local_path = r"C:/Users/samin/Documents/Python Scripts/filtered_data.tsv"
cleaned_local_path = r"C:/Users/samin/Documents/Python Scripts/cleaned_data.tsv"

# Step 1: Download filtered_data.tsv from GCS
download_from_gcs("filtered_data.tsv", filtered_local_path)

# Step 2: Load the dataset
df = pd.read_csv(filtered_local_path, sep="\t")

# Step 3: Clean artist names
df['artists'] = df['artists'].apply(eval).apply(lambda artist: list(artist.values())[0] if len(artist) == 1 else list(artist.values()))

# Step 4: Preprocess lyrics
df['cleaned_lyrics'] = df['lyrics'].apply(preprocess_lyrics)

# Step 5: Remove duplicate lyrics
df = df.drop_duplicates(subset=['lyrics'])
df = df.drop('lyrics', axis=1)

# Step 6: Save the cleaned dataset locally
df.to_csv(cleaned_local_path, sep="\t", index=False)
print(f"Cleaned data saved locally to {cleaned_local_path}")

# Step 7: Upload the cleaned dataset to GCS
upload_to_gcs(cleaned_local_path, "cleaned_music_data.tsv")
