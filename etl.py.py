#!/usr/bin/env python
# coding: utf-8

# In[105]:


import pandas as pd
import requests
from sqlalchemy import create_engine
import time
import json
import re


# In[106]:


OMDB_KEY = "b0471efd"
OMDB_BASE_URL = "http://www.omdbapi.com/"
DB_URL = "mysql://root:111@localhost:3304/moviedb"  # Change this for MySQL: "mysql://user:pass@localhost/dbname"
CACHE_FILE = "omdb_cache.json"
SLEEP_TIME = 0.1  


# In[107]:


# Create DB engine
engine = create_engine(DB_URL)


# In[108]:


def extract_csv_data():
    try:
        movies_df = pd.read_csv(r"C:\Users\Dell\Downloads\movies.csv")
        ratings_df = pd.read_csv(r"C:\Users\Dell\Downloads\ratings.csv")
        print(f"   - Movies: {len(movies_df)}")
        print(f"   - Ratings: {len(ratings_df)}")
        return movies_df, ratings_df
    except FileNotFoundError as e:
        print(f"❌ Error")
        exit()


# In[113]:


def clean_title_for_api(title):
    match = re.search(r'\(\d{4}\)$', title)
    if match:
        return title[:match.start()].strip()
    return title.strip()

def fetch_omdb_data(title):
    cleaned_title = clean_title_for_api(title)
    params = {
        't': cleaned_title,
        'apikey': OMDB_KEY,
        'plot': 'full',
    }
    
    try:
        response = requests.get(OMDB_BASE_URL, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'True':
                return {
                    'director': data.get('Director', 'N/A'),
                    'plot': data.get('Plot', 'N/A'),
                    'box_office': data.get('BoxOffice', 'N/A')
                }
            return {'director': 'Not Found', 'plot': 'Not Found', 'box_office': 'Not Found'}
            
    except requests.exceptions.RequestException as e:
        print(f"API Error for {title}: {e}")
        return {'director': 'API Error', 'plot': 'API Error', 'box_office': 'API Error'}
    
    return {'director': 'N/A', 'plot': 'N/A', 'box_office': 'N/A'}

def enrich_movies_with_omdb(movies_df):
    omdb_data = []
    
    print(f"Fetching OMDB data for {len(movies_df)} movies...")
    
    for index, row in movies_df.iterrows():
        data = fetch_omdb_data(row['title'])
        omdb_data.append({'movieId': row['movieId'], **data})
        time.sleep(SLEEP_TIME)
        
        if (index + 1) % 10 == 0:
            print(f"   - Completed {index + 1} movies...")
    
    omdb_df = pd.DataFrame(omdb_data)
    movies_df = pd.merge(movies_df, omdb_df, on='movieId', how='left')
    movies_df.fillna({'director': 'N/A', 'plot': 'N/A', 'box_office': 'N/A'}, inplace=True)
    
    print("OMDB data enrichment completed!")
    return movies_df


# In[114]:


def transform_data(movies_df, ratings_df):    
    movies_df['release_year'] = movies_df['title'].str.extract(r'\((?P<year>\d{4})\)', expand=True)['year'].astype(float).fillna(0).astype(int)
    
    genres_normalized = movies_df[['movieId', 'genres']].copy()
    genres_normalized['genre'] = genres_normalized['genres'].str.split('|')
    genres_df = genres_normalized.explode('genre').drop(columns=['genres'])
    genres_df.rename(columns={'genre': 'genre'}, inplace=True)
    
    return movies_df, ratings_df, genres_df


# In[115]:


def load_data(movies_df, ratings_df, genres_df):    
    try:
        # Manual encoding
        password = "111"
        DB_URL = f"mysql://root:111@localhost:3304/moviedb"
        
        engine = create_engine(DB_URL)
        print("🔗 Connecting to database...")
        
        # Test connection
        with engine.connect() as conn:
            conn.execute("CREATE DATABASE IF NOT EXISTS moviedb")
            conn.execute("USE moviedb")
            print("Database connection successful!")
        
        # A. loading Movies
        movies_to_load = movies_df[['movieId', 'title', 'release_year', 'director', 'plot', 'box_office']].copy()
        movies_to_load['release_year'] = movies_to_load['release_year'].replace(0, None)
        
        print(f"Loading {len(movies_to_load)} records into Movies table...")
        
        movies_to_load.to_sql('Movies', engine, if_exists='fail', index=False, chunksize=1000)
        print("Movies table loaded!")
        
        # B. loading Ratings
        print(f"Loading {len(ratings_df)} records into Ratings table...")
        ratings_df.to_sql('Ratings', engine, if_exists='fail', index=False, chunksize=5000)
        print("Ratings table loaded!")
        
        # C. loading Genres
        print(f"Loading {len(genres_df)} records into Genres table...")
        genres_df.to_sql('Genres', engine, if_exists='fail', index=False, chunksize=5000)
        print("Genres table loaded!")
        
        # Verification
        print("\n📊 Data verification:")
        movie_count = pd.read_sql("SELECT COUNT(*) as count FROM Movies", engine)
        rating_count = pd.read_sql("SELECT COUNT(*) as count FROM Ratings", engine)
        genre_count = pd.read_sql("SELECT COUNT(*) as count FROM Genres", engine)
        
        print(f"   - Movies: {movie_count.iloc[0]['count']}")
        print(f"   - Ratings: {rating_count.iloc[0]['count']}")
        print(f"   - Genres: {genre_count.iloc[0]['count']}")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return
    finally:
        engine.dispose()
        print("Load process completed!")    


# In[117]:


if __name__ == "__main__":
    
    # 1. Extraction (CSV)
    movies_df, ratings_df = extract_csv_data()
    
    # 2. Extraction & Transformation (API Enriching)
    movies_df_enriched = enrich_movies_with_omdb(movies_df)
    
    # 3. Transformation (Cleaning & Normalization)
    movies_final_df, ratings_final_df, genres_final_df = transform_data(movies_df_enriched, ratings_df)
    
    # 4. Load
    load_data(movies_final_df, ratings_final_df, genres_final_df)
    
    print("\n Completed")


# In[ ]:




