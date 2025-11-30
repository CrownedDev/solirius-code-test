import pandas as pd
import os
import logging
from utils import create_directory, read_parquet_data
from utils import FILMS_FILE_PATH, GENRES_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_unique_genres(df: pd.DataFrame) -> list[str]:
    logger.info(f"Extracting unique genres")
    all_genres = df['genres'].str.split(',').explode()
    
    # Trim whitespace
    all_genres = all_genres.str.strip()
    
    # Remove empty strings
    all_genres = all_genres[all_genres != '']
   
    unique_genres = all_genres.unique().tolist()
    
    logger.info(f"Found {len(unique_genres)} unique genres")
    return unique_genres


def find_by_genre(df: pd.DataFrame, genre: str) -> pd.DataFrame:
    #logger.info(f"Filtering films for genre: {genre}")
    found = df['genres'].str.contains(genre, na=False)
    return df[found]

def write_genre_parquet(df: pd.DataFrame, genre: str, output_dir: str) -> None:
    
    genre_df = find_by_genre(df, genre)
    
    spaceless_genre = genre.replace(' ', '_')
    filename = f"{spaceless_genre}.parquet"
    output_path = os.path.join(output_dir, filename)
    
    genre_df.to_parquet(output_path, index=False)
    logger.debug(f"Wrote {len(genre_df)} films to {output_path}")
    

def split_by_genre(input_path: str, output_dir: str) -> None:
    logger.info(f"Starting split by genre")
    create_directory(output_dir)

    df = read_parquet_data(input_path)
    
    unique_genres = get_unique_genres(df)
    
    for genre in unique_genres:
        write_genre_parquet(df, genre, output_dir)
        # logger.info(f"Writing films for genre: {genre}")
    
    logger.info(f"split by genre completed. uploaded {len(unique_genres)} files")



if __name__ == "__main__":
    split_by_genre(
        input_path=FILMS_FILE_PATH,
        output_dir=GENRES_DIR
    )