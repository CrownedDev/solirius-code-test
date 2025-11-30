import pandas as pd
import logging
from utils import read_parquet_data, split_values
from utils import ZERO_SCORE, CATEGORY_SCORE, FILMS_FILE_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def get_film_by_id(df: pd.DataFrame, film_id: int) -> pd.Series:
    film = df[df['id'] == film_id]
    if film.empty:
        logger.debug(f"Film with ID {film_id} not found")
        raise ValueError(f"Film with ID {film_id} not found")
    return film.iloc[0]


def calculate_director_score(film1: pd.Series, film2: pd.Series) -> float:
    if pd.isna(film1['director']) or pd.isna(film2['director']):
        return ZERO_SCORE
    return CATEGORY_SCORE if film1['director'] == film2['director'] else ZERO_SCORE


def calculate_genre_score(film1: pd.Series, film2: pd.Series) -> float:
    if pd.isna(film1['genres']) or pd.isna(film2['genres']):
        return ZERO_SCORE
    
    genres1 = split_values(film1['genres'])
    genres2 = split_values(film2['genres'])
    
    if not genres1 or not genres2:
        return ZERO_SCORE
    
    overlap = len(genres1 & genres2)
    total = len(genres1 | genres2)
    
    return (overlap / total) * CATEGORY_SCORE


def calculate_cast_score(film1: pd.Series, film2: pd.Series) -> float:
    if pd.isna(film1['cast']) or pd.isna(film2['cast']):
        return ZERO_SCORE
    
    cast1 = split_values(film1['cast'])
    cast2 = split_values(film2['cast'])
    
    overlap = len(cast1 & cast2)
    
    return CATEGORY_SCORE if overlap > 0 else ZERO_SCORE


def calculate_country_score(film1: pd.Series, film2: pd.Series) -> float:
    if pd.isna(film1['country']) or pd.isna(film2['country']):
        return ZERO_SCORE
    return CATEGORY_SCORE if film1['country'] == film2['country'] else ZERO_SCORE


def calculate_similarity(film1: pd.Series, film2: pd.Series) -> float:
    score = ZERO_SCORE
    score += calculate_director_score(film1, film2)
    score += calculate_genre_score(film1, film2)
    score += calculate_cast_score(film1, film2)
    score += calculate_country_score(film1, film2)
    return score

def compare_film(target_film: pd.Series, film: pd.Series, target_id: int, threshold: float) -> dict | None:
    if film['id'] == target_id:
        return None
    
    score = calculate_similarity(target_film, film)
    
    if score < threshold:
        return None
    
    return {
        'id': film['id'],
        'title': film['title'],
        'director': film['director'],
        'genres': film['genres'],
        'similarity': score
    }

def find_similar_films(
    film_id: int, 
    threshold: float, 
    data_path: str = FILMS_FILE_PATH
) -> pd.DataFrame:
    
    df = read_parquet_data(data_path)
    target_film = get_film_by_id(df, film_id)
    
    logger.info(f"Finding films with {threshold}% similarity to {target_film['title']}")
    
    results = []
    
    for _, film in df.iterrows():
        result = compare_film(target_film, film, film_id, threshold)
        if result:
            results.append(result)
    
    
    if not results:
        logger.info("No similar films found")
        return pd.DataFrame(columns=['id', 'title', 'director', 'genres', 'similarity'])
    
    results_df = pd.DataFrame(results)
    results_df = results_df.drop_duplicates(subset=['id']) 
    results_df = results_df.sort_values('similarity', ascending=False)
    
    logger.info(f"Found {len(results_df)} similar films")
    return results_df


if __name__ == "__main__":
    results = find_similar_films(film_id=18, threshold=50)
    #print(results.to_string()) 