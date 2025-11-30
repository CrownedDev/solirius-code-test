import pandas as pd
import logging
from utils import read_parquet_data
from utils import FILMS_FILE_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_exact(df: pd.DataFrame, column: str, value) -> pd.DataFrame:
    logger.debug(f"Finding by exact: {column} == {value}")
    return df[df[column] == value]



def find_substring(df: pd.DataFrame, column: str, value: str) -> pd.DataFrame:
    logger.debug(f"Finding by substring: {column} contains '{value}'")
    return df[df[column].str.contains(value, na=False)]


def find_range(df: pd.DataFrame, column: str, min_val, max_val) -> pd.DataFrame:
    logger.debug(f"Finding by range: {min_val} <= {column} <= {max_val}")
    
    if pd.api.types.is_datetime64_any_dtype(df[column]):
        return df[(df[column].dt.year >= min_val) & (df[column].dt.year <= max_val)]
    
    return df[(df[column] >= min_val) & (df[column] <= max_val)]


def find_values(df: pd.DataFrame, column: str, values: list) -> pd.DataFrame:

    logger.debug(f"Finding by values: {column} in {values}")
    return df[df[column].isin(values)]

def apply_filter(df: pd.DataFrame, column: str, condition) -> pd.DataFrame:
    
    if column not in df.columns:
        logger.error(f"Column '{column}' not found in dataframe")
        raise ValueError(f"Column '{column}' not found")
    
    if isinstance(condition, list):
        return find_values(df, column, condition)
    
    if isinstance(condition, dict):
        if 'range' in condition:
            min_val, max_val = condition['range']
            return find_range(df, column, min_val, max_val)
        
        if 'substring' in condition:
            return find_substring(df, column, condition['substring'])
        
        logger.error(f"Unknown filter type: {condition}")
        raise ValueError(f"Unknown filter type: {condition}")
    
    return find_exact(df, column, condition)


def query_films(data_path: str = FILMS_FILE_PATH, **filters) -> pd.DataFrame:
    logger.info(f"Querying films with filters: {filters}")
    
    df = read_parquet_data(data_path)
    
    for column, condition in filters.items():
        df = apply_filter(df, column, condition)
    
    logger.info(f"Query returned {len(df)} films")
    return df


if __name__ == "__main__":
    results = query_films(
        country="United States",                        
        director=['George Lucas', 'Steven Spielberg'],  
        release_year={"range": [1980, 2000]},           
        genres={"substring": "Adventure"}               
    )
    
    print(results[['title', 'director', 'release_year']].to_string())