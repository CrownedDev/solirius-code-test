import logging
import os
import pandas as pd
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
INPUT_CSV = "resources/csv/allFilms.csv"
SCHEMA_PATH = "resources/json/allFilesSchema.json"
FILMS_FILE_PATH = "output/films.parquet"
GENRES_DIR = "output/genres"
ZERO_SCORE = 0
CATEGORY_SCORE = 25  


def create_directory_file_path(file_path: str) -> None:
    parent_dir = os.path.dirname(file_path) 
    logging.info(f"Creating file path if not exists: {parent_dir}")
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    
def load_schema(schema_path: str) -> dict:
    logging.info(f"Loading schema from: {schema_path}")
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    return schema

def read_csv_data(input_csv: str) -> pd.DataFrame:
    logging.info(f"Reading CSV data from: {input_csv}")
    df = pd.read_csv(input_csv)
    return df


def create_directory(dir_path: str) -> None:
    logging.info(f"Creating directory if not exists: {dir_path}")
    os.makedirs(dir_path, exist_ok=True)
    
    
def read_parquet_data(input_path: str) -> pd.DataFrame:
    logging.info(f"Reading Parquet data from: {input_path}")
    df = pd.read_parquet(input_path)
    return df

def split_values(value: str) -> set:
    return set(item.strip() for item in value.split(','))