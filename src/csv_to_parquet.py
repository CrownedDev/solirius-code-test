import pandas as pd
import logging
from utils import create_directory_file_path, load_schema, read_csv_data
from utils import INPUT_CSV, SCHEMA_PATH, FILMS_FILE_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def format_columns(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    logger.info(f"Formatting columns started")
    # Add ID column
    df['id'] = range(len(df))
    
    for field in schema['fields']:
        name = field['name']
        type = field['type']
        
        if type == 'date' and name in df.columns:
            df[name] = pd.to_datetime(df[name], format='%Y')
        
        elif type == 'boolean' and name in df.columns:
            df[name] = df[name].astype(bool)
        
        elif type == 'integer' and name == 'durationMins':
            # remove non-numeric characters and convert to integer
            df['durationMins'] = df['duration'].str.replace(r'[^0-9]+', '', regex=True).astype('Int64')
            df = df.drop(columns=['duration'])
    
    # Reorder to match schema
    column_order = [field['name'] for field in schema['fields']]
    df = df[column_order]
    
    logger.info(f"csv to parquet: Formatting done")
    return df


def csv_to_parquet(input_csv: str, schema_path: str, output_path: str) -> str:
    logger.info(f"Starting csv to parquet")
    create_directory_file_path(output_path)
    
    schema = load_schema(schema_path)
    df = read_csv_data(input_csv)
    df = format_columns(df, schema)
    
    df.to_parquet(output_path, index=False)
    
    logger.info(f"csv to parquet: {len(df)} rows uploaded")
    return output_path


if __name__ == "__main__":
    csv_to_parquet(
        input_csv=INPUT_CSV,
        schema_path=SCHEMA_PATH,
        output_path=FILMS_FILE_PATH
    )