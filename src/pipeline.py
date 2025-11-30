import luigi
import os
import logging

from csv_to_parquet import csv_to_parquet
from split_by_genre import split_by_genre
from utils import INPUT_CSV, SCHEMA_PATH, FILMS_FILE_PATH, GENRES_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




class ExecuteCsvToParquet(luigi.Task):    
    def output(self):
       return luigi.LocalTarget(FILMS_FILE_PATH)
    
    def run(self):
        logger.info("Running Stage 1: CSV to Parquet")
        csv_to_parquet(INPUT_CSV, SCHEMA_PATH, FILMS_FILE_PATH)


class ExecuteSplitByGenreTask(luigi.Task):
    
    def requires(self):
        return ExecuteCsvToParquet()
    
    def output(self):
        return luigi.LocalTarget(os.path.join(GENRES_DIR, "_SUCCESS"))
    
    def run(self):
        logger.info("Running Stage 2: Split by Genre")
        split_by_genre(FILMS_FILE_PATH, GENRES_DIR)
        
        with self.output().open('w') as f:
            f.write("Complete")


if __name__ == "__main__":
    luigi.run(main_task_cls=ExecuteSplitByGenreTask, local_scheduler=True)