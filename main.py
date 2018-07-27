from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import json
from models import Puzzle

FILE_NAME = 'small_dict'
PUZZLE_PATHS = [
    'inputs/puzzle1.json',
    'inputs/puzzle2.json',
    'inputs/puzzle3.json'
]


def start_spark():
    spark_builder = (
        SparkSession
        .builder
        .appName('my_spark_app'))
    spark_sess = spark_builder.getOrCreate()
    return spark_sess


def extract_freq_table(spark):
    # input_path = 'inputs/small_dict.json'
    input_path = 'inputs/{}.json'.format(file_name)
    with open(input_path, 'rb') as f:
        freq_dict = json.load(f)

    schema = StructType([
        StructField('word', StringType(), False),
        StructField('frequency', IntegerType(), False)
    ])

    freq_table = []
    for word in freq_dict.keys():
        freq_table.append([word, freq_dict[word]])

    df = spark.createDataFrame(freq_table, schema)
    return df


def __sort_string(s):
    """Helper method that sorts the characters in a given string alphabetically
    Arguments:
        s {string} -- The string to be sorted
    
    Returns:
        string -- An alphabetically sorted string with the same characters as the input
    """

    return ''.join(sorted(s))


def transform_freq_table(df):
    """Transforms the frequency table and adds a 'sorted_word' column
    this additional column will allow for queries based on anagrams of words
    
    Arguments:
        df {DataFrame} -- The frequency table dataframe with a 'word' column
    
    Returns:
        DataFrame -- A newly created dataframe with a 'sorted_word' column
    """

    sort_string_udf = udf(__sort_string, StringType())
    df_transformed = df.withColumn('sorted_word', sort_string_udf(col('word')))
    return df_transformed


def load_data(df):
    pass
    # df.write.parquet('output/{}.parquet'.format(file_name))


def load_puzzles():
    puzzles = []
    for path in puzzle_paths:
        with open(path, 'r') as f:
            puzzle_dict = json.load(f)
            new_puzzle = Puzzle()
            new_puzzle.input_anagrams = puzzle_dict['input_anagrams']
            new_puzzle.sentence_indices = puzzle_dict['sentence_indices']
            new_puzzle.sentence_word_lengths = puzzle_dict['sentence_word_lengths']
            puzzles.append(new_puzzle)
    return puzzles

def main():
    # Init the spark session
    spark_sess = start_spark()

    # Load in the frequency table data
    df = extract_freq_table(spark=spark_sess)
    df_transformed = transform_freq_table(df)
    load_data(df_transformed)

    # Load in the puzzles
    puzzles = load_puzzles()
    pass


if __name__ == '__main__':
    main()
