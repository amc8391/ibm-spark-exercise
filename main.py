from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import itertools

from models import Puzzle

FREQ_DICT_FILENAME = 'freq_dict'
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
    input_path = 'inputs/{}.json'.format(FREQ_DICT_FILENAME)
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
    # df.write.parquet('output/{}.parquet'.format(FREQ_DICT_FILENAME))


def load_puzzles():
    puzzles = []
    for path in PUZZLE_PATHS:
        with open(path, 'r') as f:
            puzzle_dict = json.load(f)
            new_puzzle = Puzzle()
            new_puzzle.input_anagrams = puzzle_dict['input_anagrams']
            new_puzzle.sentence_indices = puzzle_dict['sentence_indices']
            new_puzzle.sentence_word_lengths = puzzle_dict['sentence_word_lengths']
            puzzles.append(new_puzzle)
    return puzzles


def solve_puzzle(puzzle, freq_df):
    # Solve each anagram
    anagram_words = []
    best_fits = []
    for anagram in puzzle.input_anagrams:
        valid_words = freq_df.where(freq_df.sorted_word == __sort_string(anagram)).collect()
        max_freq = 0
        best_fit = ''
        for word in valid_words:
            if word.frequency >= max_freq:
                max_freq = word.frequency
                best_fit = word
        best_fits.append(best_fit)
        anagram_words.append(valid_words)
    puzzle.anagram_solutions = best_fits

    # Solve the end sentence (if possible)
    sentence_chars = puzzle.get_sentence_characters()
    possible_sentences = []
    for perm in itertools.permutations(sentence_chars):
        word_start_index = 0
        sentence_words = []
        sentence_valid = True
        for word_length in puzzle.sentence_word_lengths:
            word_end_index = word_start_index + word_length - 1
            sentence_word = ''.join(perm[word_start_index: word_end_index])
            if freq_df.where(freq_df.word == sentence_word).count():
                sentence_words.append(freq_df.where(freq_df.word == sentence_word).collect())
            else:
                sentence_valid = False
                break
            word_start_index = word_end_index
        if sentence_valid:
            possible_sentences.append(sentence_words)

    # Restart if end sentence is incorrect
    pass


def main():
    # Init the spark session
    spark_sess = start_spark()

    # Load in the frequency table data
    df = extract_freq_table(spark=spark_sess)
    df_transformed = transform_freq_table(df)
    load_data(df_transformed)

    # Load in the puzzles
    puzzles = load_puzzles()
    for puzzle in puzzles:
        solve_puzzle(puzzle, df_transformed)
    pass


if __name__ == '__main__':
    main()
