from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
import itertools

from models import Puzzle

# TODO: These should probably be moved to command line arguments
FREQ_DICT_FILENAME = 'freq_dict'
PUZZLE_PATHS = [
    'inputs/testpuzzle.json',
    'inputs/puzzle1.json',
    'inputs/puzzle2.json',
    'inputs/puzzle3.json',
    'inputs/puzzle4.json',
    'inputs/puzzle5.json',
]
OUTPUT_DIR = 'output'


def start_spark():
    spark_builder = (
        SparkSession
        .builder
        .appName('my_spark_app'))
    spark_sess = spark_builder.getOrCreate()
    return spark_sess


def extract_freq_table(spark):
    """Loads a frequency dictionary from a json file into a DataFrame
    :param spark: A spark instance that provides the DataFrame builder
    :return: A DataFrame of words and their corresponding frequencies
    """
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
    :param s: The string to be sorted
    :return: An alphabetically sorted string with the same characters as the input
    """
    return ''.join(sorted(s))


def transform_freq_table(df):
    """Transforms the frequency table and adds a 'sorted_word' column
    this additional column will allow for queries based on anagrams of words
    :param df: The frequency table dataframe with a 'word' column
    :return: A newly created dataframe with a 'sorted_word' column
    """
    sort_string_udf = udf(__sort_string, StringType())
    df_transformed = df.withColumn('sorted_word', sort_string_udf(col('word')))
    return df_transformed


def load_puzzles():
    """Loads in the JSON files listed in the global PUZZLE_PATHS variable to Puzzle models
    :return: A list of Puzzle objects
    """
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


def solve_single_word_anagram(word, freq_df):
    """Given a string and a DataFrame, lists all the real words the word is an anagram for in the frame
    Looking back at the requirements, I realize I misunderstood the frequency rating; it seems 1 is the most frequent
    and 0 is the least frequent word rating. This is close to the inverse of what I have implemented.
    :param word: An anagram string
    :param freq_df: A DataFrame with a 'word', 'frequency' and 'sorted_word' column
    :return: A list of Rows and the best fitting anagram for the input word
    """
    valid_words = freq_df.where(freq_df.sorted_word == __sort_string(word)).collect()
    max_freq = -999
    best_fit = ''
    for valid_word in valid_words:
        if valid_word.frequency >= max_freq:
            max_freq = valid_word.frequency
            best_fit = valid_word
    return valid_words, best_fit


def get_possible_sentences(sentence_chars, word_lengths, freq_df):
    """INEFFICIENT FUNCTION that finds every possible combination of characters for the requested sentence
    :param sentence_chars: A list of characters that appear in the sentence
    :param word_lengths: A list of ints representing the length of each of the words in the sentence
    :param freq_df: A dataframe with the frequency of words
    :return: A list of all possible sentences (with redundant results)
    """
    possible_sentences = []
    for perm in itertools.permutations(sentence_chars):
        word_start_index = 0
        sentence_words = []
        sentence_valid = True
        checked_words = {}
        for word_length in word_lengths:
            word_end_index = word_start_index + word_length
            sentence_word = ''.join(perm[word_start_index: word_end_index])
            if freq_df.where(freq_df.word == sentence_word).count():
                checked_word = freq_df.where(freq_df.word == sentence_word).collect()[0]
                sentence_words.append(checked_word)
                checked_words[sentence_word] = checked_word
            else:
                checked_words[sentence_word] = False # Meaning this word is invalid
                sentence_valid = False
                break
            word_start_index = word_end_index
        if sentence_valid:
            possible_sentences.append(sentence_words)
    return possible_sentences


def determine_best_sentence(possible_sentences):
    """Given a list of sentences (word rows with word and frequency) returns the sentence with the most commonly used words
    Looking back at the requirements, I realize I misunderstood the frequency rating; it seems 1 is the most frequent
    and 0 is the least frequent word rating. This is close to the inverse of what I have implemented.
    :param possible_sentences: A list of sentences to be ranked
    :return: the most likely solution sentence of the puzzle
    """
    best_sentence = []
    best_sentence_score = -999
    for sentence in possible_sentences:
        sentence_score = sum([word.frequency for word in sentence])
        if sentence_score > best_sentence_score:
            best_sentence_score = sentence_score
            best_sentence = sentence
    return best_sentence


def solve_puzzle(puzzle, freq_df):
    """Given a Puzzle object and a frequency DataFrame, attempts to solve word jumbles
    TODO: This function is EXTREMELY slow and needs to be improved before going to production
    :param puzzle: A Puzzle object that details a word jumble
    :param freq_df:
    :return: A solved version of the input puzzle
    """
    # Solve each anagram
    anagram_words = []
    best_fits = []
    for anagram in puzzle.input_anagrams:
        valid_words, best_fit = solve_single_word_anagram(anagram, freq_df)
        anagram_words.append(valid_words)
        best_fits.append(best_fit)
    puzzle.anagram_solutions = best_fits

    # Solve the end sentence (if possible)
    possible_sentences = get_possible_sentences(puzzle.get_sentence_characters(), puzzle.sentence_word_lengths, freq_df)

    puzzle.sentence_solution = determine_best_sentence(possible_sentences)
    # Restart if end sentence is incorrect
    return puzzle


def save_solution(puzzle):
    """Given a solved Puzzle, saves the solution to the disk in the output directory specified by the global variable
    :param puzzle: The solved Puzzle object
    """
    if not os.path.exists('output'):
        os.mkdir(OUTPUT_DIR)

    solution = json.dumps({
        'puzzle_name': puzzle.puzzle_name,
        'sentence_solution': [row.word for row in puzzle.sentence_solution]
    })

    with open(OUTPUT_DIR + os.path.join(OUTPUT_DIR, puzzle.puzzle_name + '.json'), 'wb') as f:
        f.write(solution)


def main():
    # Init the spark session
    spark_sess = start_spark()

    # Load in the frequency table data
    df = extract_freq_table(spark=spark_sess)
    df_transformed = transform_freq_table(df)

    # Load in the puzzles
    puzzles = load_puzzles()
    for puzzle in puzzles:
        solve_puzzle(puzzle, df_transformed)
        save_solution(puzzle)


if __name__ == '__main__':
    main()
