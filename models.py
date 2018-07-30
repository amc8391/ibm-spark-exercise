class Puzzle:
    """
    Represents a word jumble puzzle with multiple anagrams that give hints to a final jumbled sentence
    """
    def __init__(self):
        self.puzzle_name = ''
        self.input_anagrams = []
        self.sentence_indices = []
        self.sentence_word_lengths = []
        self.anagram_solutions = []
        self.sentence_solution = []

    def get_sentence_characters(self):
        sentence_characters = []
        for indices, solution in zip(self.sentence_indices, self.anagram_solutions):
            for index in indices:
                sentence_characters.append(solution.word[index])
        return sentence_characters
