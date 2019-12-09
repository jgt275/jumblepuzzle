import pytest
import os, sys

from pyspark.sql import functions as func
from pyspark.sql import SQLContext

rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(rootDir)

from puzzle import find_anagrams, find_likely_anagram

@pytest.mark.usefixtures("spark_context")

# TEST 1: Check if function returns the most likely anagram based on lowest frequency
# Result: True if anagram matches
def test_get_likely_anagram(spark_context):
    word_freq = {"alien":1, "lane":1, "lien":10, "rarest":5, "aline":0, "raster":0, "starer":10, "elation":0, "arrest":3}
    jumbled_word = "RRSTEA"

    anagram = find_likely_anagram(jumbled_word=jumbled_word, word_freq_dict=word_freq)

    expected_results = ["arrest"]

    assert anagram == expected_results
