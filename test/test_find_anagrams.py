import pytest
import os, sys

from pyspark.sql import functions as func
from pyspark.sql import SQLContext

rootDir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(rootDir)

from puzzle import find_anagrams

@pytest.mark.usefixtures("spark_context")

# TEST 1: Check if function returns all anagrams from provided word dictionary
# Result: True if all anagrams match
def test_get_all_anagrams(spark_context):
    word_freq = {"alien":1, "lane":1, "lien":10, "rarest":5, "aline":0, "raster":0, "starer":10, "elation":0, "arrest":3}
    jumbled_word = "RRSTEA"

    anagram_list = find_anagrams(jumbled_word=jumbled_word, word_freq_dict=word_freq)

    expected_results = ["arrest", "rarest", "raster", "starer"]

    assert anagram_list.sort() == expected_results.sort()
