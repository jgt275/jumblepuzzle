import json
import os
import itertools

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

from collections import Counter
from operator import getitem, add
from functools import reduce


scriptDir = os.path.dirname(os.path.realpath(__file__))

def read_json(file):
    """ Function to load json file """
    with open(file,'r') as json_file:
        return json.load(json_file)

#Load word frequency
word_freq = read_json(scriptDir + "/input/freq_dict.json")


def find_anagrams(jumbled_word, word_freq_dict=word_freq):
    """ Function to return list of possible anagrams extracted from dictionary of words """
    """ Dictionary of words will be subset to only the words that match the length of jumbled word """
    """ List of possible anagrams will be returned by doing a dictionary match of word map on following criteria:
            - The same set of letters appears at same frequency
    """
    anagrams=[]
    word_len = len(jumbled_word)
    jumbled_word_lowercase = jumbled_word.lower()
    freq_dict_subset = {k:v for k,v in word_freq_dict.items() if len(k) == word_len}
    for word in freq_dict_subset:
        word_lowercase = word.lower()
        if (word_lowercase != jumbled_word_lowercase) and (Counter(jumbled_word_lowercase) == Counter(word_lowercase)):
            anagrams.append(word)
    return anagrams


def get_position_letters(wordlist, circled_pos):
    """ Function to return list of letters extracted from circled positions across all possible anagrams """
    circled_list = []
    for word in wordlist:
        lst = ''.join([word[i] for i in (circled_pos)])
        circled_list.append(lst)
    return circled_list


def cartesian_word_join(wordlist):
    """ Function to do a cartesian join between elements of multiple lists """
    word_join = []
    for w in itertools.product(*wordlist):
        word_join.append(''.join(w))
    return word_join


def find_likely_anagram(jumbled_word, word_freq_dict=word_freq):
    """ Function to find the most likely anagram by returning the lowest frequency """
    """ If only one anagram found return it else find the one with the least frequency
            (1=most   frequent,   9887=least   frequent,   0=not   scored   due   to infrequency  of use
    """
    """ Note: If multiple anagrams with the least frequency, only the first one is returned """
    all_anagrams = find_anagrams(jumbled_word, word_freq_dict)
    if len(all_anagrams)==1:
        return all_anagrams
    elif len(all_anagrams)>1:
        freq_dict_subset = {k: word_freq_dict[k] for k in all_anagrams}
        #dictionary containing word frequencies greater than zero
        freq_dict_subset_gt0= {key:val for key,val in freq_dict_subset.items() if val>0}
        #Get minimum frequency key from non empty dictionary
        if freq_dict_subset_gt0:
           #This will return only the first key if there are multiple keys with the same minimum value
           return [min(freq_dict_subset_gt0, key=freq_dict_subset_gt0.get)]
        else:
            #condition when all the returned anagrams have zero frequency
            return [min(freq_dict_subset, key=freq_dict_subset.get)]


def cartesian_dictionary_join(result, *args):
    """ Function to return cartesian join between keys of dictionaries and apply operation on their values """
    return { '-'.join(ks) : reduce(add, itertools.starmap(getitem,zip(args,ks))) for ks in itertools.product(*args) }


def find_phrase(jumbled_word, phrase_word_length, threshold_freq):
    """ Function to get combination of final phrases along with their combined frequency """
    #Get total length of phrase
    phrase_length = sum(phrase_word_length)

    #Generate word permutations of only lengths as that of phrase
    perm = itertools.permutations(jumbled_word, phrase_length)
    perm_list = []
    #Set operator drops any duplicates that occur with the same letter being interchangbly in different positions
    for i in set(perm):
        perm_list.append(''.join(i))

    key_index = 1

    #Loop through each phrase word length
    for i in phrase_word_length:
        beg_pos = 0
        #Subsetting frequency dictionary to only words that match length of phrase word and below the defined threshold frequency
        word_freq_subset = {key:val for key,val in word_freq.items() if len(key)==i and 0<val<=threshold_freq}
        phrase_word_dict = {}

        #Split each permutation word based on the position as the phrase word length
        #Check if the word existis in the frequency dictionary, if so return the frequency
        for words in perm_list:
            split_word = words[beg_pos:beg_pos+i]
            if split_word in word_freq_subset:
                phrase_word_dict[split_word] = word_freq_subset[split_word]

        #Create a nested dictionary of dictionary map of phrase words and frequency
        if key_index==1:
            arg = phrase_word_dict
        else:
            arg = arg,phrase_word_dict
        key_index+= 1
    return


if __name__=="__main__":

    #Initiate spark session
    spark = SparkSession.builder.master("local[*]").appName("jumble_puzzle").getOrCreate()

    #Load puzzle data and expand the list of values as separate columns
    puzzle_df = spark \
                    .read.json(scriptDir + "/input/puzzle_input.json") \
                    .select('*', f.explode('words').alias('jumbled_words')) \
                    .select("id","jumbled_words.*","final_word_letters")

    #Get anagrams of jumbled words from word frequency dictionary
    #anagram_func = f.udf(find_anagrams, t.ArrayType(t.StringType()))
    anagram_func = f.udf(find_likely_anagram, t.ArrayType(t.StringType()))
    puzzle_df = puzzle_df.select('*',anagram_func(f.col('word')).alias('anagrams'))

    #Extract the letters from anagram at circled positions
    get_position_letters_func = f.udf(get_position_letters, t.ArrayType(t.StringType()))
    puzzle_df = puzzle_df.select('*', get_position_letters_func(f.col('anagrams'), f.col('circled_position')).alias('circled_letters'))

    #Aggregate the results across every puzzle set
    puzzle_df = puzzle_df.groupby("id","final_word_letters") \
                        .agg(f.collect_list('anagrams').alias('likely_anagram'), f.collect_list('circled_letters').alias('circled_letters_list')) \
                        .sort("id")

    #Join the extracted letters from individual puzzles to create jumbled word for final puzzle
    cartesian_word_join_func = f.udf(cartesian_word_join, t.ArrayType(t.StringType()))
    puzzle_df = puzzle_df.select('*', cartesian_word_join_func(f.col('circled_letters_list')).alias('final_phrase_jumbled_letters'))

    #Print results
    for row in puzzle_df.rdd.collect():
        print(">>>>> RESULT: for Puzzle {} likely anagram for individual puzzles are {}".format(row.id, [''.join(val) for val in row.likely_anagram]))


    #puzzle_df.show(truncate=False)


    #Terminate spark session
    spark.stop()
