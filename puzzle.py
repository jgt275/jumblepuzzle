import json
import os
import itertools

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t

from collections import Counter

scriptDir = os.path.dirname(os.path.realpath(__file__))

#Setting a starting threshold frequency while finding likely phrase
threshold_freq = 2000

def read_json(file):
    """ Function to load json file """
    """ Argument: 
        file - json file
    """
    
    with open(file,'r') as json_file:
        return json.load(json_file)

#Load word frequency
word_freq = read_json(scriptDir + "/input/freq_dict.json")

def get_letter_counter(word, removechar=''):
    """ Function to return letter to frequency map """
    """ Arguments:
        word - string of letters
        removechar - char to be removed from word, default ''
    """
    
    return Counter(word.replace(removechar,''))


def find_anagrams(jumbled_word, word_freq_dict=word_freq):
    """ Function to return list of possible anagrams extracted from dictionary of words """
    """ Arguments:
        jumbled_word - jumbled string
        word_freq_dict - dictionary of words as key and frequency as values 
    """
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
        if (word_lowercase != jumbled_word_lowercase) and (get_letter_counter(jumbled_word_lowercase) == get_letter_counter(word_lowercase)):
            anagrams.append(word)
    return anagrams


def get_position_letters(wordlist, circled_pos):
    """ Function to return list of letters extracted from circled positions across all possible anagrams """
    """ Arguments:
        wordlist - list of words
        circled_pos - list of integers
    """
    
    circled_list = []
    for word in wordlist:
        lst = ''.join([word[i] for i in (circled_pos)])
        circled_list.append(lst)
    return circled_list


def cartesian_word_join(wordlist, sep=''):
    """ Function to do a cartesian join between elements of multiple lists """
    """ Arguments:
        wordlist - list of lists
        sep - separator character, default:''
    """
    
    word_join = []
    for w in itertools.product(*wordlist):
        word_join.append(sep.join(w))
    return word_join


def find_likely_anagram(jumbled_word, word_freq_dict=word_freq):
    """ Function to find the most likely anagram by returning the lowest frequency """
    """ Arguments:
        jumbled_word - jumbled string
        word_freq_dict - dictionary of words as key and frequency as values 
    """
    """ If only one anagram found return it else find the one with the least frequency
            (1=most   frequent,   9887=least   frequent,   0=not   scored   due   to infrequency  of use)
    """
    """ Note: If multiple anagrams with the least frequency, only the first one is returned """
    
    all_anagrams = find_anagrams(jumbled_word, word_freq_dict)
    if len(all_anagrams)==1:
        return all_anagrams
    elif len(all_anagrams)>1:
        freq_dict_subset = {k: word_freq_dict[k] for k in all_anagrams}
        #Dictionary containing word frequencies greater than zero
        freq_dict_subset_gt0= {key:val for key,val in freq_dict_subset.items() if val>0}
        #Get minimum frequency key from non empty dictionary
        if freq_dict_subset_gt0:
           #This will return only the first key if there are multiple keys with the same minimum value
           return [min(freq_dict_subset_gt0, key=freq_dict_subset_gt0.get)]
        else:
            #condition when all the returned anagrams have zero frequency
            return [min(freq_dict_subset, key=freq_dict_subset.get)]


def find_phrase(jumbled_word, phrase_word_length, threshold_freq=threshold_freq, word_freq_dict=word_freq):
    """ Function to get combination of final phrases along with their combined frequency """
    """ Arguments:
        jumbled_word - jumbled string
        phrase_word_length - list of integers representing lenght of words in phrase
        threshold_freq - a threshold frequency whole number
        word_freq_dict - dictionary of words as key and frequency as values 
    """
    
    list_of_phrases = []
    #Loop through each phrase word length
    for i in phrase_word_length:
        
        perm_list = []
        #Generate word permutations of only lengths as that of phrase word
        #Set operator drops any duplicates that occur with the same letter being interchangbly in different positions
        for word in set(itertools.permutations(jumbled_word,i)):
            #List of all permutations
            perm_list.append(''.join(word))
            
        word_phrase_by_length = []
        
        #Continue loop till a non empty list is returned
        while(len(word_phrase_by_length)==0):
            #Subsetting frequency dictionary to only words that match length of phrase word and below the defined threshold frequency
            word_freq_subset_list = [key for key,val in word_freq_dict.items() if len(key)==i and 0<val<=threshold_freq]
            
            #Perform intersection to retrieve elements that is present in both sets
            for element in set(perm_list).intersection(set(word_freq_subset_list)):
                word_phrase_by_length.append(element)
            
            #If no matching words were returned within the threshold, raise the threshold frequency and continue till match found
            if len(word_phrase_by_length) == 0:
                threshold_freq+= 1000
                print("Raising threshold frequency value to ", threshold_freq)
           
        #Create a list of phrase word lists
        list_of_phrases.append(word_phrase_by_length) 
        
    #perform a cartesian join to obtain different combination of phrase words
    phrase_list_combo = cartesian_word_join(list_of_phrases,sep='-')
    
    #Retain phrases whose letter map matches to the original jumbled phrase word
    final_phrase_list = []
    for w in phrase_list_combo:
        if get_letter_counter(w, removechar='-')==get_letter_counter(jumbled_word):
            final_phrase_list.append(w)

    result = find_likely_phrase(final_phrase_list, word_freq_dict)
    return result

    
def find_likely_phrase(list_of_phrases, word_freq_dict=word_freq):
    """ Function to return the likely final phrase(s) """
    """ Arguments:
        list_of_phrases - list of words
        word_freq_dict - dictionary of words as key and frequency as values
    """
    
    #If only one likely phrase, return it
    if len(list_of_phrases)==1:
        return list_of_phrases
    
    #if more than one likely phrase, get the phrase(s) with minimum frequency
    elif len(list_of_phrases)>1:
        total_phrase_freq = {}
        
        for lst in list_of_phrases:
            get_phrase_freq = {k:v for k,v in word_freq_dict.items() if k in lst.split('-')}
            total_phrase_freq[lst] = sum(get_phrase_freq.values())
        

        min_freq = min(total_phrase_freq.values())
        phrases_with_minfreq = [k for k in total_phrase_freq if total_phrase_freq[k]==min_freq]
        return phrases_with_minfreq
    else:
        return []


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
    
    #Get likely final phrase(s)
    likely_phrase = f.udf(find_phrase, t.ArrayType(t.StringType()))
    puzzle_df = puzzle_df.select('*' \
                                 ,likely_phrase(f.col('final_phrase_jumbled_letters')[0] \
                                                 ,f.col('final_word_letters') \
                                                 ).alias('likely_final_phrases'))

    #Write results
    for row in puzzle_df.rdd.collect():
        print("   >>>>> RESULT: for Puzzle {} likely anagram for individual puzzles are {} and likely final phrase(s) are {}".format(row.id, [''.join(val) for val in row.likely_anagram], row.likely_final_phrases))

    #puzzle_df.show(truncate=False)

    #Terminate spark session
    spark.stop()
    
