Jumble Puzzle

Requires spark2.2 or higher

Run spark job:
__spark-submit <path>/puzzle.py

Run unit tests:
__pytest -v <path>/test (or) pytest -v test

Result:
__The likely finaly phrase(s) are the set of words that have a total minimum frequency based on the following condition
____(1=most   frequent,   9887=least   frequent,   0=not   scored   due   to infrequency  of use)

----------
__for Puzzle 1 likely anagram for individual puzzles are ['gland', 'major', 'becalm', 'lawyer'] and likely final phrase(s) are ['job-wall-need', 'job-need-wall']
__for Puzzle 2 likely anagram for individual puzzles are ['blend', 'avoid', 'cheesy', 'camera'] and likely final phrase(s) are ['bad-hair-day', 'day-hair-bad']
__for Puzzle 4 likely anagram for individual puzzles are ['dinky', 'agile', 'encore', 'devout'] and likely final phrase(s) are ['addition']
