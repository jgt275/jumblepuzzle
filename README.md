Jumble Puzzle

Requires spark2.2 or higher

Run spark job:
&nbsp;&nbsp;spark-submit <path>/puzzle.py

Run unit tests:
&nbsp;&nbsp;pytest -v <path>/test (or) pytest -v test

Result:
&nbsp;&nbsp;The likely finaly phrase(s) are the set of words that have a total minimum frequency based on the following condition
&nbsp;&nbsp;&nbsp;&nbsp;(1=most   frequent,   9887=least   frequent,   0=not   scored   due   to infrequency  of use)

----------
&nbsp;&nbsp;for Puzzle 1 likely anagram for individual puzzles are ['gland', 'major', 'becalm', 'lawyer'] and likely final phrase(s) are ['job-wall-need', 'job-need-wall']
&nbsp;&nbsp;for Puzzle 2 likely anagram for individual puzzles are ['blend', 'avoid', 'cheesy', 'camera'] and likely final phrase(s) are ['bad-hair-day', 'day-hair-bad']
&nbsp;&nbsp;for Puzzle 4 likely anagram for individual puzzles are ['dinky', 'agile', 'encore', 'devout'] and likely final phrase(s) are ['addition']
