"""
Here are the code fragments from Lesson 3.1, `Regex primer (or reminder)'
For more information about re module: 
https://docs.python.org/3.6/library/re.html
"""

import re # import the regular expressions library

re.match(r'f', "foo") # match single character 'f' in the string 'foo'
re.match(r'f', "foo").group() # call group operator to return the string that matched the pattern
re.match(r'o', "foo") # no match returned because match searches from beginning of string
re.search(r'o', "foo").group() # using search method will search anywher in text for pattern
re.search(r'fo', "foo").group() # concatenate characters in search pattern
re.search(r'a|o', "foo").group() # use 'or' operator (veritcal pipe) to search for 'a' or 'o'
re.search(r'fo*', "foo").group() # use multiple operator (*) to search for largest number of a character 
re.sub(r'\W',"","The quick brown fox jumps over the lazy dog") # substitute non letter/word characters with an empty string
re.search(r'(?i)fo*', "FOO").group() # use (?i) to ignore case. Default search behaviour is case-sensitive. 

