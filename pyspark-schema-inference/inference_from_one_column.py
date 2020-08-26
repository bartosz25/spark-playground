import traceback
from collections import namedtuple

from pyspark import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
    .appName("One column inference")\
    .getOrCreate()

print('Letters from a single tuple')
try:
    letters = [('a'), ('b'), ('c')]
    spark.createDataFrame(letters, ['letter']).show(truncate=False)
except Exception as e:
    traceback.print_exc()

print('Letters from a dict')
letters = [{'letter': 'a'}, {'letter': 'b'}, {'letter': 'c'}]
spark.createDataFrame(letters).show(truncate=False)

print('Letters from a (tuple, list)')
letters_tuple_list = [(('letter'), ['a']), (('letter'), ['b'])]
spark.createDataFrame(letters_tuple_list).show(truncate=False)

print('Letters from Row')
LetterRow = Row('letter')
letters_from_row = [LetterRow('a'), LetterRow('b')]
spark.createDataFrame(letters_from_row).show(truncate=False)

print('Letters from namedtuple')
LetterNamedTuple = namedtuple('Letter', ['letter'])
letters_from_named_tuple = [LetterNamedTuple('a'), LetterNamedTuple('b')]
spark.createDataFrame(letters_from_named_tuple).show(truncate=False)

print('Letters from a pair')
letters_tuple_pair = [('a', 1), ('b', 1)]
spark.createDataFrame(letters_tuple_pair).show(truncate=False)

print('Letters from an object')
class LetterObject:
    def __init__(self, letter):
        self.letter = letter

letters_from_object = [LetterObject('a'), LetterObject('b')]
spark.createDataFrame(letters_from_object).show(truncate=False)
