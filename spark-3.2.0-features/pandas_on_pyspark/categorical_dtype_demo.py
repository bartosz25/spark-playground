from pyspark import pandas as pyspark_pandas

numbers = pyspark_pandas.CategoricalIndex([6, 2, 1, 4, 3], categories=[2, 4, 6],
                                          ordered=False)

# as_ordered won't order the categories! It's simply marks them as being ordered!
# these as_* functions are btw 2 new ones added after Pandas-on-PySpark release
ordered_numbers = numbers.as_ordered()
print(ordered_numbers)
print(numbers)
"""
Both prints are the same, only the oredered property changes:
```
CategoricalIndex([6, 2, nan, 4, nan], categories=[2, 4, 6], ordered=True, dtype='category')
CategoricalIndex([6, 2, nan, 4, nan], categories=[2, 4, 6], ordered=False, dtype='category')
```
"""

# But you can change the order with another new function added in PySpark
reordered_numbers = ordered_numbers.reorder_categories([6, 4, 2])
print(reordered_numbers)
"""
This time the values remain the same but the `categories` don't:
```
CategoricalIndex([6, 2, nan, 4, nan], categories=[6, 4, 2], ordered=True, dtype='category')
```
Of course, you can also notice some `nan`s in the `DataFrame`. It's the placeholder for any 
value not defined in the `categories`
"""

