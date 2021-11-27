from pyspark import pandas as pyspark_pandas
import pandas

dataframe_data = {"city": ["Paris", "Warsaw", "Berlin"], "country": ["France", "Poland", "Germany"]}
pyspark_dataframe = pyspark_pandas.DataFrame(dataframe_data)
print(pyspark_dataframe)
pandas_dataframe = pandas.DataFrame(dataframe_data)
print(pandas_dataframe)
"""
     city  country
0   Paris   France
1  Warsaw   Poland
2  Berlin  Germany
     city  country
0   Paris   France
1  Warsaw   Poland
2  Berlin  Germany
"""