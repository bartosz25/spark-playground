from pyspark.sql import SparkSession, functions as F
RED = "\033[31m"
RESET = "\033[0m"
spark = SparkSession.builder.master('local[*]').getOrCreate()

matchdays = [
    {'matchday': 1, 'date': '2025-10-10'},
    {'matchday': 2, 'date': '2025-10-17'},
    {'matchday': 3, 'date': '2025-10-24'},
    {'matchday': 4, 'date': '2025-10-31'}
]
matchdays_df = spark.createDataFrame(matchdays)

scorers = [
    {'matchday': 1, 'player': 'Joe Doe', 'goals': 2},
    {'matchday': 3, 'player': 'Joe Doe', 'goals': 4},
]
scorers_df = spark.createDataFrame(scorers)
for direction in ['forward', 'backward', 'nearest']:
    print(f'{RED}           Direction: {direction}{RESET}')
    asof_join = matchdays_df._joinAsOf(
        other=scorers_df,  how='left', leftAsOfColumn='matchday', rightAsOfColumn='matchday', direction=direction
    )
    asof_join.explain(extended=True)
    asof_join.show(truncate=False)