import datetime

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()


letters = spark.createDataFrame([
    {'lower_letter': 'a', 'upper_letter': 'A', 'event_time': datetime.datetime(2025, 12, 29)},
    {'lower_letter': 'b', 'upper_letter': 'B', 'event_time': datetime.datetime(2025, 12, 30)},
    {'lower_letter': 'a', 'upper_letter': 'A', 'event_time': datetime.datetime(2025, 12, 30)},
    {'lower_letter': 'c', 'upper_letter': 'C', 'event_time': datetime.datetime(2025, 12, 30)}
], 'lower_letter STRING, upper_letter STRING, event_time TIMESTAMP')
letters.createOrReplaceTempView('letters')

spark.sql('''
SELECT lower_letter, upper_letter, event_time
FROM (
    SELECT
        lower_letter,
        upper_letter,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY upper_letter, lower_letter ORDER BY event_time ASC) as row_number
    FROM
        letters
)
WHERE row_number = 1
''').show(truncate=False)
