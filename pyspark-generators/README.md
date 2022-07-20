# Demo scenario

## Count examples
1. Run `count_spark_code_list_approach.py` and `count_spark_code_generator_approach.py`
2. Observation: memory usage

## Collect examples
1. Open `pyspark.zip` from the downloaded PySpark package
2. Look for `serializers.py` and `workers.py` classes.
3. Replace them with the files from the `debug` package.
4. Run `collect_spark_code_generator_approach.py` and `collect_spark_code_list_approach.py`
5. Observation#1: generator vs list used to serialize the data
6. Observation#2: yield serializes data continuously, list does it all at once