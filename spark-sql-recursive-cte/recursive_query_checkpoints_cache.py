import time

from pyspark import Row
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark_session = (SparkSession.builder.master("local[*]")
                     .config("spark.sql.adaptive.enabled", False)
                     .config("spark.sql.autoBroadcastJoinThreshold", -1).getOrCreate())



    spark_session.createDataFrame(data=[
        Row(area='Europe', parent=None),
        Row(area='France', parent='Europe'), Row(area='Ile de France', parent='France'), Row(area='Paris', parent='Ile de France'),
        Row(area='Germany', parent='Europe'), Row(area='Bavaria', parent='Germany'), Row(area='Munich', parent='Bavaria'),
        Row(area='Poland', parent='Europe'), Row(area='Masovie', parent='Poland'), Row(area='Warsaw', parent='Masovie')
    ], schema='area STRING, parent STRING').createOrReplaceTempView('areas')

    recursive_cte_name = 'areas_recursive'
    anchor_query = 'SELECT DISTINCT area, parent, parent AS all_parents FROM areas'
    recursive_member_query = f"""
        SELECT rec.area, a.parent, CONCAT_WS('> ', rec.all_parents, a.parent) AS all_parents FROM {recursive_cte_name} rec
        JOIN areas a ON a.area = rec.parent
    """

    anchor_query_dataset = spark_session.sql(anchor_query)
    anchor_query_dataset.createOrReplaceTempView(recursive_cte_name)

    final_dataset = anchor_query_dataset
    current_iteration = 0
    max_iterations = 100
    spark_session.sparkContext.setCheckpointDir('/tmp/wfc/checkpoint')
    while current_iteration < max_iterations:
        print(f'Running {current_iteration}')
        recursive_query_dataset = spark_session.sql(recursive_member_query).cache()
        if current_iteration % 2 == 0:
            print('Starting the checkpoint...')
            recursive_query_dataset = recursive_query_dataset.checkpoint()
            print('...completed the checkpoint.')

        recursive_query_dataset.createOrReplaceTempView(recursive_cte_name)

        final_dataset = final_dataset.union(recursive_query_dataset)
        if recursive_query_dataset.isEmpty():
            print('No more rows to generate, leaving the while loop')
            break

        current_iteration += 1

    #final_dataset.explain(extended=True)
    final_dataset.show(truncate=False, n=10000)

    while True:
        pass