from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Title Index').getOrCreate()

    path = 'gs://data2040-part3b/starwars_pages_current.jl'
    df = spark.read.json(path)
    rdd = df.rdd
    titles = rdd.map(lambda row: (row.title, row.id))
    first100 = titles.take(100)

    for pair in first100:
        print(pair)