from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Inverted Index').getOrCreate()

    path = 'gs://data2040-part3b/starwars_pages_current.jl'
    df = spark.read.json(path)
    # TODO: Your code here
