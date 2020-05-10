import sys
assert sys.version_info >= (3, 5)

from pyspark.sql import SparkSession, functions as fu, types
spark = SparkSession.builder.appName('wikipedia popular').getOrCreate()
assert spark.version >= '2.3'

def specifySchema():
    wiki_schema = types.StructType([ # commented-out fields won't be read
    types.StructField('lang', types.StringType(), True),
    types.StructField('page_name', types.StringType(), True),
    types.StructField('viewcount', types.LongType(), True),
    types.StructField('bytes', types.LongType(), True),
    ])
    return wiki_schema

@fu.udf(returnType=types.StringType())
def path_to_hour(path):
    list_sep = path.split("/")
    filename = list_sep[-1].split(".")
    toRet = filename[0].strip("pagecounts-")
    return toRet[:-4]


def main(inputs, output):
    wiki_schema = specifySchema()

    pages = spark.read.csv(inputs, sep=" ", schema=wiki_schema).withColumn('filename', path_to_hour(fu.input_file_name()))

    filtered = pages.filter((pages['lang']==fu.lit('en')) & (pages['page_name'] != fu.lit('Main_Page')) & (~ pages['page_name'].startswith("Special:")))

    subset = filtered.select('filename','page_name','viewcount').cache()
    max_count = subset.groupBy(subset['filename']).agg(fu.max(subset['viewcount']).alias('max_view'))

    joined = subset.join(fu.broadcast(max_count),['filename'], "inner")
    # joined = subset.join(max_count,['filename'], "inner")

    max_page = joined.filter(joined['viewcount'] == joined['max_view']).drop(joined['max_view']).sort(joined['filename']).select(joined['filename'].alias('hour'),joined['page_name'],joined['viewcount'])

    max_page.coalesce(1).write.json(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
