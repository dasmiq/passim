from __future__ import print_function
import sys
from pyspark.sql import SparkSession

def guessFormat(path, default="json"):
    if path.endswith(".json"):
        return ("json", {'compression': 'gzip'})
    elif path.endswith(".parquet"):
        return ("parquet", {})
    elif path.endswith(".csv"):
        return ("csv", {'header': 'true', 'compression': 'gzip', 'escape': '"'})
    else:
        return (default, {})

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: dfconvert.py <input> <output>", file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Dataframe convert').getOrCreate()

    inpath = sys.argv[1]
    outpath = sys.argv[2]

    # TODO: Add options to set format explicitly and bypass guessing.
    (inputFormat, inputOptions) = guessFormat(inpath, "parquet")
    (outputFormat, outputOptions) = guessFormat(outpath, "json")

    raw = spark.read.format(inputFormat).load(inpath)
    raw.write.format(outputFormat).options(**outputOptions).save(outpath)

    spark.stop()
