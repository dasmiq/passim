from __future__ import print_function

import os, sys

from pyspark import SparkContext
from pyspark.sql import SQLContext

def guessFormat(path, default="json"):
    if path.endswith(".json"):
        return "json"
    elif path.endswith(".parquet"):
        return "parquet"
    elif path.endswith(".csv"):
        return "com.databricks.spark.csv"
    else:
        return default

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pload.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="DataFrame Parquet Load")
    sqlContext = SQLContext(sc)

    inpath = sys.argv[1]
    outpath = sys.argv[2]

    # TODO: Add options to set format explicitly and bypass guessing.
    inputFormat = guessFormat(inpath, "parquet")
    outputFormat = guessFormat(outpath, "json")

    raw = sqlContext.read.format(inputFormat).load(inpath)
    raw.write.format(outputFormat).save(outpath)

    sc.stop()
