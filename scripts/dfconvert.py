from __future__ import print_function

import os, sys

from pyspark import SparkContext
from pyspark.sql import SQLContext

def guessFormat(path, default="json"):
    if path.endswith(".json"):
        return ("json", {'compression': 'gzip'})
    elif path.endswith(".parquet"):
        return ("parquet", {})
    elif path.endswith(".csv"):
        return ("csv", {'header': 'true', 'compression': 'gzip'})
    else:
        return (default, {})

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: pload.py <input> <output>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="DataFrame Parquet Load")
    sqlContext = SQLContext(sc)

    inpath = sys.argv[1]
    outpath = sys.argv[2]

    # TODO: Add options to set format explicitly and bypass guessing.
    (inputFormat, inputOptions) = guessFormat(inpath, "parquet")
    (outputFormat, outputOptions) = guessFormat(outpath, "json")

    raw = sqlContext.read.format(inputFormat).load(inpath)
    raw.write.format(outputFormat).options(**outputOptions).save(outpath)

    sc.stop()
