#!/usr/bin/env bash

PASSIM_HOME="$(cd "`dirname "$0"`"/..; pwd)"

SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS"

spark-submit --packages 'com.databricks:spark-csv_2.10:1.2.0' \
	     $SPARK_SUBMIT_ARGS \
	     "$PASSIM_HOME"/scripts/pretty-cluster.py $@
