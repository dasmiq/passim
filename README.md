# passim

This project implements algorithms for detecting and aligning similar
passages in text.  It can be run either in query mode, to find quoted
passages from a reference text, or all-pairs mode, to find all pairs
of passages within longer documents with substantial alignments.

## Installation

Passim relies on [Apache Spark](http://spark.apache.org) to manage
parallel computations, either on a single machine or a cluster.  We
recommend downloading a precompiled, binary distribution of Spark,
unpacking it, and adding the `bin` subdirectory to your `PATH`.

To compile passim itself, we use [sbt](http://www.scala-sbt.org/), a
build tool for Scala, Java, and other JVM languages.  A script to
automatically download the sbt libraries is included with passim.  You
can thus run:
```
$ build/sbt package
```

This should produce a runnable .jar in
`target/scala_*/passim*.jar`. (The exact filename will depend on the
version of Scala and passim that you have.)

The `bin` subdirectory of the passim distribution contains executable
shell scripts such as `passim`.  We recommend adding this subdirectory
to your `PATH`.

Since passim defaults to the JSON format for input and output, it is
convenient to have the
[command-line JSON processor jq](http://stedolan.github.io/jq/)
installed.

## Aligning and Clustering Matching Passage Pairs

### Structuring the Input

The input to passim is a set of _documents_. Depending on the kind of
data you have, you might choose documents to be whole books, pages of
books, whole issues of newspapers, individual newspaper articles, etc.
Minimally, a document consists of an identifier string and a single
string of text content.

For most text reuse detection problems, it is useful to group
documents into _series_.  Text reuse within series will be ignored.
We may, for example, be less interested in the masthead and ads that
appear week after week in the same newspaper and more interested in
articles that propagate from one city's newspapers to another's.  In
that case, we would declare all issues of the same newspaper&mdash;or all
articles within those issues&mdash;to have the same series.  Similarly, we
might define documents to be the pages or chapters of a book and
series to be whole books.

The default input format for documents is in a file or set
of files containing JSON records.  The record for a single document
with the required `id` and `text` fields, as well as a `series` field,
would look like:
```
{"id": "d1", "series": "abc", "text": "This is text."}
```

Note that this is must be a single line in the file.  This JSON record
format has two important differences from general-purpose JSON files.
First, the JSON records for each document are concatenated together,
rather than being nested inside a top-level array.  Second, each
record must be contained on one line, rather than extending over
multiple lines until the closing curly brace.  These restrictions make
it more efficient to process in parallel large numbers of documents
spread across multiple files.

In addition to the fields `id`, `text`, and `series`, other metadata
included in the record for each document will be passed through into
the output.  In particular, a `date` field, if present, will be used
to sort passages within each cluster.

Natural language text is redundant, and adding markup and JSON
field names increases the redundancy.  Spark and passim support
several compression schemes.  For relatively small files, gzip is
adequate; however, when the input files are large enough that they do
not comfortably fit in memory, bzip2 is preferable since programs can
split it into blocks before decompressing.

### Running passim

The simplest invocation contains list of inputs and a directory name
for the output.

```
$ passim input.json output
```

Following Spark conventions, input may be a single file, a single
directory full of files, or a `*` wildcard (glob) expression.
Multiple input paths should be separated by commas.  Files may also be
compressed.

```
$ passim input.json,directory-of-json-files,some*.json.bz2 output
```

Output is written to a directory that, on completion, will contain an
`out.json` directory with `part-*` files rather than a single file.
This allows multiple workers to write it efficiently (and read it back
in) in parallel.  In addition, the output directory should contain the
parameters used to invoke passim in `conf` and the intermediate
cluster membership data in `clusters.parquet`.

The output contains one JSON record for each reused passage.  Each
record echoes the fields in the JSON input and adds the following
extra fields to describe the clustered passages:

Field | Description
----- | ------------
`cluster` | unique identifier for each cluster
`size` | number of passages in each cluster
`begin` | character offset into the document where the reused passage begins
`end` | character offset into the document where the reused passage ends
`uid` | unique internal ID for each document, used for debugging

In addition, `pages`, `regions`, and `locs` include information about
locations in the underlying text of the reused passage.  See [Marking
Locations inside Documents](#locations) below.

The default input and output format is JSON.  Use the `--input-format
parquet` and `--output-format parquet` to use the compressed Parquet
format, which can speed up later processing.

If, in addition to the clusters, you want to output pairwise
alignments between all matching passages, invoke passim with the
`--pairwise` flag.  These alignments will be in the `align.json` or
`align.parquet`, depending on which output format you choose.

Some other useful parameters are:

Parameter | Default value | Description
--------- | ------------- | -----------
`--n` | 5 | N-gram order for text-reuse detection
`--max-series` | 100 | Maximum document frequency of n-grams used.
`--min-match` | 5 | Minimum number of matching n-grams between two documents.
`--relative-overlap` | 0.5 | Proportion that two different aligned passages from the same document must overlap to be clustered together, as measured on the longer passage.

Pass parameters to the underlying Spark processes using the
`SPARK_SUBMIT_ARGS` environment variable.  For example, to run passim
on a local machine with 10 cores and 200GB of memory, run the following command:

```
$ SPARK_SUBMIT_ARGS='--master local[10] --driver-memory 200G --executor-memory 200G' passim input.json output
```

See the
[Spark documentation](https://spark.apache.org/docs/latest/index.html)
for further configuration options.

### Pruning the Alignments

The documents input to passim are indexed to determine which pairs should be aligned.  Often, document metadata can provide a priori constraints on which documents should be aligned.  If there were no constraints, every pair of documents in the input would be aligned, in both directions.  By default, however, documents with the same `series` value will not be aliged.  These constraints on alignments are expressed by two arguments to passim: `--fields` and `--filterpairs`.

The `--fields` argument tells passim which fields in the input records to index when determining which documents to align.  Fields has the syntax of SQL `FROM` clause as implemented by Apache Spark, with the exception that multiple fields are separated by semicolons.  By default, the value of the fields argument is:
```
--fields 'hashId(id) as uid;hashId(series) as gid'
```

Since document and series identifiers can be long strings, passim runs more efficiently if they are hashed to long integers by the (builtin) `hashId` function.

The `--filterpairs` argument is an SQL expression that specifies which pairs of documents are candidates for comparison.  A candidate pair consists of a "left-hand" document, whose field names are identical to those in the input, and a "right-hand" document, whose field names have a "2" appended to them.  The default value for the filterpairs argument is:
```
--filterpairs 'gid < gid2'
```
This ensures that documents from the same series are not aligned and, further, ensures that any given pair of documents is aligned in only one direction, as determined by the lexicographic ordering of the hashes of their series IDs.

As an example, consider aligning only document pairs where the "left-hand" document predates the "right-hand" document by 0 to 30 days.  To perform efficient date arithmetic, we use Apache Spark's built-in `date` function to convert a string `date` field to an integer:
```
--fields 'date(date) as day' --filterpairs 'day <= day2 AND day2 - day <= 30 AND uid <> uid2'
```
Since the dates may be equal, we also include the constraint that the hashed document ids (`uid`) be different.  Had we not done this, the output would also have included alignments of every document with itself.  The `uid` field as a hash of the `id` field is always available.  Note also the SQL inequality operator `<>`.

## <a name="locations"></a> Marking Locations inside Documents

Documents may document their extent on physical pages with the `pages` field.  This field is an array of `Page` regions with the following schema (here written in Scala):
```
case class Coords(x: Int, y: Int, w: Int, h: Int, b: Int)

case class Region(start: Int, length: Int, coords: Coords)

case class Page(id: String, seq: Int, width: Int, height: Int, dpi: Int, regions: Array[Region])
```

The `start` and `length` fields in the `Region` record indicate character offsets into the document `text` field.

## Acknowledgements

We insert the appropriate text in gratitude to our sponsors at the
Andrew W. Mellon Foundation and the National Endowment for the
Humanities. TODO.

## License

Copyright © 2012-7 David A. Smith

Distributed under the Eclipse Public License.
