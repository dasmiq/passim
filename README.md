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

To compile passim itself, install [sbt](http://www.scala-sbt.org/), a
build tool for Scala, Java, and other JVM languages.  Then run:
```
$ sbt package
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
that case, we would declare all issues of the same newspaper--or all
articles within those issues--to have the same series.  Similarly, we
might define documents to be the pages or chapters of a book and
series to be whole books.

The default input format for documents is in a file or set
of files containing JSON records.  The record for a single document
with the required `id` and `text` fields, as well as a `series` field,
would look like:
```
{"id": "d1", "series": "abc", "text": "This is text that&apos;s interpreted as <code>XML</code>; the tags are ignored by default."}
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

Natural language text is redundant, and adding XML markup and JSON
field names increases the redundancy.  Spark and passim support
several compression schemes.  For relatively small files, gzip is
adequate; however, when the input files are large enough that the do
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

Output is to a directory that, on completion, will contain an
`out.json` directory with `part-*` files rather than a single file.
This allows multiple workers to efficiently write it (and read it back
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
`begin` | offset in the document of the token where the reused passage begins
`end` | offset in the document of the token where the reused passage ends
`uid` | unique internal ID for each document, used for debugging

In addition, `pages`, `regions`, and `locs` include information about
locations in the underlying text of the reused passage.  See [Marking
Locations inside Documents](#locations) below.

Some useful parameters are:

Parameter | Default value | Description
--------- | ------------- | -----------
`--n` | 5 | N-gram order for text-reuse detection
`---max-series` | 100 | Maximum document frequency of n-grams used.
`--min-match` | 5 | Minimum number of matching n-grams between two documents.
`--relative-overlap` | 0.5 | Proportion that two different aligned passages from the same document must overlap to be clustered together, as measured on the longer passage.

Pass parameters to the underlying Spark processes using the
`SPARK_SUBMIT_ARGS` environment variable.  For example, to run passim
on a local machine with 10 cores and 200GB of memory, do:

```
$ SPARK_SUBMIT_ARGS='--master local[10] --driver-memory 200G --executor-memory 200G' passim input.json output
```

See the
[Spark documentation](https://spark.apache.org/docs/latest/index.html)
for further configuration options.

If `jq` is installed, you can convert JSON output to a tab-separated
table with `tabcluster.sh` and to CSV with `csvcluster.sh`.

## <a name="locations"></a> Marking Locations inside Documents

As mentioned above, the `text` field is interpreted as XML.  The
parser expands character entities and, for the most part, ignores
tags.

Three XML elements, however, are recorded when documents are
tokenized.  Each marks the location of structural features within the
document:
* `<pb n="..." />` marks the beginning of a page, with the `n`
  attribute parsed as a string;

* `<w coords="..." />` marks the beginning of a region on a page
  image, with the `coords=` attributed parsed as a 4-tuple of
  integers for upper left, upper right, width, and height; and

* `<loc n="..." />` marks the beginning of a citable passage
  ("locus") according to some canonical scheme such as the books,
  chapters, and verses of the Bible; acts, scenes, and Globe lines in
  Shakespeare; or Stephanus pages in Plato.  We recommend
  [Canonical Text Servive (CTS) URNs](http://www.homermultitext.org/hmt-doc/cite/index.html)
  for referring to these locations independently of any particular
  edition.  Although many canonical citations schemes have a
  hierarchical structure of books, chapters, etc., passim interprets
  the `n=` attribute as an atomic string.

These elements are empty so as not to interfere with other tags.  When
reused passages contain these location markers, the output records
will store the information in the `pages`, `regions`, and `locs`
fields.

## Quotations of Reference Texts

TODO.  For now, including the query texts in the corpus will
automatically include them in any clustering.  We are working on
including implementations of more efficient query procedures.

<!-- You can use any galago n-gram index: 4-gram, 5-gram, etc. For several -->
<!-- tasks, 5-grams seem like a good tradeoff. -->

<!-- For best results, index the reference texts---as trectext or some -->
<!-- other plaintext format---along with the target document.  This ensures -->
<!-- that any n-gram in the reference texts occurs at least once in the -->
<!-- index.  The quotes program will then automatically filter out matches -->
<!-- of a reference text with itself.  There is one other advantage of -->
<!-- including the reference texts in the index.  Since you guarantee that -->
<!-- all n-grams in the reference texts will be seen, you can shard the -->
<!-- index of the books without having any useful n-grams fall below -->
<!-- threshold (as long as you add a copy of the reference texts to each -->
<!-- shard). -->

## Acknowledgements

We insert the appropriate text in gratitude to our sponsors at the
Andrew W. Mellon Foundation and the National Endowment for the
Humanities. TODO.

## License

Copyright © 2012-5 David A. Smith

Distributed under the Eclipse Public License.
