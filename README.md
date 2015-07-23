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

    $ sbt package

This should produce a runnable .jar in
`target/scala_*/passim*.jar`. (The exact filename will depend on the
version of Scala and passim that you have.)

The `bin` subdirectory of the passim distribution contains executable
shell scripts such `passim`.  We recommend adding this subdirectory to
your `PATH`.

Since passim uses the JSON format for input and output, it is
convenient to have the
[command-line JSON processor jq](http://stedolan.github.io/jq/)
installed.

## Aligning and Clustering Matching Passage Pairs

### Structuring the Input

The input to passim is a set of _documents_. Depending on the kind of
data you have, you might choose documents to be whole books, pages of
books, whole issues of newspapers, individual newspaper articles, etc.
Minimally, a document consists of an identifier string and text
content.

For most text reuse detection problems, it is useful to group
documents into _series_.  Text reuse within series will be ignored.
We may, for example, be less interested in the masthead and ads that
appear week after week in the same newspaper and more interested in
articles that propagate from one city's newspapers to another's.  In
that case, we would declare all issues of the same newspaper--or all
articles within those issues--to have the same series.  Similarly, we
might define documents to be the pages or chapters or a book and
series to be whole books.

The default input format for documents is in a file or set
of files containing JSON records.  The record for a single document
with the required `id` and `text` fields, as well as a `series` field,
would look like:

	{"id": "d1", "series": "abc", "text": "This is text that&apos;s interpreted as <code>XML</code>; the tags are ignored by default."}

Note that this is must be a single line in the file.  This JSON record
format has two important differences from general-purpose JSON
files. First, the JSON records for each document are concatenated
together, rather than being nested inside a top-level array.  Second,
each record must be contained on one line, rather than extending over
multiple lines until the closing curly brace.  These restrictions make
it more efficient to process in parallel large numbers of documents
spread across multiple files.

In addition to the fields `id`, `text`, and `series`, other metadata
may be included in the record for each document. At present, the
fields `date`, `title`, and `url` will be handled specially by passim
in the output.

Natural language text is redundant, and adding XML markup and JSON
field names increases the redundancy.  Spark and passim support
several compression schemes.  For relatively small files, gzip is
adequate; however, when the input files are large enough that the do
not comfortably fit in memory, bzip2 is preferable since programs can
split it into blocks before decompressing.

### Running passim

Run

	$ passim input.json output

Output is a directory of JSON record files.

Multiple input files

	$ passim input.json,directory-of-json-files,some*.json.bz2 output

The JSON output is spread across several files named `part-*` in the
output subdirectory.

Some useful parameters are:

Parameter | Default value | Description
--------- | ------------- | -----------
`--n` | 5 | N-gram order for text-reuse detection
`---max-series` | 100 | Maximum document frequency of n-grams used.
`--min-match` | 5 | Minimum number of matching n-grams between two documents.
`--relative-overlap` | 0.5 | Proportion that two different aligned passages from the same document must overlap to be clustered together, as measured on the longer passage.

If `jq` is installed, you can convert JSON output to a tab-separated
table with `tabcluster.sh` and to CSV with `csvcluster.sh`.

## Quotations of Reference Texts

There are two different methods of quotation detection, depending on
the size of the query text. TODO.

<!-- The reference text format is a unique citation, followed by a tab and -->
<!-- some text: -->

<!-- 	urn:cts:englishLit:shakespeare.ham:1.1.6	You come most carefully upon your hour. -->
<!-- 	urn:cts:englishLit:shakespeare.ham:1.1.7	'Tis now struck twelve; get thee to bed, Francisco. -->

<!-- This program treats citations as unparsed, atomic strings, though URNs -->
<!-- in a standard scheme, such as the CTS citations used here, are -->
<!-- encouraged. -->

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


## License

Copyright © 2012-5 David A. Smith

Distributed under the Eclipse Public License.
