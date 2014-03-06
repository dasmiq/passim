# passim

This project implements algorithms for detecting and aligning similar
passages in text, either from the command line or the clojure REPL.
It can be run either in query mode, to find quoted passages from a
reference text, or all-pairs mode, to find all pairs of passages
within longer documents with substantial alignments.

## Installation

To compile, install the [Leiningen build tool](http://leiningen.org/)
and run:

    $ lein bin

This should produce an executable `target/passim` and copy it to your
`~/bin`.

## Aligning and Clustering Matching Passage Pairs

### Input Formats

The first step is to index the input documents with galago.  These
documents can be in any format that galago supports.  Galago requires
that the suffixes for input documents encode the format.  You can then
optionally append `.gz` or `.bz2` to filenames to indicate that
they've been compressed.  One simple but useful
format is ``trectext'', which encodes a sequence of one or more
documents along with their unique IDs like so:

	<DOC>
	<DOCNO> foo_1 </DOCNO>
	<TEXT>
	Contents
	lasting
	many lines.
	...
	</TEXT>
	</DOC>
	<DOC>
	<DOCNO> foo_23 </DOCNO>
	<TEXT>
	More text appears.
	The <emph>tags</emph> will be ignored unless otherwise specified.
	...
	</TEXT>
	</DOC>

In addition, passim supports a variant called ``metatext'' that
inserts a metadata field for every tag other than `docno` and
`text`.  Consider the following document:

	<doc>
	<docno>foo_1</docno>
	<date>1901-01-01</date>
	<url>http://example.com/</url>
	<text>
	Contents
	go here.
	</text>
	</doc>
	<doc>
	...

In addition to indexing the contents, galago will attach a `date` and
`url` field to document `foo_1`.  Those two fields, along with
`title`, are used by passim when formatting cluster output.  Note that
the tags are in lower case.

### Document Identifiers and Duplicate Detection

Many passages are duplicated among documents from the same source, and
these local instances of text reuse are uninteresting for many
applications.  For instance, different issues of the same newspaper
might repeat the same masthead or advertisements.  The search for
matching document pairs therefore uses the _series_ of each document
to suppress these pairs.  By default, the series is the initial part
of a document identifier before the first underscore (\_) or slash
(/).  In the trectext example above, the series of both documents
would be "foo".  You can override this default behavior by passing a
map from _internal_ document IDs to series numbers with the
`--series-map` option.  This would be useful if you wanted to reuse
the same document collection and index with different groupings of
documents into series.

### Using `make` to Process Data

In the `build` subdirectory, there is a Makefile and a few helper
scripts that automate the data processing pipeline.  We recommend that
you symlink these files into your working directory.  Then inside your
working directory, put a subdirectory `coll/input` with the input
files for your collection.  You directory structure under `work` would
thus look something like this:

	work/Makefile -> $SRC/passim/build/Makefile
	work/build.json -> $SRC/passim/build/build.json
	work/coll
	work/coll/input
	work/coll/input/data1.trectext.gz
	work/coll/input/data2.trectext.gz
	work/galago -> $SRC/passim/build/galago
	work/lsf-galago -> $SRC/passim/build/lsf-galago
	work/window.json -> $SRC/passim/build/window.json

The basic pipeline uses the subcommands `index`, `pairs`, `align`,
`cluster`, `clinfo`.

To build the file of pairwise local alignments, run

	$ make align

To run single-link clustering of these aligned passages, run

	$ make clinfo

You can pass several arguments to alter the behavior of make.  For
example, to change the collection subdirectory from `coll` to `bills`
and to change the n-gram feature length from 5 to 10, run

	$ make COLL=bills NGRAM=10 clinfo

Some useful parameters are:

Parameter | Default value | Description
--------- | ------------- | -----------
`COLL` | coll | Subdirectory containing collection input in `input`.
`NGRAM` | 5 | N-gram order for test-reuse detection
`UPPER` | 100 | Maximum document frequency of n-grams used.
`MINREP` | 5 | Minimum number of matching n-grams between two documents.
`RELOVER` | 0.5 | Proportion that two different aligned passages from the same document must overlap to be clustered together, as measured on the longer passage.

Use `make clean` to remove temporary build directories and `make
distclean` to remove all output files.

## Quotations of Reference Texts

Run with a galago n-gram index and reference text(s):

	$ passim quotes [options] <n-gram index> <reference text file>

A reference text file of `-` will read the standard input.  The only
notable option is `--pretty` to pretty-print the JSON output.

The reference text format is a unique citation, followed by a tab and
some text:

	urn:cts:englishLit:shakespeare.ham:1.1.6	You come most carefully upon your hour.
	urn:cts:englishLit:shakespeare.ham:1.1.7	'Tis now struck twelve; get thee to bed, Francisco.

This program treats citations as unparsed, atomic strings, though URNs
in a standard scheme, such as the CTS citations used here, are
encouraged.

You can use any galago n-gram index: 4-gram, 5-gram, etc. For several
tasks, 5-grams seem like a good tradeoff.

For best results, index the reference texts---as trectext or some
other plaintext format---along with the target document.  This ensures
that any n-gram in the reference texts occurs at least once in the
index.  The quotes program will then automatically filter out matches
of a reference text with itself.  There is one other advantage of
including the reference texts in the index.  Since you guarantee that
all n-grams in the reference texts will be seen, you can shard the
index of the books without having any useful n-grams fall below
threshold (as long as you add a copy of the reference texts to each
shard).


## License

Copyright © 2012-3 David A. Smith

Distributed under the Eclipse Public License, the same as Clojure.
