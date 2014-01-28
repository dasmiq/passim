# passim

This project implements algorithms for detecting and aligning similar
passages in text, either from the command line or the clojure REPL.
It can be run either in query mode, to find quoted passages from a
reference text, or all-pairs mode, to find all pairs of passages
within longer documents with substantial alignments.

## Installation

To compile, run:

    $ lein bin

This should produce an executable `target/passim-0.1.0-SNAPSHOT`.

## Aligning and Clustering Matching Passage Pairs

The basic pipeline uses the subcommands `pairs`, `scores`, `cluster`,
`format`.

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
