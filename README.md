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

## Quotes

The format is:

	urn:cts:englishLit:shakespeare.ham:1.1.6	You come most carefully upon your hour.
	urn:cts:englishLit:shakespeare.ham:1.1.7	'Tis now struck twelve; get thee to bed, Francisco.

## All-Pairs Mode

## License

Copyright © 2012-3 David A. Smith

Distributed under the Eclipse Public License, the same as Clojure.
