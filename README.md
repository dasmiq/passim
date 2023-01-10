# passim

This project implements algorithms for detecting and aligning similar passages in text.  It can be run either in query mode, to find quoted passages from a reference text, or all-pairs mode, to find all pairs of passages within longer documents with substantial alignments.

## Documentation

Besides the documentation in this README file, you might find it helpful to read:

* [Getting Started with `passim`](docs/passim_quickstart.ipynb), an IPython notebook that walks through installation and basic usage; and
* [Detecting Text Reuse with Passim 2.0](https://github.com/dasmiq/ph-submissions/blob/passim2-replace/en/published/originals/detecting-text-reuse-with-passim.md), an updated version of the tutorial on [_Programming Historian_](https://programminghistorian.org/).

If you are using the earlier _Programming Historian_ tutorial on passim version 1, see [this updated version](https://github.com/dasmiq/ph-submissions/blob/passim1-release/en/published/originals/detecting-text-reuse-with-passim.md) that corrects links to the source code.

## Installation

Passim relies on [Apache Spark](http://spark.apache.org) to manage parallel computations, either on a single machine or a cluster.  Spark, in turn, requires Java to run with the `java` command.  In its default configuration, it also expects Python 3 to be runnable with the `python` command.  There are ways to configure your system differently using various environment variables, but having these defaults will make your life easier.

Once you have java and python installed, the easiest way to install the latest version of passim is with the `pip` command.  Since passim requires Python 3, you may need to use `pip3` on some systems.

The easiest way to install Passim is to have `pip` download from github directly:
```
$ pip install git+https://github.com/dasmiq/passim.git@seriatim#egg=passim
```

If pip complains that you don't have access to install python packages on the system, you can use the `--user` option to install in your home directory, like so:
```
$ pip install --user git+https://github.com/dasmiq/passim.git@seriatim#egg=passim
```

If you've cloned the source code, you can run
```
pip install .
```
with or without the `--user` option, as needed.

It is possible that `pip` might install passim in a location not in your `PATH`. If so, run `pip show --files passim` to see where it is; you can then modify your `PATH` to find it.

Since passim defaults to the JSON format for input and output, it is convenient to have the [command-line JSON processor jq](http://stedolan.github.io/jq/) installed.

## Aligning and Clustering Matching Passage Pairs

### Structuring the Input

The input to passim is a set of _documents_.  Depending on the kind of data you have, you might choose documents to be whole books, pages of books, whole issues of newspapers, individual newspaper articles, etc.  Minimally, a document consists of an identifier string and a single string of text content.

For many text reuse detection problems, it is useful to group documents into _series_.  Text reuse within series could then be ignored. We may, for example, be less interested in the masthead and ads that appear week after week in the same newspaper and more interested in articles that propagate from one city's newspapers to another's.  In that case, we would declare all issues of the same newspaper&mdash;or all articles within those issues&mdash;to have the same series.  Similarly, we might define documents to be the pages or chapters of a book and series to be whole books.

The default input format for documents is in a file or set of files containing one JSON record per line, i.e., the [JSON Lines format](https://jsonlines.org/).  The record for a single document with the required `id` and `text` fields, as well as a `series` field, would look like:
```
{"id": "d1", "series": "abc", "text": "This is text."}
```

Note that this must be a single line in the file.  This JSON record format has two important differences from general-purpose JSON files.  First, the JSON records for each document are concatenated together, rather than being nested inside a top-level array.  Second, each record must be contained on one line, rather than extending over multiple lines until the closing curly brace.  These restrictions make it more efficient to process in parallel large numbers of documents spread across multiple files.

In addition to the fields `id`, `text`, and `series`, other metadata included in the record for each document will be passed through into the output.

Natural language text is redundant, and adding markup and JSON field names increases the redundancy.  Spark and passim support several compression schemes.  For relatively small files, gzip is adequate; however, when the input files are large enough that they do not comfortably fit in memory, bzip2 is preferable since programs can split these files into blocks before decompressing.

### Running passim

The simplest invocation contains list of inputs and a directory name for the output.
```
$ passim input.json output
```

Following Spark conventions, input may be a single file, a single directory full of files, or a `*` wildcard (glob) expression.  Multiple input paths should be separated by commas.  Files may also be compressed.
```
$ passim "{input.json,directory-of-json-files,some*.json.bz2}" output
```

Output is written to a directory that, on completion, will contain an `out.json` directory with `part-*` files rather than a single file. This allows multiple workers to write it efficiently (and read it back in) in parallel.

The output contains one JSON record for each reused passage.  Each record echoes the fields in the JSON input and adds the following extra fields to describe the clustered passages:

Field | Description
----- | ------------
`cluster` | unique identifier for each cluster
`size` | number of passages in each cluster
`begin` | character offset into the document where the reused passage begins
`end` | character offset into the document where the reused passage ends
`uid` | unique internal ID for each document, used for debugging
`src` | preceding passage in the cluster inferred to be the source of this passage
`pboiler` | proportion of links in the cluster among texts from the same series

In addition, `pages`, `regions`, and `locs` include information about
locations in the underlying text of the reused passage.  See [Marking
Locations inside Documents](#locations) below.

The default input and output format is JSON.  Use the `--input-format parquet` and `--output-format parquet` to use the compressed binary Parquet format, which can speed up later processing.

If, in addition to the clusters, you want to output pairwise alignments between all matching passages, invoke passim with the `--pairwise` flag.  These alignments will be in `align.json` or `align.parquet`, depending on which output format you choose.

Use the `--help` option to see a full list of options.

Pass parameters to the underlying Spark processes using the `SPARK_SUBMIT_ARGS` environment variable.  For example, to run passim on a local machine with 10 cores and 200GB of memory, run the following command:
```
$ SPARK_SUBMIT_ARGS='--master local[10] --driver-memory 200G --executor-memory 200G' passim input.json output
```

See the [Spark documentation](https://spark.apache.org/docs/latest/index.html) for further configuration options.

### Controlling passim's n-gram filtering

One of the main goals of passim was to work well with large collections. To do that, passim indexes spans of _n_ input characters (i.e., n-grams) and then selects document pairs for alignment only if they share sufficient n-grams. After these pairs of input documents are selected for alignment, passim runs a full character-level alignment algorithm starting from the matching n-grams to produce the full output alignment.

By default, passim indexes character n-grams starting at word boundaries in order to find candidate passages to align.  If your text is very noisy, you can index other n-grams, starting at the middle of words, with the `--floating-ngrams` flag.  In any case, passim only indexes alphanumeric characters (as determined by python's `isalnum` function), skipping whitespace, punctuation, and other characters.

Some useful parameters are:

Parameter | Default value | Description
--------- | ------------- | -----------
`-n` | 25 | Character n-gram order for text-reuse detection
`-l` or `--minDF` | 2 | Minimum document frequency of n-grams indexed.
`-u` or `--maxDF` | 100 | Maximum document frequency of n-grams indexed.
`-m` or `--min-match` | 5 | Minimum number of matching n-grams between two documents.
`-a` or `--min-align` | 50 | Minimum number of characters in aligned passages.
`-g` or `--gap` | 600 | Passages more than this number of characters apart will be aligned separately.

When selecting values for these parameters, you are managing a precision-recall tradeoff. Shorter n-grams or lower minimum matches might produce more output alignments, but they might be less relevant.

### Pruning Alignments

The text of documents input to passim is indexed to determine which pairs should be aligned.  Often, document metadata can provide additional constraints on which documents should be aligned.  If there were no constraints, every pair of documents in the input would be aligned, in both directions.  These constraints on alignments are expressed by two arguments to passim: `--fields` and `--filterpairs`.

The `--fields` argument tells passim which fields in the input records to index when determining which documents to align.  Fields has the syntax of SQL `FROM` clause as implemented by Apache Spark.  Multiple fields may be passed by successfive command-line arguments.  Use quotes to surround fields definitions containing spaces.  In the following example, we ask passim to index the `date` field as is and to index the `series` field by hashing it to a 64-bit integer and calling it `gid`:
```
--fields date 'xxhash64(series) as gid'
```

Since document and series identifiers can be long strings, passim runs more efficiently if they are hashed to long integers by the (builtin) `xxhash64` function.

The `--filterpairs` argument is an SQL expression that specifies which pairs of documents are candidates for comparison.  A candidate pair consists of a "source" document, whose field names are identical to those in the input, and a "target" document, whose field names have a "2" appended to them.  This is similar to the left- and right-handed sides of a SQL JOIN; in fact, passim is effectively performing a (massively pruned) self-join on the table of input documents.

As an example, consider aligning only document pairs where the "source" document predates the "target" document by 0 to 30 days.  To perform efficient date arithmetic, we use Apache Spark's built-in `date` function to convert a string `date` field to an integer:
```
--fields 'date(date) as day' --filterpairs 'day < day2 AND (day2 - day) <= 30 AND uid <> uid2'
```
Since the dates may be equal, we also include the constraint that the hashed document ids (`uid`) be different.  Had we not done this, the output would also have included alignments of every document with itself.  The `uid` field as a hash of the `id` field is always available.  Note also the SQL inequality operator `<>`.

By default, passim uses `--filterpairs uid < uid2` to prune comparisons.  In other words, passim imposes a total order on all documents based on the arbitrary but easily-comparable hashes of document identifiers.

### passim vs. seriatim

As in previous versions, `passim` aligns pairs of passages that pass the filter criteria specified above.  There is now also a top-level `seriatim` command that, for a given target passage, returns the source passage with the _best_ alignment.  This is useful for constructing directed graphs of text reuse.  (In fact, `passim` is now equivalent to invoking `seriatim` with the `--all-pairs` option.)

### Producing Aligned Output

For the purposes of collating related texts or aligning different transcriptions of the same text, one can produce output using the `--docwise` or `--linewise` flags.  Each output record in `out.json` or `out.parquet` will then contain a document or line from the right-hand, "target" text, along with information about corresponding passages in the left-hand, "witness" or "source" text.

In the case of `--docwise` output. each output document contains an array of target line records, and each line record contains zero or more passages from the left-hand side that are aligned to that target line.  Note that we specify "passages" from the left-hand side because the line breaks in the witness, if any, may not correspond to line breaks in the target texts.  Both the target line and witness passage data may contain information about image coordinates, when available.

For `--linewise` output, each output document contains a single newline-delimited line from a target document.  This line is identified by the input document `id` and a character offset `begin` into the input text.  The corresponding witness passage is identified with `wid` and `wbegin`.

In both of these output variants, target lines and witness passages are presented in their original textual form and in their aligned form, with hyphens to pad insertions and deletions.  An example of a target line aligned to a single witness passage is:
```
{
  "id": "scheffel_ekkehard_1855#f0261z751",
  "begin": 721,
  "text": "Grammatik iſt ein hohes Weib, anders erſcheint ſie Holzhackern, an-\n",
  "wid": "scheffel_ekkehard_1855/0261",
  "wbegin": 739,
  "wtext": "Grammatik iſt ein hohes Weib, anders erſcheint ſie HolzhaFern , an=\n",
  "talg": "Grammatik iſt ein hohes Weib, anders erſcheint ſie Holzhackern-, an‐\n",
  "walg": "Grammatik iſt ein hohes Weib, anders erſcheint ſie Holzha-Fern , an=\n"
}
```

## <a name="locations"></a> Marking Locations inside Documents

Documents may record their extent on physical pages with the `pages` field.

```
          "pages": [
            {
              "id": "scheffel_ekkehard_1855/0261",
              "seq": 0,
              "width": 1600,
              "height": 2525,
              "dpi": 0,
              "regions": [
                {
                  "start": 14,
                  "length": 6,
                  "coords": {
                    "x": 100,
                    "y": 195,
                    "w": 117,
                    "h": 47,
                    "b": 47
                  }
                },
                {
                  "start": 21,
                  "length": 6,
                  "coords": {
                    "x": 252,
                    "y": 194,
                    "w": 114,
                    "h": 44,
                    "b": 44
                  }
                },
		...
	      ]
	    }
	  ]
 ```

Although, as stated above, JSON records should occupy a single line, we format the data here on several lines for legibility.  The `start` and `length` fields of a record in `regions` indicate character offsets into the document `text` field.  The `pages` field is an array since a document may span several pages.

## Acknowledgements

We insert the appropriate text in gratitude to our sponsors at the Andrew W. Mellon Foundation and the National Endowment for the Humanities. TODO.

## License

Copyright © 2012-21 David A. Smith

Distributed under the MIT License.
