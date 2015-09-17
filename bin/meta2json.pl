#!/usr/bin/perl

use strict;

while (<>) {
    s/\\/\\\\/g;
    s/\"/\\"/g;
    s#</?DELETED>##g;
    s/<doc>/\{/;
    s#</doc>#\}#g;
    s#<docno>\s*(\S+)\s*</docno>#"id": "$1",#;
    s#<text>#"text": "#;
    s#</text>#"#;
    s#^<([A-Za-z][a-zA-Z0-9\-]*)>([^<]+)</[^>]+>\s*$#"$1": "$2",\n#;
    print;
}
