#!/usr/bin/perl

use strict;
use IO::File;

my($pref, $suff) = splice @ARGV, 0, 2;

my %h = ();

while (<>) {
    my($bin, $data) = split ' ', $_, 2;

    my $f = "$pref/$bin/$suff";

    # print $f, "\n";
    # next;

    $h{$f} = IO::File->new("| gzip -c > $f") unless exists($h{$f});

    my $x = $h{$f};
    
    print $x $data;
}
