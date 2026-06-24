#! /usr/bin/perl
#
# Copyright (c) 2007-2017, PostgreSQL Global Development Group
#
# src/backend/utils/mb/Unicode/UCS_to_GB18030.pl
#
# Generate UTF-8 <--> GB18030 code conversion tables from
# "GB18030.b2c", obtained from
# http://www.nits.org.cn/index/article/4034


use strict;
use convutils;

my $this_script = $0;

# Read the input

my $in_file = "GB18030.b2c";

open(my $in, '<', $in_file) || die("cannot open $in_file");

my @mapping;

while (<$in>)
{
        next if (!m/([0-9A-F]+)\s+([0-9A-F]+)/);
        my ($c, $u) = ($1, $2);
        $c =~ s/ //g;
        my $ucs  = hex($u);
        my $code = hex($c);
        if ($code >= 0x80 && $ucs >= 0x0080)
        {
                push @mapping,
                  { ucs       => $ucs,
                        code      => $code,
                        direction => BOTH,
                        f         => $in_file,
                        l         => $. };
        }
}
close($in);

print_conversion_tables($this_script, "GB18030_2022", \@mapping);