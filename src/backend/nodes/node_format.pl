#!/usr/bin/perl

my $ident = '[a-zA-Z_][a-zA-Z0-9_]*';

while(<>)
{
	#skip comment
	s/\/\*.*\*\///;
	next if /^\s*$/;

	#skip multi-line comment
	my $line = $_;
	if ($line =~ /\/\*/)
	{
		until($line =~ /\*\//)
		{
			$line = $line . <>;
		}
		$line =~ s/\/\*(.|\n)*\*\///g;
	}

	#skip empty line
	next if $line =~ /^\s*$/;

	#skip #define
	if ($line =~ /^\s*#\s*define\s/)
	{
		while($line =~ /\\$/)
		{
			$line = <>;
		}
		next;
	}

	#combin multi-line function declare
	if ($line =~ /^\s*(const\s+){0,1}(struct\s+){0,1}$ident(\s*\**\s*|\s+)\(\s*\*\s*($ident)\s*\)/)
	{
		$line =~ s/\/\*.*\*\///g;
		until($line =~ /;\s*$/)
		{
			$line =~ s/\n//;
			$line .= <>;
		}
	}
	print $line;
}
