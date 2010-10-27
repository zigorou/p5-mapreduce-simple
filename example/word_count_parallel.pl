#!/usr/bin/perl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/../lib";

use Data::Dumper;
use ExtUtils::Installed;
use File::Slurp qw(slurp);
use MapReduce::Simple qw(:all);

my $mrs = MapReduce::Simple->new;

$mrs->add_job(
    mapper {
	my ( $mapper, $key, $value ) = @_;
	return [ map { $_ => 1 } split(/\W+/, $value) ];
    } ( class => 'Parallel', args => +{ max_process => 2 } )
);

$mrs->add_job(
    reducer {
	my ( $reducer, $key, $value ) = @_;
	return scalar @$value;
    } ( class => 'Parallel', args => +{ max_process => 2 } )
);

my $inst = ExtUtils::Installed->new;
my $i = 1;
my $rv = $mrs->run(
    +{
	map { $i++ => $_ }
	$inst->modules
    }
);

# warn Dumper $rv;
my @sorted = 
    sort { $rv->{$b} <=> $rv->{$a} }
    keys %$rv;

my @best_ten = splice( @sorted, 0, 10 );

warn Dumper \@best_ten;
