use strict;
use warnings;

use Test::More;
use Test::Exception;
use MapReduce::Simple qw(:all);

sub test_mapreduce {
    my %specs = @_;
    my ( $input, $expects, $desc ) = @specs{qw/input expects desc/};
    subtest $desc => sub {
        my $mrs = MapReduce::Simple->new(
            jobs => $input->{jobs},
        );
        my $rv;

        lives_ok {
            $rv = $mrs->run( $input->{data} );
        } 'run() lives ok';

        is_deeply( $rv, $expects, 'got data' );
        done_testing;
    };
}

test_mapreduce(
    input => +{
        jobs => [],
        data => +{},
    },
    expects => +{},
    desc => 'no jobs',
);

test_mapreduce(
    input => +{
        jobs => [
            mapper {
                my ( $mapper, $key, $value ) = @_;
                return [ map { $_ => 1 } split(/\W+/, $value) ];
            },
        ],
        data => +{
            1 => 'foo bar baz',
            2 => 'hello world',
        },
    },
    expects => +{
        foo => [1],
        bar => [1],
        baz => [1],
        hello => [1],
        world => [1],
    },
    desc => 'simple mapper, running sequencial',
);

test_mapreduce(
    input => +{
        jobs => [
            mapper {
                my ( $mapper, $key, $value ) = @_;
                return [ map { $_ => 1 } split(/\W+/, $value) ];
            } ( class => 'Parallel', args => +{ max_process => 3 } ),
        ],
        data => +{
            1 => 'foo bar baz',
            2 => 'hello world',
        },
    },
    expects => +{
        foo => [1],
        bar => [1],
        baz => [1],
        hello => [1],
        world => [1],
    },
    desc => 'simple mapper, running parallel',
);

test_mapreduce(
    input => +{
        jobs => [
            reducer {
                my ( $reducer, $key, $value ) = @_;
                return length $key;
            },
        ],
        data => +{
            foo => [1],
            bar => [1],
            baz => [1],
            hello => [1],
            world => [1],
        },
    },
    expects => +{
        foo => 3,
        bar => 3,
        baz => 3,
        hello => 5,
        world => 5,
    },
    desc => 'simple reducer, running sequencial',
);

test_mapreduce(
    input => +{
        jobs => [
            reducer {
                my ( $reducer, $key, $value ) = @_;
                return length $key;
            } ( class => 'Parallel', args => +{ max_process => 3 } ),
        ],
        data => +{
            foo => [1],
            bar => [1],
            baz => [1],
            hello => [1],
            world => [1],
        },
    },
    expects => +{
        foo => 3,
        bar => 3,
        baz => 3,
        hello => 5,
        world => 5,
    },
    desc => 'simple reducer, running parallel',
);

test_mapreduce(
    input => +{
        jobs => [
            mapper {
                my ( $mapper, $key, $value ) = @_;
                return [ map { $_ => 1 } split( /\W+/, $value ) ];
            },
            reducer {
                my ( $reducer, $key, $value ) = @_;
                return length $key;
            },
            mapper {
                my ( $reducer, $key, $value ) = @_;
                return [ 1 => $value ];
            },
            reducer {
                my ( $mapper, $key, $value ) = @_;
                my $sum = 0;
                map { $sum += $_ } @$value;
                return $sum;
            },
        ],
        data => +{
            1 => 'Shut the fuck up and write some code',
            2 => 'There is more than one way to do it'
        },
    },
    expects => +{
        1 => 56,
    },
    desc => 'complex mapper and reducer, running sequencial',
);

test_mapreduce(
    input => +{
        jobs => [
            ( mapper {
                my ( $mapper, $key, $value ) = @_;
                return [ map { $_ => 1 } split( /\W+/, $value ) ];
            } ( class => 'Parallel' ) ),
            ( reducer {
                my ( $reducer, $key, $value ) = @_;
                return length $key;
            } ( class => 'Parallel' ) ),
            ( mapper {
                my ( $reducer, $key, $value ) = @_;
                return [ 1 => $value ];
            } ( class => 'Parallel' ) ),
            ( reducer {
                my ( $mapper, $key, $value ) = @_;
                my $sum = 0;
                map { $sum += $_ } @$value;
                return $sum;
            } ( class => 'Parallel' ) ),
        ],
        data => +{
            1 => 'Shut the fuck up and write some code',
            2 => 'There is more than one way to do it'
        },
    },
    expects => +{
        1 => 56,
    },
    desc => 'complex mapper and reducer, running parallel',
);

done_testing;

# Local Variables:
# mode: perl
# perl-indent-level: 4
# indent-tabs-mode: nil
# coding: utf-8-unix
# End:
#
# vim: expandtab shiftwidth=4:
