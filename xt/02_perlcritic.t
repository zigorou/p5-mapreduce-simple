use strict;
use Test::More;
eval {
    require Test::Perl::Critic;
    Test::Perl::Critic->import( -profile => 'xt/perlcriticrc', -exclude => [ qw/ProhibitSubroutinePrototypes/] );
};
plan skip_all => "Test::Perl::Critic is not installed." if $@;
all_critic_ok('lib');
