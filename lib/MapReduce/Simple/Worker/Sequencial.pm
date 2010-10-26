package MapReduce::Simple::Worker::Sequencial;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);

our $VERSION = '0.01';

sub new {
    my ( $class, $args ) = @_;
    $args ||= +{};
    $class->SUPER::new( %$args );
}

sub run_mapper {
    my ( $self, $mapper, $data ) = @_;
    my $rs = +{};
    while ( my ($key, $value) = each %$data ) {
	my $rv = $mapper->run( $key, $value );
	while ( my ( $k, $v ) = splice(@$rv, 0, 2) ) {
	    $rs->{$k} ||= [];
	    push(@{$rs->{$k}}, $v);
	}
    }
    return $rs;
}

sub run_reducer {
    my ( $self, $reducer, $data ) = @_;
    my $rs = +{};
    while ( my ($key, $value) = each %$data ) {
	$value = $reducer->run( $key, $value );
	$rs->{$key} = $value;
    }
    return $rs;
}

1;

__END__

=head1 NAME

MapReduce::Simple::Worker::Sequencial - write short description for MapReduce::Simple::Worker::Sequencial

=head1 SYNOPSIS

  use MapReduce::Simple::Worker::Sequencial;

=head1 DESCRIPTION

=head1 METHODS

=head1 AUTHOR

Toru Yamaguchi E<lt>zigorou@dena.jp<gt>

=head1 LICENSE

This module is licensed under the same terms as Perl itself.

=head1 SEE ALSO

=cut

# Local Variables:
# mode: perl
# perl-indent-level: 4
# indent-tabs-mode: nil
# coding: utf-8-unix
# End:
#
# vim: expandtab shiftwidth=4:
