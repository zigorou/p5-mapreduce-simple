package MapReduce::Simple::Job::Reducer;

use strict;
use warnings;
use parent qw(MapReduce::Simple::Job);
use Carp;
use Try::Tiny;

our $VERSION = '0.01';

sub new {
    my ( $class, $code, $opts ) = @_;

    $opts                ||= +{};
    $opts->{identifier}  ||= 'reducer';

    $class->SUPER::new( $code, $opts );
}

1;

__END__

=head1 NAME

MapReduce::Simple::Job::Reducer - write short description for MapReduce::Simple::Job::Reducer

=head1 SYNOPSIS

  use MapReduce::Simple::Job::Reducer;

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
