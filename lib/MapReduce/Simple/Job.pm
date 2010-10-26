package MapReduce::Simple::Job;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);
use Carp;
use Try::Tiny;

our $VERSION = '0.01';

__PACKAGE__->mk_accessors(qw/code counter identifier worker/);

sub new {
    my ( $class, $code, $opts ) = @_;

    $opts                ||= +{};
    $opts->{counter}     ||= 0;
    $opts->{worker}      ||= +{
        class => 'Sequencial',
        args => +{},
    };

    $class->SUPER::new(+{ code => $code, %$opts });
}

sub run {
    my ( $self, $key, $value ) = @_;

    my $rv;
    try {
        $rv = $self->{code}->( $self, $key, $value );
    }
    catch {
        croak $_;
    };
    return $rv;
}

1;

__END__

=head1 NAME

MapReduce::Simple::Job - write short description for MapReduce::Simple::Job

=head1 SYNOPSIS

  use MapReduce::Simple::Job;

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
