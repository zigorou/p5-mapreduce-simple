package MapReduce::Simple::Worker::Parallel;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);
use Data::Dumper;
use File::Spec;
use File::Slurp;
use File::Temp qw(tempdir);
use Parallel::ForkManager;

our $VERSION = '0.01';

__PACKAGE__->mk_accessors(qw/tmpdir max_process/);

sub new {
    my ( $class, $args ) = @_;
    $args->{max_process} ||= 3;
    $args->{tmpdir} =
      tempdir( sprintf( '%d_%d_XXXXXXXXXX', time, $$ ), CLEANUP => 1 );
    $class->SUPER::new($args);
}

sub run_mapper {
    my ( $self, $mapper, $data ) = @_;

    my $rs = +{};
    my $pm = Parallel::ForkManager->new( $self->max_process );

    while ( my ( $key, $value ) = each %$data ) {
        my $pid = $pm->start and next;
        my $rv = $mapper->run( $key, $value );
        $self->store_file( $key, $rv );
        $pm->finish;
    }

    $pm->wait_all_children;

    my $rv = $self->load_file;
    for my $value ( values %$rv ) {
        while ( my ( $k, $v ) = splice(@$value, 0, 2) ) {
            $rs->{$k} ||= [];
            push(@{$rs->{$k}}, $v);
        }
    }

    return $rs;
}

sub run_reducer {
    my ( $self, $reducer, $data ) = @_;

    my $pm = Parallel::ForkManager->new( $self->max_process );

    while ( my ( $key, $value ) = each %$data ) {
        my $pid = $pm->start and next;
        my $rv = $reducer->run( $key, $value );
        $self->store_file( $key, $rv );
        # $rs->{$key} = $value;

        $pm->finish;
    }

    $pm->wait_all_children;
    my $rs = $self->load_files;
    
    return $rs;
}

sub store_file {
    my ( $self, $key, $rv ) = @_;
    my $data_file = File::Spec->catfile( $self->tmpdir, $key . ".perl" );
    my $serialized = Data::Dumper->new( [$rv] )->Indent(0)->Terse(1)->Dump;
    open( my $fh, '>', $data_file ) or croak $!;
    syswrite( $fh, $serialized, length $serialized );
    close $fh;
}

sub load_file {
    my $self = shift;
    my $rv = +{};
    opendir(my $dh, $self->tmpdir) or croak($!);
    my @files =
        grep { -f $self->tmpdir . '/' . $_ && /\.perl$/ }
        readdir($dh);
    closedir($dh);

    for my $file ( @files ) {
        my ( $key ) = ( $file =~ m/^(.+)\.perl$/ );
        my $serialized = File::Slurp::slurp( $self->tmpdir . '/' . $file );
        $rv->{$key} = eval $serialized;
    }

    return $rv;
}

1;

__END__

=head1 NAME

MapReduce::Simple::Worker::Parallel - write short description for MapReduce::Simple::Worker::Parallel

=head1 SYNOPSIS

  use MapReduce::Simple::Worker::Parallel;

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
package MapReduce::Simple::Worker::Parallel;

use strict;
use warnings;

our $VERSION = '0.01';

1;

__END__

=head1 NAME

MapReduce::Simple::Worker::Parallel - write short description for MapReduce::Simple::Worker::Parallel

=head1 SYNOPSIS

  use MapReduce::Simple::Worker::Parallel;

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
