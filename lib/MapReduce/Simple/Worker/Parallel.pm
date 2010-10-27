package MapReduce::Simple::Worker::Parallel;

use strict;
use warnings;
use parent qw(Class::Accessor::Fast);
use Carp;
use Data::Dumper;
use DBI;
use File::Temp qw(tempfile);
use Parallel::ForkManager;
use Try::Tiny;

our $VERSION = '0.01';

__PACKAGE__->mk_accessors(qw/tmp_file max_process/);

sub new {
    my ( $class, $args ) = @_;
    $args->{max_process} ||= 3;
    my ( undef, $filename ) = tempfile;
    $args->{tmp_file} = $filename;
    $class->SUPER::new($args);
}

sub run_mapper {
    my ( $self, $mapper, $data ) = @_;

    $self->setup_temp_db;
    my $rs = +{};
    my $pm = Parallel::ForkManager->new( $self->max_process );

    while ( my ( $key, $value ) = each %$data ) {
        my $pid = $pm->start and next;
        my $rv = $mapper->run( $key, $value );
        $self->store_db( $key, $rv );
        $pm->finish;
    }

    $pm->wait_all_children;

    my $rv = $self->load_db;
    for my $value ( values %$rv ) {
        while ( my ( $k, $v ) = splice(@$value, 0, 2) ) {
            $rs->{$k} ||= [];
            push(@{$rs->{$k}}, $v);
        }
    }
    $self->finalize_temp_db;

    return $rs;
}

sub run_reducer {
    my ( $self, $reducer, $data ) = @_;

    $self->setup_temp_db;
    my $pm = Parallel::ForkManager->new( $self->max_process );

    while ( my ( $key, $value ) = each %$data ) {
        my $pid = $pm->start and next;
        my $rv = $reducer->run( $key, $value );
        $self->store_file( $key, $rv );
        # $rs->{$key} = $value;

        $pm->finish;
    }

    $pm->wait_all_children;
    my $rs = $self->load_db;

    $self->finalize_temp_db;

    return $rs;
}

sub temp_dbh {
    my $self = shift;
    my $dbh = DBI->connect('dbi:SQLite:dbname=' . $self->temp_file, "", "", +{ AutoCommit => 0, RaiseError => 1, });
    return $dbh;
}

sub setup_temp_db {
    my $self = shift;
    my $dbh = $self->temp_dbh;
    $dbh->do(<< 'SQL');
CREATE TABLE IF NOT EXISTS mapreduce_data (
  key TEXT not null,
  value TEXT not null
);
SQL
}

sub finalize_temp_db {
    my $self = shift;
    my $dbh = $self->temp_dbh;
    $dbh->do('TRUNCATE TABLE mapreduce_data');
}

sub store_db {
    my ( $self, $key, $rv ) = @_;
    my $serialized = Data::Dumper->new( [$rv] )->Indent(0)->Terse(1)->Dump;
    try {
        my $dbh = $self->temp_dbh;
        $dbh->do('INSERT INTO mapreduce_data(key, value) VALUES(?, ?)', undef, $key, $serialized);
        $dbh->commit;
    }
    catch {
        croak $_;
    };
}

sub load_db {
    my $self = shift;
    my $rv = +{};
    
    my $dbh = $self->temp_db;
    return $dbh->selectall_arrayref( 'SELECT key, value FROM mapreduce_data' );
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
