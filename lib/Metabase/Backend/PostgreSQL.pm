use 5.008001;
use strict;
use warnings;

package Metabase::Backend::PostgreSQL;
# ABSTRACT: Metabase backend implemented using PostgreSQL

our $VERSION = '1.001';

use Moose::Role;
use namespace::autoclean;

with 'Metabase::Backend::SQL';

has db_name => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

sub _build_dsn {
  my $self = shift;
  return "dbi:Pg:dbname=" . $self->db_name;
}

sub _build_db_user { return "" }

sub _build_db_pass { return "" }

sub _build_db_type { return "PostgreSQL" }

sub _fixup_sql_diff {
  my ($self, $diff) = @_;
  # Strip DROP INDEX
  $diff =~ s/DROP INDEX .*;//mg;
  # Strip ALTER TABLES constraint drops
  $diff =~ s/ALTER TABLE \S+ ADD CONSTRAINT.*;//mg;
  return $diff;
}

around _build_dbis => sub {
  my $orig = shift;
  my $self = shift;
  my $dbis = $self->$orig;
  $dbis->abstract = SQL::Abstract->new(
    quote_char => q{"},
  );
  return $dbis;
};

sub _build__blob_field_params {
  return {
    data_type => 'text'
  };
}

sub _build__guid_field_params {
  return {
    data_type => 'uuid'
  }
}

my $hex = qr/[0-9a-f]/i;
sub _munge_guid {
  my ($self, $guid) = @_;
  $guid = "00000000-0000-0000-0000-000000000000"
    unless $guid =~ /${hex}{8}-${hex}{4}-${hex}{4}-${hex}{4}-${hex}{12}/;
  return $guid;
}

sub _unmunge_guid { lc return $_[1] }

1;

=for Pod::Coverage method_names_here

=head1 SYNOPSIS

  use Metabase::Archive::PostgreSQL;
  use Metabase::Index::PostgreSQL;

  my $archive = Metabase::Archive::PostgreSQL->new(
    db_name => "cpantesters",
    db_user => "johndoe",
    db_pass => "PaSsWoRd",
  );

  my $index = Metabase::Index::PostgreSQL->new(
    db_name => "cpantesters",
    db_user => "johndoe",
    db_pass => "PaSsWoRd",
  );

=head1 DESCRIPTION

This distribution provides a backend for L<Metabase> using PostgreSQL.  There
are two modules included, L<Metabase::Index::PostgreSQL> and
L<Metabase::Archive::PostgreSQL>.  They can be used separately or together (see
L<Metabase::Librarian> for details).

The L<Metabase::Backend::PostgreSQL> module is a L<Moose::Role> that provides
common attributes and private helpers and is not intended to be used directly.

Common attributes are described further below.

=attr db_name

Database name

=attr db_user

Database username

=attr db_pass

Database password

=cut

# vim: ts=2 sts=2 sw=2 et:
