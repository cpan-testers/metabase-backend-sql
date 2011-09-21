use 5.008001;
use strict;
use warnings;

package Metabase::Backend::SQLite;
# VERSION
# ABSTRACT: Metabase backend implemented using SQLite

use MooseX::Types::Path::Class;
use Moose::Role;
use DBD::SQLite;
use namespace::autoclean;

with 'Metabase::Backend::SQL';

has 'filename' => (
    is       => 'ro',
    isa      => 'Path::Class::File',
    coerce   => 1,
    required => 1,
);

has 'synchronous' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
);

sub _build_dsn {
  my $self = shift;
  return "dbi:SQLite:dbname=" . $self->filename;
}

sub _build_db_user { return "" }

sub _build_db_pass { return "" }

sub _build_db_type { return "SQLite" }

around _build_dbis => sub {
  my $orig = shift;
  my $self = shift;
  my $dbis = $self->$orig;
  my $toggle = $self->synchronous ? "ON" : "OFF";
  $dbis->query("PRAGMA synchronous = $toggle");
  return $dbis;
};

sub _fixup_sql_diff {
  my ($self, $diff) = @_;
  # Fix up BEGIN/COMMIT
  $diff =~ s/BEGIN;/BEGIN TRANSACTION;/mg;
  $diff =~ s/COMMIT;/COMMIT TRANSACTION;/mg;
  # Strip comments
  $diff =~ s/^--[^\n]*$//msg;
  # strip empty lines
  $diff =~ s/^\n//msg;
  return $diff;
}

sub _build__guid_field_params {
  return {
    data_type => 'char',
    size => 36,
  }
}

sub _build__blob_field_params {
  return {
    data_type => 'blob'
  };
}

my $hex = qr/[0-9a-f]/i;
sub _munge_guid {
  my ($self, $guid) = @_;
  $guid = "00000000-0000-0000-0000-000000000000"
    unless $guid =~ /${hex}{8}-${hex}{4}-${hex}{4}-${hex}{4}-${hex}{12}/;
  return $guid;
}

sub _unmunge_guid {
  my ($self, $guid) = @_;
  return $guid;
}

1;

=for Pod::Coverage method_names_here

=head1 SYNOPSIS

XXX consolidate synopses from modules

=head1 DESCRIPTION

This distribution provides a backend for L<Metabase> using SQLite.
There are two modules included, L<Metabase::Index::SQLite> and
L<Metabase::Archive::SQLite>.  They can be used separately or together (see
L<Metabase::Librarian> for details).

The L<Metabase::Backend::SQLite> module is a L<Moose::Role> that provides
common attributes and private helpers and is not intended to be used directly.

Common attributes are described further below.

=attr filename

Path to an SQLite database

=attr synchronous

Controls how SQLite should set the C<synchronous> pragma.  Defaults to false,
which is faster, but less safe.

=cut

# vim: ts=2 sts=2 sw=2 et:
