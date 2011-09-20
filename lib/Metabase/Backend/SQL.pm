use 5.008001;
use strict;
use warnings;

package Metabase::Backend::SQL;
# VERSION
# ABSTRACT: Metabase backend role for SQL-based backends

use Class::Load qw/load_class try_load_class/;
use Storable qw/nfreeze/;

use Moose::Role;

has [qw/dsn db_user db_pass db_type/] => (
  is => 'ro',
  isa => 'Str',
  lazy_build => 1,
);

has dbis => (
  is => 'ro',
  isa => 'DBIx::Simple',
  lazy_build => 1,
  handles => [qw/dbh/],
);

has schema => (
  is => 'ro',
  isa => 'SQL::Translator::Schema',
  lazy_build => 1,
);

#--------------------------------------------------------------------------#
# to be implemented by Metabase::Backend::${DBNAME}
#--------------------------------------------------------------------------#

requires '_build_dsn';
requires '_build_db_user';
requires '_build_db_pass';
requires '_build_db_type';
requires '_fixup_sql_diff';

#--------------------------------------------------------------------------#

sub _build_dbis {
  my ($self) = @_;
  my @connect = map { $self->$_ } qw/dsn db_user db_pass/;
  my $dbis = eval { DBIx::Simple->connect(@connect) }
    or die "Could not connect via " . join(":",map { qq{'$_'} } @connect[0,1],"...")
    . " because: $@\n";
  return $dbis;
}

sub _build_schema {
  my $self = shift;
  return SQL::Translator::Schema->new(
    name => 'Metabase',
    database => $self->db_type,
  );
}

sub _deploy_schema {
  my ($self) = @_;

  my $schema = $self->schema;

  # Blow up if this doesn't seem OK
  $schema->is_valid or die "Could not validate schema: $schema->error";
#  use Data::Dumper;
#  warn "Schema: " . Dumper($schema);

  my $db_type = $self->db_type;
  # See what we already have
  my $existing = SQL::Translator->new(
    parser => 'DBI',
    parser_args => {
      dbh => $self->dbh,
    },
    producer => $db_type,
    show_warnings => 0, # suppress warning from empty DB
  );
  {
    # shut up P::RD when there is no text -- the SQL::Translator parser
    # forces things on when loaded.  Gross.
    no warnings 'once';
    load_class( "SQL::Translator::Parser::" . $self->db_type );
    local *main::RD_ERRORS;
    local *main::RD_WARN;
    local *main::RD_HINT;
    my $existing_sql = $existing->translate();
#    warn "*** Existing schema: " . $existing_sql;
  }

  # Convert our target schema
  my $fake = SQL::Translator->new(
    parser => 'Storable',
    producer => $db_type,
  );
  my $fake_sql = $fake->translate( \( nfreeze($schema) ) );
#  warn "*** Fake schema: $fake_sql";

  my $target = SQL::Translator->new(
    parser => $db_type,
    producer => $db_type,
  );
  my $target_sql = $target->translate(\$fake_sql);
#  warn "*** Target schema: $target_sql";

  my $diff = SQL::Translator::Diff::schema_diff(
    $existing->schema, $db_type, $target->schema, $db_type
  );

  $diff = $self->_fixup_sql_diff($diff);

  # DBIx::RunSQL requires a file (ugh)
  my ($fh, $sqlfile) = File::Temp::tempfile();
  print {$fh} $diff;
  close $fh;
#  warn "*** Schema Diff:\n$diff\n"; # XXX

  $self->clear_dbis; # ensure we re-initailize handle
  unless ( $diff =~ /-- No differences found/i ) {
    DBIx::RunSQL->create(
      dbh => $self->dbh,
      sql => $sqlfile,
    );
    $self->dbh->disconnect;
  }

  # must reset the connection
  $self->clear_dbis;
  $self->dbis; # rebuild

#  my ($count) = $self->dbis->query(qq{select count(*) from "core"})->list;
#  warn "Initialized with $count records";
  return;
}

1;

=for Pod::Coverage method_names_here

=head1 SYNOPSIS

XXX consolidate synopses from modules

=head1 DESCRIPTION

This role provides common attributes for SQL-based Metabase backends.
It is not intended to be used directly.

=attr dsn

Database connection string

=attr db_user

Database username

=attr db_pass

Database password

=attr db_type

SQL::Translator sub-type for a given database.  E.g. "SQLite" or "PostgreSQL".

=attr dbis

DBIx::Simple class connected to the database

=attr schema

SQL::Translator::Schema class

=head1 REQUIRED METHODS

The following builders must be provided by consuming classes.

  _build_dsn        # a DSN string for DBI
  _build_db_user    # a username for DBI
  _build_db_pass    # a password for DBI
  _build_db_type    # a SQL::Translator type for the DB vendor

The following method must be provided to modify the output of
SQL::Translator::Diff to fix up any dialect quirks

  _fixup_sql_diff

=cut

# vim: ts=2 sts=2 sw=2 et:
