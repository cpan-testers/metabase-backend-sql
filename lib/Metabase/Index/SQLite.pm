use 5.006;
use strict;
use warnings;

package Metabase::Index::SQLite;
# VERSION

use Moose;
use MooseX::Types::Path::Class;

use Class::Load qw/try_load_class/;
use Data::Stream::Bulk::Array;
use Data::Stream::Bulk::Nil;
use DBD::SQLite;
use DBIx::RunSQL;
use DBIx::Simple;
use List::AllUtils qw/uniq/;
use SQL::Abstract;
use SQL::Translator::Schema;
use SQL::Translator::Diff;
use SQL::Translator::Utils qw/normalize_name/;
use Path::Class ();
use SQL::Translator;
use Try::Tiny;
use Metabase::Fact;
use Storable qw/nfreeze/;

with 'Metabase::Backend::SQLite';
with 'Metabase::Index';

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

has core_table => (
  is => 'ro',
  isa => 'Str',
  default => sub { "core_meta" },
);

has _requested_content_type => (
  is => 'rw',
  isa => 'Str',
  clearer => '_clear_requested_content_type',
);

has _requested_resource_type => (
  is => 'rw',
  isa => 'Str',
  clearer => '_clear_requested_resource_type',
);

has _query_fields => (
  traits => ['Array'],
  is => 'ro',
  isa => 'ArrayRef[Str]',
  lazy_build => 1,
  handles => {
    _push_query_fields => 'push',
    _grep_query_fields => 'grep',
    _all_query_fields => 'elements',
  },
);

has _content_tables => (
  traits => ['Array'],
  is => 'ro',
  isa => 'ArrayRef[Str]',
  lazy_build => 1,
  handles => {
    _push_content_tables => 'push',
    _grep_content_tables => 'grep',
    _all_content_tables => 'elements',
  },
);

sub _build__content_tables { return [] }

has _resource_tables => (
  traits => ['Array'],
  is => 'ro',
  isa => 'ArrayRef[Str]',
  lazy_build => 1,
  handles => {
    _push_resource_tables => 'push',
    _grep_resource_tables => 'grep',
    _all_resource_tables => 'elements',
  },
);

sub _build__resource_tables { return [] }

sub _build__query_fields { return [] }

sub _build_dbis {
  my ($self) = @_;
  my $fn = $self->filename;
  my $dbis = DBIx::Simple->connect("dbi:SQLite:dbname=$fn","","")
    or die "Could not connect to $fn\n";
  my $toggle = $self->synchronous ? "ON" : "OFF";
  $dbis->query("PRAGMA synchronous = $toggle");
  return $dbis;
}

sub _build_schema {
  my $self = shift;
  return SQL::Translator::Schema->new(
    name => 'Metabase',
    database => 'SQLite',
  );
}

sub initialize {
  use IO::Handle;
  STDERR->autoflush(1); # XXX
  STDOUT->autoflush(1); # XXX
  my ($self, $classes, $resources) = @_;
  @$resources = uniq ( @$resources, "Metabase::Resource::metabase::user" );
  my $schema = $self->schema;
  # Core table
  $schema->add_table(
    $self->_table_from_meta( $self->core_table, Metabase::Fact->core_metadata_types )
  );
  # Fact tables
  my @expanded =
    map { $_->fact_classes }
    grep { $_->isa("Metabase::Report") }
    @$classes;
  for my $c ( @$classes, @expanded ) {
    next unless try_load_class($c);
#    warn "Scanning $c\n";
    my $name = normalize_name( lc($c->type) );
    my $types = $c->content_metadata_types;
    next unless $types && keys %$types;
    $self->_push_content_tables($name);
    $schema->add_table(
      $self->_table_from_meta( $name, $types )
    );
  }
  # Resource tables
  for my $r ( @$resources ) {
    next unless try_load_class($r);
    my $name = $r;
    $name =~ s/^Metabase::Resource:://;
    $name =~ s/::/_/g;
    $name = normalize_name( lc $name );
    my $types = $r->metadata_types;
    next unless keys %$types;
    $self->_push_resource_tables($name);
    $schema->add_table(
      $self->_table_from_meta( $name, $types )
    );
  }
  # Blow up if this doesn't seem OK
  $schema->is_valid or die "Could not generate schema: $schema->error";
#  use Data::Dumper;
#  warn "Schema: " . Dumper($schema);


  # See what we already have
  my $existing = SQL::Translator->new(
    parser => 'DBI',
    parser_args => {
#      dsn => 'dbi:SQLite:dbname=' . $self->filename,
      dbh => $self->dbis->dbh,
    },
    producer => 'SQLite',
    show_warnings => 0, # suppress warning from empty DB
  );
  {
    # shut up P::RD when there is no text -- the SQLite parser
    # forces things on when loaded.  Gross.
    no warnings 'once';
    require SQL::Translator::Parser::SQLite;
    local *main::RD_ERRORS;
    local *main::RD_WARN;
    local *main::RD_HINT;
    my $existing_sql = $existing->translate();
    #  warn "Existing schema: " . $existing_sql;
  }

  # Convert our target schema
  my $fake = SQL::Translator->new(
    parser => 'Storable',
    producer => 'SQLite',
  );
  my $fake_sql = $fake->translate( \( nfreeze($schema) ) );
#  warn "Fake schema: $fake_sql";

  my $target = SQL::Translator->new(
    parser => 'SQLite',
    producer => 'SQLite',
  );
  my $target_sql = $target->translate(\$fake_sql);
#  warn "Target schema: $target_sql";

  my $diff = SQL::Translator::Diff::schema_diff(
    $existing->schema, 'SQLite', $target->schema, 'SQLite'
  );

  # Fix up BEGIN/COMMIT
  $diff =~ s/BEGIN;/BEGIN TRANSACTION;/mg;
  $diff =~ s/COMMIT;/COMMIT TRANSACTION;/mg;
  # Strip comments
  $diff =~ s/^--[^\n]*$//msg;
  # strip empty lines
  $diff =~ s/^\n//msg;

  # DBIx::RunSQL requires a file (ugh)
  my ($fh, $sqlfile) = File::Temp::tempfile();
  print {$fh} $diff;
  close $fh;
#  warn "Schema Diff:\n$diff\n"; # XXX

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

my %typemap = (
  '//str'   => 'varchar(255)',
  '//num'   => 'integer',
  '//bool'  => 'boolean',
);

sub _table_from_meta {
  my ($self, $name, $typehash) = @_;
  $typehash->{_guid} = "//str"; # always the PK
  my $table = SQL::Translator::Schema::Table->new( name => $name );
  for my $k ( sort keys %$typehash ) {
#    warn "Adding $k\n";
    $table->add_field(
      name => normalize_name($k),
      data_type => $typemap{$typehash->{$k}} || "//str",
    );
  }
  return $table;
}

sub _content_table {
  my ($self, $name) = @_;
  return normalize_name( lc $name );
}

sub _resource_table {
  my ($self, $name) = @_;
  $name =~ s/^Metabase-Resource-//;
  return normalize_name( lc $name );
}

sub _get_search_sql {
  my ( $self, $select, $spec ) = @_;

  # clear type constraints before analyzing query
  $self->_clear_requested_content_type;
  $self->_clear_requested_resource_type;
  $self->_clear_query_fields;

  my ($where, $limit) = $self->get_native_query($spec);

  my ($saw_content_field, $saw_resource_field);
  for my $f ( $self->_all_query_fields ) {
    $saw_content_field++ if $f =~ qr{^content\.};
    $saw_resource_field++ if $f =~ qr{^resource\.};
    return unless $f =~ qr{^(?:core|content|resource)\.};
  }

  if ( $saw_content_field && ! $self->_requested_content_type ) {
    Carp::confess("query requested content metadata without content type constraint");
  }
  if ( $saw_resource_field && ! $self->_requested_resource_type ) {
    Carp::confess("query requested resource metadata without resource type constraint");
  }

  # based on requests, conduct joins
  my @from = qq{from "core_meta" core};
  return unless $self->_check_query_fields($self->core_table, 'core');

  if ( my $content_type = $self->_requested_content_type ) {
    my $content_table = $self->_content_table($content_type);
    return unless $self->_check_query_fields($content_table, 'content');
    push @from, qq{join "$content_table" content on core.guid = content._guid};
  }
  if ( my $resource_type = $self->_requested_resource_type ) {
    my $resource_table = $self->_resource_table($resource_type);
    return unless $self->_check_query_fields($resource_table, 'resource');
    push @from, qq{join "$resource_table" resource on core.guid = resource._guid};
  }

  my $sql = join(" ", $select, @from, $where);
  return ($sql, $limit);
}

sub _check_query_fields {
  my ($self, $table, $type) = @_; # type 'core', 'resource' or 'content'
  my $table_obj = $self->schema->get_table("$table");
  for my $f ( $self->_all_query_fields ) {
    next unless $f =~ /^$type\.(.+)$/;
    my $name = $1;
    return unless $table_obj->get_field($name);
  }
  return 1;
}

sub add {
    my ( $self, $fact ) = @_;

    Carp::confess("can't index a Fact without a GUID") unless $fact->guid;

    try {
      $self->dbis->begin_work();
      my $core_meta = $fact->core_metadata;
      $core_meta->{resource} = "$core_meta->{resource}"; #stringify obj
#        use Data::Dumper;
#        warn "Adding " . Dumper $core_meta;
      $self->dbis->insert( 'core_meta', $core_meta );
      my $content_meta = $fact->content_metadata;
      # not all facts have content metadata
      if ( keys %$content_meta ) {
        $content_meta->{_guid} = $fact->guid;
#        use Data::Dumper;
#        warn "Adding " . Dumper $content_meta;
        my $content_table = $self->_content_table( $fact->type );
        $self->dbis->insert( $content_table, $content_meta );
      }
      # XXX eventually, add resource metadata -- dagolden, 2011-08-24
      my $resource_meta = $fact->resource_metadata;
      # not all facts have resource metadata
      if ( keys %$resource_meta ) {
        $resource_meta->{_guid} = $fact->guid;
#        use Data::Dumper;
#        warn "Adding " . Dumper $resource_meta;
        my $resource_table = $self->_resource_table( $resource_meta->{type} );
        $self->dbis->insert( $resource_table, $resource_meta );
      }
      $self->dbis->commit;
    }
    catch {
      $self->dbis->rollback;
      Carp::confess("Error inserting record: $_");
    };

}

sub count {
  my ( $self, %spec) = @_;

  my ($sql, $limit) = $self->_get_search_sql("select count(*)", \%spec);

  return 0 unless $sql;
#  warn "COUNT: $sql\n";

  my ($count) = $self->dbis->query($sql)->list;

  return $count;
}

sub query {
  my ( $self, %spec) = @_;

  my ($sql, $limit) = $self->_get_search_sql("select core.guid", \%spec);

  return Data::Stream::Bulk::Nil->new
    unless $sql;

#  warn "QUERY: $sql\n";
  my $result = $self->dbis->query($sql);

  return Data::Stream::Bulk::Array->new(
    array => [ map { $_->[0] } $result->arrays ]
  );
}

sub delete {
    my ( $self, $guid ) = @_;

    Carp::confess("can't delete without a GUID") unless $guid;

    try {
      $self->dbis->begin_work();
      $self->dbis->delete( 'core_meta', { 'guid' => $guid } );
      # XXX need to track _content_tables
      for my $table ( uniq $self->_all_content_tables ) {
        $self->dbis->delete( $table, { '_guid' => $guid } );
      }
      for my $table ( uniq $self->_all_resource_tables ) {
        $self->dbis->delete( $table, { '_guid' => $guid } );
      }
      # XXX eventually, add resource metadata -- dagolden, 2011-08-24
      $self->dbis->commit;
    }
    catch {
      $self->dbis->rollback;
      Carp::confess("Error deleting record: $_");
    };
    # delete
}

#--------------------------------------------------------------------------#
# required by Metabase::Query
#--------------------------------------------------------------------------#

# We need to track type constraints to determine which tables to join
before op_eq => sub {
  my ($self, $field, $value) = @_;
  if ($field eq 'core.type') {
    $self->_requested_content_type( $value );
  }
  if ($field eq 'resource.type') {
    $self->_requested_resource_type( $value );
  }
};

sub _quote_field {
  my ($self, $field) = @_;
  $self->_push_query_fields($field);
#  my ($first,$second) = $field =~ m{^([^.]+).(.*)$};
  return qq{$field};
}

sub _quote_val {
  my ($self, $value) = @_;
  $value =~ s{'}{''}g;
  return qq{'$value'};
}

sub translate_query {
  my ( $self, $spec ) = @_;

  my (@parts, $limit);

  # where
  if ( defined $spec->{-where} ) {
    push @parts, "where " . $self->dispatch_query_op( $spec->{-where} );
  }

  # order
  if ( defined $spec->{-order} and ref $spec->{-order} eq 'ARRAY') {
    my @clauses;
    my @order = @{$spec->{-order}};
    while ( @order ) {
      my ($dir, $field) = splice( @order, 0, 2);
      $field = $self->_quote_field( $field );
      $dir =~ s/^-//;
      $dir = uc $dir;
      push @clauses, "$field $dir";
    }
    push @parts, qq{order by } . join(", ", @clauses);
  }

  # limit
  if ( $limit = $spec->{-limit} ) {
    push @parts, qq{limit $limit};
  }

  return join( q{ }, @parts ), $limit;
}

sub op_eq {
  my ($self, $field, $val) = @_;
  return $self->_quote_field($field) . " = " . $self->_quote_val($val);
}

sub op_ne {
  my ($self, $field, $val) = @_;
  return $self->_quote_field($field) . " != " . $self->_quote_val($val);
}

sub op_gt {
  my ($self, $field, $val) = @_;
  return $self->_quote_field($field) . " > " . $self->_quote_val($val);
}

sub op_lt {
  my ($self, $field, $val) = @_;
  return $self->_quote_field($field) . " < " . $self->_quote_val($val);
}

sub op_ge {
  my ($self, $field, $val) = @_;
  return $self->_quote_field($field) . " >= " . $self->_quote_val($val);
}

sub op_le {
  my ($self, $field, $val) = @_;
  return $self->_quote_field($field) . " <= " . $self->_quote_val($val);
}

sub op_between {
  my ($self, $field, $low, $high) = @_;
  return $self->_quote_field($field) . " between "
    . $self->_quote_val($low) . " and " . $self->_quote_val($high);
}

sub op_like {
  my ($self, $field, $val) = @_;
  # XXX really should quote/check $val
  return $self->_quote_field($field) . " like " . $self->_quote_val($val);
}

sub op_not {
  my ($self, $pred) = @_;
  my $clause = $self->dispatch_query_op($pred);
  return "NOT ($clause)";
}

sub op_or {
  my ($self, @args) = @_;
  my @predicates = map { $self->dispatch_query_op($_) } @args;
  return join(" or ", map { "($_)" } @predicates);
}

sub op_and {
  my ($self, @args) = @_;
  my @predicates = map { $self->dispatch_query_op($_) } @args;
  return join(" and ", map { "($_)" } @predicates);
}

1;

# ABSTRACT: Metabase index backend using SQLite

__END__

=for Pod::Coverage::TrustPod add query delete count
translate_query op_eq op_ne op_gt op_lt op_ge op_le op_between op_like
op_not op_or op_and

=head1 SYNOPSIS

  use Metabase::Index::SimpleDB;

  Metabase::Index:SimpleDB->new(
    filename => '/tmp/cpantesters.sqlite',
  );

=head1 DESCRIPTION

This is an implementation of the L<Metabase::Index> and L<Metabase::Query>
roles using SQLite

=head1 USAGE

See below for constructor attributes.  See L<Metabase::Index>,
L<Metabase::Query> and L<Metabase::Librarian> for details on usage.

=cut
