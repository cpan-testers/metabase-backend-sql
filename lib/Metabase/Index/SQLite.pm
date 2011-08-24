use 5.006;
use strict;
use warnings;

package Metabase::Index::SQLite;
# VERSION

use Moose;
use MooseX::Types::Path::Class;

use Class::Load qw/try_load_class/;
use Data::Stream::Bulk::Callback;
use DBD::SQLite;
use DBIx::RunSQL;
use DBIx::Simple;
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

sub _build_dbis {
  my ($self) = @_;
  my $fn = $self->filename;
  my $dbis = DBIx::Simple->connect("dbi:SQLite:dbname=$fn","","")
    or die "Could not connect to $fn\n";
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
  my ($self, $classes, $resources) = @_;
  my $schema = $self->schema;
  # Core table
  $schema->add_table(
    $self->_table_from_meta( 'core', Metabase::Fact->core_metadata_types )
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
    next unless keys %$types;
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
    dbh => $self->dbh,
  );
  $existing->translate;
  my $target = SQL::Translator->new(
    parser => 'Storable',
    data => nfreeze($schema),
    producer => 'SQLite',
  );
  my $out_sql = $target->translate;
  $target = SQL::Translator->new(
    parser => 'SQLite',
    data => $out_sql,
  );

  my $diff = SQL::Translator::Diff::schema_diff(
    $existing->schema, 'SQLite', $target->schema, 'SQLite'
  );

  # DBIx::RunSQL requires a file (ugh)
  my ($fh, $sqlfile) = File::Temp::tempfile();
  print {$fh} $diff;
  close $fh;
#  warn "Schema Diff: $diff\n"; # XXX

  unless ( $diff =~ /-- No differences found/i ) {
    DBIx::RunSQL->create(
      dbh => $self->dbh,
      sql => $sqlfile,
    );
  }

  # must reset the connection
  $self->clear_dbis;

  return;
}

my %typemap = (
  '//str'   => 'varchar(255)',
  '//num'   => 'integer',
  '//bool'  => 'boolean',
);

sub _table_from_meta {
  my ($self, $name, $typehash) = @_;
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

sub _get_search_sql {
  my ( $self, $select, $spec ) = @_;

  my ($where, $limit) = $self->get_native_query($spec);

  my $db = $self->n;
  my $sql = qq{$select from "XXXXX" $where};

  return ($sql, $limit);
}

sub add {
    my ( $self, $fact ) = @_;

    Carp::confess("can't index a Fact without a GUID") unless $fact->guid;

    my $metadata = $self->clone_metadata( $fact );

    # add it
}

sub count {
  my ( $self, %spec) = @_;

  # count it
}

sub query {
  my ( $self, %spec) = @_;

  return Data::Stream::Bulk::Callback->new(
    callback => sub { ... }
  );
}

sub delete {
    my ( $self, $guid ) = @_;

    Carp::confess("can't delete without a GUID") unless $guid;

    # delete
}

#--------------------------------------------------------------------------#
# required by Metabase::Query
#--------------------------------------------------------------------------#

sub _quote_field {
  my ($self, $field) = @_;
  $field = normalize_name($field);
  return qq{"$field"};
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
