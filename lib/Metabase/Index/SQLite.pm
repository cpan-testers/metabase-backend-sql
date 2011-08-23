use 5.006;
use strict;
use warnings;

package Metabase::Index::SQLite;
# VERSION

use Moose;
use MooseX::Types::Path::Class;

use Data::Stream::Bulk::Callback;
use DBD::SQLite;
use DBIx::RunSQL;
use DBIx::Simple;
use Path::Class ();
use SQL::Translator;
use Try::Tiny;

with 'Metabase::Backend::SQLite';
with 'Metabase::Index';


sub _get_search_sql {
  my ( $self, $select, $spec ) = @_;

  my ($where, $limit) = $self->get_native_query($spec);

  my $domain = $self->domain;
  my $sql = qq{$select from `$domain` $where};

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
  # why is this a Bulk object in other backends?
}

my $_item_extractor = sub {
  my $response = shift;
  my $items = $response->{SelectResult}{Item};

  # the following may not be necessary as of SimpleDB::Class 1.0000
  $items = [ $items ] unless ref $items eq 'ARRAY';

  my $result = [ map { $_->{Name} } @$items ];
  return $result, scalar @$result;
};

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
  return qq{"$field"};
}

sub _quote_value {
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
  return $self->_quote_field($field) . " <=" . $self->_quote_val($val);
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
