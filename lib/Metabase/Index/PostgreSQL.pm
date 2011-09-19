use 5.006;
use strict;
use warnings;

package Metabase::Index::PostgreSQL;
# ABSTRACT: Metabase index backend using SQLite
# VERSION

use Moose;
use DBD::Pg;

with 'Metabase::Index::SQL';

has db_name => (
  is => 'ro',
  isa => 'Str',
  required => 1,
);

sub _build_dsn {
  my $self = shift;
  return "dbi:SQLite:dbname=" . $self->db_name;
}

sub _build_db_user { return "" }

sub _build_db_pass { return "" }

sub _build_db_type { return "PostgreSQL" }

sub _fixup_sql_diff { return $_[1] }

sub _build_typemap {
  return {
    '//str'   => 'text',
    '//num'   => 'integer',
    '//bool'  => 'boolean',
  };
}

sub _quote_field {
  my ($self, $field) = @_;
  return qq{$field}; # XXX we assume the identifiers don't need quoting
}

sub _quote_val {
  my ($self, $value) = @_;
  $value =~ s{'}{''}g;
  return qq{'$value'};
}

1;

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

This is an implementation of the L<Metabase::Index::SQL> role using SQLite.

=head1 USAGE

See below for constructor attributes.  See L<Metabase::Index>,
L<Metabase::Query> and L<Metabase::Librarian> for details on usage.

=cut
