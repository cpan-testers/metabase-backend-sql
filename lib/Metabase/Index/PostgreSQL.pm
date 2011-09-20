use 5.006;
use strict;
use warnings;

package Metabase::Index::PostgreSQL;
# ABSTRACT: Metabase index backend using SQLite
# VERSION

use Moose;

with 'Metabase::Backend::PostgreSQL';
with 'Metabase::Index::SQL';

sub _build_typemap {
  return {
    '//str'   => 'text',
    '//num'   => 'integer',
    '//bool'  => 'boolean',
  };
}

sub _quote_field {
  my ($self, $field) = @_;
  return join(".", map { qq{"$_"} } split qr/\./, $field);
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

  use Metabase::Index::PostgreSQL;

  Metabase::Index::PostgreSQL->new(
    db_name => 'cpantesters',
    db_user => "johndoe",
    db_pass => "PaSsWoRd",
  );

=head1 DESCRIPTION

This is an implementation of the L<Metabase::Index::SQL> role using SQLite.

=head1 USAGE

See below for constructor attributes.  See L<Metabase::Index>,
L<Metabase::Query> and L<Metabase::Librarian> for details on usage.

=cut
