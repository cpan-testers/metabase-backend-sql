use 5.006;
use strict;
use warnings;

package Metabase::Index::SQLite;
# ABSTRACT: Metabase index backend using SQLite

our $VERSION = '1.002';

use Moose;

with 'Metabase::Backend::SQLite';
with 'Metabase::Index::SQL';

sub _build_typemap {
  return {
    '//str'   => 'varchar(255)',
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

  use Metabase::Index::SQLite;

  my $index = Metabase::Index:SQLite->new(
    filename => $sqlite_file,
  );

=head1 DESCRIPTION

This is an implementation of the L<Metabase::Index::SQL> role using SQLite.

=head1 USAGE

See L<Metabase::Index>, L<Metabase::Query> and L<Metabase::Librarian>.

=cut
