use 5.006;
use strict;
use warnings;

package Metabase::Archive::PostgreSQL;
# ABSTRACT: Metabase archive backend using PostgreSQL

our $VERSION = '1.002';

use Moose;

with 'Metabase::Backend::PostgreSQL';
with 'Metabase::Archive::SQL';

1;

__END__

=for Pod::Coverage::TrustPod store extract delete iterator initialize

=head1 SYNOPSIS

  use Metabase::Archive::PostgreSQL;

  my $archive = Metabase::Archive::PostgreSQL->new(
    db_name => "cpantesters",
    db_user => "johndoe",
    db_pass => "PaSsWoRd",
  );

=head1 DESCRIPTION

This is an implementation of the L<Metabase::Archive::SQL> role using
PostgreSQL.

=head1 USAGE

See L<Metabase::Backend::PostgreSQL>, L<Metabase::Archive> and
L<Metabase::Librarian>.

=cut
