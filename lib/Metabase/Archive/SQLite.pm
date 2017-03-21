use 5.006;
use strict;
use warnings;

package Metabase::Archive::SQLite;
# ABSTRACT: Metabase storage using SQLite

our $VERSION = '1.002';

use Moose;

with 'Metabase::Backend::SQLite';
with 'Metabase::Archive::SQL';

1;

__END__

=for Pod::Coverage::TrustPod store extract delete iterator initialize

=head1 SYNOPSIS

  use Metabase::Archive::SQLite;

  my $archive = Metabase::Archive::SQLite->new(
    filename => $sqlite_file,
  ); 

=head1 DESCRIPTION

This is an implementation of the L<Metabase::Archive::SQL> role using SQLite.

=head1 USAGE

See L<Metabase::Archive> and L<Metabase::Librarian>.

=cut
