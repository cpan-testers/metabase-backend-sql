use 5.006;
use strict;
use warnings;

package Metabase::Archive::PostgreSQL;
# ABSTRACT: Metabase storage using PostgreSQL
# VERSION

use Moose;

with 'Metabase::Backend::PostgreSQL';
with 'Metabase::Archive::SQL';

sub _build__blob_type { 'blob' }

1;

__END__

=for Pod::Coverage::TrustPod store extract delete iterator initialize

=head1 SYNOPSIS

  require Metabase::Archive::SQLite;

  $archive = Metabase::Archive::SQLite->new(
    filename => $sqlite_file,
  ); 

=head1 DESCRIPTION

Store facts in a SQLite database.

=head1 USAGE

See L<Metabase::Archive> and L<Metabase::Librarian>.

TODO: document optional C<compressed> option (default 1), C<synchronized>
option and C<schema> option (sensible default provided).

=cut
