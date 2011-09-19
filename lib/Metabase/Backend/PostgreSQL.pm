use 5.008001;
use strict;
use warnings;

package Metabase::Backend::PostgreSQL;
# VERSION
# ABSTRACT: Metabase backend implemented using PostgreSQL

use Moose::Role;
use namespace::autoclean;

1;

=for Pod::Coverage method_names_here

=head1 SYNOPSIS

XXX consolidate synopses from modules

=head1 DESCRIPTION

This distribution provides a backend for L<Metabase> using PostgreSQL.  There
are two modules included, L<Metabase::Index::PostgreSQL> and
L<Metabase::Archive::PostgreSQL>.  They can be used separately or together (see
L<Metabase::Librarian> for details).

The L<Metabase::Backend::PostgreSQL> module is a L<Moose::Role> that provides
common attributes and private helpers and is not intended to be used directly.

Common attributes are described further below.

=attr db_name

Database name

=attr db_user

Database username

=attr db_pass

Database password

=cut

# vim: ts=2 sts=2 sw=2 et:
