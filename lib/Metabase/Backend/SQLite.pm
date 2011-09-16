use 5.008001;
use strict;
use warnings;

package Metabase::Backend::SQLite;
# VERSION
# ABSTRACT: Metabase backend implemented using Amazon Web Services

use MooseX::Types::Path::Class;
use Moose::Role;
use namespace::autoclean;

has 'filename' => (
    is       => 'ro',
    isa      => 'Path::Class::File',
    coerce   => 1,
    required => 1,
);

has 'synchronous' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
);

1;

=for Pod::Coverage method_names_here

=head1 SYNOPSIS

XXX consolidate synopses from modules

=head1 DESCRIPTION

This distribution provides a backend for L<Metabase> using Amazon Web Services.
There are two modules included, L<Metabase::Index::SQLite> and
L<Metabase::Archive::SQLite>.  They can be used separately or together (see
L<Metabase::Librarian> for details).

The L<Metabase::Backend::SQLite> module is a L<Moose::Role> that provides
common attributes and private helpers and is not intended to be used directly.

Common attributes are described further below.

=attr filename

Path to an SQLite database

=attr synchronous

Controls how SQLite should set the C<synchronous> pragma.  Defaults to false,
which is faster, but less safe.

=cut

# vim: ts=2 sts=2 sw=2 et:
