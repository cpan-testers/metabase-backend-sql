use 5.006;
use strict;
use warnings;

package Metabase::Archive::SQLite;
# ABSTRACT: Metabase storage using SQLite
# VERSION

use Moose;
use Moose::Util::TypeConstraints;
use MooseX::Types::Path::Class;
use Path::Class ();

use Metabase::Fact;
use Carp        ();
use Data::GUID  ();
use Data::Stream::Bulk::DBIC ();
use JSON 2      ();
use DBI         1 ();
use DBD::SQLite 1 ();
use Compress::Zlib 2 qw(compress uncompress);
use SQL::Translator 0.11006 (); # required for deploy()
use Metabase::Archive::Schema;

with 'Metabase::Backend::SQLite';
with 'Metabase::Archive::SQL';

has 'compressed' => (
    is      => 'rw',
    isa     => 'Bool',
    default => 1,
);

sub _build__blob_type { 'blob' }

sub _build_dsn {
  my $self = shift;
  return "dbi:SQLite:dbname=" . $self->filename;
}

sub _build_db_user { return "" }

sub _build_db_pass { return "" }

sub _build_db_type { return "SQLite" }

around _build_dbis => sub {
  my $orig = shift;
  my $self = shift;
  my $dbis = $self->$orig;
  my $toggle = $self->synchronous ? "ON" : "OFF";
  $dbis->query("PRAGMA synchronous = $toggle");
  return $dbis;
};

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
