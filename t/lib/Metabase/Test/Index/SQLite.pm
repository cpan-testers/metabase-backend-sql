use strict;
use warnings;
package Metabase::Test::Index::SQLite;

use Test::More;
use Test::Routine;
use Test::Routine::Util;
use File::Temp ();
use File::Spec::Functions qw/catfile/;

use Metabase::Index::SQLite;

has tempdir => (
  is => 'ro',
  isa => 'Object',
  default => sub {
    return File::Temp->newdir;
  },
);

sub _build_index {
  my $self = shift;
  my $index = Metabase::Index::SQLite->new(
    filename => catfile( $self->tempdir, "test" . int(rand(2**31)) ),
  );
#  $index->initialize;
  return $index;
}

1;
