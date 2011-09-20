use strict;
use warnings;

use Class::Load qw/try_load_class/;
use Test::More;
use Test::Routine;
use Test::Routine::Util;

use Metabase::Archive::PostgreSQL;

plan skip_all => 'No $ENV{METABASE_TEST_PG_DBNAME}'
  unless $ENV{METABASE_TEST_PG_DBNAME};

plan skip_all => 'DBD::Pg not installed'
  unless try_load_class("DBD::Pg");

sub _build_archive {
  my $self = shift;
  my $archive = Metabase::Archive::PostgreSQL->new(
    db_name => $ENV{METABASE_TEST_PG_DBNAME},
  );
  $archive->initialize;
  return $archive;
}

run_tests(
  "Run Archive tests on Metabase::Archive::PostgreSQL",
  ["main", "Metabase::Test::Archive"]
);

done_testing;
