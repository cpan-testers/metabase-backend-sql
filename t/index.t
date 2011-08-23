use strict;
use warnings;

use Test::More;
use Test::Routine;
use Test::Routine::Util;
use File::Temp ();
use File::Spec::Functions qw/catfile/;

use Metabase::Index::SQLite;

with 'Metabase::Test::Index::SQLite';

run_tests(
  "Run index tests on Metabase::Index::SQLite",
  ["main", "Metabase::Test::Index"]
);

done_testing;
