use 5.010;
use strict;
use warnings;
package Test::Metabase::Setup;

use Class::Load qw/try_load_class/;
use Test::More;
use Exporter;
use File::Temp;
our @ISA = qw/Exporter/;
our @EXPORT = qw/setup test_db/;

sub setup {
  try_load_class("DBD::SQLite")
    or BAIL_OUT("DBD::SQLite not installed for testing");
}

my $test_dir = File::Temp->newdir();

sub test_db {
  return $test_dir . "/" . int(rand(2**31)) . ".sqlite";
}

1;

# ABSTRACT: goes here


