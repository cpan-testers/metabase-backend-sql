use 5.010;
use strict;
use warnings;

use Test::More 0.92;
use Test::Deep;
use Try::Tiny;

use lib 't/lib';
use Test::Metabase::StringFact;
use Test::Metabase::Setup;

#-------------------------------------------------------------------------#
# Setup
#--------------------------------------------------------------------------#

setup();

my $fact = Test::Metabase::StringFact->new(
  resource => 'cpan:///distfile/JOHNDOE/Foo-Bar-1.23.tar.gz',
  content  => "Hello World",
);

my $string = "Everything is fine"; # we need length later

my $fact2 = Test::Metabase::StringFact->new(
  resource => 'cpan:///distfile/JOHNDOE/Foo-Bar-1.23.tar.gz',
  content  => $string,
);

#--------------------------------------------------------------------------#
# Tests here
#--------------------------------------------------------------------------#

require_ok( 'Metabase::Archive::SQLite' );


my $testdb = test_db();

my $archive = new_ok( 'Metabase::Archive::SQLite', [ filename => $testdb ] );
$archive->initialize;

ok( my $guid = $archive->store( $fact->as_struct ), "stored a fact" );

is( $fact->guid, $guid, "GUID returned matched GUID in fact" );

my $copy_struct = $archive->extract( $guid );
my $class = Metabase::Fact->class_from_type($copy_struct->{metadata}{core}{type});

ok( my $copy = $class->from_struct( $copy_struct ),
    "got a fact from archive"
);

cmp_deeply( $copy, $fact, "Extracted fact matches original" );

ok( $archive->store( $fact2->as_struct ), "stored fact 2" );

my $iter = $archive->iterator;
my @facts;
while( my $block = $iter->next ) {
    foreach my $item ( @$block ) {
        push @facts, $item;
    }
}

is( scalar @facts, 2, "iterator found both facts" );

ok( $archive->delete( $guid ), "deleted fact 1" );

$iter = $archive->iterator;
@facts = ();
while( my $block = $iter->next ) {
    foreach my $item ( @$block ) {
        push @facts, $item;
    }
}

is( scalar @facts, 1, "iterator found one fact" );

#--------------------------------------------------------------------------#
# teardown
#--------------------------------------------------------------------------#
done_testing;